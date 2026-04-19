// Photo freshness checker — runs as a GitHub Action.
// For each profile with photos, verifies the first photo is a real image.
// If it's a soft-404 (HTML returned instead of image), re-scrapes the profile
// page and updates the photos array with current URLs.

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const UA = 'Mozilla/5.0 (compatible; BrothelSearch/1.0; +https://brothelsearch.com)';
const MAX_CHECKS_PER_VENUE = parseInt(process.env.MAX_CHECKS_PER_VENUE || '30', 10);

// Site configs — oldUrl pattern + image extraction logic
const SITES = [
  {
    id: 'ginzaempire', name: 'Ginza Empire', file: 'ginzaempire.json',
    baseUrl: 'https://479ginza.com.au', urlPattern: /\/Girls\/(\d+)/,
    format: 'empire',
  },
  {
    id: 'ginzaclub', name: 'Ginza Club', file: 'ginzaclub.json',
    baseUrl: 'https://www.ginzaclub.com.au', urlPattern: /\/Girls\/(\d+)/,
    format: 'empire',
  },
  { id: 'kyoto206', name: 'Kyoto 206', file: 'kyoto206.json', format: 'wordpress' },
  { id: 'sakura57', name: 'Sakura 57', file: 'sakura57.json', format: 'wordpress' },
  { id: 'top127', name: 'Top 127', file: 'top127.json', format: 'wordpress' },
  { id: 'fantasyclub35', name: 'Fantasy Club 35', file: 'fantasyclub35.json', format: 'wordpress' },
  { id: '429city', name: '429 City', file: '429city.json', format: 'wordpress' },
];

async function isValidImage(url) {
  try {
    const resp = await fetch(url, { method: 'GET', headers: { 'User-Agent': UA }, redirect: 'follow' });
    const ct = resp.headers.get('content-type') || '';
    if (!resp.ok) return false;
    if (!ct.startsWith('image/')) return false;
    return true;
  } catch (e) {
    return false;
  }
}

// Ginza-format extractor (used by empire + club): <a href="/data/upload/YYYY-MM/FILE.ext">
function extractGinzaImages(html, baseUrl) {
  const images = [];
  const seen = new Set();
  const re = /<a[^>]+href="(\/data\/upload\/[^"]+\.\w+)"[^>]*>/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    const src = m[1];
    if (/s\.\w+$/i.test(src)) continue; // skip thumbs (end with s.ext)
    if (!/\.(jpe?g|png|webp|gif)$/i.test(src)) continue;
    const full = baseUrl + src;
    if (!seen.has(full)) { seen.add(full); images.push(full); }
  }
  return images;
}

// WordPress-format extractor: wp-content/uploads/YYYY/MM/FILE.ext
function extractWpImages(html) {
  const images = [];
  const seen = new Set();
  const re = /https?:\/\/[^"']+\/wp-content\/uploads\/\d{4}\/\d{2}\/[^"'?\s]+\.(?:jpe?g|png|webp|gif)/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    let url = m[0];
    // Strip size suffixes like -300x400, -scaled
    url = url.replace(/-\d+x\d+(\.\w+)$/i, '$1');
    if (!seen.has(url)) { seen.add(url); images.push(url); }
  }
  return images;
}

async function rescrapeProfile(site, girl) {
  const resp = await fetch(girl.oldUrl, { headers: { 'User-Agent': UA }, redirect: 'follow' });
  if (!resp.ok) return null;
  const html = await resp.text();
  if (site.format === 'empire') {
    return extractGinzaImages(html, site.baseUrl);
  } else if (site.format === 'wordpress') {
    return extractWpImages(html);
  }
  return null;
}

async function checkVenue(site) {
  const filePath = path.join(__dirname, '..', 'profiles', site.file);
  const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));

  // Step 1: Dead profile detection — HEAD check oldUrl for non-deleted profiles
  // Only flag profiles not currently rostered and not yet flagged
  const calendar = data.calendar || {};
  const rosteredNames = new Set();
  for (const [name, slots] of Object.entries(calendar)) {
    if (name === '_published') continue;
    if (slots && typeof slots === 'object') rosteredNames.add(name);
  }

  const deadCandidates = (data.girls || []).filter(g =>
    g.oldUrl && g.deleted !== 'Yes' && !rosteredNames.has(g.name)
  );
  deadCandidates.sort(() => Math.random() - 0.5);
  const deadToCheck = deadCandidates.slice(0, MAX_CHECKS_PER_VENUE);
  let flaggedDead = 0;
  for (const g of deadToCheck) {
    try {
      const resp = await fetch(g.oldUrl, { method: 'HEAD', headers: { 'User-Agent': UA }, redirect: 'follow' });
      if (resp.status === 404) {
        const entry = data.girls.find(x => x.oldUrl === g.oldUrl && x.name === g.name);
        if (entry) {
          entry.deleted = 'Yes';
          entry.deletedAt = new Date().toISOString();
          flaggedDead++;
          console.log(`[${site.name}] Flagged dead: ${g.name} (${g.oldUrl})`);
        }
      }
      await new Promise(r => setTimeout(r, 200));
    } catch (e) { /* keep on error */ }
  }

  // Step 2: Photo freshness — for non-deleted profiles
  const girls = (data.girls || []).filter(g => g.deleted !== 'Yes' && g.photos && g.photos.length > 0 && g.oldUrl);
  girls.sort(() => Math.random() - 0.5);
  const toCheck = girls.slice(0, MAX_CHECKS_PER_VENUE);

  let checked = 0, updated = 0;
  for (const g of toCheck) {
    checked++;
    const firstPhoto = g.photos[0];
    const valid = await isValidImage(firstPhoto);
    if (valid) {
      await new Promise(r => setTimeout(r, 150));
      continue;
    }
    // Broken — re-scrape
    console.log(`[${site.name}] ${g.name}: first photo broken, re-scraping ${g.oldUrl}`);
    try {
      const newPhotos = await rescrapeProfile(site, g);
      if (!newPhotos || newPhotos.length === 0) {
        console.log(`[${site.name}] ${g.name}: rescrape found no photos (skipping)`);
      } else {
        const oldSet = new Set(g.photos);
        const different = newPhotos.length !== g.photos.length || newPhotos.some(p => !oldSet.has(p));
        if (different) {
          // Find the full entry in data.girls and mutate it
          const entry = data.girls.find(x => x.oldUrl === g.oldUrl && x.name === g.name);
          if (entry) {
            entry.photos = newPhotos;
            entry.lastModified = new Date().toISOString();
            updated++;
            console.log(`[${site.name}] ${g.name}: updated ${newPhotos.length} photos`);
          }
        }
      }
    } catch (e) {
      console.error(`[${site.name}] ${g.name}: rescrape error:`, e.message);
    }
    await new Promise(r => setTimeout(r, 500));
  }

  if (updated > 0 || flaggedDead > 0) {
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2) + '\n', 'utf8');
    console.log(`[${site.name}] Wrote ${updated} photo updates + ${flaggedDead} dead flags to ${site.file}`);
  }
  return { site: site.name, checked, updated, flaggedDead };
}

async function main() {
  console.log('[PhotoCheck] Starting...');
  const results = [];
  for (const site of SITES) {
    try {
      const r = await checkVenue(site);
      results.push(r);
    } catch (e) {
      console.error(`[${site.name}] Fatal error:`, e.message);
    }
  }
  console.log('\n[PhotoCheck] Summary:');
  let totalUpdated = 0, totalDead = 0;
  for (const r of results) {
    console.log(`  ${r.site}: ${r.checked} checked, ${r.updated} photo updates, ${r.flaggedDead} flagged dead`);
    totalUpdated += r.updated;
    totalDead += r.flaggedDead;
  }

  // Commit if there are changes
  if (totalUpdated > 0 || totalDead > 0) {
    try {
      execSync('git config user.name "github-actions[bot]"');
      execSync('git config user.email "github-actions[bot]@users.noreply.github.com"');
      execSync('git add profiles/', { stdio: 'inherit' });
      const parts = [];
      if (totalUpdated > 0) parts.push(`Refresh ${totalUpdated} photo set${totalUpdated > 1 ? 's' : ''}`);
      if (totalDead > 0) parts.push(`Flag ${totalDead} dead profile${totalDead > 1 ? 's' : ''}`);
      execSync(`git commit -m "[PhotoCheck] ${parts.join(', ')}"`, { stdio: 'inherit' });
      execSync('git push', { stdio: 'inherit' });
      console.log('[PhotoCheck] Committed and pushed');
    } catch (e) {
      console.error('[PhotoCheck] Git error:', e.message);
    }
  } else {
    console.log('[PhotoCheck] No changes to commit');
  }
}

main().catch(e => { console.error('[PhotoCheck] Fatal:', e); process.exit(1); });
