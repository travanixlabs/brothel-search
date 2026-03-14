#!/usr/bin/env node
/**
 * Bulk scraper for Sakura 57 (surryhillsbrothel.com.au)
 *
 * Usage:
 *   node scripts/scrape-sakura57.js              # scrape all new profiles
 *   node scripts/scrape-sakura57.js --limit 5    # scrape up to 5 new profiles
 *   node scripts/scrape-sakura57.js --dry-run    # list profiles without scraping
 *
 * Requires: Node 18+ (uses native fetch)
 */

const fs = require('fs');
const path = require('path');
const https = require('https');

const SITE_DOMAIN = 'www.surryhillsbrothel.com.au';
const BASE_URL = `https://${SITE_DOMAIN}`;
const LISTING_URL = `${BASE_URL}/our-girls/`;
const SITE_NAME = 'Sakura 57';
const REPO = 'travanixlabs/brothel-search';
const IMG_PREFIX = 'profiles/sakura57';
const JSON_PATH = path.join(__dirname, '..', 'profiles', 'sakura57', 'sakura57.json');
const PROFILES_DIR = path.join(__dirname, '..', 'profiles', 'sakura57');

const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';

const COUNTRY_MAP = {
  'japan': ['Japanese'], 'korea': ['Korean'], 'china': ['Chinese'],
  'thailand': ['Thai'], 'vietnam': ['Vietnamese'], 'indonesia': ['Indonesian'],
  'malaysia': ['Malaysian'], 'singapore': ['Singaporean'], 'taiwan': ['Taiwanese'],
  'hong kong': ['Hong Konger'], 'hong-kong': ['Hong Konger'],
  'latina': ['Latina'], 'eurasian': ['Eurasian'], 'eurasia': ['Eurasian'],
};

const LANG_FROM_COUNTRY = {
  'Japanese': 'Japanese, Limited English', 'Korean': 'Korean, Limited English',
  'Thai': 'Thai, Limited English', 'Chinese': 'Mandarin, Limited English',
  'Vietnamese': 'Vietnamese, Limited English', 'Indonesian': 'Indonesian, Limited English',
  'Malaysian': 'English', 'Singaporean': 'English',
  'Taiwanese': 'Mandarin, Limited English', 'Hong Konger': 'Cantonese, Limited English',
  'Latina': 'English', 'Eurasian': 'English',
};

const LABEL_PATTERNS = [
  ['Double Lesbian', /\blesbian\s*double\b/i], ['Shower Together', /\bshower\s*together\b/i],
  ['Pussy Slide', /\bpussy\s*slide\b/i], ['DFK', /\bDFK\b/i], ['BBBJ', /\bBBBJ\b/i],
  ['DATY', /\bDATY\b|dining\s*at\s*the\s*y/i], ['69', /\b69\b/], ['CIM', /\bCIM\b/i],
  ['COB', /\bCOB\b/i], ['COF', /\bCOF\b/i], ['Rimming', /\brimming\b/i],
  ['Anal', /\ban[- ]?al\b/i], ['Double', /\bdouble\b/i], ['Swallow', /\bswallow\b/i],
  ['2 Men', /\b2\s*m[ae]n\b/i], ['Couple', /\bcouple\b/i], ['Filming', /\b(?:filming|video)\b/i],
  ['GFE', /\bGFE\b/i], ['PSE', /\bPSE\b/i], ['Massage', /\bmassage\b/i],
  ['Toys', /\btoys?\b/i], ['Costume', /\bcostume\b/i],
];

function extractLabels(desc) {
  if (!desc) return [];
  return LABEL_PATTERNS.filter(([, re]) => re.test(desc)).map(([label]) => label);
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function fetchHtml(url) {
  const resp = await fetch(url, { headers: { 'User-Agent': UA }, redirect: 'follow' });
  if (!resp.ok) throw new Error(`HTTP ${resp.status} for ${url}`);
  return resp.text();
}

/* ── Parse listing page ── */

async function scrapeListingPage() {
  console.log('Fetching listing page...');
  const html = await fetchHtml(LISTING_URL);

  const linkRe = new RegExp(`href="(https?://${SITE_DOMAIN.replace(/\./g, '\\.')}/project/[^"]+)"`, 'gi');
  const seen = new Set();
  const urls = [];
  let m;

  while ((m = linkRe.exec(html)) !== null) {
    const url = m[1].replace(/\/$/, '') + '/';
    if (seen.has(url)) continue;
    seen.add(url);
    urls.push(url);
  }

  console.log(`Found ${urls.length} profile URLs`);
  return urls;
}

function decodeHtmlEntities(str) {
  return str
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(parseInt(n)))
    .replace(/&#x([0-9a-f]+);/gi, (_, n) => String.fromCharCode(parseInt(n, 16)))
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"').replace(/&nbsp;/g, ' ');
}

function parsePageTitle(html) {
  const titleMatch = html.match(/<title>([^<]+)<\/title>/i);
  let titleText = '';

  if (titleMatch) {
    let raw = decodeHtmlEntities(titleMatch[1]);
    titleText = raw.replace(/\s*[–—|\-]\s*Sakura\s*57.*$/i, '').trim();
  }

  if (!titleText) {
    const h2 = html.match(/<h2[^>]*>([^<]+)<\/h2>/i);
    if (h2) titleText = h2[1].trim();
  }

  if (!titleText) return { name: '', country: [], special: '' };

  let special = '';
  let clean = titleText;
  const parenParts = [];
  clean = clean.replace(/[（(]([^）)]+)[）)]/g, (_, inner) => {
    parenParts.push(inner.trim());
    return '';
  }).trim();

  let name = clean.replace(/\s+/g, ' ').trim();
  if (name === name.toUpperCase() && name.length > 1) {
    name = name.charAt(0).toUpperCase() + name.slice(1).toLowerCase();
  }
  if (!name) return { name: '', country: [], special: '' };

  let country = [];
  for (const pp of parenParts) {
    const lower = pp.toLowerCase();
    if (/porn\s*star|new|retired/i.test(lower)) {
      special = special ? `${special}, ${pp}` : pp;
      continue;
    }
    if (lower.includes('mix')) {
      for (const mp of lower.split(/\s*mix\s*/)) {
        const c = COUNTRY_MAP[mp.trim()];
        if (c) country.push(...c);
        else if (mp.trim()) country.push(mp.trim().charAt(0).toUpperCase() + mp.trim().slice(1));
      }
    } else if (COUNTRY_MAP[lower]) {
      country.push(...COUNTRY_MAP[lower]);
    } else {
      const found = Object.keys(COUNTRY_MAP).find(k => lower.includes(k));
      if (found) country.push(...COUNTRY_MAP[found]);
      else if (lower) country.push(lower.charAt(0).toUpperCase() + lower.slice(1));
    }
  }

  return { name, country, special };
}

/* ── Parse individual profile page ── */

async function scrapeProfile(profileUrl, girlNameHint) {
  const html = await fetchHtml(profileUrl);

  const titleInfo = parsePageTitle(html);
  const girlName = girlNameHint || titleInfo.name;

  const age = (html.match(/Age:\s*(\d+)/i) || [])[1] || '';
  const height = (html.match(/Height:\s*(1[3-9]\d|20\d)/i) || html.match(/(1[4-8]\d)\s*cm/i) || [])[1] || '';
  const cup = (html.match(/(?:Cup|Bust)\s*(?:Size)?\s*:?\s*([A-HJ-Z](?:-[A-HJ-Z])?)\b/i) || [])[1] || '';
  if (cup) cup.toUpperCase();

  let val1 = '', val2 = '', val3 = '';
  const p30 = html.match(/30\s*min\w*\s*\$?\s*(\d+)/i);
  const p45 = html.match(/45\s*min\w*\s*\$?\s*(\d+)/i);
  const p60 = html.match(/60\s*min\w*\s*\$?\s*(\d+)/i);
  if (p30) val1 = p30[1]; if (p45) val2 = p45[1]; if (p60) val3 = p60[1];
  if (!val1) {
    const pb = html.match(/\$(\d+)\s*(?:\/|,|\s)\s*\$(\d+)\s*(?:\/|,|\s)\s*\$(\d+)/);
    if (pb) { val1 = pb[1]; val2 = pb[2]; val3 = pb[3]; }
  }

  // Extract images - only from main profile section (before portfolio grid)
  // Split HTML at portfolio/navigation section markers
  const mainHtml = html.split(/In Portfolios|class="portfolio|class="related|id="portfolio/i)[0] || html;

  const imgDomainRe = new RegExp(`(https?://${SITE_DOMAIN.replace(/\./g, '\\.')}/wp-content/uploads/[^\\s"']+\\.(?:jpe?g|png|webp))`, 'gi');
  const allImages = [];
  let im;
  while ((im = imgDomainRe.exec(mainHtml)) !== null) allImages.push(im[1]);

  // Filter by name
  const nameLower = (girlName || '').toLowerCase();
  const nameVariants = [nameLower];
  for (let i = 1; i < nameLower.length; i++) {
    if (!'aeiou'.includes(nameLower[i])) {
      nameVariants.push(nameLower.slice(0, i + 1) + nameLower[i] + nameLower.slice(i + 1));
    }
  }
  const slugMatch = profileUrl.match(/\/project\/([^/]+)/);
  if (slugMatch) {
    const slugName = decodeURIComponent(slugMatch[1]).split('-')[0].toLowerCase();
    if (slugName && !nameVariants.includes(slugName)) nameVariants.push(slugName);
  }

  let girlImages = allImages.filter(url => {
    const filename = url.split('/').pop().toLowerCase();
    return nameVariants.some(v => filename.includes(v));
  });

  // Fallback: if no name-matched images, grab non-junk images from main section
  if (girlImages.length === 0) {
    girlImages = allImages.filter(url => {
      const fn = url.split('/').pop().toLowerCase();
      if (fn.includes('logo') || fn.includes('qr') || fn.includes('微信')) return false;
      if (/-160x160\./.test(url) || /-746x548\./.test(url) || /-300x300\./.test(url)) return false;
      return true;
    });
  }

  // Group by base, prefer scaled > resolution > original
  const imageGroups = {};
  for (const imgUrl of girlImages) {
    const base = imgUrl.replace(/-scaled\.(jpe?g|png|webp)$/i, '.$1').replace(/-\d+x\d+\.(jpe?g|png|webp)$/i, '.$1');
    if (!imageGroups[base]) imageGroups[base] = { scaled: null, resolution: null, original: null, resPixels: 0 };
    if (/-scaled\./i.test(imgUrl)) imageGroups[base].scaled = imgUrl;
    else if (/-\d+x\d+\./i.test(imgUrl)) {
      const res = imgUrl.match(/-(\d+)x(\d+)\./);
      const px = res ? parseInt(res[1]) * parseInt(res[2]) : 0;
      if (px > imageGroups[base].resPixels) { imageGroups[base].resolution = imgUrl; imageGroups[base].resPixels = px; }
    } else imageGroups[base].original = imgUrl;
  }

  const images = [];
  for (const group of Object.values(imageGroups)) {
    const pick = group.scaled || group.resolution || group.original;
    if (pick) images.push(pick);
  }

  let earliestUpload = null;
  for (const imgUrl of images) {
    const dm = imgUrl.match(/\/uploads\/(\d{4})\/(\d{2})\//);
    if (dm) { const d = `${dm[1]}-${dm[2]}-01`; if (!earliestUpload || d < earliestUpload) earliestUpload = d; }
  }

  let desc = '';
  const descPatterns = [
    /<div[^>]+class="[^"]*entry-content[^"]*"[^>]*>([\s\S]*?)<\/div>/i,
    /<div[^>]+class="[^"]*project-description[^"]*"[^>]*>([\s\S]*?)<\/div>/i,
  ];
  for (const re of descPatterns) {
    const dm = html.match(re);
    if (dm) { desc = dm[1].replace(/<br\s*\/?>/gi, ' ').replace(/<[^>]*>/g, '').replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').replace(/\s+/g, ' ').trim(); if (desc.length > 20) break; }
  }

  return { age, height, cup: cup ? cup.toUpperCase() : '', val1, val2, val3, images, earliestUpload, desc, titleInfo };
}

/* ── Download image ── */

async function downloadImage(url, destPath) {
  const dir = path.dirname(destPath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  return new Promise((resolve, reject) => {
    const doFetch = (fetchUrl, redirects = 0) => {
      if (redirects > 5) return reject(new Error('Too many redirects'));
      const mod = fetchUrl.startsWith('https') ? https : require('http');
      mod.get(fetchUrl, { headers: { 'User-Agent': UA } }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) return doFetch(res.headers.location, redirects + 1);
        if (res.statusCode !== 200) { res.resume(); return reject(new Error(`HTTP ${res.statusCode}`)); }
        const file = fs.createWriteStream(destPath);
        res.pipe(file);
        file.on('finish', () => { file.close(); resolve(); });
        file.on('error', reject);
      }).on('error', reject);
    };
    doFetch(url);
  });
}

/* ── Main ── */

async function main() {
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run');
  const limitIdx = args.indexOf('--limit');
  const limit = limitIdx >= 0 ? parseInt(args[limitIdx + 1], 10) : Infinity;

  let data;
  try { data = JSON.parse(fs.readFileSync(JSON_PATH, 'utf8')); }
  catch { data = { girls: [], calendar: { _published: [] }, lastGirlsSync: '', lastCalendarSync: '' }; }

  const knownNames = new Set(data.girls.map(g => g.name));
  const knownUrls = new Set(data.girls.map(g => g.oldUrl).filter(Boolean));

  const allUrls = await scrapeListingPage();
  const newUrls = allUrls.filter(url => !knownUrls.has(url));

  console.log(`\n${allUrls.length} total URLs, ${knownUrls.size} existing, ${newUrls.length} new`);

  if (dryRun) {
    console.log('\n--- Dry Run ---');
    for (const url of newUrls.slice(0, limit)) console.log(`  ${url}`);
    return;
  }

  const toProcess = newUrls.slice(0, limit);
  console.log(`Processing ${toProcess.length} profiles...\n`);

  const now = new Date().toISOString();
  let processed = 0;

  for (const profileUrl of toProcess) {
    try {
      console.log(`[${processed + 1}/${toProcess.length}] Scraping ${profileUrl}...`);
      await sleep(1000);

      const detail = await scrapeProfile(profileUrl, null);
      const titleInfo = detail.titleInfo;

      if (!titleInfo.name) { console.log(`  SKIP: no name`); continue; }
      if (knownNames.has(titleInfo.name)) { console.log(`  SKIP: ${titleInfo.name} exists`); continue; }

      const name = titleInfo.name;
      console.log(`  -> ${name} (${titleInfo.country.join(', ')})`);

      const entry = {
        name, country: titleInfo.country.length ? titleInfo.country : [],
        age: detail.age || '', height: detail.height || '', cup: detail.cup || '',
        val1: detail.val1 || '', val2: detail.val2 || '', val3: detail.val3 || '',
        startDate: detail.earliestUpload || now.split('T')[0],
        oldUrl: profileUrl,
      };
      if (titleInfo.special) entry.special = titleInfo.special;
      if (titleInfo.country.length) entry.lang = LANG_FROM_COUNTRY[titleInfo.country[0]] || '';

      entry.desc = detail.desc || '';
      entry.labels = extractLabels(detail.desc);
      entry.originalSite = 'Exists';
      entry.lastModified = now;
      entry.lastRostered = '';

      const photos = [];
      const girlDir = path.join(PROFILES_DIR, name);

      for (let i = 0; i < detail.images.length; i++) {
        try {
          const imgUrl = detail.images[i];
          const ext = (imgUrl.match(/\.(jpe?g|png|webp)$/i) || [])[1] || 'jpeg';
          const fileName = `${name}_${i + 1}.${ext}`;
          await downloadImage(imgUrl, path.join(girlDir, fileName));
          photos.push(`https://raw.githubusercontent.com/${REPO}/main/${IMG_PREFIX}/${name}/${fileName}`);
          await sleep(300);
          process.stdout.write('.');
        } catch (e) {
          process.stdout.write('x');
          console.error(`\n  Image error #${i + 1}: ${e.message}`);
        }
      }
      console.log(` ${photos.length} images`);

      entry.photos = photos;
      data.girls.push(entry);
      knownNames.add(name);
      knownUrls.add(profileUrl);
      processed++;

      if (processed % 5 === 0) {
        data.lastGirlsSync = new Date().toISOString();
        fs.writeFileSync(JSON_PATH, JSON.stringify(data, null, 2));
        console.log(`  [Saved progress: ${processed} profiles]`);
      }
    } catch (e) {
      console.error(`  ERROR processing ${profileUrl}: ${e.message}`);
    }
  }

  data.lastGirlsSync = new Date().toISOString();
  fs.writeFileSync(JSON_PATH, JSON.stringify(data, null, 2));
  console.log(`\nDone! ${processed} profiles added. Total: ${data.girls.length}`);
}

main().catch(e => { console.error('Fatal error:', e); process.exit(1); });
