/*
 * Bulk scraper for 429 City (www.429city.com)
 * WordPress site with custom URL slugs
 * Pricing based on country, not listed on profile pages
 *
 * Usage: node scripts/scrape-429city.js
 */

const https = require('https');
const fs = require('fs');
const path = require('path');

const SITE = 'https://www.429city.com';
const JSON_PATH = 'profiles/429city/429city.json';
const IMG_DIR = 'profiles/429city';
const REPO = 'travanixlabs/brothel-search';
const SAVE_EVERY = 5;

// Pricing by country category
const PRICING = {
  japanese: { val1: '210', val2: '260', val3: '320' },
  western:  { val1: '230', val2: '280', val3: '350' },
  other:    { val1: '170', val2: '240', val3: '300' },
};

const JAPANESE_COUNTRIES = ['japanese', 'japan'];
const WESTERN_COUNTRIES = ['australian', 'american', 'british', 'european', 'russian', 'latina', 'brazilian', 'colombian', 'italian', 'french', 'german', 'spanish', 'western'];

function getPricing(country) {
  if (!country || country.length === 0) return PRICING.other;
  const lower = country[0].toLowerCase();
  if (JAPANESE_COUNTRIES.some(c => lower.includes(c))) return PRICING.japanese;
  if (WESTERN_COUNTRIES.some(c => lower.includes(c))) return PRICING.western;
  return PRICING.other;
}

function fetchUrl(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' } }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetchUrl(res.headers.location).then(resolve).catch(reject);
      }
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve(data));
    }).on('error', reject);
  });
}

function download(url, dest) {
  return new Promise((resolve, reject) => {
    const dir = path.dirname(dest);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    https.get(url, { headers: { 'User-Agent': 'Mozilla/5.0' } }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return download(res.headers.location, dest).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) { res.resume(); return reject(new Error('HTTP ' + res.statusCode)); }
      const file = fs.createWriteStream(dest);
      res.pipe(file);
      file.on('finish', () => { file.close(); resolve(); });
    }).on('error', reject);
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function decodeHtmlEntities(str) {
  return str
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(parseInt(n)))
    .replace(/&#x([0-9a-f]+);/gi, (_, n) => String.fromCharCode(parseInt(n, 16)))
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"').replace(/&#039;/g, "'").replace(/&nbsp;/g, ' ');
}

const COUNTRY_MAP = {
  'japanese': 'Japanese', 'japan': 'Japanese',
  'chinese': 'Chinese', 'china': 'Chinese',
  'korean': 'Korean', 'korea': 'Korean',
  'vietnamese': 'Vietnamese', 'vietnam': 'Vietnamese',
  'thai': 'Thai', 'thailand': 'Thai',
  'hong kong': 'Hong Konger', 'hongkong': 'Hong Konger', 'hk': 'Hong Konger',
  'taiwanese': 'Taiwanese', 'taiwan': 'Taiwanese',
  'singaporean': 'Singaporean', 'singapore': 'Singaporean',
  'malaysian': 'Malaysian', 'malaysia': 'Malaysian',
  'indonesian': 'Indonesian', 'indonesia': 'Indonesian',
  'indian': 'Indian', 'india': 'Indian',
  'colombian': 'Colombian', 'colombiana': 'Colombian',
  'brazilian': 'Brazilian', 'brazil': 'Brazilian',
  'australian': 'Australian', 'australia': 'Australian',
  'european': 'European', 'russian': 'Russian', 'latina': 'Latina',
};

const LANG_MAP = {
  'Japanese': 'Japanese, Limited English', 'Chinese': 'Chinese, English',
  'Korean': 'Korean, English', 'Vietnamese': 'Vietnamese, English',
  'Thai': 'Thai, English', 'Hong Konger': 'Cantonese, English',
  'Taiwanese': 'Chinese, English', 'Malaysian': 'Malay, English',
  'Indian': 'Hindi, English', 'Colombian': 'Spanish, English',
  'Brazilian': 'Portuguese, English', 'Australian': 'English',
  'European': 'English', 'Russian': 'Russian, English', 'Latina': 'Spanish, English',
};

async function scrapeAllUrls() {
  const allUrls = new Set();
  const NON_PROFILE = new Set([
    SITE + '/', SITE + '/ladies/', SITE + '/roster/', SITE + '/contact/',
    SITE + '/feed/', SITE + '/comments/feed/', SITE + '/rate/',
    SITE + '/escort/', SITE + '/job/',
  ]);

  function isProfileUrl(url) {
    if (!url.startsWith(SITE + '/')) return false;
    if (NON_PROFILE.has(url)) return false;
    if (url.includes('/wp-') || url.includes('/xmlrpc') || url.includes('/feed')
      || url.includes('/category/') || url.includes('/tag/') || url.includes('/author/')
      || url.includes('#') || url.includes('.xml') || url.includes('.php')) return false;
    // Must look like a profile slug: /something/ or /something-2/
    const path = url.replace(SITE, '');
    if (/^\/ladies\/page\/\d+/.test(path)) return false;
    // Profile paths: /name/, /name-2/, /new-girl-name/, /miki-%e3%81%bf%e3%81%95/
    if (/^\/[a-z0-9][\w%\-]*\/?$/i.test(path)) return true;
    // Also allow multi-segment slugs like /new-girl-lisa/ or /sophie-friday-saturday.../
    if (/^\/[\w%][\w%\-]*\/?$/i.test(path)) return true;
    return false;
  }

  // Scrape /ladies/ with pagination
  let page = 1;
  while (true) {
    const pageUrl = page === 1 ? SITE + '/ladies/' : SITE + '/ladies/page/' + page + '/';
    console.log(`Fetching ${pageUrl}...`);
    let html;
    try { html = await fetchUrl(pageUrl); } catch (e) { break; }
    if (html.includes('404') && html.includes('Nothing Found')) break;

    const linkRe = /href=["'](https?:\/\/www\.429city\.com\/[^"']+)["']/gi;
    let m, found = 0;
    while ((m = linkRe.exec(html)) !== null) {
      const url = m[1].replace(/\/$/, '/');
      if (isProfileUrl(url) && !allUrls.has(url)) { allUrls.add(url); found++; }
    }
    console.log(`  Found ${found} new profiles (total: ${allUrls.size})`);
    if (found === 0) break;
    page++;
    await sleep(500);
  }

  // Also check /roster/ for any additional profile links
  console.log('Fetching /roster/...');
  const rosterHtml = await fetchUrl(SITE + '/roster/');
  const rosterRe = /href="(https?:\/\/www\.429city\.com\/[^"]+)"/gi;
  let m, rosterFound = 0;
  while ((m = rosterRe.exec(rosterHtml)) !== null) {
    const url = m[1].replace(/\/$/, '/');
    if (isProfileUrl(url) && !allUrls.has(url)) { allUrls.add(url); rosterFound++; }
  }
  console.log(`  Found ${rosterFound} additional from roster (total: ${allUrls.size})`);

  console.log(`Total unique profile URLs: ${allUrls.size}`);
  return [...allUrls];
}

async function scrapeProfile(url) {
  const html = await fetchUrl(url);

  // Name from h2 tag or title
  let name = '';
  const h2Match = html.match(/<h2>([^<]+)<\/h2>/i);
  if (h2Match) {
    name = decodeHtmlEntities(h2Match[1]).trim();
  } else {
    const titleMatch = html.match(/<title>([^<]+)<\/title>/i);
    if (titleMatch) {
      name = decodeHtmlEntities(titleMatch[1]).replace(/\s*[-–—|]\s*429\s*City.*/i, '').trim();
    }
  }

  // Clean name: remove prefixes like "New girl", "New Japanese girl", "Party girl"
  name = name.replace(/^(?:new\s+(?:girl\s+|japanese\s+girl\s+)?|party\s+girl\s+)/i, '').trim();
  // Capitalize first letter
  if (name) name = name.charAt(0).toUpperCase() + name.slice(1);

  // Country/Nationality
  let country = [];
  const natMatch = html.match(/Nationalit[^:]*:\s*([^<]+)/i);
  if (natMatch) {
    const raw = decodeHtmlEntities(natMatch[1]).trim().toLowerCase()
      .replace(/[🇻🇳🇨🇳🇯🇵🇰🇷🇹🇭🇦🇺🇮🇳🇧🇷🇨🇴\u{1F1E0}-\u{1F1FF}]/gu, '').trim();
    for (const [key, val] of Object.entries(COUNTRY_MAP)) {
      if (raw.includes(key)) { country = [val]; break; }
    }
    if (country.length === 0 && raw.length > 1) {
      country = [raw.charAt(0).toUpperCase() + raw.slice(1)];
    }
  }

  // Age, height, cup
  const age = (html.match(/Age:\s*(\d{2})/i) || html.match(/(\d{2})\s*yo\b/i) || [])[1] || '';
  const height = (html.match(/Height:\s*(1[3-9]\d|20\d)/i) || html.match(/(1[4-7]\d)\s*cm/i) || [])[1] || '';
  const cup = (html.match(/Bust\s*Size:\s*([A-HJ-Z])/i) || html.match(/Cup[^:]*:\s*([A-HJ-Z])/i) || [])[1] || '';

  // Description
  let desc = '';
  const descMatch = html.match(/<div[^>]*class="[^"]*lady-description[^"]*"[^>]*>([\s\S]*?)<\/div>/i);
  if (descMatch) {
    desc = descMatch[1].replace(/<br\s*\/?>/gi, ' ').replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
    desc = decodeHtmlEntities(desc).slice(0, 1000);
  }
  if (!desc) {
    // Try getting text after the info section
    const pMatches = html.match(/<p>([^<]{20,})<\/p>/gi);
    if (pMatches) {
      const texts = pMatches.map(p => decodeHtmlEntities(p.replace(/<[^>]+>/g, '').trim()))
        .filter(t => !t.match(/^(Nationalit|Age|Height|Weight|Bust|Service)/i) && t.length > 20);
      if (texts.length) desc = texts.join(' ').slice(0, 1000);
    }
  }

  // Images from the lady_photos section or main content
  let contentArea = html;
  const photosSection = html.match(/class="lady_photos"([\s\S]*?)(?=class="(?:av_|avia-|comment)|<footer|$)/i);
  if (photosSection) contentArea = photosSection[1];

  const imgRe = /(https?:\/\/www\.429city\.com\/wp-content\/uploads\/[^\s"']+\.(?:jpe?g|png|webp))/gi;
  const allImgs = [];
  let im;
  while ((im = imgRe.exec(contentArea)) !== null) allImgs.push(im[1]);

  // Dedupe: prefer original > scaled > highest res
  const groups = {};
  for (const u of allImgs) {
    // Skip tiny thumbnails
    if (/-\d+x\d+\./.test(u)) {
      const r = u.match(/-(\d+)x(\d+)\./);
      if (r && parseInt(r[1]) <= 300 && parseInt(r[2]) <= 300) continue;
    }
    const base = u.replace(/-scaled\.(jpe?g|png|webp)$/i, '.$1').replace(/-\d+x\d+\.(jpe?g|png|webp)$/i, '.$1');
    if (!groups[base]) groups[base] = { scaled: null, res: null, orig: null, px: 0 };
    if (/-scaled\./i.test(u)) groups[base].scaled = u;
    else if (/-\d+x\d+\./i.test(u)) {
      const r = u.match(/-(\d+)x(\d+)\./);
      const px = r ? parseInt(r[1]) * parseInt(r[2]) : 0;
      if (px > groups[base].px) { groups[base].res = u; groups[base].px = px; }
    } else groups[base].orig = u;
  }
  const images = Object.values(groups).map(g => g.scaled || g.orig || g.res).filter(Boolean);

  // Start date from uploads path or datePublished
  let earliest = null;
  images.forEach(u => {
    const dm = u.match(/\/uploads\/(\d{4})\/(\d{2})\//);
    if (dm) { const d = dm[1] + '-' + dm[2] + '-01'; if (!earliest || d < earliest) earliest = d; }
  });
  const pubMatch = html.match(/"datePublished"\s*:\s*"([^"]+)"/i)
    || html.match(/property="article:published_time"\s+content="([^"]+)"/i);
  if (pubMatch) { const pd = pubMatch[1].slice(0, 10); if (!earliest || pd < earliest) earliest = pd; }

  // Pricing based on country
  const pricing = getPricing(country);
  const lang = country.length ? (LANG_MAP[country[0]] || '') : '';

  return {
    name, country, age, height, cup,
    val1: pricing.val1, val2: pricing.val2, val3: pricing.val3,
    desc, lang, images,
    startDate: earliest || new Date().toISOString().split('T')[0],
  };
}

async function main() {
  const data = JSON.parse(fs.readFileSync(JSON_PATH, 'utf8'));
  const existing = data.girls || [];
  const knownUrls = new Set(existing.map(g => g.oldUrl));
  const knownNames = new Set(existing.map(g => g.name));

  console.log('Scraping listing pages...');
  const allUrls = await scrapeAllUrls();

  const newUrls = allUrls.filter(u => !knownUrls.has(u));
  console.log(`New: ${newUrls.length}, Already scraped: ${allUrls.length - newUrls.length}`);

  let added = 0;
  for (let i = 0; i < newUrls.length; i++) {
    const url = newUrls[i];
    console.log(`[${existing.length + 1}/${allUrls.length}] Scraping ${url}...`);

    try {
      const profile = await scrapeProfile(url);

      if (!profile.name || profile.name.length < 2) {
        console.log('  SKIP: no valid name');
        continue;
      }

      // Handle duplicate names by appending number
      let finalName = profile.name;
      if (knownNames.has(finalName)) {
        let n = 2;
        while (knownNames.has(finalName + ' ' + n)) n++;
        finalName = finalName + ' ' + n;
        console.log(`  Name collision: ${profile.name} -> ${finalName}`);
      }

      console.log(`  -> ${finalName} (${profile.country.join(', ') || '?'})`);

      // Download images
      const photos = [];
      const imgDir = path.join(IMG_DIR, finalName);
      for (let j = 0; j < profile.images.length; j++) {
        const ext = (profile.images[j].match(/\.(jpe?g|png|webp)$/i) || [])[1] || 'jpeg';
        const fname = `${finalName}_${j + 1}.${ext}`;
        const localPath = path.join(imgDir, fname);
        try {
          await download(profile.images[j], localPath);
          photos.push(`https://raw.githubusercontent.com/${REPO}/main/${IMG_DIR}/${encodeURIComponent(finalName)}/${encodeURIComponent(fname)}`);
          process.stdout.write('.');
          await sleep(300);
        } catch (e) {
          process.stdout.write('x');
        }
      }
      console.log(` ${photos.length} images`);

      const entry = {
        name: finalName,
        country: profile.country.length ? profile.country : undefined,
        age: profile.age || undefined,
        height: profile.height || undefined,
        cup: profile.cup || undefined,
        val1: profile.val1,
        val2: profile.val2,
        val3: profile.val3,
        startDate: profile.startDate,
        oldUrl: url,
        desc: profile.desc || '',
        lang: profile.lang || '',
        labels: [],
        originalSite: 'Exists',
        lastModified: new Date().toISOString(),
        lastRostered: profile.startDate,
        photos: photos || [],
      };

      for (const k of Object.keys(entry)) {
        if (entry[k] === undefined) delete entry[k];
      }

      existing.push(entry);
      knownNames.add(finalName);
      added++;

      if (added % SAVE_EVERY === 0) {
        data.girls = existing;
        fs.writeFileSync(JSON_PATH, JSON.stringify(data, null, 2));
        console.log(`  [Saved progress: ${existing.length} profiles]`);
      }

      await sleep(500);
    } catch (e) {
      console.error(`  ERROR: ${e.message}`);
    }
  }

  data.girls = existing;
  data.lastGirlsSync = new Date().toISOString();
  fs.writeFileSync(JSON_PATH, JSON.stringify(data, null, 2));
  console.log(`\nDone! ${added} profiles added. Total: ${existing.length}`);
}

main().catch(console.error);
