/*
 * Bulk scraper for Fantasy Club 35 (fantasyclub35.com.au)
 * WordPress site with /listing_type/ profile URLs
 *
 * Usage: node scripts/scrape-fantasyclub35.js
 */

const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');

const SITE = 'https://fantasyclub35.com.au';
const JSON_PATH = 'profiles/fantasyclub35/fantasyclub35.json';
const IMG_DIR = 'profiles/fantasyclub35';
const REPO = 'travanixlabs/brothel-search';
const SAVE_EVERY = 5;

function fetchUrl(url) {
  const mod = url.startsWith('https') ? https : http;
  return new Promise((resolve, reject) => {
    mod.get(url, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' } }, res => {
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
  const mod = url.startsWith('https') ? https : http;
  return new Promise((resolve, reject) => {
    const dir = path.dirname(dest);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    mod.get(url, { headers: { 'User-Agent': 'Mozilla/5.0' } }, res => {
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
    .replace(/&quot;/g, '"').replace(/&nbsp;/g, ' ')
    .replace(/\uff08/g, '(').replace(/\uff09/g, ')')  // fullwidth parens
    .replace(/\uff0c/g, ',').replace(/\u3001/g, ',');  // fullwidth/ideographic comma
}

const COUNTRY_MAP = {
  'japanese': 'Japanese', 'japan': 'Japanese',
  'chinese': 'Chinese', 'china': 'Chinese',
  'korean': 'Korean', 'korea': 'Korean',
  'vietnamese': 'Vietnamese', 'vietnam': 'Vietnamese',
  'thai': 'Thai', 'thailand': 'Thai',
  'hongkong': 'Hong Konger', 'hong kong': 'Hong Konger', 'hk': 'Hong Konger',
  'taiwanese': 'Taiwanese', 'taiwan': 'Taiwanese',
  'singaporean': 'Singaporean', 'singapore': 'Singaporean',
  'malaysian': 'Malaysian', 'malaysia': 'Malaysian',
  'indonesian': 'Indonesian', 'indonesia': 'Indonesian',
};

function parseTitle(title) {
  // Title format: "Name(Country)" or "Name (Country)" or just "Name"
  title = decodeHtmlEntities(title).trim();
  // Remove site suffix
  title = title.replace(/\s*[-–—]\s*Fantasy\s*Club\s*35\s*/i, '').trim();

  let name = title;
  let country = [];

  // Extract country from parentheses
  const pMatch = title.match(/^(.+?)\s*[\(\uff08]([^)\uff09]+)[\)\uff09]/);
  if (pMatch) {
    name = pMatch[1].trim();
    const rawCountry = pMatch[2].trim().toLowerCase();
    const mapped = COUNTRY_MAP[rawCountry];
    if (mapped) country = [mapped];
  }

  return { name, country };
}

async function scrapeListingPage() {
  const html = await fetchUrl(SITE + '/');
  const re = /href="(https?:\/\/fantasyclub35\.com\.au\/listing_type\/[^"]+)"/gi;
  const urls = new Set();
  let m;
  while ((m = re.exec(html)) !== null) {
    urls.add(m[1].replace(/\/$/, '/'));
  }
  return [...urls];
}

async function scrapeProfile(url) {
  const html = await fetchUrl(url);

  // Title
  const titleMatch = html.match(/<title>([^<]+)<\/title>/i);
  const titleRaw = titleMatch ? decodeHtmlEntities(titleMatch[1]) : '';
  const titleInfo = parseTitle(titleRaw);

  // Also try to get country from content if not in title
  if (titleInfo.country.length === 0) {
    const natMatch = html.match(/(?:Nationality|Country|Origin)\s*:?\s*([A-Za-z\s]+)/i);
    if (natMatch) {
      const raw = natMatch[1].trim().toLowerCase();
      const mapped = COUNTRY_MAP[raw];
      if (mapped) titleInfo.country = [mapped];
    }
  }

  // Age, height, weight, cup
  const age = (html.match(/(?:Age|年龄)\s*:?\s*(\d{2})/i) || html.match(/\/(\d{2})yo/i) || [])[1] || '';
  const height = (html.match(/(?:Height|身高)\s*:?\s*(1[3-9]\d|20\d)\s*(?:cm)?/i) || html.match(/\/(1[4-7]\d)cm/i) || [])[1] || '';
  const cup = (html.match(/(?:Cup|Bust)\s*(?:Size)?\s*:?\s*([A-HJ-Z])\s*(?:cup)?/i) || html.match(/\/([A-H])\s*cup/i) || [])[1] || '';

  // Diamond pricing - look for Diamond Service section
  let val1 = '', val2 = '', val3 = '';
  const diamondMatch = html.match(/Diamond\s*Service[\s\S]*?\$(\d+)\/30\s*min[\s\S]*?\$(\d+)\/45\s*min[\s\S]*?\$(\d+)\/60\s*min/i);
  if (diamondMatch) {
    val1 = diamondMatch[1]; val2 = diamondMatch[2]; val3 = diamondMatch[3];
  } else {
    // Try generic pricing pattern
    const priceMatch = html.match(/\$(\d+)\/30\s*min[^\$]*\$(\d+)\/45\s*min[^\$]*\$(\d+)\/60\s*min/i);
    if (priceMatch) { val1 = priceMatch[1]; val2 = priceMatch[2]; val3 = priceMatch[3]; }
  }

  // Description - get full text content
  let desc = '';
  const descMatch = html.match(/<div[^>]*class="[^"]*listing-section[^"]*"[^>]*>([\s\S]*?)<\/div>/i)
    || html.match(/<div[^>]*class="[^"]*entry-content[^"]*"[^>]*>([\s\S]*?)<\/div>/i);
  if (descMatch) {
    desc = descMatch[1]
      .replace(/<br\s*\/?>/gi, ' ')
      .replace(/<[^>]+>/g, '')
      .replace(/\s+/g, ' ')
      .trim();
    desc = decodeHtmlEntities(desc).slice(0, 1000);
  }

  // Language
  let lang = '';
  if (titleInfo.country.length) {
    const countryLangs = {
      'Japanese': 'Japanese', 'Chinese': 'Chinese', 'Korean': 'Korean',
      'Vietnamese': 'Vietnamese', 'Thai': 'Thai', 'Hong Konger': 'Cantonese',
      'Taiwanese': 'Chinese', 'Singaporean': 'English', 'Malaysian': 'Malay',
    };
    lang = countryLangs[titleInfo.country[0]] || '';
    if (lang && lang !== 'English') lang += ', English';
  }

  // Images - get all from wp-content/uploads in main content area
  // First try to find the main content boundary (before portfolio/related section)
  let contentArea = html;
  const portfolioIdx = html.search(/<div[^>]*class="[^"]*(?:portfolio|related|navigation|nav-links)[^"]*"/i);
  if (portfolioIdx > 0) contentArea = html.substring(0, portfolioIdx);

  const imgRe = /(https?:\/\/fantasyclub35\.com\.au\/wp-content\/uploads\/[^\s"']+\.(?:jpe?g|png|webp))/gi;
  const allImgs = [];
  let im;
  while ((im = imgRe.exec(contentArea)) !== null) allImgs.push(im[1]);

  // Dedupe by base, prefer scaled > original > highest resolution
  const groups = {};
  for (const u of allImgs) {
    const fn = u.split('/').pop().toLowerCase();
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

  // Start date from upload path
  let earliest = null;
  images.forEach(u => {
    const dm = u.match(/\/uploads\/(\d{4})\/(\d{2})\//);
    if (dm) {
      const d = dm[1] + '-' + dm[2] + '-01';
      if (!earliest || d < earliest) earliest = d;
    }
  });

  // datePublished from JSON-LD or meta
  const pubMatch = html.match(/"datePublished"\s*:\s*"([^"]+)"/i)
    || html.match(/property="article:published_time"\s+content="([^"]+)"/i);
  if (pubMatch) {
    const pubDate = pubMatch[1].slice(0, 10);
    if (!earliest || pubDate < earliest) earliest = pubDate;
  }

  return {
    titleInfo,
    age, height, cup,
    val1, val2, val3,
    desc, lang,
    images,
    startDate: earliest || new Date().toISOString().split('T')[0],
  };
}

async function main() {
  const data = JSON.parse(fs.readFileSync(JSON_PATH, 'utf8'));
  const existing = data.girls || [];
  const knownUrls = new Set(existing.map(g => g.oldUrl));
  const knownNames = new Set(existing.map(g => g.name));

  console.log('Scraping listing page...');
  const allUrls = await scrapeListingPage();
  console.log(`Found ${allUrls.length} profile URLs`);

  const newUrls = allUrls.filter(u => !knownUrls.has(u));
  console.log(`New: ${newUrls.length}, Already scraped: ${allUrls.length - newUrls.length}`);

  let added = 0;
  for (let i = 0; i < newUrls.length; i++) {
    const url = newUrls[i];
    console.log(`[${existing.length + 1}/${allUrls.length}] Scraping ${url}...`);

    try {
      const profile = await scrapeProfile(url);
      const { titleInfo } = profile;

      if (!titleInfo.name) {
        console.log('  SKIP: no name found');
        continue;
      }

      if (knownNames.has(titleInfo.name)) {
        console.log(`  SKIP: ${titleInfo.name} already exists`);
        continue;
      }

      const name = titleInfo.name;
      console.log(`  -> ${name} (${titleInfo.country.join(', ') || '?'})`);

      // Download images
      const photos = [];
      const imgDir = path.join(IMG_DIR, name);
      for (let j = 0; j < profile.images.length; j++) {
        const ext = (profile.images[j].match(/\.(jpe?g|png|webp)$/i) || [])[1] || 'jpeg';
        const fname = `${name}_${j + 1}.${ext}`;
        const localPath = path.join(imgDir, fname);
        try {
          await download(profile.images[j], localPath);
          photos.push(`https://raw.githubusercontent.com/${REPO}/main/${IMG_DIR}/${name}/${fname}`);
          process.stdout.write('.');
          await sleep(300);
        } catch (e) {
          process.stdout.write('x');
        }
      }
      console.log(` ${photos.length} images`);

      const entry = {
        name,
        country: titleInfo.country.length ? titleInfo.country : undefined,
        age: profile.age || undefined,
        height: profile.height || undefined,
        cup: profile.cup || undefined,
        val1: profile.val1 || undefined,
        val2: profile.val2 || undefined,
        val3: profile.val3 || undefined,
        startDate: profile.startDate,
        oldUrl: url,
        desc: profile.desc || '',
        lang: profile.lang || '',
        labels: [],
        originalSite: 'Exists',
        lastModified: new Date().toISOString(),
        lastRostered: '',
        photos: photos || [],
      };

      // Clean undefined
      for (const k of Object.keys(entry)) {
        if (entry[k] === undefined) delete entry[k];
      }

      existing.push(entry);
      knownNames.add(name);
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
