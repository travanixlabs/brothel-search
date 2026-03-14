#!/usr/bin/env node
/**
 * Bulk scraper for Top 127 (127city.com)
 *
 * Usage:
 *   node scripts/scrape-top127.js              # scrape all new profiles
 *   node scripts/scrape-top127.js --limit 5    # scrape up to 5 new profiles
 *   node scripts/scrape-top127.js --dry-run    # list profiles without scraping
 */

const fs = require('fs');
const path = require('path');
const https = require('https');

const SITE_DOMAIN = '127city.com';
const BASE_URL = `https://${SITE_DOMAIN}`;
const LISTING_URL = `${BASE_URL}/ladies/`;
const SITE_NAME = 'Top 127';
const REPO = 'travanixlabs/brothel-search';
const IMG_PREFIX = 'profiles/top127';
const JSON_PATH = path.join(__dirname, '..', 'profiles', 'top127', 'top127.json');
const PROFILES_DIR = path.join(__dirname, '..', 'profiles', 'top127');

const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';

const COUNTRY_PREFIX = {
  'j': ['Japanese'], 'k': ['Korean'], 'c': ['Chinese'],
  'v': ['Vietnamese'], 's': ['Singaporean'], 't': ['Thai'],
  'm': ['Malaysian'],
};

const LANG_FROM_COUNTRY = {
  'Japanese': 'Japanese, Limited English', 'Korean': 'Korean, Limited English',
  'Thai': 'Thai, Limited English', 'Chinese': 'Mandarin, Limited English',
  'Vietnamese': 'Vietnamese, Limited English', 'Indonesian': 'Indonesian, Limited English',
  'Malaysian': 'English', 'Singaporean': 'English',
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

function decodeHtmlEntities(str) {
  return str
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(parseInt(n)))
    .replace(/&#x([0-9a-f]+);/gi, (_, n) => String.fromCharCode(parseInt(n, 16)))
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"').replace(/&nbsp;/g, ' ');
}

/* ── Parse listing page ── */

async function scrapeListingPage() {
  console.log('Fetching listing page...');
  const html = await fetchHtml(LISTING_URL);

  const linkRe = new RegExp(`href="(https?://${SITE_DOMAIN.replace(/\./g, '\\.')}/[^"]+/)"`, 'gi');
  const seen = new Set();
  const urls = [];
  let m;

  while ((m = linkRe.exec(html)) !== null) {
    const url = m[1];
    // Skip non-profile URLs
    if (/\/(ladies|contact|about|feed|comments|wp-json|rates|employment|wp-content|wp-admin|wp-includes)\b/i.test(url)) continue;
    if (url === BASE_URL + '/') continue;
    if (seen.has(url)) continue;
    seen.add(url);
    urls.push(url);
  }

  console.log(`Found ${urls.length} profile URLs`);
  return urls;
}

/* ── Parse profile page ── */

async function scrapeProfile(profileUrl) {
  const html = await fetchHtml(profileUrl);

  // Parse title: "J Mizuki ~ Diamond ~ *Diamond$190/$260/$300 30/45/60 - Top 127"
  const titleMatch = html.match(/<title>([^<]+)<\/title>/i);
  let name = '', country = [], special = '';

  if (titleMatch) {
    let raw = decodeHtmlEntities(titleMatch[1]).replace(/\s*[–—|\-]\s*Top\s*127.*$/i, '').trim();

    // Split by ~ to get parts
    const parts = raw.split('~').map(p => p.trim());
    const namePart = parts[0] || '';

    // Parse country prefix: "J Mizuki" -> J = Japanese, name = Mizuki
    const prefixMatch = namePart.match(/^([JKCVSTM])\s+(.+)$/i);
    if (prefixMatch) {
      const prefix = prefixMatch[1].toLowerCase();
      country = COUNTRY_PREFIX[prefix] || [];
      name = prefixMatch[2].trim();
    } else {
      name = namePart;
    }

    // Remove "New" prefix
    name = name.replace(/^New\s+/i, '').trim();

    // Normalize case
    if (name === name.toUpperCase() && name.length > 1) {
      name = name.charAt(0).toUpperCase() + name.slice(1).toLowerCase();
    }
  }

  // Extract from page body
  const nationality = (html.match(/Nationalit[^:]*:\s*([A-Za-z]+)/i) || [])[1] || '';
  if (!country.length && nationality) {
    const lower = nationality.toLowerCase();
    if (lower.includes('japan')) country = ['Japanese'];
    else if (lower.includes('korea')) country = ['Korean'];
    else if (lower.includes('china') || lower.includes('chinese')) country = ['Chinese'];
    else if (lower.includes('vietnam')) country = ['Vietnamese'];
    else if (lower.includes('singapore')) country = ['Singaporean'];
    else if (lower.includes('thai')) country = ['Thai'];
    else country = [nationality];
  }

  const age = (html.match(/Age:\s*(\d+)/i) || [])[1] || '';
  const height = (html.match(/Height:\s*(1[3-9]\d|20\d)/i) || html.match(/(1[4-8]\d)\s*cm/i) || [])[1] || '';
  const cupMatch = html.match(/Bust\s*Size:\s*([A-HJ-Z](?:-[A-HJ-Z])?)/i);
  const cup = cupMatch ? cupMatch[1].toUpperCase() : '';

  // Pricing from title or page: "$190/$260/$300 30/45/60"
  let val1 = '', val2 = '', val3 = '';
  const priceMatch = html.match(/\$(\d+)\s*\/\s*\$(\d+)\s*\/\s*\$(\d+)/);
  if (priceMatch) {
    val1 = priceMatch[1]; val2 = priceMatch[2]; val3 = priceMatch[3];
  }
  // Fallback from URL
  if (!val1) {
    const urlPrice = profileUrl.match(/(\d{3})-(\d{3})-(\d{3})-30/);
    if (urlPrice) { val1 = urlPrice[1]; val2 = urlPrice[2]; val3 = urlPrice[3]; }
  }

  // Images - from main content before any portfolio section
  const mainHtml = html.split(/In Portfolios|class="portfolio|class="related|id="portfolio/i)[0] || html;
  const imgRe = new RegExp(`(https?://${SITE_DOMAIN.replace(/\./g, '\\.')}/wp-content/uploads/[^\\s"']+\\.(?:jpe?g|png|webp))`, 'gi');
  const allImages = [];
  let im;
  while ((im = imgRe.exec(mainHtml)) !== null) allImages.push(im[1]);

  // Filter by name
  const nameLower = (name || '').toLowerCase();
  const nameVariants = [nameLower];
  for (let i = 1; i < nameLower.length; i++) {
    if (!'aeiou'.includes(nameLower[i])) {
      nameVariants.push(nameLower.slice(0, i + 1) + nameLower[i] + nameLower.slice(i + 1));
    }
  }

  let girlImages = allImages.filter(url => {
    const filename = url.split('/').pop().toLowerCase();
    return nameVariants.some(v => filename.includes(v));
  });

  // Fallback for hash filenames
  if (girlImages.length === 0) {
    girlImages = allImages.filter(url => {
      const fn = url.split('/').pop().toLowerCase();
      if (fn.includes('logo') || fn.includes('qr')) return false;
      if (/-160x160\./.test(url) || /-746x548\./.test(url) || /-300x300\./.test(url)) return false;
      return true;
    });
  }

  // Group by base, prefer scaled > original > highest res
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

  const images = Object.values(imageGroups).map(g => g.scaled || g.original || g.resolution).filter(Boolean);

  // Published date from JSON-LD or meta tag
  const jsonLdDate = html.match(/"datePublished"\s*:\s*"([^"]+)"/i);
  const metaDate = html.match(/property="article:published_time"\s+content="([^"]+)"/i);
  const publishedDate = (jsonLdDate || metaDate)?.[1]?.slice(0, 10) || null;

  let earliestUpload = null;
  for (const imgUrl of images) {
    const dm = imgUrl.match(/\/uploads\/(\d{4})\/(\d{2})\//);
    if (dm) { const d = `${dm[1]}-${dm[2]}-01`; if (!earliestUpload || d < earliestUpload) earliestUpload = d; }
  }

  // Description
  let desc = '';
  const textBlocks = html.match(/<p[^>]*>([^<]{40,})<\/p>/gi);
  if (textBlocks && textBlocks.length) {
    const longest = textBlocks.sort((a, b) => b.length - a.length)[0];
    desc = longest.replace(/<[^>]*>/g, '').replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').replace(/\s+/g, ' ').trim();
  }

  return { name, country, special, age, height, cup, val1, val2, val3, images, publishedDate, earliestUpload, desc };
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

      const detail = await scrapeProfile(profileUrl);

      if (!detail.name) { console.log(`  SKIP: no name`); continue; }
      if (knownNames.has(detail.name)) { console.log(`  SKIP: ${detail.name} exists`); continue; }

      const name = detail.name;
      console.log(`  -> ${name} (${detail.country.join(', ')})`);

      const entry = {
        name, country: detail.country.length ? detail.country : [],
        age: detail.age || '', height: detail.height || '', cup: detail.cup || '',
        val1: detail.val1 || '', val2: detail.val2 || '', val3: detail.val3 || '',
        startDate: detail.publishedDate || detail.earliestUpload || now.split('T')[0],
        oldUrl: profileUrl,
      };
      if (detail.country.length) entry.lang = LANG_FROM_COUNTRY[detail.country[0]] || '';

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
