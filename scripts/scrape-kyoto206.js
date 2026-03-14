#!/usr/bin/env node
/**
 * Bulk scraper for Kyoto 206 (citybrothel.com.au)
 *
 * Usage:
 *   node scripts/scrape-kyoto206.js              # scrape all new profiles
 *   node scripts/scrape-kyoto206.js --limit 5    # scrape up to 5 new profiles
 *   node scripts/scrape-kyoto206.js --dry-run    # list profiles without scraping
 *
 * Requires: Node 18+ (uses native fetch)
 */

const fs = require('fs');
const path = require('path');
const https = require('https');

const BASE_URL = 'https://citybrothel.com.au';
const LISTING_URL = `${BASE_URL}/our-girls/`;
const REPO = 'travanixlabs/brothel-search';
const IMG_PREFIX = 'profiles/kyoto206';
const JSON_PATH = path.join(__dirname, '..', 'profiles', 'kyoto206', 'kyoto206.json');
const PROFILES_DIR = path.join(__dirname, '..', 'profiles', 'kyoto206');

const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';

const COUNTRY_MAP = {
  'japan': ['Japanese'],
  'korea': ['Korean'],
  'china': ['Chinese'],
  'thailand': ['Thai'],
  'vietnam': ['Vietnamese'],
  'indonesia': ['Indonesian'],
  'malaysia': ['Malaysian'],
  'singapore': ['Singaporean'],
  'taiwan': ['Taiwanese'],
  'hong kong': ['Hong Konger'],
  'hong-kong': ['Hong Konger'],
  'latina': ['Latina'],
  'eurasian': ['Eurasian'],
  'eurasia': ['Eurasian'],
};

const LANG_FROM_COUNTRY = {
  'Japanese': 'Japanese, Limited English',
  'Korean': 'Korean, Limited English',
  'Thai': 'Thai, Limited English',
  'Chinese': 'Mandarin, Limited English',
  'Vietnamese': 'Vietnamese, Limited English',
  'Indonesian': 'Indonesian, Limited English',
  'Malaysian': 'English',
  'Singaporean': 'English',
  'Taiwanese': 'Mandarin, Limited English',
  'Hong Konger': 'Cantonese, Limited English',
  'Latina': 'English',
  'Eurasian': 'English',
};

const LABEL_PATTERNS = [
  ['Double Lesbian', /\blesbian\s*double\b/i],
  ['Shower Together', /\bshower\s*together\b/i],
  ['Pussy Slide', /\bpussy\s*slide\b/i],
  ['DFK', /\bDFK\b/i],
  ['BBBJ', /\bBBBJ\b/i],
  ['DATY', /\bDATY\b|dining\s*at\s*the\s*y/i],
  ['69', /\b69\b/],
  ['CIM', /\bCIM\b/i],
  ['COB', /\bCOB\b/i],
  ['COF', /\bCOF\b/i],
  ['Rimming', /\brimming\b/i],
  ['Anal', /\ban[- ]?al\b/i],
  ['Double', /\bdouble\b/i],
  ['Swallow', /\bswallow\b/i],
  ['2 Men', /\b2\s*m[ae]n\b/i],
  ['Couple', /\bcouple\b/i],
  ['Filming', /\b(?:filming|video)\b/i],
  ['GFE', /\bGFE\b/i],
  ['PSE', /\bPSE\b/i],
  ['Massage', /\bmassage\b/i],
  ['Toys', /\btoys?\b/i],
  ['Costume', /\bcostume\b/i],
];

function extractLabels(desc) {
  if (!desc) return [];
  return LABEL_PATTERNS.filter(([, re]) => re.test(desc)).map(([label]) => label);
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function fetchHtml(url) {
  const resp = await fetch(url, {
    headers: { 'User-Agent': UA },
    redirect: 'follow',
  });
  if (!resp.ok) throw new Error(`HTTP ${resp.status} for ${url}`);
  return resp.text();
}

/* ── Parse listing page ── */

async function scrapeListingPage() {
  console.log('Fetching listing page...');
  const html = await fetchHtml(LISTING_URL);

  // Extract all unique /project/ URLs from listing
  const linkRe = /href="(https?:\/\/citybrothel\.com\.au\/project\/[^"]+)"/gi;
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
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&nbsp;/g, ' ');
}

function parsePageTitle(html) {
  // Extract from <title>Name (Country) &#8211; Kyoto 206</title>
  const titleMatch = html.match(/<title>([^<]+)<\/title>/i);
  let titleText = '';

  if (titleMatch) {
    let raw = decodeHtmlEntities(titleMatch[1]);
    // Remove site suffix: "– Kyoto 206" or "| Kyoto 206" or "- Kyoto 206"
    titleText = raw.replace(/\s*[–—|\-]\s*Kyoto\s*206.*$/i, '').trim();
  }

  if (!titleText) {
    // Fallback: first h2 in the page
    const h2 = html.match(/<h2[^>]*>([^<]+)<\/h2>/i);
    if (h2) titleText = h2[1].trim();
  }

  if (!titleText) return { name: '', country: [], special: '' };

  // Parse "Ayani (Japan)" or "SHINON (JAPAN) (Porn Star)"
  let special = '';
  let clean = titleText;

  const parenParts = [];
  clean = clean.replace(/[（(]([^）)]+)[）)]/g, (_, inner) => {
    parenParts.push(inner.trim());
    return '';
  }).trim();

  // Normalize name: "AYANI" -> "Ayani"
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
      const mixParts = lower.split(/\s*mix\s*/);
      for (const mp of mixParts) {
        const c = COUNTRY_MAP[mp.trim()];
        if (c) country.push(...c);
        else if (mp.trim()) country.push(mp.trim().charAt(0).toUpperCase() + mp.trim().slice(1));
      }
    } else if (COUNTRY_MAP[lower]) {
      country.push(...COUNTRY_MAP[lower]);
    } else {
      const cKeys = Object.keys(COUNTRY_MAP);
      const found = cKeys.find(k => lower.includes(k));
      if (found) country.push(...COUNTRY_MAP[found]);
      else if (lower) country.push(lower.charAt(0).toUpperCase() + lower.slice(1));
    }
  }

  return { name, country, special };
}

/* ── Parse individual profile page ── */

async function scrapeProfile(profileUrl, girlNameHint) {
  const html = await fetchHtml(profileUrl);

  // Parse name/country from page title
  const titleInfo = parsePageTitle(html);
  const girlName = girlNameHint || titleInfo.name;

  // Extract age
  const ageMatch = html.match(/Age:\s*(\d+)/i)
                || html.match(/<[^>]*>\s*Age\s*:\s*(\d+)/i);
  const age = ageMatch ? ageMatch[1] : '';

  // Extract height
  const heightMatch = html.match(/Height:\s*(1[3-9]\d|20\d)/i)
                   || html.match(/<[^>]*>\s*Height\s*:\s*(1[3-9]\d|20\d)/i)
                   || html.match(/(1[4-8]\d)\s*cm/i);
  const height = heightMatch ? heightMatch[1] : '';

  // Extract cup size
  const cupMatch = html.match(/(?:Cup|Bust)\s*(?:Size)?\s*:?\s*([A-HJ-Z](?:-[A-HJ-Z])?)\b/i)
                || html.match(/\b([A-H])\s*(?:cup|Cup)\b/i);
  const cup = cupMatch ? cupMatch[1].toUpperCase() : '';

  // Extract pricing: "30mins $200", "45mins $260", "60mins $320"
  // Also try: "$200 / $260 / $320" or "200/260/320"
  let val1 = '', val2 = '', val3 = '';

  const price30 = html.match(/30\s*min\w*\s*\$?\s*(\d+)/i);
  const price45 = html.match(/45\s*min\w*\s*\$?\s*(\d+)/i);
  const price60 = html.match(/60\s*min\w*\s*\$?\s*(\d+)/i);

  if (price30) val1 = price30[1];
  if (price45) val2 = price45[1];
  if (price60) val3 = price60[1];

  // Fallback: try "D - 30 minutes" followed by prices
  if (!val1) {
    const priceBlock = html.match(/\$(\d+)\s*(?:\/|,|\s)\s*\$(\d+)\s*(?:\/|,|\s)\s*\$(\d+)/);
    if (priceBlock) {
      val1 = priceBlock[1]; val2 = priceBlock[2]; val3 = priceBlock[3];
    }
  }

  // Extract images from wp-content/uploads - ONLY those matching the girl's name
  const imgRe = /(https?:\/\/citybrothel\.com\.au\/wp-content\/uploads\/[^\s"']+\.(?:jpe?g|png|webp))/gi;
  const allImages = [];
  let im;

  while ((im = imgRe.exec(html)) !== null) {
    allImages.push(im[1]);
  }

  // Filter to only images whose filename contains the girl's name
  // Also try common variations (e.g., Bela -> Bella)
  const nameLower = girlName.toLowerCase();
  const nameVariants = [nameLower];
  // Double last consonant + 'a' (Bela -> Bella)
  if (nameLower.length >= 3) {
    const lastChar = nameLower[nameLower.length - 1];
    const secLast = nameLower[nameLower.length - 2];
    if (!'aeiou'.includes(lastChar) || 'aeiou'.includes(secLast)) {
      nameVariants.push(nameLower + nameLower[nameLower.length - 1] + 'a');
    }
  }
  // Also try with double letters (Bela -> Bella)
  for (let i = 1; i < nameLower.length; i++) {
    if (!'aeiou'.includes(nameLower[i])) {
      nameVariants.push(nameLower.slice(0, i + 1) + nameLower[i] + nameLower.slice(i + 1));
    }
  }

  // Extract name from page title as well: "Ayani (Japan) – Kyoto 206"
  const titleMatch = html.match(/<title>([^<]+)<\/title>/i);
  if (titleMatch) {
    const titleName = titleMatch[1].split(/[(\u2013\u2014–—-]/)[0].trim().toLowerCase();
    if (titleName && !nameVariants.includes(titleName)) nameVariants.push(titleName);
  }

  // Also extract from slug in URL
  const slugMatch = profileUrl.match(/\/project\/([^/]+)/);
  if (slugMatch) {
    const slugName = decodeURIComponent(slugMatch[1]).split('-')[0].toLowerCase();
    if (slugName && !nameVariants.includes(slugName)) nameVariants.push(slugName);
  }

  const girlImages = allImages.filter(url => {
    const filename = url.split('/').pop().toLowerCase();
    return nameVariants.some(v => filename.includes(v));
  });

  // Group images by base name, prefer -scaled, fallback to highest resolution
  const imageGroups = {};
  for (const imgUrl of girlImages) {
    const base = imgUrl
      .replace(/-scaled\.(jpe?g|png|webp)$/i, '.$1')
      .replace(/-\d+x\d+\.(jpe?g|png|webp)$/i, '.$1');

    if (!imageGroups[base]) imageGroups[base] = { scaled: null, resolution: null, original: null, resPixels: 0 };

    if (/-scaled\./i.test(imgUrl)) {
      imageGroups[base].scaled = imgUrl;
    } else if (/-\d+x\d+\./i.test(imgUrl)) {
      const res = imgUrl.match(/-(\d+)x(\d+)\./);
      const pixels = res ? parseInt(res[1]) * parseInt(res[2]) : 0;
      if (pixels > imageGroups[base].resPixels) {
        imageGroups[base].resolution = imgUrl;
        imageGroups[base].resPixels = pixels;
      }
    } else {
      imageGroups[base].original = imgUrl;
    }
  }

  // Select best image for each group: prefer scaled, then highest resolution, then original
  const images = [];
  for (const [base, group] of Object.entries(imageGroups)) {
    const pick = group.scaled || group.resolution || group.original;
    if (pick) images.push(pick);
  }

  // Extract earliest upload date from image paths
  let earliestUpload = null;
  for (const imgUrl of images) {
    const dm = imgUrl.match(/\/uploads\/(\d{4})\/(\d{2})\//);
    if (dm) {
      const d = `${dm[1]}-${dm[2]}-01`;
      if (!earliestUpload || d < earliestUpload) earliestUpload = d;
    }
  }

  // Extract description (WordPress content)
  let desc = '';
  const descPatterns = [
    /<div[^>]+class="[^"]*entry-content[^"]*"[^>]*>([\s\S]*?)<\/div>/i,
    /<div[^>]+class="[^"]*project-description[^"]*"[^>]*>([\s\S]*?)<\/div>/i,
    /<div[^>]+class="[^"]*portfolio-description[^"]*"[^>]*>([\s\S]*?)<\/div>/i,
  ];
  for (const re of descPatterns) {
    const dm = html.match(re);
    if (dm) {
      desc = dm[1].replace(/<br\s*\/?>/gi, ' ').replace(/<[^>]*>/g, '').replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').replace(/\s+/g, ' ').trim();
      if (desc && desc.length > 20) break;
    }
  }

  // Fallback: look for longer text blocks
  if (!desc || desc.length < 20) {
    const textBlocks = html.match(/<p[^>]*>([^<]{40,})<\/p>/gi);
    if (textBlocks && textBlocks.length) {
      const longest = textBlocks.sort((a, b) => b.length - a.length)[0];
      desc = longest.replace(/<[^>]*>/g, '').replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').replace(/\s+/g, ' ').trim();
    }
  }

  return { age, height, cup, val1, val2, val3, images, earliestUpload, desc, titleInfo };
}

/* ── Download image to local disk ── */

async function downloadImage(url, destPath) {
  const dir = path.dirname(destPath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  return new Promise((resolve, reject) => {
    const doFetch = (fetchUrl, redirects = 0) => {
      if (redirects > 5) return reject(new Error('Too many redirects'));
      const mod = fetchUrl.startsWith('https') ? https : require('http');
      mod.get(fetchUrl, { headers: { 'User-Agent': UA } }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          return doFetch(res.headers.location, redirects + 1);
        }
        if (res.statusCode !== 200) {
          res.resume();
          return reject(new Error(`HTTP ${res.statusCode}`));
        }
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

  // Load existing data
  let data;
  try {
    data = JSON.parse(fs.readFileSync(JSON_PATH, 'utf8'));
  } catch {
    data = { girls: [], calendar: { _published: [] }, lastGirlsSync: '', lastCalendarSync: '' };
  }

  const knownNames = new Set(data.girls.map(g => g.name));
  const knownUrls = new Set(data.girls.map(g => g.oldUrl).filter(Boolean));

  // Scrape listing
  const allUrls = await scrapeListingPage();

  // Filter to new URLs only
  const newUrls = allUrls.filter(url => !knownUrls.has(url));

  console.log(`\n${allUrls.length} total URLs, ${knownUrls.size} existing, ${newUrls.length} new`);

  if (dryRun) {
    console.log('\n--- Dry Run: New profile URLs ---');
    for (const url of newUrls.slice(0, limit)) {
      console.log(`  ${url}`);
    }
    return;
  }

  const toProcess = newUrls.slice(0, limit);
  console.log(`Processing ${toProcess.length} profiles...\n`);

  const now = new Date().toISOString();
  let processed = 0;

  for (const profileUrl of toProcess) {
    try {
      console.log(`[${processed + 1}/${toProcess.length}] Scraping ${profileUrl}...`);
      await sleep(1000); // rate limit

      const detail = await scrapeProfile(profileUrl, null);
      const titleInfo = detail.titleInfo;

      if (!titleInfo.name) {
        console.log(`  SKIP: could not extract name from page`);
        continue;
      }

      // Check if we already have this name (may have been added during this run)
      if (knownNames.has(titleInfo.name)) {
        console.log(`  SKIP: ${titleInfo.name} already exists`);
        continue;
      }

      const name = titleInfo.name;
      console.log(`  -> ${name} (${titleInfo.country.join(', ')})`);

      const entry = {
        name,
        country: titleInfo.country.length ? titleInfo.country : [],
        age: detail.age || '',
        height: detail.height || '',
        cup: detail.cup || '',
        val1: detail.val1 || '',
        val2: detail.val2 || '',
        val3: detail.val3 || '',
        startDate: detail.earliestUpload || now.split('T')[0],
        oldUrl: profileUrl,
      };
      if (titleInfo.special) entry.special = titleInfo.special;

      // Language from country
      if (titleInfo.country.length) {
        entry.lang = LANG_FROM_COUNTRY[titleInfo.country[0]] || '';
      }

      entry.desc = detail.desc || '';
      entry.labels = extractLabels(detail.desc);
      entry.originalSite = 'Exists';
      entry.lastModified = now;
      entry.lastRostered = '';

      // Download images
      const photos = [];
      const girlDir = path.join(PROFILES_DIR, name);

      for (let i = 0; i < detail.images.length; i++) {
        try {
          const imgUrl = detail.images[i];
          const ext = (imgUrl.match(/\.(jpe?g|png|webp)$/i) || [])[1] || 'jpeg';
          const fileName = `${name}_${i + 1}.${ext}`;
          const localPath = path.join(girlDir, fileName);
          const repoPath = `${IMG_PREFIX}/${name}/${fileName}`;

          await downloadImage(imgUrl, localPath);
          photos.push(`https://raw.githubusercontent.com/${REPO}/main/${repoPath}`);

          await sleep(300); // rate limit between images
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

      // Save after each profile (incremental save)
      if (processed % 5 === 0) {
        data.lastGirlsSync = new Date().toISOString();
        fs.writeFileSync(JSON_PATH, JSON.stringify(data, null, 2));
        console.log(`  [Saved progress: ${processed} profiles]`);
      }

    } catch (e) {
      console.error(`  ERROR processing ${profileUrl}: ${e.message}`);
    }
  }

  // Final save
  data.lastGirlsSync = new Date().toISOString();
  fs.writeFileSync(JSON_PATH, JSON.stringify(data, null, 2));
  console.log(`\nDone! ${processed} profiles added. Total: ${data.girls.length}`);
}

main().catch(e => {
  console.error('Fatal error:', e);
  process.exit(1);
});
