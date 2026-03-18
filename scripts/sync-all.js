/**
 * Local sync script — girl profiles & calendars for all 7 venues.
 *
 * Usage:
 *   node scripts/sync-all.js              # sync everything
 *   node scripts/sync-all.js --calendar   # calendars only
 *   node scripts/sync-all.js --girls      # girls only
 *   node scripts/sync-all.js --venue empire,club  # specific venues
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');

const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36';

/* ── Fetch helper ── */

function fetchUrl(url, { binary = false } = {}) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    mod.get(url, { headers: { 'User-Agent': UA } }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        let loc = res.headers.location;
        if (!loc.startsWith('http')) loc = new URL(loc, url).href;
        return fetchUrl(loc, { binary }).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      if (binary) {
        const chunks = []; res.on('data', c => chunks.push(c));
        res.on('end', () => resolve(Buffer.concat(chunks)));
      } else {
        let data = ''; res.on('data', c => data += c);
        res.on('end', () => resolve(data));
      }
    }).on('error', reject);
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

/* ── Site configs ── */

const SITES = {
  empire: {
    name: 'Ginza Empire',
    baseUrl: 'https://479ginza.com.au',
    girlsUrl: 'https://479ginza.com.au/Girls',
    rosterUrl: 'https://479ginza.com.au/Roster',
    jsonPath: 'profiles/ginzaempire/ginzaempire.json',
    imgPrefix: 'profiles/ginzaempire',
    rosterFormat: 'empire',
    siteType: 'ginza',
  },
  club: {
    name: 'Ginza Club',
    baseUrl: 'https://www.ginzaclub.com.au',
    girlsUrl: 'https://www.ginzaclub.com.au/Girls',
    rosterUrl: 'https://www.ginzaclub.com.au/Roster',
    jsonPath: 'profiles/ginzaclub/ginzaclub.json',
    imgPrefix: 'profiles/ginzaclub',
    rosterFormat: 'club',
    siteType: 'ginza',
  },
  kyoto206: {
    name: 'Kyoto 206',
    baseUrl: 'https://citybrothel.com.au',
    girlsUrl: 'https://citybrothel.com.au/our-girls/',
    rosterUrl: 'https://citybrothel.com.au/girls-roster/',
    jsonPath: 'profiles/kyoto206/kyoto206.json',
    imgPrefix: 'profiles/kyoto206',
    siteType: 'wordpress',
    rosterFormat: 'kyoto206',
  },
  sakura57: {
    name: 'Sakura 57',
    baseUrl: 'https://www.surryhillsbrothel.com.au',
    girlsUrl: 'https://www.surryhillsbrothel.com.au/our-girls/',
    rosterUrl: 'https://www.surryhillsbrothel.com.au/girls-roster/',
    jsonPath: 'profiles/sakura57/sakura57.json',
    imgPrefix: 'profiles/sakura57',
    siteType: 'wordpress',
    rosterFormat: 'kyoto206',
  },
  top127: {
    name: 'Top 127',
    baseUrl: 'https://127city.com',
    girlsUrl: 'https://127city.com/ladies/',
    rosterUrl: 'https://127city.com/',
    jsonPath: 'profiles/top127/top127.json',
    imgPrefix: 'profiles/top127',
    siteType: 'wordpress',
    rosterFormat: 'top127',
    embedPhotos: true,
  },
  fantasyclub35: {
    name: 'Fantasy Club 35',
    baseUrl: 'https://fantasyclub35.com.au',
    girlsUrl: 'https://fantasyclub35.com.au/',
    rosterUrl: 'https://fantasyclub35.com.au/roster/',
    jsonPath: 'profiles/fantasyclub35/fantasyclub35.json',
    imgPrefix: 'profiles/fantasyclub35',
    siteType: 'wordpress',
    rosterFormat: 'fantasyclub35',
    listingSelector: 'listing_type',
    paginationParam: 'pg',
    excludeUrls: ['trendy-wendy', 'shop-online', 'product'],
    embedPhotos: true,
  },
  city429: {
    name: '429 City',
    baseUrl: 'https://www.429city.com',
    girlsUrl: 'https://www.429city.com/ladies/',
    rosterUrl: 'https://www.429city.com/roster/',
    jsonPath: 'profiles/429city/429city.json',
    imgPrefix: 'profiles/429city',
    siteType: 'wordpress',
    rosterFormat: '429city',
    excludeUrlPatterns: ['/ladies/', '/roster/', '/contact/', '/feed/', '/comments/', '/rate/', '/escort/', '/job/', '/page/', '/author/', '/wp-admin/', '/wp-login/'],
    pricingByCountry: {
      japanese: { val1: '210', val2: '260', val3: '320' },
      western: { val1: '230', val2: '280', val3: '350' },
      other: { val1: '170', val2: '240', val3: '300' },
    },
  },
};

/* ── Date / time helpers ── */

function getAEDTDate() {
  return new Date(new Date().toLocaleString('en-US', { timeZone: 'Australia/Sydney' }));
}

function fmtDate(d) {
  return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
}

const today = fmtDate(getAEDTDate());

function parseTime12to24(timeStr) {
  const t = timeStr.trim().toLowerCase();
  if (t === 'close') return '05:00';
  const m = t.match(/^(\d{1,2})(?:[:.](\d{2}))?([ap]m)$/i);
  if (!m) return null;
  let h = parseInt(m[1], 10);
  const min = m[2] ? parseInt(m[2], 10) : 0;
  if (m[3].toLowerCase() === 'pm' && h !== 12) h += 12;
  if (m[3].toLowerCase() === 'am' && h === 12) h = 0;
  return String(h).padStart(2, '0') + ':' + String(min).padStart(2, '0');
}

const MONTH_MAP = {
  january: 0, february: 1, march: 2, april: 3, may: 4, june: 5,
  july: 6, august: 7, september: 8, october: 9, november: 10, december: 11,
};

function resolveDate(day, monthName) {
  const now = getAEDTDate();
  const month = MONTH_MAP[monthName.toLowerCase()];
  if (month === undefined) return null;
  let year = now.getFullYear();
  if (month < now.getMonth() - 2) year++;
  return fmtDate(new Date(year, month, day));
}

/* ── Shared constants ── */

const COUNTRY_PREFIX = {
  J: 'Japanese', K: 'Korean', T: 'Thai', C: 'Chinese',
  V: 'Vietnamese', M: 'Malaysian', S: 'Singaporean',
};

const LANG_FROM_COUNTRY = {
  Japanese: 'Japanese, Limited English',
  Korean: 'Korean, Limited English',
  Thai: 'Thai, Limited English',
  Chinese: 'Mandarin, Limited English',
  Vietnamese: 'Vietnamese, Limited English',
  Malaysian: 'English',
  Singaporean: 'English',
  Indonesian: 'Indonesian, Limited English',
  Taiwanese: 'Mandarin, Limited English',
  'Hong Konger': 'Cantonese, Limited English',
  Latina: 'English',
  Eurasian: 'English',
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

const WP_COUNTRY_MAP = {
  japan: ['Japanese'], korea: ['Korean'], china: ['Chinese'],
  thailand: ['Thai'], vietnam: ['Vietnamese'], indonesia: ['Indonesian'],
  malaysia: ['Malaysian'], singapore: ['Singaporean'], taiwan: ['Taiwanese'],
  'hong kong': ['Hong Konger'], latina: ['Latina'], eurasian: ['Eurasian'],
};

/* ── Ginza sites: girls listing ── */

function parseGirlTitle(raw) {
  let special = '';
  let clean = raw;
  const parens = [];
  clean = clean.replace(/\(([^)]+)\)/g, (_, inner) => { parens.push(inner.trim()); return ''; }).trim();
  if (parens.length) special = parens.join(', ');
  clean = clean.replace(/([a-zA-Z])(\d+\s*(?:MINS?|HRS?|HOURS?))/gi, '$1 $2');
  const minRe = /\b(\d+\s*(?:MINS?|MINUTES?|HRS?|HOURS?)\s*(?:MINIMUM|MIN)?)\b/gi;
  let minM;
  while ((minM = minRe.exec(clean)) !== null) {
    special = special ? special + ', ' + minM[1].trim() : minM[1].trim();
  }
  clean = clean.replace(minRe, '').trim();
  const restrictRe = /\b(No\s+\w+|Asian\s+only|Japanese\s+only)\b/gi;
  let rm;
  while ((rm = restrictRe.exec(clean)) !== null) {
    special = special ? special + ', ' + rm[1].trim() : rm[1].trim();
  }
  clean = clean.replace(restrictRe, '').trim();
  clean = clean.replace(/\b\w+\s+Class\b/gi, '').trim();
  clean = clean.replace(/^(New\s+)+/i, '').trim();
  const words = clean.split(/\s+/).filter(Boolean);
  let country = [];
  for (const w of words.slice(0, -1)) {
    if (COUNTRY_PREFIX[w]) country = [COUNTRY_PREFIX[w]];
  }
  if (!country.length && words.length > 1) {
    const prefix = words.slice(0, -1).join(' ').toLowerCase();
    if (prefix.includes('japan')) country = ['Japanese'];
    else if (prefix.includes('korea')) country = ['Korean'];
    else if (prefix.includes('thai')) country = ['Thai'];
    else if (prefix.includes('chin')) country = ['Chinese'];
    else if (prefix.includes('vietnam')) country = ['Vietnamese'];
    else if (prefix.includes('brazil')) country = ['Brazilian'];
    else if (prefix.includes('malay')) country = ['Malaysian'];
  }
  let name = (words[words.length - 1] || '').replace(/\./g, '');
  return { name, country, special };
}

async function scrapeGinzaListing(site) {
  const html = await fetchUrl(site.girlsUrl);
  const cards = [];
  const cardRe = /<a\s+href="\/Girls\/(\d+)"[^>]*>([\s\S]*?)<\/a>/g;
  let m;
  while ((m = cardRe.exec(html)) !== null) {
    const id = m[1];
    const cardHtml = m[2];
    const h3 = cardHtml.match(/<h3>(.*?)<\/h3>/);
    if (!h3) continue;
    const rawTitle = h3[1].replace(/<[^>]*>/g, '').trim();
    const parsed = parseGirlTitle(rawTitle);
    if (!parsed.name) continue;
    const age    = (cardHtml.match(/Age:(\d+)/)        || [])[1] || '';
    const body   = (cardHtml.match(/Body Size:(\d+)/)   || [])[1] || '';
    const cup    = (cardHtml.match(/Cup Size:([\w\-+]+)/) || [])[1] || '';
    const height = (cardHtml.match(/Height:(\d+)/)       || [])[1] || '';
    cards.push({ id, ...parsed, age, body, cup, height });
  }
  return cards;
}

async function scrapeGinzaProfile(site, id) {
  const html = await fetchUrl(`${site.girlsUrl}/${id}`);
  const bk = html.match(/Booking:?\s*<\/(?:label|dt)>\s*<dd>\s*([\d,.\/ ]+)/i)
    || html.match(/Booking:<\/label>\s*([\d,.\/ ]+)/i)
    || html.match(/Booking:\s*([\d,.\/ ]+)/i);
  let val1 = '', val2 = '', val3 = '';
  if (bk) {
    const p = bk[1].trim().split(/[,.\/ ]+/);
    val1 = p[0] || ''; val2 = p[1] || ''; val3 = p[2] || '';
  }
  const htMatch = html.match(/Height:?\s*<\/(?:label|dt)>\s*<dd>\s*(1[3-9]\d|20\d)/i);
  const profileHeight = htMatch ? htMatch[1] : '';
  const typeMatch = html.match(/Type:<\/label>\s*([^<]+)/i);
  const profileType = typeMatch ? typeMatch[1].replace(/&nbsp;/g, ' ').trim() : '';
  const langMatch = html.match(/Language:<\/label>\s*([^<]+)/i);
  const profileLang = langMatch ? langMatch[1].replace(/&nbsp;/g, ' ').trim() : '';
  const expMatch = html.match(/Speciality:<\/label>\s*([^<]+)/i)
    || html.match(/Experience:<\/label>\s*([^<]+)/i);
  const profileExp = expMatch ? expMatch[1].replace(/&nbsp;/g, ' ').trim() : '';

  const imgRe = /<a[^>]+href="(\/data\/upload\/[^"]+\.\w+)"[^>]*>/gi;
  const images = [];
  let earliestUpload = null;
  let im;
  while ((im = imgRe.exec(html)) !== null) {
    const src = im[1];
    if (/s\.\w+$/i.test(src)) continue;
    if (/\.(jpe?g|png|webp)$/i.test(src)) {
      images.push(site.baseUrl + src);
      const dm = src.match(/\/data\/upload\/(\d{4})-(\d{2})\//);
      if (dm) {
        const d = `${dm[1]}-${dm[2]}-01`;
        if (!earliestUpload || d < earliestUpload) earliestUpload = d;
      }
    }
  }

  let desc = '';
  const descPatterns = [
    /<div class="(?:about|description|text|info-text|detail)"[^>]*>([\s\S]*?)<\/div>/i,
    /<div class="row"><label>(?:Description|About|Info):<\/label>\s*([\s\S]*?)<\/div>/i,
  ];
  for (const re of descPatterns) {
    const dm = html.match(re);
    if (dm) {
      desc = dm[1].replace(/<br\s*\/?>/gi, ' ').replace(/<[^>]*>/g, '').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim();
      if (desc) break;
    }
  }

  return { val1, val2, val3, images, desc, profileHeight, profileType, profileLang, profileExp, earliestUpload };
}

function isValidGirlName(name) {
  if (!name) return false;
  if (/[|&!=<>{}[\]@#$%^*]/.test(name)) return false;
  if (name.length > 20) return false;
  if (name.trim().split(/\s+/).length > 2) return false;
  return true;
}

/* ── WordPress sites: girls listing ── */

function decodeHtmlEntities(str) {
  return str
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(parseInt(n)))
    .replace(/&#x([0-9a-f]+);/gi, (_, n) => String.fromCharCode(parseInt(n, 16)))
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"').replace(/&nbsp;/g, ' ');
}

function parseWpPageTitle(html, site) {
  const titleMatch = html.match(/<title>([^<]+)<\/title>/i);
  if (!titleMatch) return { name: '', country: [], special: '' };
  let titleText = decodeHtmlEntities(titleMatch[1])
    .replace(/\s*[–—|\-]\s*(?:Kyoto\s*206|Sakura\s*57|Top\s*127|Fantasy\s*Club\s*35|429\s*City).*$/i, '')
    .replace(/\s*[–—|\-]\s*(?:citybrothel|surryhillsbrothel|127city|fantasyclub35|429city).*$/i, '')
    .trim();

  let special = '';
  const parenParts = [];
  titleText = titleText.replace(/[（(]([^）)]+)[）)]/g, (_, inner) => {
    parenParts.push(inner.trim()); return '';
  }).trim();

  let name = titleText.replace(/\s+/g, ' ').trim();
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
        const c = WP_COUNTRY_MAP[mp.trim()];
        if (c) country.push(...c);
        else if (mp.trim()) country.push(mp.trim().charAt(0).toUpperCase() + mp.trim().slice(1));
      }
    } else if (WP_COUNTRY_MAP[lower]) {
      country.push(...WP_COUNTRY_MAP[lower]);
    } else if (lower) {
      const found = Object.keys(WP_COUNTRY_MAP).find(k => lower.includes(k));
      if (found) country.push(...WP_COUNTRY_MAP[found]);
      else country.push(lower.charAt(0).toUpperCase() + lower.slice(1));
    }
  }
  return { name, country, special };
}

async function scrapeWpListing(site) {
  const domain = new URL(site.baseUrl).hostname.replace(/\./g, '\\.');
  const pathType = site.listingSelector || 'project';
  const linkRe = new RegExp(`href="(https?://${domain}/${pathType}/[^"]+)"`, 'gi');
  const seen = new Set();
  const urls = [];
  const paginationParam = site.paginationParam || null;

  let page = 1;
  while (true) {
    const pageUrl = paginationParam && page > 1
      ? `${site.girlsUrl}?${paginationParam}=${page}`
      : site.girlsUrl;
    try {
      const html = await fetchUrl(pageUrl);
      let found = 0;
      let m;
      while ((m = linkRe.exec(html)) !== null) {
        const url = m[1].replace(/\/$/, '') + '/';
        if (!seen.has(url) && !(site.excludeUrls || []).some(pat => url.toLowerCase().includes(pat))) {
          seen.add(url); urls.push(url); found++;
        }
      }
      if (!paginationParam || found === 0) break;
      page++;
    } catch (e) {
      console.error(`  Listing page ${page} error: ${e.message}`);
      break;
    }
  }
  return urls;
}

async function scrapeWpProfile(site, profileUrl) {
  const html = await fetchUrl(profileUrl);
  const titleInfo = parseWpPageTitle(html, site);

  const ageMatch = html.match(/Age:\s*(\d+)/i);
  const age = ageMatch ? ageMatch[1] : '';
  const heightMatch = html.match(/Height:\s*(1[3-9]\d|20\d)/i) || html.match(/(1[4-8]\d)\s*cm/i);
  const height = heightMatch ? heightMatch[1] : '';
  const cupMatch = html.match(/(?:Cup|Bust)\s*(?:Size)?\s*:?\s*([A-HJ-Z](?:-[A-HJ-Z])?)\b/i);
  const cup = cupMatch ? cupMatch[1].toUpperCase() : '';

  let val1 = '', val2 = '', val3 = '';
  const p30 = html.match(/30\s*min\w*\s*\$?\s*(\d+)/i);
  const p45 = html.match(/45\s*min\w*\s*\$?\s*(\d+)/i);
  const p60 = html.match(/60\s*min\w*\s*\$?\s*(\d+)/i);
  if (p30) val1 = p30[1];
  if (p45) val2 = p45[1];
  if (p60) val3 = p60[1];
  if (!val1) {
    const pb = html.match(/\$(\d+)\s*(?:\/|,|\s)\s*\$(\d+)\s*(?:\/|,|\s)\s*\$(\d+)/);
    if (pb) { val1 = pb[1]; val2 = pb[2]; val3 = pb[3]; }
  }

  // Images: prefer Elementor gallery hrefs
  const galleryRe = /e-gallery-item[^>]*href="([^"]+)"/gi;
  const galleryImages = [];
  let im;
  while ((im = galleryRe.exec(html)) !== null) {
    if (/\.(?:jpe?g|png|webp)$/i.test(im[1])) galleryImages.push(im[1]);
  }

  let images;
  if (galleryImages.length > 0) {
    images = galleryImages;
  } else {
    const mainHtml = html.split(/In Portfolios|class="portfolio|class="related|id="portfolio/i)[0] || html;
    const domain = new URL(site.baseUrl).hostname.replace(/\./g, '\\.');
    const imgRe = new RegExp(`(https?://${domain}/wp-content/uploads/[^\\s"']+\\.(?:jpe?g|png|webp))`, 'gi');
    const allImages = [];
    while ((im = imgRe.exec(mainHtml)) !== null) allImages.push(im[1]);

    const name = titleInfo.name;
    const nameLower = name.toLowerCase();
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

    let girlImgs = allImages.filter(url => {
      const filename = url.split('/').pop().toLowerCase();
      return nameVariants.some(v => filename.includes(v));
    });

    if (girlImgs.length === 0) {
      const portfolioNames = new Set();
      allImages.forEach(url => {
        const fn = url.split('/').pop();
        const nameMatch = fn.match(/^([A-Z][a-z]+)-/);
        if (nameMatch) portfolioNames.add(nameMatch[1].toLowerCase());
      });
      girlImgs = allImages.filter(url => {
        const fn = url.split('/').pop().toLowerCase();
        if (fn.includes('logo') || fn.includes('qr') || fn.includes('微信')) return false;
        if (/-160x160\./.test(url) || /-746x548\./.test(url) || /-300x300\./.test(url)) return false;
        const namePrefix = fn.match(/^([a-z]+)-/);
        if (namePrefix && portfolioNames.has(namePrefix[1]) && !nameVariants.includes(namePrefix[1])) return false;
        return true;
      });
    }

    // Group by base, prefer -scaled, then highest resolution
    const groups = {};
    for (const imgUrl of girlImgs) {
      const base = imgUrl.replace(/-scaled\.(jpe?g|png|webp)$/i, '.$1')
        .replace(/-\d+x\d+\.(jpe?g|png|webp)$/i, '.$1');
      if (!groups[base]) groups[base] = { scaled: null, resolution: null, original: null, resPixels: 0 };
      if (/-scaled\./i.test(imgUrl)) groups[base].scaled = imgUrl;
      else if (/-\d+x\d+\./i.test(imgUrl)) {
        const res = imgUrl.match(/-(\d+)x(\d+)\./);
        const px = res ? parseInt(res[1]) * parseInt(res[2]) : 0;
        if (px > groups[base].resPixels) { groups[base].resolution = imgUrl; groups[base].resPixels = px; }
      } else groups[base].original = imgUrl;
    }

    images = [];
    for (const g of Object.values(groups)) {
      const pick = g.scaled || g.resolution || g.original;
      if (pick) images.push(pick);
    }
  }

  let earliestUpload = null;
  for (const pick of images) {
    const dm = pick.match(/\/uploads\/(\d{4})\/(\d{2})\//);
    if (dm) {
      const d = `${dm[1]}-${dm[2]}-01`;
      if (!earliestUpload || d < earliestUpload) earliestUpload = d;
    }
  }

  return { titleInfo, age, height, cup, val1, val2, val3, images, earliestUpload };
}

/* ── 429 City special: listing from two pages ── */

async function scrape429CityListing(site) {
  const domain = new URL(site.baseUrl).hostname.replace(/\./g, '\\.');
  const seen = new Set();
  const urls = [];

  for (const pageUrl of [site.girlsUrl, site.rosterUrl]) {
    try {
      const html = await fetchUrl(pageUrl);
      const linkRe = new RegExp(`href="(https?://${domain}/[a-z0-9%\\-]+/?)"`, 'gi');
      let m;
      while ((m = linkRe.exec(html)) !== null) {
        const url = m[1].replace(/\/$/, '') + '/';
        const pathPart = url.replace(`https://${new URL(site.baseUrl).hostname}/`, '').replace(/\/$/, '');
        if (!pathPart) continue;
        if (site.excludeUrlPatterns.some(p => url.toLowerCase().includes(p))) continue;
        if (!seen.has(url)) { seen.add(url); urls.push(url); }
      }
    } catch (e) {
      console.error(`  429 City listing page error: ${e.message}`);
    }
  }
  return urls;
}

/* ── Image download ── */

async function downloadImage(imageUrl, localPath) {
  const dir = path.dirname(localPath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  if (fs.existsSync(localPath)) return; // skip existing
  try {
    const buffer = await fetchUrl(imageUrl, { binary: true });
    fs.writeFileSync(localPath, buffer);
  } catch (e) {
    console.error(`    Image download failed: ${e.message}`);
  }
}

/* ── Girls sync ── */

async function syncGirls(site) {
  console.log(`\n=== ${site.name}: Girls Sync ===`);
  const data = JSON.parse(fs.readFileSync(site.jsonPath, 'utf8'));
  const existing = data.girls || [];
  const knownNames = new Set(existing.map(g => g.name));
  const knownUrls = new Set(existing.map(g => g.oldUrl).filter(Boolean));

  let activeSet;
  let newEntries = [];

  if (site.siteType === 'ginza') {
    const cards = await scrapeGinzaListing(site);
    const activeNames = new Set(cards.map(c => c.name));
    activeSet = activeNames;

    // Update originalSite
    for (const g of existing) {
      g.originalSite = activeNames.has(g.name) ? 'Exists' : '';
    }

    const newCards = cards.filter(c => !knownNames.has(c.name) && !knownUrls.has(`${site.girlsUrl}/${c.id}`));
    if (newCards.length === 0) {
      console.log('  No new profiles');
    } else {
      console.log(`  ${newCards.length} new profiles found`);
      for (const card of newCards) {
        try {
          await sleep(1000);
          const profile = await scrapeGinzaProfile(site, card.id);
          const now = new Date().toISOString();
          const entry = {
            name: card.name,
            country: card.country.length ? card.country : undefined,
            age: card.age || undefined,
            body: card.body || undefined,
            height: card.height || profile.profileHeight || undefined,
            cup: card.cup || undefined,
            val1: profile.val1 || undefined,
            val2: profile.val2 || undefined,
            val3: profile.val3 || undefined,
          };
          if (card.special) entry.special = card.special;
          entry.exp = profile.profileExp || 'Inexperienced';
          entry.startDate = profile.earliestUpload || today;
          entry.lang = profile.profileLang || (card.country.length ? LANG_FROM_COUNTRY[card.country[0]] || '' : '');
          entry.oldUrl = `${site.girlsUrl}/${card.id}`;
          entry.type = profile.profileType || '';
          entry.desc = profile.desc || '';
          entry.originalSite = 'Exists';

          // Download images locally
          const photos = [];
          for (let i = 0; i < profile.images.length; i++) {
            const ext = (profile.images[i].match(/\.(jpe?g|png|webp)$/i) || [])[1] || 'jpeg';
            const localPath = `${site.imgPrefix}/${card.name}/${card.name}_${i + 1}.${ext}`;
            await downloadImage(profile.images[i], localPath);
            photos.push(`https://raw.githubusercontent.com/travanixlabs/brothel-search/main/${localPath}`);
            await sleep(300);
          }
          entry.photos = photos;
          entry.labels = extractLabels(profile.desc);
          entry.lastModified = now;
          entry.lastRostered = '';

          for (const k of Object.keys(entry)) { if (entry[k] === undefined) delete entry[k]; }
          newEntries.push(entry);
          console.log(`  + ${card.name} (${photos.length} photos)`);
        } catch (e) {
          console.error(`  Failed: ${card.name}: ${e.message}`);
        }
      }
    }
  } else {
    // WordPress sites
    let allUrls;
    if (site.rosterFormat === '429city') {
      allUrls = await scrape429CityListing(site);
    } else {
      allUrls = await scrapeWpListing(site);
    }
    const activeUrls = new Set(allUrls);
    const skippedUrls = new Set(data._skippedUrls || []);

    // Update originalSite
    for (const g of existing) {
      g.originalSite = activeUrls.has(g.oldUrl) ? 'Exists' : '';
    }

    const newUrls = allUrls.filter(url => !knownUrls.has(url) && !skippedUrls.has(url));
    if (newUrls.length === 0) {
      console.log('  No new profiles');
    } else {
      console.log(`  ${newUrls.length} new profile URLs found`);
      for (const profileUrl of newUrls) {
        try {
          await sleep(1000);
          const profile = await scrapeWpProfile(site, profileUrl);
          const { titleInfo } = profile;
          if (!titleInfo.name || knownNames.has(titleInfo.name) || !isValidGirlName(titleInfo.name)) {
            skippedUrls.add(profileUrl);
            continue;
          }
          const name = titleInfo.name;
          const now = new Date().toISOString();
          const entry = {
            name,
            country: titleInfo.country.length ? titleInfo.country : undefined,
            age: profile.age || undefined,
            height: profile.height || undefined,
            cup: profile.cup || undefined,
            val1: profile.val1 || undefined,
            val2: profile.val2 || undefined,
            val3: profile.val3 || undefined,
          };
          if (titleInfo.special) entry.special = titleInfo.special;
          entry.startDate = profile.earliestUpload || today;
          entry.lang = titleInfo.country.length ? LANG_FROM_COUNTRY[titleInfo.country[0]] || '' : '';
          entry.oldUrl = profileUrl;
          entry.desc = '';
          entry.originalSite = 'Exists';

          const photos = [];
          if (site.embedPhotos) {
            photos.push(...profile.images);
          } else {
            for (let i = 0; i < profile.images.length; i++) {
              const ext = (profile.images[i].match(/\.(jpe?g|png|webp)$/i) || [])[1] || 'jpeg';
              const localPath = `${site.imgPrefix}/${name}/${name}_${i + 1}.${ext}`;
              await downloadImage(profile.images[i], localPath);
              photos.push(`https://raw.githubusercontent.com/travanixlabs/brothel-search/main/${localPath}`);
              await sleep(300);
            }
          }
          entry.photos = photos;
          entry.labels = [];
          entry.lastModified = now;
          entry.lastRostered = '';

          for (const k of Object.keys(entry)) { if (entry[k] === undefined) delete entry[k]; }
          newEntries.push(entry);
          knownNames.add(name);
          console.log(`  + ${name} (${photos.length} photos)`);
        } catch (e) {
          console.error(`  Failed: ${profileUrl}: ${e.message}`);
        }
      }
      if (skippedUrls.size > 0) data._skippedUrls = [...skippedUrls];
    }
  }

  if (newEntries.length > 0) {
    data.girls = [...existing, ...newEntries];
    data.lastGirlsSync = new Date().toISOString();
    fs.writeFileSync(site.jsonPath, JSON.stringify(data, null, 2));
    console.log(`  Saved ${newEntries.length} new profiles`);
  } else {
    // Still save originalSite changes
    data.girls = existing;
    data.lastGirlsSync = new Date().toISOString();
    fs.writeFileSync(site.jsonPath, JSON.stringify(data, null, 2));
    console.log('  Girls sync complete (no new profiles)');
  }

  return newEntries.length;
}

/* ── Roster scraping per format ── */

async function scrapeRosterEmpireClub(site) {
  const html = await fetchUrl(site.rosterUrl);
  const text = html.replace(/<[^>]*>/g, '').replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').replace(/&#\d+;/g, '').replace(/&[a-z]+;/g, '');
  const lines = text.split('\n').map(l => l.trim()).filter(Boolean);

  const empireHeaderRe = /Happy\s+\w+\s+(\d+)\w*\s+of\s+(\w+)/i;
  const clubHeaderRe = /Wow\s+\w+\s+(\d{1,2})\/(\d{1,2})\/(\d{4})/i;
  const entryRe = /(\w[\w .]*?)\s+(\d{1,2}(?:[:.]?\d{2})?[ap]m)-(\d{1,2}(?:[:.]?\d{2})?[ap]m)/i;

  const result = {};
  let currentDate = null;

  for (const line of lines) {
    if (site.rosterFormat === 'empire') {
      const dayMatch = line.match(empireHeaderRe);
      if (dayMatch) { currentDate = resolveDate(parseInt(dayMatch[1], 10), dayMatch[2]); continue; }
    } else {
      const dayMatch = line.match(clubHeaderRe);
      if (dayMatch) {
        const d = parseInt(dayMatch[1], 10), mo = parseInt(dayMatch[2], 10), y = parseInt(dayMatch[3], 10);
        currentDate = y + '-' + String(mo).padStart(2, '0') + '-' + String(d).padStart(2, '0');
        continue;
      }
    }
    if (!currentDate) continue;

    const entryMatch = line.match(entryRe);
    if (entryMatch) {
      const rawName = entryMatch[1].trim().replace(/\b\w+\s+Class\b/gi, '').trim();
      const nameParts = rawName.split(/\s+/);
      const name = nameParts[nameParts.length - 1].replace(/\./g, '');
      if (name.toLowerCase() === 'open') continue;
      const start = parseTime12to24(entryMatch[2]);
      const end = parseTime12to24(entryMatch[3]);
      if (!start || !end) continue;
      if (!result[currentDate]) result[currentDate] = [];
      result[currentDate].push({ name, start, end });
    }
  }
  return result;
}

async function scrapeRosterKyoto206(site) {
  const html = await fetchUrl(site.rosterUrl);
  const result = {};
  const sectionRe = /roster-date-title[^>]*>\s*\w+\s*-\s*(\w+)\s+(\d+)\s*<\/div>([\s\S]*?)(?=roster-date-title|$)/gi;
  let sm;
  while ((sm = sectionRe.exec(html)) !== null) {
    const dateStr = resolveDate(parseInt(sm[2], 10), sm[1]);
    if (!dateStr) continue;
    const tableHtml = sm[3];
    const rowRe = /col-name[^>]*>([^<]+)<\/td>[\s\S]*?col-time[^>]*>([^<]+)<\/td>/gi;
    let rm;
    while ((rm = rowRe.exec(tableHtml)) !== null) {
      const name = rm[1].trim();
      const timeRaw = rm[2].trim();
      if (!name) continue;
      const timeParts = timeRaw.split(/\s*-\s*/);
      if (timeParts.length !== 2) continue;
      const start = parseTime12to24(timeParts[0]);
      const end = parseTime12to24(timeParts[1]);
      if (!start || !end) continue;
      if (!result[dateStr]) result[dateStr] = [];
      result[dateStr].push({ name, start, end });
    }
  }
  return result;
}

async function scrapeRosterTop127(site) {
  const html = await fetchUrl(site.rosterUrl);
  const dateMatch = html.match(/(?:Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday)\s+(\d{1,2})\/(\d{1,2})\/(\d{4})/i);
  if (!dateMatch) return {};
  const day = parseInt(dateMatch[1], 10), month = parseInt(dateMatch[2], 10), year = parseInt(dateMatch[3], 10);
  const dateStr = year + '-' + String(month).padStart(2, '0') + '-' + String(day).padStart(2, '0');
  const dayOfWeek = new Date(year, month - 1, day).getDay();
  const isFriSat = dayOfWeek === 5 || dayOfWeek === 6;
  const start = '12:00', end = isFriSat ? '03:00' : '02:00';

  const rosterSection = html.split(/ROSTER/i).pop() || html;
  const nameRe = /(?:^|\n|>)\s*(?:[JKCVSTM]\s+)?([A-Z][a-z]+)\s*(?:~|–)/gm;
  const names = [];
  let m;
  while ((m = nameRe.exec(rosterSection)) !== null) {
    const name = m[1].trim();
    if (name && !names.includes(name)) names.push(name);
  }
  if (names.length === 0) return {};
  const result = {};
  result[dateStr] = names.map(name => ({ name, start, end }));
  return result;
}

async function scrapeRosterFC35(site) {
  const html = await fetchUrl(site.rosterUrl);
  const weekMatch = html.match(/Week\s+(\d{1,2})\/(\d{1,2})\/(\d{4})\s+to\s+(\d{1,2})\/(\d{1,2})\/(\d{4})/i);
  if (!weekMatch) return {};

  const startDay = parseInt(weekMatch[1]), startMonth = parseInt(weekMatch[2]), startYear = parseInt(weekMatch[3]);
  const result = {};

  for (let tabNum = 1; tabNum <= 7; tabNum++) {
    const tabMarker = 'kt-inner-tab-' + tabNum;
    const tabStart = html.indexOf(tabMarker);
    if (tabStart === -1) continue;
    const nextTab = html.indexOf('kt-inner-tab-' + (tabNum + 1), tabStart);
    const section = html.substring(tabStart, nextTab > tabStart ? nextTab : tabStart + 5000);
    const text = section.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ');

    const d = new Date(startYear, startMonth - 1, startDay + (tabNum - 1));
    const dateStr = d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');

    const entries = [];
    const entryRe = /([A-Za-z]+)\s*[\(\uff08]\s*(?:HK|CN|JP|VN|TH|SG|TW)\s*[\)\uff09]\s*(?:NEW\s+)?(\d{1,2}[ap]m)\s*-\s*(\d{1,2}[ap]m)/gi;
    let em;
    while ((em = entryRe.exec(text)) !== null) {
      const name = em[1].trim();
      const start = parseTime12to24(em[2]);
      const end = parseTime12to24(em[3]);
      if (start && end) entries.push({ name, start, end });
    }
    if (entries.length > 0) result[dateStr] = entries;
  }
  return result;
}

async function scrapeRoster429City(site) {
  const html = await fetchUrl(site.rosterUrl);
  const re = /href=["']?(https?:\/\/www\.429city\.com\/[a-z0-9%\-]+\/?)["']?/gi;
  const links = new Set();
  let m;
  while ((m = re.exec(html)) !== null) {
    const url = m[1].replace(/\/$/, '/');
    const p = url.replace('https://www.429city.com/', '').replace(/\/$/, '');
    if (p && !['ladies', 'roster', 'contact', 'rate', 'escort', 'work-for-us', 'wp-content', 'feed', 'comments', 'wp-includes', 'wp-json', 'xmlrpc', 'job'].some(x => p.includes(x))) {
      links.add(url);
    }
  }
  const result = {};
  result[today] = [];
  for (const url of links) {
    result[today].push({ url, start: '10:00', end: '05:00' });
  }
  return { _429cityUrls: true, ...result };
}

/* ── Calendar sync ── */

async function syncCalendar(site) {
  console.log(`\n=== ${site.name}: Calendar Sync ===`);
  let scraped;
  try {
    if (site.rosterFormat === 'empire' || site.rosterFormat === 'club') scraped = await scrapeRosterEmpireClub(site);
    else if (site.rosterFormat === 'kyoto206') scraped = await scrapeRosterKyoto206(site);
    else if (site.rosterFormat === 'top127') scraped = await scrapeRosterTop127(site);
    else if (site.rosterFormat === 'fantasyclub35') scraped = await scrapeRosterFC35(site);
    else if (site.rosterFormat === '429city') scraped = await scrapeRoster429City(site);
    else { console.log('  Unknown roster format'); return; }
  } catch (e) {
    console.error(`  Roster fetch error: ${e.message}`);
    return;
  }

  if (Object.keys(scraped).filter(k => !k.startsWith('_')).length === 0) {
    console.log('  No roster data found');
    return;
  }

  const data = JSON.parse(fs.readFileSync(site.jsonPath, 'utf8'));
  const calendar = data.calendar || {};
  const validNames = new Set((data.girls || []).map(g => g.name));
  const girlsByName = {};
  const girlsByUrl = {};
  for (const g of (data.girls || [])) {
    girlsByName[g.name] = g;
    if (g.oldUrl) girlsByUrl[g.oldUrl.replace(/\/$/, '/').toLowerCase()] = g;
  }

  const is429City = scraped._429cityUrls;
  delete scraped._429cityUrls;

  let changed = false;
  let matchCount = 0;
  let unmatchedNames = new Set();

  for (const [dateStr, entries] of Object.entries(scraped)) {
    if (dateStr.startsWith('_')) continue;
    for (const entry of entries) {
      let girl;
      if (is429City && entry.url) {
        girl = girlsByUrl[entry.url.replace(/\/$/, '/').toLowerCase()];
      } else {
        girl = girlsByName[entry.name];
      }
      if (!girl) {
        if (!is429City) unmatchedNames.add(entry.name);
        continue;
      }

      const { start, end } = entry;
      if (!calendar[girl.name]) calendar[girl.name] = {};
      const existing = calendar[girl.name][dateStr];
      if (!existing || existing.start !== start || existing.end !== end) {
        calendar[girl.name][dateStr] = { start, end };
        changed = true;
      }
      matchCount++;

      if (girl && (!girl.lastRostered || dateStr > girl.lastRostered)) {
        girl.lastRostered = dateStr;
        changed = true;
      }
    }
  }

  // Auto-publish scraped dates
  if (!Array.isArray(calendar._published)) calendar._published = [];
  for (const dateStr of Object.keys(scraped)) {
    if (!dateStr.startsWith('_') && !calendar._published.includes(dateStr)) {
      calendar._published.push(dateStr);
      changed = true;
    }
  }
  calendar._published.sort();

  // Prune dates older than 2 days
  const cutoffDate = getAEDTDate();
  cutoffDate.setDate(cutoffDate.getDate() - 2);
  const cutoff = fmtDate(cutoffDate);
  for (const key of Object.keys(calendar)) {
    if (key.startsWith('_')) continue;
    const sched = calendar[key];
    if (typeof sched !== 'object') continue;
    for (const dateStr of Object.keys(sched)) {
      if (dateStr < cutoff) { delete sched[dateStr]; changed = true; }
    }
  }
  const before = calendar._published.length;
  calendar._published = calendar._published.filter(d => d >= cutoff);
  if (calendar._published.length !== before) changed = true;

  const dates = Object.keys(scraped).filter(k => !k.startsWith('_'));
  console.log(`  Dates: ${dates.join(', ')}`);
  console.log(`  Matched: ${matchCount} entries`);
  if (unmatchedNames.size > 0) console.log(`  Unmatched: ${[...unmatchedNames].join(', ')}`);

  data.calendar = calendar;
  data.lastCalendarSync = new Date().toISOString();
  fs.writeFileSync(site.jsonPath, JSON.stringify(data, null, 2));
  console.log(`  Calendar saved`);
}

/* ── Main ── */

(async () => {
  const args = process.argv.slice(2);
  const calendarOnly = args.includes('--calendar');
  const girlsOnly = args.includes('--girls');
  const venueArg = args.find(a => a.startsWith('--venue'));
  const venueNext = venueArg ? null : args[args.indexOf('--venue') + 1];

  let venueFilter = null;
  const venueIdx = args.indexOf('--venue');
  if (venueIdx !== -1 && args[venueIdx + 1]) {
    venueFilter = args[venueIdx + 1].split(',');
  }

  const doGirls = !calendarOnly;
  const doCalendar = !girlsOnly;

  console.log(`Sync started at ${new Date().toISOString()}`);
  console.log(`Today (AEDT): ${today}`);
  console.log(`Mode: ${doGirls && doCalendar ? 'girls + calendar' : doGirls ? 'girls only' : 'calendar only'}`);

  const venues = venueFilter
    ? Object.entries(SITES).filter(([k]) => venueFilter.includes(k))
    : Object.entries(SITES);

  console.log(`Venues: ${venues.map(([, s]) => s.name).join(', ')}`);

  let totalNewGirls = 0;

  // Girls sync first (sequential to avoid hammering sites)
  if (doGirls) {
    for (const [key, site] of venues) {
      try {
        const count = await syncGirls(site);
        totalNewGirls += count;
      } catch (e) {
        console.error(`\n[${site.name}] Girls sync FAILED: ${e.message}`);
      }
    }
  }

  // Calendar sync (sequential)
  if (doCalendar) {
    for (const [key, site] of venues) {
      try {
        await syncCalendar(site);
      } catch (e) {
        console.error(`\n[${site.name}] Calendar sync FAILED: ${e.message}`);
      }
    }
  }

  console.log(`\n${'='.repeat(50)}`);
  console.log(`Sync complete. New profiles: ${totalNewGirls}`);
})();
