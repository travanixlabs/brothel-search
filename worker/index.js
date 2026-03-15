/*
 * Cloudflare Worker — Brothel Search Data Sync
 *
 * Scrapes sites and maintains JSON files in the brothel-search repo:
 *   profiles/ginzaempire/ginzaempire.json  — from 479ginza.com.au
 *   profiles/ginzaclub/ginzaclub.json      — from www.ginzaclub.com.au
 *   profiles/kyoto206/kyoto206.json        — from citybrothel.com.au
 *
 * Cron schedule:
 *   8:00 UTC  (7pm AEDT) — sync girls + calendar (all sites)
 *   20:00 UTC (7am AEDT) — sync girls + calendar (all sites)
 *
 * Secrets required (set via Cloudflare dashboard or `wrangler secret put`):
 *   GITHUB_TOKEN — GitHub personal access token (contents read/write scope)
 */

const REPO = 'travanixlabs/brothel-search';
const GH_API = 'https://api.github.com';
const UA = 'Mozilla/5.0 (compatible; BrothelSearchBot/1.0)';

/* ── Site configs ── */

const SITES = {
  empire: {
    name: 'Ginza Empire',
    baseUrl: 'https://479ginza.com.au',
    girlsUrl: 'https://479ginza.com.au/Girls',
    rosterUrl: 'https://479ginza.com.au/Roster',
    jsonPath: 'profiles/ginzaempire/ginzaempire.json',
    imgPrefix: 'profiles/ginzaempire',
    rosterFormat: 'empire', // "Happy Thursday 13th of March"
  },
  club: {
    name: 'Ginza Club',
    baseUrl: 'https://www.ginzaclub.com.au',
    girlsUrl: 'https://www.ginzaclub.com.au/Girls',
    rosterUrl: 'https://www.ginzaclub.com.au/Roster',
    jsonPath: 'profiles/ginzaclub/ginzaclub.json',
    imgPrefix: 'profiles/ginzaclub',
    rosterFormat: 'club', // "Wow Friday 13/3/2026"
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
  },
  fantasyclub35: {
    name: 'Fantasy Club 35',
    baseUrl: 'https://fantasyclub35.com.au',
    girlsUrl: 'https://fantasyclub35.com.au/',
    rosterUrl: 'https://fantasyclub35.com.au/roster/',
    rosterFormat: 'fantasyclub35',
    jsonPath: 'profiles/fantasyclub35/fantasyclub35.json',
    imgPrefix: 'profiles/fantasyclub35',
    siteType: 'wordpress',
    listingSelector: 'listing_type',
    paginationParam: 'pg',
  },
};

/* ── GitHub helpers ── */

function ghHeaders(env) {
  return {
    Authorization: `token ${env.GITHUB_TOKEN}`,
    Accept: 'application/vnd.github.v3+json',
    'User-Agent': 'brothel-search-worker',
    'Content-Type': 'application/json',
  };
}

function decContent(base64) {
  const raw = atob(base64.replace(/\n/g, ''));
  return JSON.parse(decodeURIComponent(escape(raw)));
}

function encContent(obj) {
  return btoa(unescape(encodeURIComponent(JSON.stringify(obj, null, 2))));
}

async function ghGet(env, path) {
  const r = await fetch(`${GH_API}/repos/${REPO}/contents/${path}`, {
    headers: ghHeaders(env),
  });
  if (!r.ok) throw new Error(`GitHub GET ${r.status} ${path}`);
  const d = await r.json();
  return { content: decContent(d.content), sha: d.sha };
}

async function ghPut(env, path, content, sha, message) {
  const body = { message, content: encContent(content) };
  if (sha) body.sha = sha;
  const r = await fetch(`${GH_API}/repos/${REPO}/contents/${path}`, {
    method: 'PUT',
    headers: ghHeaders(env),
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`GitHub PUT ${r.status} ${path}`);
  return r.json();
}

/* ── Date / time helpers ── */

function getAEDTDate() {
  return new Date(new Date().toLocaleString('en-US', { timeZone: 'Australia/Sydney' }));
}

function fmtDate(d) {
  return d.getFullYear() + '-' +
    String(d.getMonth() + 1).padStart(2, '0') + '-' +
    String(d.getDate()).padStart(2, '0');
}

function parseTime12to24(timeStr) {
  // Handle both "10:30am" and "10.30am" formats
  const m = timeStr.match(/^(\d{1,2})(?:[:.](\d{2}))?([ap]m)$/i);
  if (!m) return null;
  let h = parseInt(m[1], 10);
  const min = m[2] ? parseInt(m[2], 10) : 0;
  const pm = m[3].toLowerCase() === 'pm';
  if (pm && h !== 12) h += 12;
  if (!pm && h === 12) h = 0;
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

/* ── Girls scraping (shared) ── */

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

  // Remove "Diamond Class" / "Gold Class" etc.
  clean = clean.replace(/\b\w+\s+Class\b/gi, '').trim();

  clean = clean.replace(/^(New\s+)+/i, '').trim();

  const words = clean.split(/\s+/).filter(Boolean);
  let country = [];

  for (const w of words.slice(0, -1)) {
    if (COUNTRY_PREFIX[w]) {
      country = [COUNTRY_PREFIX[w]];
    }
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

async function scrapeGirlsListing(site) {
  const resp = await fetch(site.girlsUrl, { headers: { 'User-Agent': UA } });
  if (!resp.ok) throw new Error(`Girls listing fetch failed: ${resp.status}`);
  const html = await resp.text();

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

async function scrapeGirlProfile(site, id) {
  const resp = await fetch(`${site.girlsUrl}/${id}`, { headers: { 'User-Agent': UA } });
  if (!resp.ok) throw new Error(`Profile fetch ${resp.status} for /Girls/${id}`);
  const html = await resp.text();

  const bk = html.match(/Booking:?\s*<\/(?:label|dt)>\s*<dd>\s*([\d,.\/ ]+)/i)
          || html.match(/Booking:<\/label>\s*([\d,.\/ ]+)/i)
          || html.match(/Booking:\s*([\d,.\/ ]+)/i);
  let val1 = '', val2 = '', val3 = '';
  if (bk) {
    const p = bk[1].trim().split(/[,.\/ ]+/);
    val1 = p[0] || ''; val2 = p[1] || ''; val3 = p[2] || '';
  }

  const htMatch = html.match(/Height:?\s*<\/(?:label|dt)>\s*<dd>\s*(1[3-9]\d|20\d)/i)
               || html.match(/Height:<\/label>\s*(1[3-9]\d|20\d)/i);
  const profileHeight = htMatch ? htMatch[1] : '';

  const typeMatch = html.match(/Type:<\/label>\s*([^<]+)/i);
  const profileType = typeMatch ? typeMatch[1].replace(/&nbsp;/g, ' ').trim() : '';

  const langMatch = html.match(/Language:<\/label>\s*([^<]+)/i);
  const profileLang = langMatch ? langMatch[1].replace(/&nbsp;/g, ' ').trim() : '';

  const expMatch = html.match(/Speciality:<\/label>\s*([^<]+)/i)
                || html.match(/Experience:<\/label>\s*([^<]+)/i);
  const profileExp = expMatch ? expMatch[1].replace(/&nbsp;/g, ' ').trim() : '';

  // Images: source URLs + extract earliest upload date
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
  if (!desc) {
    const textBlocks = html.match(/<(?:p|div)[^>]*>([^<]{80,})<\/(?:p|div)>/gi);
    if (textBlocks && textBlocks.length) {
      const longest = textBlocks.sort((a, b) => b.length - a.length)[0];
      desc = longest.replace(/<[^>]*>/g, '').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim();
    }
  }

  return { val1, val2, val3, images, desc, profileHeight, profileType, profileLang, profileExp, earliestUpload };
}

/* ── WordPress site scraping (Kyoto 206, Sakura 57, etc.) ── */

const WP_COUNTRY_MAP = {
  japan: ['Japanese'], korea: ['Korean'], china: ['Chinese'],
  thailand: ['Thai'], vietnam: ['Vietnamese'], indonesia: ['Indonesian'],
  malaysia: ['Malaysian'], singapore: ['Singaporean'], taiwan: ['Taiwanese'],
  'hong kong': ['Hong Konger'], latina: ['Latina'], eurasian: ['Eurasian'],
};

function decodeHtmlEntities(str) {
  return str
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(parseInt(n)))
    .replace(/&#x([0-9a-f]+);/gi, (_, n) => String.fromCharCode(parseInt(n, 16)))
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"').replace(/&nbsp;/g, ' ');
}

function parseWpPageTitle(html) {
  const titleMatch = html.match(/<title>([^<]+)<\/title>/i);
  if (!titleMatch) return { name: '', country: [], special: '' };

  let titleText = decodeHtmlEntities(titleMatch[1])
    .replace(/\s*[–—|\-]\s*(?:Kyoto\s*206|Sakura\s*57|Top\s*127).*$/i, '').trim();

  let special = '';
  const parenParts = [];
  titleText = titleText.replace(/[（(]([^）)]+)[）)]/g, (_, inner) => {
    parenParts.push(inner.trim());
    return '';
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
    const resp = await fetch(pageUrl, { headers: { 'User-Agent': UA } });
    if (!resp.ok) break;
    const html = await resp.text();
    let found = 0;
    let m;
    while ((m = linkRe.exec(html)) !== null) {
      const url = m[1].replace(/\/$/, '') + '/';
      if (!seen.has(url)) { seen.add(url); urls.push(url); found++; }
    }
    if (!paginationParam || found === 0) break;
    page++;
  }
  return urls;
}

async function scrapeWpProfile(site, profileUrl, girlName) {
  const resp = await fetch(profileUrl, { headers: { 'User-Agent': UA } });
  if (!resp.ok) throw new Error(`WP profile fetch ${resp.status} for ${profileUrl}`);
  const html = await resp.text();

  const titleInfo = parseWpPageTitle(html);
  const name = girlName || titleInfo.name;

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

  // Images: filter to those matching girl's name, or fallback to non-portfolio images
  const mainHtml = html.split(/In Portfolios|class="portfolio|class="related|id="portfolio/i)[0] || html;
  const domain = new URL(site.baseUrl).hostname.replace(/\./g, '\\.');
  const imgRe = new RegExp(`(https?://${domain}/wp-content/uploads/[^\\s"']+\\.(?:jpe?g|png|webp))`, 'gi');
  const allImages = [];
  let im;
  while ((im = imgRe.exec(mainHtml)) !== null) allImages.push(im[1]);

  const nameLower = name.toLowerCase();
  const nameVariants = [nameLower];
  // Try name variants for fuzzy matching (e.g. Bela -> Bella)
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

  // Fallback: if no name-matched images, grab non-portfolio images (hash filenames, not other girls)
  // Portfolio thumbnails use -746x548 or -160x160 and belong to other profiles
  if (girlImgs.length === 0) {
    // Collect all known girl-name prefixes from portfolio thumbs to exclude them
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
      // Skip images with other girl names (portfolio images)
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

  const images = [];
  let earliestUpload = null;
  for (const g of Object.values(groups)) {
    const pick = g.scaled || g.resolution || g.original;
    if (pick) {
      images.push(pick);
      const dm = pick.match(/\/uploads\/(\d{4})\/(\d{2})\//);
      if (dm) {
        const d = `${dm[1]}-${dm[2]}-01`;
        if (!earliestUpload || d < earliestUpload) earliestUpload = d;
      }
    }
  }

  return { titleInfo, age, height, cup, val1, val2, val3, images, earliestUpload };
}

/* ── Sync: Kyoto 206 Girls ── */

async function syncWpGirls(env, site) {
  const { data, sha } = await loadData(env, site);
  const existing = data.girls || [];
  const skippedUrls = new Set(data._skippedUrls || []);
  const knownUrls = new Set(existing.map(g => g.oldUrl).filter(Boolean));

  const allUrls = await scrapeWpListing(site);
  const activeUrls = new Set(allUrls);

  // Update originalSite for existing girls
  let siteChanged = false;
  for (const g of existing) {
    const shouldBe = activeUrls.has(g.oldUrl) ? 'Exists' : '';
    if (g.originalSite !== shouldBe) {
      g.originalSite = shouldBe;
      siteChanged = true;
    }
  }

  const newUrls = allUrls.filter(url => !knownUrls.has(url) && !skippedUrls.has(url));

  if (newUrls.length === 0) {
    if (siteChanged) {
      data.girls = existing;
      data.lastGirlsSync = new Date().toISOString();
      await ghPut(env, site.jsonPath, data, sha, `[${site.name}] Update originalSite status`);
    }
    console.log(`[${site.name}] Girls sync: no new profiles`);
    return { added: 0, remaining: 0, names: [] };
  }

  const toProcess = newUrls.slice(0, MAX_NEW_PER_RUN);
  const remaining = newUrls.length - toProcess.length;
  console.log(`[${site.name}] Girls sync: ${newUrls.length} new, processing ${toProcess.length} (${remaining} remaining)`);

  const now = new Date().toISOString();
  const todayStr = now.split('T')[0];
  const addedNames = [];
  const knownNames = new Set(existing.map(g => g.name));

  for (const profileUrl of toProcess) {
    try {
      await new Promise(r => setTimeout(r, 1000));
      const profile = await scrapeWpProfile(site, profileUrl, null);
      const { titleInfo } = profile;

      if (!titleInfo.name || knownNames.has(titleInfo.name)) {
        console.log(`[${site.name}] Skip ${profileUrl}: ${!titleInfo.name ? 'no name' : 'duplicate'}`);
        skippedUrls.add(profileUrl);
        continue;
      }

      const name = titleInfo.name;
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
      entry.startDate = profile.earliestUpload || todayStr;
      entry.lang = titleInfo.country.length ? LANG_FROM_COUNTRY[titleInfo.country[0]] || '' : '';
      entry.oldUrl = profileUrl;
      entry.desc = '';
      entry.originalSite = 'Exists';

      // Upload images
      const photos = [];
      for (let i = 0; i < profile.images.length; i++) {
        try {
          const ext = (profile.images[i].match(/\.(jpe?g|png|webp)$/i) || [])[1] || 'jpeg';
          const path = `${site.imgPrefix}/${name}/${name}_${i + 1}.${ext}`;
          const ghUrl = await uploadImage(env, profile.images[i], path);
          photos.push(ghUrl);
          await new Promise(r => setTimeout(r, 500));
        } catch (e) {
          console.error(`[${site.name}] Image error ${name} #${i + 1}: ${e.message}`);
        }
      }
      entry.photos = photos;
      entry.labels = [];
      entry.lastModified = now;
      entry.lastRostered = '';

      for (const k of Object.keys(entry)) {
        if (entry[k] === undefined) delete entry[k];
      }

      existing.push(entry);
      knownNames.add(name);
      addedNames.push(name);
      console.log(`[${site.name}] Added ${name} (${photos.length} photos)`);
    } catch (e) {
      console.error(`[${site.name}] Failed to process ${profileUrl}: ${e.message}`);
    }
  }

  if (skippedUrls.size > 0) data._skippedUrls = [...skippedUrls];

  if (addedNames.length > 0 || siteChanged || skippedUrls.size > (data._skippedUrls || []).length) {
    data.girls = existing;
    data.lastGirlsSync = now;
    await ghPut(env, site.jsonPath, data, sha,
      `[${site.name}] Auto-sync new girls: ${addedNames.length ? addedNames.join(', ') : 'skipped duplicates'}`);
  }

  return { added: addedNames.length, remaining, names: addedNames };
}

/* ── Kyoto 206 Roster scraping ── */

function parseKyoto206Time(timeStr) {
  // "close" = 05:00
  const t = timeStr.trim().toLowerCase();
  if (t === 'close') return '05:00';
  return parseTime12to24(t);
}

async function scrapeKyoto206Roster(site) {
  const resp = await fetch(site.rosterUrl, { headers: { 'User-Agent': UA } });
  if (!resp.ok) throw new Error(`Kyoto 206 roster fetch failed: ${resp.status}`);
  const html = await resp.text();

  const result = {};

  // Find today/tomorrow sections with date titles
  // Format: <div class="roster-date-title">Saturday - MARCH 14</div>
  // Followed by table rows: <td class="col-name">Name</td><td ...>Country</td><td class="col-time">Time</td>
  const sectionRe = /roster-date-title[^>]*>\s*\w+\s*-\s*(\w+)\s+(\d+)\s*<\/div>([\s\S]*?)(?=roster-date-title|$)/gi;
  let sm;

  while ((sm = sectionRe.exec(html)) !== null) {
    const monthName = sm[1];
    const day = parseInt(sm[2], 10);
    const dateStr = resolveDate(day, monthName);
    if (!dateStr) continue;

    const tableHtml = sm[3];
    // Extract rows: <td class="col-name">Name</td>...<td class="col-time">Time</td>
    const rowRe = /col-name[^>]*>([^<]+)<\/td>[\s\S]*?col-time[^>]*>([^<]+)<\/td>/gi;
    let rm;

    while ((rm = rowRe.exec(tableHtml)) !== null) {
      const name = rm[1].trim();
      const timeRaw = rm[2].trim();
      if (!name) continue;

      // Parse time range: "12pm - 2am" or "3pm - close"
      const timeParts = timeRaw.split(/\s*-\s*/);
      if (timeParts.length !== 2) continue;

      const start = parseKyoto206Time(timeParts[0]);
      const end = parseKyoto206Time(timeParts[1]);
      if (!start || !end) continue;

      if (!result[dateStr]) result[dateStr] = [];
      result[dateStr].push({ name, start, end });
    }
  }

  return result;
}

/* ── Top 127 Roster scraping ── */

async function scrapeTop127Roster(site) {
  const resp = await fetch(site.rosterUrl, { headers: { 'User-Agent': UA } });
  if (!resp.ok) throw new Error(`Top 127 roster fetch failed: ${resp.status}`);
  const html = await resp.text();

  // Find date: "Sunday 15/03/2026" or similar near "ROSTER"
  const dateMatch = html.match(/(?:Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday)\s+(\d{1,2})\/(\d{1,2})\/(\d{4})/i);
  if (!dateMatch) return {};

  const day = parseInt(dateMatch[1], 10);
  const month = parseInt(dateMatch[2], 10);
  const year = parseInt(dateMatch[3], 10);
  const dateStr = year + '-' + String(month).padStart(2, '0') + '-' + String(day).padStart(2, '0');

  // Determine day of week for default times: Fri/Sat = 12pm-3am, else 12pm-2am
  const dayOfWeek = new Date(year, month - 1, day).getDay();
  const isFriSat = dayOfWeek === 5 || dayOfWeek === 6;
  const start = '12:00';
  const end = isFriSat ? '03:00' : '02:00';

  // Extract names: "J Sana", "C Angela", "Chanel" etc.
  // Look for roster section names - patterns like "J Name" or just "Name" with ~ separator
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

async function scrapeFantasyClub35Roster(site) {
  const resp = await fetch(site.rosterUrl, { headers: { 'User-Agent': UA } });
  if (!resp.ok) throw new Error(`Fantasy Club 35 roster fetch failed: ${resp.status}`);
  const html = await resp.text();

  // Extract week range: "Week 09/3/2026 to 15/3/2026"
  const weekMatch = html.match(/Week\s+(\d{1,2})\/(\d{1,2})\/(\d{4})\s+to\s+(\d{1,2})\/(\d{1,2})\/(\d{4})/i);
  if (!weekMatch) return {};

  const startDay = parseInt(weekMatch[1]);
  const startMonth = parseInt(weekMatch[2]);
  const startYear = parseInt(weekMatch[3]);

  // Build date for each day: Mon=0, Tue=1, ..., Sun=6
  const dayNames = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'];
  const dates = {};
  for (let i = 0; i < 7; i++) {
    const d = new Date(startYear, startMonth - 1, startDay + i);
    const dateStr = d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
    dates[dayNames[i]] = dateStr;
  }

  // Split HTML by day tabs/sections
  const result = {};
  for (const dayName of dayNames) {
    // Find section for this day - look for tab content
    const dayRe = new RegExp(dayName + '[\\s\\S]*?(?=' + (dayNames[dayNames.indexOf(dayName) + 1] || '$') + '|$)', 'i');
    const section = html.match(dayRe);
    if (!section) continue;

    const dateStr = dates[dayName];
    const entries = [];
    // Match: "Name（XX）" or "Name(XX)" followed by time like "11am-5am"
    const entryRe = /([A-Z][a-z]+)\s*[\(\uff08][^)\uff09]*[\)\uff09]\s*(?:NEW\s+)?(\d{1,2}[ap]m)\s*-\s*(\d{1,2}[ap]m)/gi;
    let m;
    while ((m = entryRe.exec(section[0])) !== null) {
      const name = m[1].trim();
      let startH = parseInt(m[2]);
      const startAmPm = m[2].replace(/\d+/, '').toLowerCase();
      let endH = parseInt(m[3]);
      const endAmPm = m[3].replace(/\d+/, '').toLowerCase();
      const start = (startAmPm === 'pm' && startH !== 12 ? startH + 12 : startAmPm === 'am' && startH === 12 ? 0 : startH);
      const end = (endAmPm === 'pm' && endH !== 12 ? endH + 12 : endAmPm === 'am' && endH === 12 ? 0 : endH);
      entries.push({ name, start: String(start).padStart(2, '0') + ':00', end: String(end).padStart(2, '0') + ':00' });
    }

    if (entries.length > 0) result[dateStr] = entries;
  }

  return result;
}

/* ── Image upload ── */

function arrayBufferToBase64(buffer) {
  const bytes = new Uint8Array(buffer);
  const chunks = [];
  for (let i = 0; i < bytes.length; i += 32768) {
    chunks.push(String.fromCharCode.apply(null, bytes.subarray(i, i + 32768)));
  }
  return btoa(chunks.join(''));
}

async function uploadImage(env, imageUrl, repoPath) {
  const resp = await fetch(imageUrl, { headers: { 'User-Agent': UA } });
  if (!resp.ok) throw new Error(`Image fetch ${resp.status}`);

  const buffer = await resp.arrayBuffer();
  const base64 = arrayBufferToBase64(buffer);

  // Check if file already exists (to get sha for update)
  let sha = null;
  try {
    const r = await fetch(`${GH_API}/repos/${REPO}/contents/${repoPath}`, { headers: ghHeaders(env) });
    if (r.ok) sha = (await r.json()).sha;
  } catch {}

  const body = { message: `Add ${repoPath}`, content: base64 };
  if (sha) body.sha = sha;

  const r = await fetch(`${GH_API}/repos/${REPO}/contents/${repoPath}`, {
    method: 'PUT',
    headers: ghHeaders(env),
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`Image upload ${r.status} for ${repoPath}`);

  return `https://raw.githubusercontent.com/${REPO}/main/${repoPath}`;
}

/* ── Roster scraping ── */

async function scrapeRoster(site) {
  const resp = await fetch(site.rosterUrl, { headers: { 'User-Agent': UA } });
  if (!resp.ok) throw new Error(`Roster fetch failed: ${resp.status}`);
  const html = await resp.text();

  const text = html
    .replace(/<[^>]*>/g, '')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&#\d+;/g, '')
    .replace(/&[a-z]+;/g, '');

  const lines = text.split('\n').map(l => l.trim()).filter(Boolean);

  // Empire: "Happy Thursday 13th of March"
  const empireHeaderRe = /Happy\s+\w+\s+(\d+)\w*\s+of\s+(\w+)/i;
  // Club: "Wow Friday 13/3/2026" or "Wow  Friday 13/3/2026"
  const clubHeaderRe = /Wow\s+\w+\s+(\d{1,2})\/(\d{1,2})\/(\d{4})/i;
  // Entry: name with time range (supports both : and . in time)
  const entryRe = /(\w[\w .]*?)\s+(\d{1,2}(?:[:.]?\d{2})?[ap]m)-(\d{1,2}(?:[:.]?\d{2})?[ap]m)/i;

  const result = {};
  let currentDate = null;

  for (const line of lines) {
    // Try empire format first, then club format
    if (site.rosterFormat === 'empire') {
      const dayMatch = line.match(empireHeaderRe);
      if (dayMatch) {
        currentDate = resolveDate(parseInt(dayMatch[1], 10), dayMatch[2]);
        continue;
      }
    } else {
      const dayMatch = line.match(clubHeaderRe);
      if (dayMatch) {
        const day = parseInt(dayMatch[1], 10);
        const month = parseInt(dayMatch[2], 10);
        const year = parseInt(dayMatch[3], 10);
        currentDate = year + '-' + String(month).padStart(2, '0') + '-' + String(day).padStart(2, '0');
        continue;
      }
    }
    if (!currentDate) continue;

    const entryMatch = line.match(entryRe);
    if (entryMatch) {
      const rawName = entryMatch[1].trim();
      // Remove "Diamond Class", "Gold Class" etc.
      const cleanedName = rawName.replace(/\b\w+\s+Class\b/gi, '').trim();
      const nameParts = cleanedName.split(/\s+/);
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

/* ── Load existing JSON (or default) ── */

async function loadData(env, site) {
  try {
    const { content, sha } = await ghGet(env, site.jsonPath);
    return { data: content, sha };
  } catch {
    return {
      data: { girls: [], calendar: {}, lastGirlsSync: null, lastCalendarSync: null },
      sha: null,
    };
  }
}

/* ── Sync: Girls ── */

const MAX_NEW_PER_RUN = 50;

async function syncGirls(env, site) {
  const { data, sha } = await loadData(env, site);
  const existing = data.girls || [];
  const knownNames = new Set(existing.map(g => g.name));
  const knownUrls = new Set(existing.map(g => g.oldUrl).filter(Boolean));

  const cards = await scrapeGirlsListing(site);
  const activeNames = new Set(cards.map(c => c.name));

  // Update originalSite for all existing girls
  let siteChanged = false;
  for (const g of existing) {
    const shouldBe = activeNames.has(g.name) ? 'Exists' : '';
    if (g.originalSite !== shouldBe) {
      g.originalSite = shouldBe;
      siteChanged = true;
    }
  }

  const allNew = cards.filter(c => {
    const url = `${site.girlsUrl}/${c.id}`;
    return !knownNames.has(c.name) && !knownUrls.has(url);
  });

  if (allNew.length === 0) {
    // Still save if originalSite flags changed
    if (siteChanged) {
      data.girls = existing;
      data.lastGirlsSync = new Date().toISOString();
      await ghPut(env, site.jsonPath, data, sha,
        `[${site.name}] Update originalSite status`);
    }
    console.log(`[${site.name}] Girls sync: no new profiles`);
    return { added: 0, remaining: 0, names: [] };
  }

  const newCards = allNew.slice(0, MAX_NEW_PER_RUN);
  const remaining = allNew.length - newCards.length;

  console.log(`[${site.name}] Girls sync: ${allNew.length} new, processing ${newCards.length} (${remaining} remaining)`);
  const now = new Date().toISOString();
  const todayStr = now.split('T')[0];
  const addedNames = [];

  for (const card of newCards) {
    try {
      await new Promise(r => setTimeout(r, 1000));
      const profile = await scrapeGirlProfile(site, card.id);

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
      entry.startDate = profile.earliestUpload || todayStr;
      entry.lang = profile.profileLang || (card.country.length ? LANG_FROM_COUNTRY[card.country[0]] || '' : '');
      entry.oldUrl = `${site.girlsUrl}/${card.id}`;
      entry.type = profile.profileType || '';
      entry.desc = profile.desc || '';
      entry.originalSite = 'Exists';

      // Download & upload images
      const photos = [];
      for (let i = 0; i < profile.images.length; i++) {
        try {
          const ext = (profile.images[i].match(/\.(jpe?g|png|webp)$/i) || [])[1] || 'jpeg';
          const path = `${site.imgPrefix}/${card.name}/${card.name}_${i + 1}.${ext}`;
          const ghUrl = await uploadImage(env, profile.images[i], path);
          photos.push(ghUrl);
          await new Promise(r => setTimeout(r, 500));
        } catch (e) {
          console.error(`[${site.name}] Image error ${card.name} #${i + 1}: ${e.message}`);
        }
      }
      entry.photos = photos;
      entry.labels = extractLabels(profile.desc);
      entry.lastModified = now;

      for (const k of Object.keys(entry)) {
        if (entry[k] === undefined) delete entry[k];
      }

      existing.push(entry);
      addedNames.push(card.name);
      console.log(`[${site.name}] Added ${card.name} (${profile.images.length} photos)`);
    } catch (e) {
      console.error(`[${site.name}] Failed to process ${card.name}: ${e.message}`);
    }
  }

  if (addedNames.length > 0) {
    data.girls = existing;
    data.lastGirlsSync = now;
    await ghPut(env, site.jsonPath, data, sha,
      `[${site.name}] Auto-sync new girls: ${addedNames.join(', ')}`);
  }

  return { added: addedNames.length, remaining, names: addedNames };
}

/* ── Sync: Calendar ── */

async function syncCalendar(env, site) {
  const scraped = site.rosterFormat === 'kyoto206'
    ? await scrapeKyoto206Roster(site)
    : site.rosterFormat === 'top127'
    ? await scrapeTop127Roster(site)
    : site.rosterFormat === 'fantasyclub35'
    ? await scrapeFantasyClub35Roster(site)
    : await scrapeRoster(site);
  if (Object.keys(scraped).length === 0) {
    console.log(`[${site.name}] Roster scrape: no data found`);
    return false;
  }

  const { data, sha } = await loadData(env, site);
  const calendar = data.calendar || {};
  const validNames = new Set((data.girls || []).map(g => g.name));

  let changed = false;

  // Auto-create profiles for unmatched rostered names by scanning listing pages
  {
    const unmatchedNames = new Set();
    for (const entries of Object.values(scraped)) {
      for (const { name } of entries) {
        if (!validNames.has(name)) unmatchedNames.add(name);
      }
    }
    if (unmatchedNames.size > 0) {
      console.log(`[${site.name}] Unmatched roster names: ${[...unmatchedNames].join(', ')}. Scanning listing pages...`);

      if (site.siteType === 'wordpress') {
        // WordPress sites: scrape listing URLs, then check each profile page title
        const allUrls = await scrapeWpListing(site);
        const knownUrls = new Set((data.girls || []).map(g => g.oldUrl).filter(Boolean));
        const newUrls = allUrls.filter(u => !knownUrls.has(u));

        for (const pUrl of newUrls) {
          if (unmatchedNames.size === 0) break;
          try {
            await new Promise(r => setTimeout(r, 1000));
            const profile = await scrapeWpProfile(site, pUrl, null);
            const pName = profile.titleInfo.name;
            if (!pName || !unmatchedNames.has(pName)) continue;

            const now = new Date().toISOString();
            const entry = {
              name: pName,
              country: profile.titleInfo.country.length ? profile.titleInfo.country : undefined,
              age: profile.age || undefined, height: profile.height || undefined,
              cup: profile.cup || undefined, val1: profile.val1 || undefined,
              val2: profile.val2 || undefined, val3: profile.val3 || undefined,
              startDate: profile.earliestUpload || now.split('T')[0], oldUrl: pUrl,
              desc: '', lang: profile.titleInfo.country.length ? (LANG_FROM_COUNTRY[profile.titleInfo.country[0]] || '') : '',
              labels: [], originalSite: 'Exists', lastModified: now, lastRostered: '', photos: [],
            };
            for (let i = 0; i < profile.images.length; i++) {
              try {
                const ext = (profile.images[i].match(/\.(jpe?g|png|webp)$/i) || [])[1] || 'jpeg';
                const imgPath = `${site.imgPrefix}/${pName}/${pName}_${i + 1}.${ext}`;
                const ghUrl = await uploadImage(env, profile.images[i], imgPath);
                entry.photos.push(ghUrl);
                await new Promise(r => setTimeout(r, 500));
              } catch (e) { console.error(`[${site.name}] Image error ${pName}: ${e.message}`); }
            }
            for (const k of Object.keys(entry)) { if (entry[k] === undefined) delete entry[k]; }
            data.girls.push(entry);
            validNames.add(pName);
            unmatchedNames.delete(pName);
            changed = true;
            console.log(`[${site.name}] Auto-created from roster: ${pName} (${entry.photos.length} photos)`);
          } catch (e) { console.error(`[${site.name}] Failed scanning ${pUrl}: ${e.message}`); }
        }
      } else {
        // Ginza sites: scrape listing cards, match by name
        const cards = await scrapeGirlsListing(site);
        const knownNames = new Set((data.girls || []).map(g => g.name));

        for (const card of cards) {
          if (unmatchedNames.size === 0) break;
          if (!unmatchedNames.has(card.name)) continue;
          try {
            await new Promise(r => setTimeout(r, 1000));
            const profile = await scrapeGirlProfile(site, card.id);
            const now = new Date().toISOString();
            const todayStr = now.split('T')[0];
            const entry = {
              name: card.name,
              country: card.country.length ? card.country : undefined,
              age: card.age || undefined, body: card.body || undefined,
              height: card.height || profile.profileHeight || undefined,
              cup: card.cup || undefined,
              val1: profile.val1 || undefined, val2: profile.val2 || undefined, val3: profile.val3 || undefined,
            };
            if (card.special) entry.special = card.special;
            entry.exp = profile.profileExp || 'Inexperienced';
            entry.startDate = profile.earliestUpload || todayStr;
            entry.lang = profile.profileLang || (card.country.length ? LANG_FROM_COUNTRY[card.country[0]] || '' : '');
            entry.oldUrl = `${site.girlsUrl}/${card.id}`;
            entry.type = profile.profileType || '';
            entry.desc = profile.desc || '';
            entry.originalSite = 'Exists';
            const photos = [];
            for (let i = 0; i < profile.images.length; i++) {
              try {
                const ext = (profile.images[i].match(/\.(jpe?g|png|webp)$/i) || [])[1] || 'jpeg';
                const imgPath = `${site.imgPrefix}/${card.name}/${card.name}_${i + 1}.${ext}`;
                const ghUrl = await uploadImage(env, profile.images[i], imgPath);
                photos.push(ghUrl);
                await new Promise(r => setTimeout(r, 500));
              } catch (e) { console.error(`[${site.name}] Image error ${card.name}: ${e.message}`); }
            }
            entry.photos = photos;
            entry.labels = extractLabels(profile.desc);
            entry.lastModified = now;
            entry.lastRostered = '';
            for (const k of Object.keys(entry)) { if (entry[k] === undefined) delete entry[k]; }
            data.girls.push(entry);
            validNames.add(card.name);
            unmatchedNames.delete(card.name);
            changed = true;
            console.log(`[${site.name}] Auto-created from roster: ${card.name} (${photos.length} photos)`);
          } catch (e) { console.error(`[${site.name}] Failed creating ${card.name}: ${e.message}`); }
        }
      }

      if (unmatchedNames.size > 0) {
        console.log(`[${site.name}] Still unmatched: ${[...unmatchedNames].join(', ')}`);
      }
    }
  }

  const girlsByName = {};
  for (const g of (data.girls || [])) girlsByName[g.name] = g;

  for (const [dateStr, entries] of Object.entries(scraped)) {
    for (const { name, start, end } of entries) {
      if (!validNames.has(name)) continue;

      if (!calendar[name]) calendar[name] = {};

      const existing = calendar[name][dateStr];
      if (!existing || existing.start !== start || existing.end !== end) {
        calendar[name][dateStr] = { start, end };
        changed = true;
      }
    }
  }

  // Update lastRostered on each girl profile
  for (const [dateStr, entries] of Object.entries(scraped)) {
    for (const { name } of entries) {
      const girl = girlsByName[name];
      if (girl && (!girl.lastRostered || dateStr > girl.lastRostered)) {
        girl.lastRostered = dateStr;
        changed = true;
      }
    }
  }

  // Auto-publish scraped dates
  if (!Array.isArray(calendar._published)) calendar._published = [];
  for (const dateStr of Object.keys(scraped)) {
    if (!calendar._published.includes(dateStr)) {
      calendar._published.push(dateStr);
      changed = true;
    }
  }
  calendar._published.sort();

  // Prune dates older than 2 days (AEDT)
  const now2 = getAEDTDate();
  now2.setDate(now2.getDate() - 2);
  const cutoff = fmtDate(now2);

  for (const key of Object.keys(calendar)) {
    if (key.startsWith('_')) continue;
    const sched = calendar[key];
    if (typeof sched !== 'object') continue;
    for (const dateStr of Object.keys(sched)) {
      if (dateStr < cutoff) {
        delete sched[dateStr];
        changed = true;
      }
    }
  }

  const before = calendar._published.length;
  calendar._published = calendar._published.filter(d => d >= cutoff);
  if (calendar._published.length !== before) changed = true;

  if (!changed) {
    console.log(`[${site.name}] Calendar sync: no changes needed`);
    return true;
  }

  const now = new Date().toISOString();
  data.calendar = calendar;
  data.lastCalendarSync = now;

  await ghPut(env, site.jsonPath, data, sha,
    `[${site.name}] Auto-sync roster`);

  console.log(`[${site.name}] Calendar sync: updated`);
  return true;
}

/* ── Export ── */

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const json = h => new Response(JSON.stringify(h), { headers: { 'Content-Type': 'application/json' } });

    // Empire endpoints
    if (url.pathname === '/sync-girls' && request.method === 'POST') {
      try { return json(await syncGirls(env, SITES.empire)); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-calendar' && request.method === 'POST') {
      try { return json({ success: await syncCalendar(env, SITES.empire) }); }
      catch (e) { return json({ error: e.message }); }
    }

    // Club endpoints
    if (url.pathname === '/sync-club-girls' && request.method === 'POST') {
      try { return json(await syncGirls(env, SITES.club)); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-club-calendar' && request.method === 'POST') {
      try { return json({ success: await syncCalendar(env, SITES.club) }); }
      catch (e) { return json({ error: e.message }); }
    }

    // Kyoto 206 endpoints
    if (url.pathname === '/sync-kyoto206-girls' && request.method === 'POST') {
      try { return json(await syncWpGirls(env, SITES.kyoto206)); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-kyoto206-calendar' && request.method === 'POST') {
      try { return json({ success: await syncCalendar(env, SITES.kyoto206) }); }
      catch (e) { return json({ error: e.message }); }
    }

    // Sakura 57 endpoints
    if (url.pathname === '/sync-sakura57-girls' && request.method === 'POST') {
      try { return json(await syncWpGirls(env, SITES.sakura57)); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-sakura57-calendar' && request.method === 'POST') {
      try { return json({ success: await syncCalendar(env, SITES.sakura57) }); }
      catch (e) { return json({ error: e.message }); }
    }

    // Top 127 endpoints
    if (url.pathname === '/sync-top127-girls' && request.method === 'POST') {
      try { return json(await syncWpGirls(env, SITES.top127)); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-top127-calendar' && request.method === 'POST') {
      try { return json({ success: await syncCalendar(env, SITES.top127) }); }
      catch (e) { return json({ error: e.message }); }
    }

    // Fantasy Club 35 endpoints
    if (url.pathname === '/sync-fantasyclub35-girls' && request.method === 'POST') {
      try { return json(await syncWpGirls(env, SITES.fantasyclub35)); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-fantasyclub35-calendar' && request.method === 'POST') {
      try { return json({ success: await syncCalendar(env, SITES.fantasyclub35) }); }
      catch (e) { return json({ error: e.message }); }
    }

    return new Response('Not found', { status: 404 });
  },

  async scheduled(event, env, ctx) {
    const hour = new Date(event.scheduledTime).getUTCHours();

    // 8:00 UTC (7pm AEDT) or 20:00 UTC (7am AEDT) — sync girls first, then calendar
    if (hour === 8 || hour === 20) {
      ctx.waitUntil((async () => {
        // Step 1: Girls sync — all venues in parallel, each loops until no remaining
        async function syncAllGirls(fn, site) {
          let result;
          do {
            result = await fn(env, site).catch(e => { console.error(`[${site.name}] Girls sync error:`, e); return { remaining: 0 }; });
            console.log(`[${site.name}] Girls batch: added=${result.added || 0}, remaining=${result.remaining || 0}`);
          } while (result.remaining > 0);
        }

        await Promise.all([
          syncAllGirls(syncGirls, SITES.empire),
          syncAllGirls(syncGirls, SITES.club),
          syncAllGirls(syncWpGirls, SITES.kyoto206),
          syncAllGirls(syncWpGirls, SITES.sakura57),
          syncAllGirls(syncWpGirls, SITES.top127),
          syncAllGirls(syncWpGirls, SITES.fantasyclub35),
        ]);

        console.log('All girls syncs complete. Starting calendar syncs...');

        // Step 2: Calendar sync — all venues in parallel, runs after girls sync
        await Promise.all([
          syncCalendar(env, SITES.empire).catch(e => console.error('[Empire] Calendar sync error:', e)),
          syncCalendar(env, SITES.club).catch(e => console.error('[Club] Calendar sync error:', e)),
          syncCalendar(env, SITES.kyoto206).catch(e => console.error('[Kyoto 206] Calendar sync error:', e)),
          syncCalendar(env, SITES.sakura57).catch(e => console.error('[Sakura 57] Calendar sync error:', e)),
          syncCalendar(env, SITES.top127).catch(e => console.error('[Top 127] Calendar sync error:', e)),
          syncCalendar(env, SITES.fantasyclub35).catch(e => console.error('[Fantasy Club 35] Calendar sync error:', e)),
        ]);

        console.log('All calendar syncs complete.');
      })());
    }
  },
};
