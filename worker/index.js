/*
 * Cloudflare Worker — Brothel Search Data Sync
 *
 * Scrapes sites and maintains JSON files in the brothel-search repo:
 *   profiles/ginzaempire.json  — from 479ginza.com.au
 *   profiles/ginzaclub.json      — from www.ginzaclub.com.au
 *   profiles/kyoto206.json        — from citybrothel.com.au
 *
 * Cron schedule:
 *   8:00 UTC  (7pm AEDT) — sync girls + calendar (all sites)
 *   20:00 UTC (7am AEDT) — sync girls + calendar (all sites)
 *
 * Secrets required (set via Cloudflare dashboard or `wrangler secret put`):
 *   GITHUB_TOKEN — GitHub personal access token (contents read/write scope)
 */

/* ── Stripe webhook signature verification ── */
async function verifyStripeSignature(payload, sigHeader, secret) {
  const parts = Object.fromEntries(sigHeader.split(',').map(p => { const [k, v] = p.split('='); return [k, v]; }));
  const timestamp = parts.t;
  const signature = parts.v1;
  if (!timestamp || !signature) return false;
  const signedPayload = `${timestamp}.${payload}`;
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  const sig = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(signedPayload));
  const expected = Array.from(new Uint8Array(sig)).map(b => b.toString(16).padStart(2, '0')).join('');
  return expected === signature;
}

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
    jsonPath: 'profiles/ginzaempire.json',
    imgPrefix: 'profiles/ginzaempire',
    rosterFormat: 'empire', // "Happy Thursday 13th of March"
    embedPhotos: true,
  },
  club: {
    name: 'Ginza Club',
    baseUrl: 'https://www.ginzaclub.com.au',
    girlsUrl: 'https://www.ginzaclub.com.au/Girls',
    rosterUrl: 'https://www.ginzaclub.com.au/Roster',
    jsonPath: 'profiles/ginzaclub.json',
    imgPrefix: 'profiles/ginzaclub',
    rosterFormat: 'club', // "Wow Friday 13/3/2026"
    embedPhotos: true,
  },
  kyoto206: {
    name: 'Kyoto 206',
    baseUrl: 'https://citybrothel.com.au',
    girlsUrl: 'https://citybrothel.com.au/our-girls/',
    rosterUrl: 'https://citybrothel.com.au/girls-roster/',
    jsonPath: 'profiles/kyoto206.json',
    imgPrefix: 'profiles/kyoto206',
    siteType: 'wordpress',
    rosterFormat: 'kyoto206',
    embedPhotos: true,
  },
  sakura57: {
    name: 'Sakura 57',
    baseUrl: 'https://www.surryhillsbrothel.com.au',
    girlsUrl: 'https://www.surryhillsbrothel.com.au/our-girls/',
    rosterUrl: 'https://www.surryhillsbrothel.com.au/girls-roster/',
    jsonPath: 'profiles/sakura57.json',
    imgPrefix: 'profiles/sakura57',
    siteType: 'wordpress',
    rosterFormat: 'kyoto206',
    embedPhotos: true,
  },
  top127: {
    name: 'Top 127',
    baseUrl: 'https://127city.com',
    girlsUrl: 'https://127city.com/ladies/',
    rosterUrl: 'https://127city.com/',
    jsonPath: 'profiles/top127.json',
    imgPrefix: 'profiles/top127',
    siteType: 'wordpress',
    rosterFormat: 'top127',
    embedPhotos: true,
    listingRegex: '[cjktv]-[a-z][a-z0-9-]+/',
  },
  fantasyclub35: {
    name: 'Fantasy Club 35',
    baseUrl: 'https://fantasyclub35.com.au',
    girlsUrl: 'https://fantasyclub35.com.au/',
    rosterUrl: 'https://fantasyclub35.com.au/roster/',
    rosterFormat: 'fantasyclub35',
    jsonPath: 'profiles/fantasyclub35.json',
    imgPrefix: 'profiles/fantasyclub35',
    siteType: 'wordpress',
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
    rosterFormat: '429city',
    jsonPath: 'profiles/429city.json',
    imgPrefix: 'profiles/429city',
    siteType: 'wordpress',
    listingSelector: null,
    customListingUrls: ['https://www.429city.com/ladies/', 'https://www.429city.com/roster/'],
    excludeUrlPatterns: ['/ladies/', '/roster/', '/contact/', '/feed/', '/comments/', '/rate/', '/escort/', '/job/', '/page/', '/author/', '/wp-admin/', '/wp-login/'],
    pricingByCountry: {
      japanese: { val1: '210', val2: '260', val3: '320' },
      western: { val1: '230', val2: '280', val3: '350' },
      other: { val1: '170', val2: '240', val3: '300' },
    },
    embedPhotos: true,
  },
  pennys77: {
    name: "Penny's 77",
    baseUrl: 'https://pennys77.com.au',
    girlsUrl: 'https://pennys77.com.au/our-girls/',
    rosterUrl: 'https://pennys77.com.au/',
    rosterFormat: 'pennys77',
    jsonPath: 'profiles/pennys77.json',
    imgPrefix: 'profiles/pennys77',
    siteType: 'wordpress',
    pricingByCountry: {
      asian: { val1: '180', val2: '260', val3: '310' },
      other: { val1: '170', val2: '250', val3: '300' },
    },
    embedPhotos: true,
  },
  thegoldenapple: {
    name: 'The Golden Apple',
    baseUrl: 'https://www.thegoldenapple.com.au',
    girlsUrl: 'https://www.thegoldenapple.com.au/escorts/',
    rosterUrl: 'https://www.thegoldenapple.com.au/roster/',
    rosterFormat: 'thegoldenapple',
    jsonPath: 'profiles/thegoldenapple.json',
    imgPrefix: 'profiles/thegoldenapple',
    siteType: 'wordpress',
    defaultPricing: { val1: '260', val2: '330', val3: '400' },
    embedPhotos: true,
  },
  blackcatparlour: {
    name: 'Black Cat Parlour',
    baseUrl: 'https://blackcatparlour.com.au',
    girlsUrl: 'https://blackcatparlour.com.au/our-ladies/',
    rosterUrl: 'https://blackcatparlour.com.au/our-roster/',
    rosterFormat: 'blackcatparlour',
    jsonPath: 'profiles/blackcatparlour.json',
    imgPrefix: 'profiles/blackcatparlour',
    siteType: 'custom',
    defaultPricing: { val1: '260', val2: '330', val3: '400' },
    embedPhotos: true,
  },
  bellevue12: {
    name: 'Bellevue 12',
    baseUrl: 'https://bellevue12.com.au',
    girlsUrl: 'https://bellevue12.com.au/ladies/',
    rosterUrl: 'https://bellevue12.com.au/roster/',
    rosterFormat: 'bellevue12',
    jsonPath: 'profiles/bellevue12.json',
    imgPrefix: 'profiles/bellevue12',
    siteType: 'wordpress',
    defaultPricing: { val1: '80', val2: '130', val3: '160' },
    embedPhotos: true,
  },
  thegatewayclub: {
    name: 'The Gateway Club',
    baseUrl: 'https://www.gatewayclub.com.au',
    girlsUrl: 'https://www.gatewayclub.com.au/gateway-club-private-girls-sydney/',
    rosterUrl: 'https://www.gatewayclub.com.au/gateway-club-roster/',
    rosterFormat: 'gateway',
    jsonPath: 'profiles/thegatewayclub.json',
    imgPrefix: 'profiles/thegatewayclub',
    siteType: 'wordpress',
    embedPhotos: true,
  },
  marrickvillebrothel: {
    name: 'Marrickville Brothel',
    baseUrl: 'https://www.marrickvillebrothel.com',
    girlsUrl: 'https://www.marrickvillebrothel.com/ladies.php',
    rosterUrl: 'https://www.marrickvillebrothel.com/roster.php',
    rosterFormat: 'marrickville',
    jsonPath: 'profiles/marrickvillebrothel.json',
    imgPrefix: 'profiles/marrickvillebrothel',
    siteType: 'php',
    embedPhotos: true,
  },
  springhouse: {
    name: 'Spring House',
    baseUrl: 'https://46springhouse.com.au',
    girlsUrl: 'https://46springhouse.com.au/ladies/',
    rosterUrl: 'https://46springhouse.com.au/roster/',
    rosterFormat: 'springhouse',
    jsonPath: 'profiles/springhouse.json',
    imgPrefix: 'profiles/springhouse',
    siteType: 'wordpress',
    embedPhotos: true,
  },
  stiletto: {
    name: 'Stiletto',
    baseUrl: 'https://www.stilettosydney.com',
    girlsUrl: 'https://www.stilettosydney.com/ladies-of-stiletto/',
    rosterUrl: 'https://www.stilettosydney.com/wp-json/roster-manager/v1/availability/current',
    rosterFormat: 'stiletto-api',
    jsonPath: 'profiles/stiletto.json',
    imgPrefix: 'profiles/stiletto',
    siteType: 'wordpress',
    embedPhotos: true,
  },
  wivesonly: {
    name: 'Wives Only',
    baseUrl: 'https://wivesonly.com.au',
    girlsUrl: 'https://wivesonly.com.au/wives-only-ladies/',
    rosterUrl: 'https://wivesonly.com.au/ladies-roster/',
    rosterFormat: 'wivesonly',
    jsonPath: 'profiles/wivesonly.json',
    imgPrefix: 'profiles/wivesonly',
    siteType: 'wordpress',
    embedPhotos: true,
  },
  jinia: {
    name: 'Jinia',
    baseUrl: 'https://jinia.com.au',
    girlsUrl: 'https://jinia.com.au/strathfield-hooker/',
    rosterUrl: 'https://jinia.com.au/',
    rosterFormat: 'jinia',
    jsonPath: 'profiles/jinia.json',
    imgPrefix: 'profiles/jinia',
    siteType: 'wordpress',
    embedPhotos: true,
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

async function ghPutRaw(env, path, text, sha, message) {
  const body = { message, content: btoa(unescape(encodeURIComponent(text))) };
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

function ampmTo24(t) {
  if (!t) return '04:00';
  const s = t.trim().toLowerCase().replace(/\s/g, '');
  if (s === 'close' || s === 'late' || s === 'open') return '04:00';
  const m = s.match(/^(\d{1,2})(?::(\d{2}))?(am|pm)$/);
  if (!m) return '04:00';
  let h = parseInt(m[1]);
  const mins = m[2] || '00';
  if (m[3] === 'pm' && h !== 12) h += 12;
  if (m[3] === 'am' && h === 12) h = 0;
  return String(h).padStart(2, '0') + ':' + mins;
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
  Jav: 'Japanese', JAV: 'Japanese',
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

const LABEL_RENAMES = {
  '2 men at once': '2 Men', 'Anal ( with regular clientele )': 'Anal', 'Anal (On Discussion)': 'Anal',
  'Couples': 'Couple', 'Dinner Companion': 'Social Escort', 'Bondage (on you)': 'Bondage',
  'Dom & Sub Play': 'BDSM', 'Domination': 'BDSM', 'Doubles': 'Double',
  'Doubles ( with select ladies)': 'Double', 'Fisting (on you)': 'Fisting',
  'Girlfriend Experience': 'GFE', 'Greek': 'Anal', 'Heavy BDSM': 'BDSM',
  'Heavy BDSM upon request with regular clients': 'BDSM', 'Kissing (On Discussion)': 'DFK',
  'Kissing (on body)': 'DFK', 'Kissing': 'DFK', 'Lesbian Doubles': 'Double Lesbian',
  'Light BDSM': 'BDSM', 'Light Bondage': 'BDSM',
  'Multiple Positions': '', 'Massage': '', 'Mutual Oral': 'BBBJ,DATY',
  'Pornstar Experience': 'PSE', 'Remedial Massage': '', 'Rimming on me': 'Rimming',
  'Rimming on you': 'Rimming', 'Sakura Branch': '', 'Shots': '', 'Single Females': '',
  'Social outing to impress': 'Social Escort', 'Spanish': '', 'Sub play': 'BDSM',
  'Submission': 'BDSM', 'Tantric massage': '', 'Toy Play': 'Toys',
  'Anal play (on me)': 'Anal Play', 'Ball licking': '', 'ABDL': '',
  'Bondage': 'BDSM', 'Foot fetish': 'Foot Fetish',
};
function normalizeLabels(labels) {
  const out = [];
  for (const l of labels) {
    const renamed = LABEL_RENAMES[l] !== undefined ? LABEL_RENAMES[l] : l;
    if (!renamed) continue; // empty = remove
    for (const r of renamed.split(',')) {
      if (r && !out.includes(r)) out.push(r);
    }
  }
  return out;
}

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
  return normalizeLabels(LABEL_PATTERNS.filter(([, re]) => re.test(desc)).map(([label]) => label));
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

  let countryIdx = -1;
  for (let i = 0; i < words.length - 1; i++) {
    if (COUNTRY_PREFIX[words[i]]) {
      country = [COUNTRY_PREFIX[words[i]]];
      countryIdx = i;
    }
  }

  if (!country.length && words.length > 1) {
    const prefix = words.slice(0, -1).join(' ').toLowerCase();
    if (prefix.includes('japan')) { country = ['Japanese']; countryIdx = 0; }
    else if (prefix.includes('korea')) { country = ['Korean']; countryIdx = 0; }
    else if (prefix.includes('thai')) { country = ['Thai']; countryIdx = 0; }
    else if (prefix.includes('chin')) { country = ['Chinese']; countryIdx = 0; }
    else if (prefix.includes('vietnam')) { country = ['Vietnamese']; countryIdx = 0; }
    else if (prefix.includes('brazil')) { country = ['Brazilian']; countryIdx = 0; }
    else if (prefix.includes('malay')) { country = ['Malaysian']; countryIdx = 0; }
  }

  let name = countryIdx >= 0
    ? words.slice(countryIdx + 1).join(' ').replace(/\./g, '')
    : (words[words.length - 1] || '').replace(/\./g, '');
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
    const cupRaw = (cardHtml.match(/Cup Size:([^<]+)/) || [])[1] || '';
    const cup = cupRaw.trim().replace(/\s*[Cc]up\s*/, '').replace(/\s+/g, '').trim();
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
    if (/\.(jpe?g|png|webp|gif)$/i.test(src)) {
      images.push(site.baseUrl + src);
      const dm = src.match(/\/data\/upload\/(\d{4})-(\d{2})\//);
      if (dm) {
        const now = new Date();
        const uploadMonth = `${dm[1]}-${dm[2]}`;
        const currentMonth = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0');
        const d = uploadMonth === currentMonth ? fmtDate(now) : `${dm[1]}-${dm[2]}-01`;
        if (!earliestUpload || d < earliestUpload) earliestUpload = d;
      }
    }
  }

  let desc = '';
  const descPatterns = [
    /<div class="(?:about|description|text|info-text|detail)"[^>]*>([\s\S]*?)<\/div>/i,
    /<div class="row"><label>(?:Holder Description|Description|About|Info):<\/label>\s*([\s\S]*?)<\/div>/i,
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
    .replace(/\s*[–—|\-]\s*(?:Kyoto\s*206|Sakura\s*57|Top\s*127|Fantasy\s*Club\s*35|429\s*City|Penny'?s\s*77).*$/i, '')
    .replace(/\s*~.*$/, '') // Strip everything after first ~ (Top 127 format: "J Winnie ~ New ~ Diamond ~ ...")
    .trim();

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

  // Handle Top 127 prefix format: "C Winnie", "J Momoko", "HK Maggie", "C K Nana"
  let country = [];
  const top127Prefixes = { C: 'Chinese', J: 'Japanese', K: 'Korean', V: 'Vietnamese', HK: 'Hong Konger', T: 'Thai' };
  const prefixMatch = name.match(/^([A-Z]{1,2})\s+(?:([A-Z])\s+)?([A-Z][a-z]+.*)$/);
  if (prefixMatch) {
    const [, p1, p2, rest] = prefixMatch;
    if (top127Prefixes[p1]) {
      country.push(top127Prefixes[p1]);
      if (p2 && top127Prefixes[p2]) country.push(top127Prefixes[p2]);
      name = rest.trim();
    }
  }
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
  const linkRe = site.listingRegex
    ? new RegExp(`href="(https?://${domain}/${site.listingRegex})"`, 'gi')
    : new RegExp(`href="(https?://${domain}/${pathType}/[^"]+)"`, 'gi');
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
      if (!seen.has(url) && !(site.excludeUrls || []).some(pat => url.toLowerCase().includes(pat))) {
        seen.add(url); urls.push(url); found++;
      }
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

  const cupMatch = html.match(/([A-Z](?:DD)?)\s*[Cc]up/i) || html.match(/(?:Cup|Bust)\s*(?:Size)?\s*:?\s*([A-Z](?:DD)?)\b/i);
  const cup = cupMatch ? cupMatch[1].toUpperCase() : '';

  let val1 = '', val2 = '', val3 = '';
  const p30 = html.match(/30\s*min\w*:?\s*\$?\s*(\d+)/i);
  const p45 = html.match(/45\s*min\w*:?\s*\$?\s*(\d+)/i);
  const p60 = html.match(/60\s*min\w*:?\s*\$?\s*(\d+)/i);
  if (p30) val1 = p30[1];
  if (p45) val2 = p45[1];
  if (p60) val3 = p60[1];
  if (!val1) {
    const pb = html.match(/\$(\d+)\s*(?:\/|,|\s)\s*\$(\d+)\s*(?:\/|,|\s)\s*\$(\d+)/);
    if (pb) { val1 = pb[1]; val2 = pb[2]; val3 = pb[3]; }
  }

  // Images: prefer Elementor gallery hrefs (Fantasy Club 35 uses these)
  const galleryRe = /e-gallery-item[^>]*href="([^"]+)"/gi;
  const galleryImages = [];
  let im;
  while ((im = galleryRe.exec(html)) !== null) {
    if (/\.(?:jpe?g|png|webp|gif)$/i.test(im[1])) galleryImages.push(im[1]);
  }

  let images;
  if (galleryImages.length > 0) {
    images = galleryImages;
  } else {
    // Fallback: broad regex for other WP sites (Kyoto 206 etc.)
    const mainHtml = html.split(/avia-post-nav|avia-related-posts|In Portfolios|class="related-projects|id="portfolio-grid|class="post-navigation/i)[0] || html;
    const domain = new URL(site.baseUrl).hostname.replace(/\./g, '\\.');
    const imgRe = new RegExp(`(https?://${domain}/wp-content/uploads/[^\\s"']+\\.(?:jpe?g|png|webp|gif))`, 'gi');
    const dataSrcRe = new RegExp(`data-(?:src|lazy-src)="(https?://${domain}/wp-content/uploads/[^"]+\\.(?:jpe?g|png|webp|gif))"`, 'gi');
    const allImages = [];
    while ((im = imgRe.exec(mainHtml)) !== null) allImages.push(im[1]);
    while ((im = dataSrcRe.exec(mainHtml)) !== null) allImages.push(im[1]);

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
        if (fn.includes('logo') || fn.includes('qr') || fn.includes('微信') || fn.includes('二维码') || fn.includes('lllo') || fn.includes('new57') || fn.includes('d2a035bd')) return false;
        if (/-150x150\./.test(url) || /-160x160\./.test(url) || /-450x450\./.test(url) || /-80x80\./.test(url) || /-746x548\./.test(url) || /-300x300\./.test(url)) return false;
        const namePrefix = fn.match(/^([a-z]+)-/);
        if (namePrefix && portfolioNames.has(namePrefix[1]) && !nameVariants.includes(namePrefix[1])) return false;
        return true;
      });
    }

    // Group by base, prefer -scaled, then highest resolution
    const groups = {};
    for (const imgUrl of girlImgs) {
      const base = imgUrl.replace(/-scaled\.(jpe?g|png|webp|gif)$/i, '.$1')
                         .replace(/-\d+x\d+\.(jpe?g|png|webp|gif)$/i, '.$1');
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
      const now = new Date();
      const uploadMonth = `${dm[1]}-${dm[2]}`;
      const currentMonth = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0');
      const d = uploadMonth === currentMonth ? fmtDate(now) : `${dm[1]}-${dm[2]}-01`;
      if (!earliestUpload || d < earliestUpload) earliestUpload = d;
    }
  }

  return { titleInfo, age, height, cup, val1, val2, val3, images, earliestUpload };
}

/* ── Name validation ── */

function classifyGirl(entry) {
  const text = [entry.desc || '', entry.type || '', entry.exp || '', entry.special || '', entry.lang || '', entry.name || '', ...(entry.labels || [])].join(' ');
  // AV/Pornstar detection
  const avRe = /\b(pornstar|porn\s*star|porn\s*actress|AV\s*(actress|star|idol|debut|girl)|JAV|adult\s*(video|film))\b/i;
  entry.pornstar = avRe.test(text) ? 'Pornstar' : '';
  // English level
  const langText = ((entry.lang || '') + ' ' + (entry.desc || '')).toLowerCase();
  if (/no english/i.test(langText)) entry.englishLevel = 'No English';
  else if (/limited english|basic english/i.test(langText)) entry.englishLevel = 'Limited English';
  else if (/english/i.test(langText)) entry.englishLevel = 'English';
  else entry.englishLevel = '';
  // Experience level
  const expText = ((entry.exp || '') + ' ' + (entry.desc || '')).toLowerCase();
  if (/inexperienced|very new|brand new|first time|green apple/i.test(expText)) entry.experienceLevel = 'Inexperienced';
  else if (/experienced/i.test(expText)) entry.experienceLevel = 'Experienced';
  else entry.experienceLevel = '';
  // Country from name (JAV = Japanese)
  if (!entry.country || !entry.country.length) {
    if (/\bJAV\b/i.test(entry.name || '')) entry.country = ['Japanese'];
  }
}

function isValidGirlName(name) {
  if (!name) return false;
  // Reject names with special chars (|, &, !, =, etc.) — these are page titles, not real names
  if (/[|&!=<>{}[\]@#$%^*]/.test(name)) return false;
  // Reject names longer than 20 chars — real names are short
  if (name.length > 20) return false;
  // Reject names with more than 2 words
  if (name.trim().split(/\s+/).length > 2) return false;
  return true;
}

/* ── Sync: WordPress Girls ── */

async function syncWpGirls(env, site) {
  const { data, sha } = await loadData(env, site);
  const existing = data.girls || [];
  const skippedUrls = new Set(data._skippedUrls || []);
  const knownUrls = new Set(existing.map(g => g.oldUrl).filter(Boolean));

  const allUrls = await scrapeWpListing(site);
  const activeUrls = new Set(allUrls);

  // Update originalSite for existing girls
  let siteChanged = false;
  let updated = 0;
  const knownNames = new Set(existing.map(g => g.name));
  for (const g of existing) {
    const shouldBe = activeUrls.has(g.oldUrl) ? 'Exists' : '';
    if (g.originalSite !== shouldBe) {
      g.originalSite = shouldBe;
      siteChanged = true;
      updated++;
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
    return { added: 0, remaining: 0, names: [], updated };
  }

  const toProcess = newUrls.slice(0, MAX_NEW_PER_RUN);
  const remaining = newUrls.length - toProcess.length;
  console.log(`[${site.name}] Girls sync: ${newUrls.length} new, processing ${toProcess.length} (${remaining} remaining)`);

  const now = new Date().toISOString();
  const todayStr = now.split('T')[0];
  const addedNames = [];

  for (const profileUrl of toProcess) {
    try {
      await new Promise(r => setTimeout(r, 1000));
      const profile = await scrapeWpProfile(site, profileUrl, null);
      const { titleInfo } = profile;

      if (!titleInfo.name || knownNames.has(titleInfo.name) || !isValidGirlName(titleInfo.name)) {
        console.log(`[${site.name}] Skip ${profileUrl}: ${!titleInfo.name ? 'no name' : !isValidGirlName(titleInfo.name) ? 'invalid name: ' + titleInfo.name : 'duplicate'}`);
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

      // Photos: embed source URLs directly or upload to GitHub
      const photos = [];
      if (site.embedPhotos) {
        photos.push(...profile.images);
      } else {
        for (let i = 0; i < profile.images.length; i++) {
          try {
            const ext = (profile.images[i].match(/\.(jpe?g|png|webp|gif)$/i) || [])[1] || 'jpeg';
            const path = `${site.imgPrefix}/${name}/${name}_${i + 1}.${ext}`;
            const ghUrl = await uploadImage(env, profile.images[i], path);
            photos.push(ghUrl);
            await new Promise(r => setTimeout(r, 500));
          } catch (e) {
            console.error(`[${site.name}] Image error ${name} #${i + 1}: ${e.message}`);
          }
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

  return { added: addedNames.length, remaining, names: addedNames, updated };
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

  // Split by roster-date-title to get day sections
  const sections = html.split('roster-date-title');
  for (const section of sections) {
    // Parse date: ">Monday - APRIL 06<"
    const dateMatch = section.match(/>\s*\w+\s*-\s*(\w+)\s+(\d+)\s*</);
    if (!dateMatch) continue;
    const dateStr = resolveDate(parseInt(dateMatch[2], 10), dateMatch[1]);
    if (!dateStr) continue;

    // Extract all rows: col-name then col-time in same <tr>
    const rowRe = /col-name[^>]*>([^<]+)<\/td>.*?col-time[^>]*>([^<]+)/g;
    let rm;
    while ((rm = rowRe.exec(section)) !== null) {
      const name = rm[1].trim();
      const timeRaw = rm[2].trim();
      if (!name) continue;
      const timeParts = timeRaw.split(/\s*-\s*/);
      if (timeParts.length !== 2) continue;
      const start = parseKyoto206Time(timeParts[0]);
      const end = parseKyoto206Time(timeParts[1]);
      if (!start || !end) continue;
      if (!result[dateStr]) result[dateStr] = [];
      if (!result[dateStr].some(e => e.name === name)) {
        result[dateStr].push({ name, start, end });
      }
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

  // Extract names: "J Sana", "C Angela", "HK Maggie", "Chanel" etc.
  // Look for roster section names - patterns like prefix + Name with ~ separator
  const rosterSection = html.split(/ROSTER/i).pop() || html;
  const nameRe = /(?:^|\n|>)\s*(?:[A-Z]{1,3}\s+)?([A-Z][a-z]+)\s*(?:~|–)/gm;
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

  // Calculate dates: tab 1 = Monday of current week
  const aest = getAEDTDate();
  const currentDow = aest.getDay(); // 0=Sun
  const mondayOffset = currentDow === 0 ? -6 : 1 - currentDow;
  const monday = new Date(aest);
  monday.setDate(monday.getDate() + mondayOffset);

  // Tabs use kt-inner-tab-1 through kt-inner-tab-7 (Mon=1, Sun=7)
  const result = {};
  for (let tabNum = 1; tabNum <= 7; tabNum++) {
    const tabMarker = 'kt-inner-tab-' + tabNum;
    const tabStart = html.indexOf(tabMarker);
    if (tabStart === -1) continue;
    const nextTab = html.indexOf('kt-inner-tab-' + (tabNum + 1), tabStart);
    const section = html.substring(tabStart, nextTab > tabStart ? nextTab : tabStart + 10000);
    const text = section.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ');

    const d = new Date(monday);
    d.setDate(d.getDate() + (tabNum - 1));
    const dateStr = fmtDate(d);

    const entries = [];
    // Match: Name followed by (Country) then optional NEW then Xam-Xam
    // Use broad pattern to handle fullwidth/garbled parentheses
    const entryRe = /([A-Za-z]{2,})\s*\W+[A-Za-z]+\W+\s*(?:NEW\s+)?(\d{1,2}[ap]m)\s*-\s*(\d{1,2}[ap]m)/gi;
    let m;
    while ((m = entryRe.exec(text)) !== null) {
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

  console.log(`[Fantasy Club 35] Roster scraped: ${Object.keys(result).length} days, ${Object.values(result).reduce((s, e) => s + e.length, 0)} entries`);
  return result;
}

async function scrape429CityRoster(site) {
  const resp = await fetch(site.rosterUrl, { headers: { 'User-Agent': UA } });
  if (!resp.ok) throw new Error(`429 City roster fetch failed: ${resp.status}`);
  const html = await resp.text();

  // Get current week's dates starting from today (AEDT)
  const aest = getAEDTDate();
  const dayMap = { sun: 0, mon: 1, tue: 2, wed: 3, thur: 4, fri: 5, sat: 6 };
  const currentDayOfWeek = aest.getDay(); // 0=Sun

  const result = {};
  const excludePaths = ['ladies', 'roster', 'contact', 'rate', 'escort', 'work-for-us', 'wp-content', 'feed', 'comments', 'wp-includes', 'wp-json', 'xmlrpc', 'job'];

  // 429 City shows today's roster duplicated across all 7 day tabs
  // Only assign to today's date
  const todayStr = fmtDate(aest);

  {
    const dateStr = todayStr;

    // Extract girl profile links from the whole page (all tabs have same data)
    const re = /href=['"]?(https?:\/\/www\.429city\.com\/[a-z0-9%\-]+\/?)['"]?/gi;
    const links = new Set();
    let lm;
    while ((lm = re.exec(html)) !== null) {
      const url = lm[1].replace(/\/$/, '/');
      const path = url.replace('https://www.429city.com/', '').replace(/\/$/, '');
      if (path && !excludePaths.some(x => path.includes(x))) {
        links.add(url);
      }
    }

    if (links.size) {
      result[dateStr] = [];
      for (const url of links) {
        result[dateStr].push({ url, start: '10:00', end: '05:00' });
      }
    }
  }

  console.log(`[429 City] Roster scraped: ${Object.keys(result).length} days, ${Object.values(result).reduce((s, e) => s + e.length, 0)} entries`);
  return { _429cityUrls: true, ...result };
}

/* ── Custom girl sync for new venues ── */

async function syncPennys77Girls(env, site) {
  const { data, sha } = await loadData(env, site);
  const existing = data.girls || [];
  const existingUrls = new Set(existing.map(g => g.oldUrl));

  const BROWSER_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
  const urls = new Set();

  // Fetch all pages
  for (let page = 1; page <= 10; page++) {
    const pageUrl = page === 1 ? site.girlsUrl : site.girlsUrl + page + '/';
    const resp = await fetch(pageUrl, { headers: { 'User-Agent': BROWSER_UA } });
    if (!resp.ok) break;
    const html = await resp.text();
    const linkRe = /href="(https:\/\/pennys77\.com\.au\/[^"]+)"/gi;
    let m;
    while ((m = linkRe.exec(html)) !== null) {
      const url = m[1];
      if (!/our-girls|contact|rate|service|feed|wp-|about|faq|privacy|xmlrpc|wp-json|category|tag|page/i.test(url) && url !== 'https://pennys77.com.au/') {
        urls.add(url);
      }
    }
    // Stop if no next page link found
    if (!html.includes('our-girls/' + (page + 1) + '/')) break;
    await new Promise(r => setTimeout(r, 500));
  }

  const addedNames = [];
  const todayStr = fmtDate(getAEDTDate());

  for (const url of urls) {
    if (existingUrls.has(url)) continue;
    try {
      const pResp = await fetch(url, { headers: { 'User-Agent': BROWSER_UA } });
      if (!pResp.ok) continue;
      const pHtml = await pResp.text();

      // Extract from og:description: "Name: Nina Age : 25yo Nationality: Thai Boobs: C cup Height : 165cm"
      const ogDescMatch = pHtml.match(/property="og:description"\s+content="([^"]+)"/i);
      const metaText = ogDescMatch ? ogDescMatch[1].replace(/&#\d+;/g, ' ') : '';

      const nameMatch = metaText.match(/Name:\s*([A-Za-z]+)/i);
      const name = nameMatch ? nameMatch[1].trim() : '';
      if (!name || name.length < 2) continue;

      const ageMatch = metaText.match(/Age\s*:?\s*(\d+)/i);
      const heightMatch = metaText.match(/Height\s*:?\s*(\d+)\s*cm/i);
      const cupMatch = metaText.match(/Boobs:\s*([A-Z](?:DD)?)/i) || metaText.match(/([A-Z](?:DD)?)\s*cup/i);
      const natMatch = metaText.match(/Nationality:\s*[^\w]*([A-Za-z]+)/i);

      // Start date from article:published_time
      const pubMatch = pHtml.match(/article:published_time"\s+content="(\d{4}-\d{2}-\d{2})/);
      const startDate = pubMatch ? pubMatch[1] : todayStr;

      // Photos from wp-block-image src attributes (gallery images)
      const galleryRe = /wp-block-image[^>]*>.*?src="(https:\/\/pennys77\.com\.au\/wp-content\/uploads\/[^"]+)"/gi;
      const photos = [];
      let im;
      while ((im = galleryRe.exec(pHtml)) !== null) {
        const src = im[1];
        if (!/cropped|logo|icon|android|favicon|banner/i.test(src) && !/-\d+x\d+\./.test(src) && !photos.includes(src)) photos.push(src);
      }
      // Fallback: og:image
      if (!photos.length) {
        const ogImg = pHtml.match(/og:image"\s+content="(https:\/\/pennys77\.com\.au\/wp-content\/uploads\/[^"]+)"/i);
        if (ogImg) photos.push(ogImg[1]);
      }

      const countryMap = { thai: 'Thai', thailand: 'Thai', vietnamese: 'Vietnamese', vietnam: 'Vietnamese', chinese: 'Chinese', china: 'Chinese', japanese: 'Japanese', japan: 'Japanese', korean: 'Korean', korea: 'Korean', australian: 'Australian', aussie: 'Australian', polish: 'Polish', european: 'European', caucasian: 'Australian', indian: 'Indian' };
      const country = natMatch ? (countryMap[natMatch[1].toLowerCase()] || natMatch[1]) : '';

      const asianNats = ['Thai','Japanese','Chinese','Korean','Vietnamese','Taiwanese','Filipino','Malaysian','Indonesian','Singaporean','Cambodian','Indian','Hong Kong'];
      const isAsian = asianNats.includes(country);
      const pricing = isAsian ? (site.pricingByCountry?.asian || {}) : (site.pricingByCountry?.other || site.defaultPricing || {});
      const entry = {
        name, country: country ? [country] : [], age: ageMatch ? ageMatch[1] : '',
        height: heightMatch ? heightMatch[1] : '', cup: cupMatch ? cupMatch[1].toUpperCase() : '',
        body: '', val1: pricing.val1 || '', val2: pricing.val2 || '', val3: pricing.val3 || '',
        startDate, oldUrl: url, photos, labels: [], originalSite: 'Exists',
      };
      existing.push(entry);
      addedNames.push(name);
      await new Promise(r => setTimeout(r, 500));
    } catch (e) { console.error(`[Penny's 77] Error scraping ${url}:`, e.message); }
  }

  if (addedNames.length) {
    data.girls = existing;
    data.lastGirlsSync = new Date().toISOString();
    await ghPut(env, site.jsonPath, data, sha, `[Penny's 77] Auto-sync: ${addedNames.join(', ')}`);
  }
  return { added: addedNames.length, remaining: 0, names: addedNames };
}

async function syncBlackCatGirls(env, site) {
  const { data, sha } = await loadData(env, site);
  const existing = data.girls || [];
  const existingNames = new Set(existing.map(g => g.name));

  const resp = await fetch(site.girlsUrl, { headers: { 'User-Agent': UA } });
  if (!resp.ok) return { added: 0, remaining: 0, names: [] };
  const html = await resp.text();

  // Parse girl blocks: data comes before name in HTML
  // Pattern: Age: X ... Dress Size: X ... Hair: X ... Bust: X Cup ... Nationality: X ... src="thumb.php..." ... girl-name">Name
  const girlBlocks = html.split('class="girl"');
  const addedNames = [];
  const todayStr = fmtDate(getAEDTDate());

  for (const block of girlBlocks) {
    const nameMatch = block.match(/class="girl-name">([^<]+)/);
    if (!nameMatch) continue;
    const name = nameMatch[1].trim();
    if (existingNames.has(name)) continue;

    const ageMatch = block.match(/Age:\s*(\d+)/);
    const bustMatch = block.match(/Bust:\s*([A-Z](?:DD)?)\s*Cup/i);
    const natMatch = block.match(/Nationality:\s*([A-Za-z ]+)/);
    const dressMatch = block.match(/Dress Size:\s*(\d+)/);
    const heightMatch = block.match(/Height:\s*(\d+)&#039;(\d+)/);
    const imgMatch = block.match(/src="(https:\/\/blackcatparlour\.com\.au\/wp-content\/themes\/blackcatparlour\/thumb\.php\?[^"]+)"/);

    // Convert feet'inches to cm: 5'6 = 167cm
    let heightCm = '';
    if (heightMatch) {
      const feet = parseInt(heightMatch[1]);
      const inches = parseInt(heightMatch[2]);
      heightCm = String(Math.round(feet * 30.48 + inches * 2.54));
    }

    const country = natMatch ? natMatch[1].trim() : '';
    const photos = imgMatch ? [imgMatch[1]] : [];

    const dp = site.defaultPricing || {};
    const entry = {
      name, country: country ? [country] : [], age: ageMatch ? ageMatch[1] : '',
      height: heightCm, cup: bustMatch ? bustMatch[1].toUpperCase() : '', body: dressMatch ? dressMatch[1] : '',
      val1: dp.val1 || '', val2: dp.val2 || '', val3: dp.val3 || '',
      startDate: todayStr, oldUrl: site.girlsUrl, photos, labels: [], originalSite: 'Exists',
    };
    existing.push(entry);
    existingNames.add(name);
    addedNames.push(name);
  }

  if (addedNames.length) {
    data.girls = existing;
    data.lastGirlsSync = new Date().toISOString();
    await ghPut(env, site.jsonPath, data, sha, `[Black Cat] Auto-sync: ${addedNames.join(', ')}`);
  }
  return { added: addedNames.length, remaining: 0, names: addedNames };
}

async function syncBellevue12Girls(env, site) {
  const { data, sha } = await loadData(env, site);
  const existing = data.girls || [];
  const existingUrls = new Set(existing.map(g => g.oldUrl));

  const resp = await fetch(site.girlsUrl, { headers: { 'User-Agent': UA } });
  if (!resp.ok) return { added: 0, remaining: 0, names: [] };
  const html = await resp.text();

  // Extract profile URLs: /YYYY/MM/DD/name/
  const linkRe = /href="(https:\/\/bellevue12\.com\.au\/\d{4}\/\d{2}\/\d{2}\/[^"]+)"/gi;
  const urls = new Set();
  let m;
  while ((m = linkRe.exec(html)) !== null) urls.add(m[1]);

  const addedNames = [];
  const todayStr = fmtDate(getAEDTDate());

  for (const url of urls) {
    if (existingUrls.has(url)) continue;
    try {
      const pResp = await fetch(url, { headers: { 'User-Agent': UA } });
      if (!pResp.ok) continue;
      const pHtml = await pResp.text();

      // Name from URL slug
      const slugMatch = url.match(/\/([a-z0-9\-]+)\/?$/);
      let name = slugMatch ? slugMatch[1].replace(/-?\d+$/, '').replace(/-/g, ' ') : '';
      name = name.charAt(0).toUpperCase() + name.slice(1);
      if (!name || name.length < 2) continue;

      // Photos
      const imgRe = /wp-content\/uploads\/[^"'\s]+\.(?:jpe?g|png|webp)/gi;
      const photos = [];
      let im;
      while ((im = imgRe.exec(pHtml)) !== null) {
        const src = 'https://bellevue12.com.au/' + im[0];
        if (!/slider|slash_it|logo|icon|cropped|banner/i.test(src) && !/-\d{2,3}x\d{2,3}\./.test(src) && !/1500x430|1210x423/.test(src) && !photos.includes(src)) photos.push(src);
      }

      // Try to get details from h3 tags or content
      const textContent = pHtml.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ');
      const cupMatch = textContent.match(/([A-Z])\s*(?:cup|cap)/i);
      const heightMatch = textContent.match(/(1[4-8]\d)\s*cm/i);
      const ageMatch = textContent.match(/(\d{2})\s*(?:yo|years? old|y\.o)/i) || textContent.match(/age\s*:?\s*(\d{2})/i);
      const countryMap = { singapore: 'Singaporean', singaporean: 'Singaporean', china: 'Chinese', chinese: 'Chinese', taiwan: 'Taiwanese', taiwanese: 'Taiwanese', thailand: 'Thai', thai: 'Thai', japan: 'Japanese', japanese: 'Japanese', korea: 'Korean', korean: 'Korean', vietnam: 'Vietnamese', vietnamese: 'Vietnamese', hongkong: 'Hong Kong', 'hong kong': 'Hong Kong', malaysia: 'Malaysian', malaysian: 'Malaysian', indonesia: 'Indonesian', indonesian: 'Indonesian' };
      let country = '';
      for (const [key, val] of Object.entries(countryMap)) {
        if (textContent.toLowerCase().includes(key)) { country = val; break; }
      }

      // Extract start date from URL: /YYYY/MM/DD/
      const dateMatch = url.match(/\/(\d{4})\/(\d{2})\/(\d{2})\//);
      const startDate = dateMatch ? dateMatch[1] + '-' + dateMatch[2] + '-' + dateMatch[3] : todayStr;

      const dp = site.defaultPricing || {};
      const entry = {
        name, country: country ? [country] : [], age: ageMatch ? ageMatch[1] : '', height: heightMatch ? heightMatch[1] : '',
        cup: cupMatch ? cupMatch[1].toUpperCase() : '', body: '',
        val1: dp.val1 || '', val2: dp.val2 || '', val3: dp.val3 || '',
        startDate, oldUrl: url, photos, labels: [], originalSite: 'Exists',
      };
      existing.push(entry);
      addedNames.push(name);
      await new Promise(r => setTimeout(r, 500));
    } catch (e) { console.error(`[Bellevue 12] Error scraping ${url}:`, e.message); }
  }

  if (addedNames.length) {
    data.girls = existing;
    data.lastGirlsSync = new Date().toISOString();
    await ghPut(env, site.jsonPath, data, sha, `[Bellevue 12] Auto-sync: ${addedNames.join(', ')}`);
  }
  return { added: addedNames.length, remaining: 0, names: addedNames };
}

/* ── The Gateway Club custom scraper (WAF-protected, may 403) ── */
async function syncGatewayClubGirls(env, site) {
  // Gateway Club WAF blocks browser UA but allows default/bot UA
  const { data, sha } = await loadData(env, site);
  const existing = data.girls || [];
  const existingNames = new Set(existing.map(g => g.name));
  const existingByUrl = {};
  for (const g of existing) { if (g.oldUrl) existingByUrl[g.oldUrl] = g; }

  const resp = await fetch(site.girlsUrl, { headers: { 'User-Agent': UA } });
  if (!resp.ok) return { added: 0, remaining: 0, names: [] };
  const html = await resp.text();
  if (html.includes('403 - Forbidden')) return { added: 0, remaining: 0, names: [] };

  // Collect new profiles from listing page
  const blocks = html.split('sl_col_glry');
  const newProfiles = [];
  const addedNames = [];
  let siteChanged = false;
  const todayStr = fmtDate(getAEDTDate());
  const countryMap = { aussie: 'Australian', australian: 'Australian', singaporean: 'Singaporean', chinese: 'Chinese', thai: 'Thai', japanese: 'Japanese', korean: 'Korean', vietnamese: 'Vietnamese', brazilian: 'Brazilian', kiwi: 'New Zealander', indian: 'Indian', european: 'European', filipina: 'Filipino', indonesian: 'Indonesian', persian: 'Persian', colombian: 'Colombian' };

  for (const block of blocks) {
    const nameMatch = block.match(/<h5>([^<]+)<\/h5>/);
    if (!nameMatch) continue;
    let name = nameMatch[1].trim();
    if (!name || name === 'SYDNEY LADIES') continue;
    name = name.charAt(0).toUpperCase() + name.slice(1).toLowerCase();
    if (!isValidGirlName(name)) continue;

    const urlMatch = block.match(/href="(https:\/\/www\.gatewayclub\.com\.au\/ladies\/[^"]+)"/);
    // Handle renames — URL exists with different name
    if (urlMatch && existingByUrl[urlMatch[1]] && existingByUrl[urlMatch[1]].name !== name) {
      const g = existingByUrl[urlMatch[1]];
      const oldName = g.name;
      if (data.calendar && data.calendar[oldName]) { data.calendar[name] = data.calendar[oldName]; delete data.calendar[oldName]; }
      existingNames.delete(oldName); existingNames.add(name);
      g.name = name;
      addedNames.push(name + ' (renamed from ' + oldName + ')');
      siteChanged = true;
      console.log(`[Gateway Club] Renamed: ${oldName} -> ${name}`);
      continue;
    }
    if (existingNames.has(name)) continue;
    if (urlMatch && existingByUrl[urlMatch[1]]) continue;
    const ageMatch = block.match(/Age:<\/td><td>(\d+)/);
    const bustMatch = block.match(/Bust:<\/td><td>([A-Z](?:DD)?)/);
    const heightMatch = block.match(/Height:<\/td><td>(\d+)cm/);
    const dressMatch = block.match(/Dress [Ss]ize:<\/td><td>([\d\-]+)/);
    const descText = block.replace(/<[^>]+>/g, ' ').toLowerCase();
    let country = '';
    for (const [key, val] of Object.entries(countryMap)) { if (descText.includes(key)) { country = val; break; } }

    newProfiles.push({ name, profileUrl: urlMatch ? urlMatch[1] : '', age: ageMatch ? ageMatch[1] : '', height: heightMatch ? heightMatch[1] : '', cup: bustMatch ? bustMatch[1] : '', body: dressMatch ? dressMatch[1] : '', country });
  }

  function parseGatewayProfile(pHtml) {
    // Photos from slides: <li><img src="URL" ...></li> — skip logo/telegram
    const photos = [];
    const photoSet = new Set();
    const photoRe = /src="(https:\/\/www\.gatewayclub\.com\.au\/wp-content\/uploads\/[^"]+)"/gi;
    let pm;
    while ((pm = photoRe.exec(pHtml)) !== null) {
      let src = pm[1];
      if (/logo|telegram|250x250|right\.png/i.test(src)) continue;
      src = src.replace(/-\d+x\d+(\.\w+)$/, '$1');
      if (!photoSet.has(src)) { photoSet.add(src); photos.push(src); }
    }
    // Services: <h4>SERVICES</h4>...</div><div class="cl_2"><ul><li>...</li></ul>
    const svcBlock = pHtml.match(/SERVICES<\/h4>[\s\S]*?<ul>([\s\S]*?)<\/ul>/i);
    const labels = normalizeLabels(svcBlock ? [...svcBlock[1].matchAll(/<li>([^<]+)<\/li>/gi)].map(m => m[1].trim()).filter(Boolean) : []);
    // Description from og:description or MEET section
    const ogDesc = pHtml.match(/og:description"\s+content="([^"]+)"/i);
    const desc = ogDesc ? ogDesc[1].replace(/&#8217;/g, "'").replace(/&#8220;/g, '"').replace(/&#8221;/g, '"').replace(/&hellip;/g, '...').replace(/&amp;/g, '&').trim() : '';
    // Availability table: row1=day names, row2=Day Time/Night Time/NA
    const roster = {};
    const availMatch = pHtml.match(/AVAILABILITY[\s\S]*?<tbody>([\s\S]*?)<\/tbody>/i);
    if (availMatch) {
      const rows = [...availMatch[1].matchAll(/<tr>([\s\S]*?)<\/tr>/gi)];
      if (rows.length >= 2) {
        const days = [...rows[0][1].matchAll(/<td>([^<]+)<\/td>/gi)].map(m => m[1].trim());
        const avails = [...rows[1][1].matchAll(/<td>([\s\S]*?)<\/td>/gi)].map(m => m[1].replace(/<br\s*\/?>/g, ' ').replace(/<[^>]*>/g, '').trim());
        const DAY_MAP = { sunday: 0, monday: 1, tuesday: 2, wednesday: 3, thursday: 4, thursday: 4, friday: 5, saturday: 6 };
        const today = getAEDTDate();
        for (let i = 0; i < days.length && i < avails.length; i++) {
          const avail = avails[i].toLowerCase();
          if (avail === 'na' || !avail) continue;
          const dow = DAY_MAP[days[i].toLowerCase()];
          if (dow === undefined) continue;
          // Find the date for this day of week (today + offset)
          let offset = dow - today.getDay();
          if (offset < 0) offset += 7;
          const d = new Date(today); d.setDate(today.getDate() + offset);
          const dateStr = fmtDate(d);
          // Gateway Club shifts:
          // Day Time (Morning GIRLS) = 6am-12pm
          // Afternoon Time (Afternoon GIRLS) = 12pm-6pm
          // Night Time (Night GIRLS) = 6pm-6am
          // Day Time + Night Time = 6am-6am (full day)
          const hasDay = avail.includes('day time');
          const hasNight = avail.includes('night time');
          if (hasDay && hasNight) {
            roster[dateStr] = { start: '06:00', end: '06:00' }; // full day
          } else if (hasDay) {
            roster[dateStr] = { start: '06:00', end: '12:00' }; // morning
          } else if (hasNight) {
            roster[dateStr] = { start: '18:00', end: '06:00' }; // night
          }
        }
      }
    }

    return { photos, labels, desc, roster };
  }

  // Fetch profile pages for new girls (batch of 15)
  const BATCH = Math.min(15, MAX_NEW_PER_RUN);
  const toProcess = newProfiles.slice(0, BATCH);

  for (const p of toProcess) {
    let photos = [], labels = [], desc = '', roster = {};
    if (p.profileUrl) {
      try {
        await new Promise(r => setTimeout(r, 500));
        const pResp = await fetch(p.profileUrl, { headers: { 'User-Agent': UA } });
        if (pResp.ok) {
          const parsed = parseGatewayProfile(await pResp.text());
          photos = parsed.photos;
          labels = parsed.labels;
          desc = parsed.desc;
          roster = parsed.roster || {};
          if (Object.keys(roster).length) {
            if (!data.calendar) data.calendar = {};
            if (!data.calendar[p.name]) data.calendar[p.name] = {};
            Object.assign(data.calendar[p.name], roster);
          }
        }
      } catch (e) { console.error(`[Gateway Club] Error fetching ${p.name}:`, e.message); }
    }
    const startDate = (() => { for (const ph of photos) { const m = ph.match(/\/uploads\/(\d{4})\/(\d{2})\//); if (m) return m[1] + '-' + m[2] + '-01'; } return '2026-01-01'; })();
    const entry = {
      name: p.name, country: p.country ? [p.country] : [], age: p.age, height: p.height,
      cup: p.cup, body: p.body, desc,
      val1: '260', val2: '320', val3: '400',
      startDate, lastRostered: startDate, oldUrl: p.profileUrl || site.girlsUrl,
      photos, labels, originalSite: 'Exists',
    };
    existing.push(entry);
    existingNames.add(p.name);
    addedNames.push(p.name);
  }

  // Backfill existing girls missing photos/labels/desc (batch remaining)
  const needBackfill = existing.filter(g => (!g.labels || !g.labels.length || !g.desc || g.photos.length <= 1) && g.oldUrl && g.oldUrl.includes('gatewayclub.com.au/ladies/'));
  const backfillBatch = needBackfill.slice(0, Math.max(0, BATCH - toProcess.length));
  for (const g of backfillBatch) {
    try {
      await new Promise(r => setTimeout(r, 500));
      const pResp = await fetch(g.oldUrl, { headers: { 'User-Agent': UA } });
      if (pResp.ok) {
        const parsed = parseGatewayProfile(await pResp.text());
        let updated = false;
        if (parsed.photos.length > g.photos.length) { g.photos = parsed.photos; updated = true; }
        if ((!g.labels || !g.labels.length) && parsed.labels.length) { g.labels = parsed.labels; updated = true; }
        if (!g.desc && parsed.desc) { g.desc = parsed.desc; updated = true; }
        // Roster from availability table
        if (parsed.roster && Object.keys(parsed.roster).length) {
          if (!data.calendar) data.calendar = {};
          if (!data.calendar[g.name]) data.calendar[g.name] = {};
          Object.assign(data.calendar[g.name], parsed.roster);
          updated = true;
        }
        // Update startDate from photos if still default
        if (g.startDate === '2026-01-01' || !g.startDate) {
          for (const ph of g.photos) { const m = ph.match(/\/uploads\/(\d{4})\/(\d{2})\//); if (m) { g.startDate = m[1] + '-' + m[2] + '-01'; g.lastRostered = g.startDate; break; } }
        }
        if (updated) addedNames.push(g.name + ' (details)');
      }
    } catch (e) {}
  }

  // Second pass: backfill roster for girls who have profile data but no calendar yet
  if (!data.calendar) data.calendar = {};
  const calNames = new Set(Object.keys(data.calendar).filter(k => !k.startsWith('_')));
  const needRoster = existing.filter(g => !calNames.has(g.name) && g.oldUrl && g.oldUrl.includes('gatewayclub.com.au/ladies/'));
  const rosterBatch = needRoster.slice(0, 10);
  for (const g of rosterBatch) {
    try {
      await new Promise(r => setTimeout(r, 500));
      const pResp = await fetch(g.oldUrl, { headers: { 'User-Agent': UA } });
      if (pResp.ok) {
        const parsed = parseGatewayProfile(await pResp.text());
        if (parsed.roster && Object.keys(parsed.roster).length) {
          if (!data.calendar[g.name]) data.calendar[g.name] = {};
          Object.assign(data.calendar[g.name], parsed.roster);
          addedNames.push(g.name + ' (roster)');
        } else {
          // Mark as checked with empty calendar so we don't re-check
          data.calendar[g.name] = {};
          addedNames.push(g.name + ' (no roster)');
        }
      }
    } catch (e) {}
  }

  if (addedNames.length) {
    data.girls = existing;
    data.lastGirlsSync = new Date().toISOString();
    await ghPut(env, site.jsonPath, data, sha, `[Gateway Club] Auto-sync: ${addedNames.join(', ')}`);
  }
  return { added: addedNames.length, remaining: newProfiles.length - toProcess.length, names: addedNames };
}

/* ── Marrickville Brothel custom scraper (plain PHP site) ── */
async function syncMarrickvilleBrothelGirls(env, site) {
  const BROWSER_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
  const { data, sha } = await loadData(env, site);
  const existing = data.girls || [];
  const existingNames = new Set(existing.map(g => g.name));

  const resp = await fetch(site.girlsUrl, { headers: { 'User-Agent': BROWSER_UA } });
  if (!resp.ok) return { added: 0, remaining: 0, names: [] };
  const html = await resp.text();

  // Parse: <h5><a href="page.php?ID=N"><img src="/data/files/X.jpg" height="260">\n<span class="blue">Name</span></a></h5>
  const liRe = /<h5><a href="page\.php\?ID=(\d+)"><img src="([^"]+)"[^>]*>\s*<span class="blue">([^<]+)<\/span>/gi;
  const addedNames = [];
  const todayStr = fmtDate(getAEDTDate());
  let m;

  const countryMap = { chinese: 'Chinese', thai: 'Thai', japanese: 'Japanese', korean: 'Korean', vietnamese: 'Vietnamese', malaysian: 'Malaysian', singaporean: 'Singaporean', indonesian: 'Indonesian', taiwanese: 'Taiwanese', australian: 'Australian', indian: 'Indian', filipina: 'Filipino', 'hong kong': 'Hong Kong' };

  while ((m = liRe.exec(html)) !== null) {
    const [, id, imgPath, rawName] = m;
    const name = rawName.trim();
    if (!name || existingNames.has(name) || !isValidGirlName(name)) continue;

    const photo = imgPath.startsWith('http') ? imgPath : site.baseUrl + imgPath;
    const profileUrl = site.baseUrl + '/page.php?ID=' + id;

    // Fetch profile page for details
    let age = '', height = '', cup = '', country = '';
    try {
      await new Promise(r => setTimeout(r, 500));
      const pResp = await fetch(profileUrl, { headers: { 'User-Agent': BROWSER_UA } });
      if (pResp.ok) {
        const pHtml = await pResp.text().then(t => t.replace(/&nbsp;/g, ' ').replace(/\s+/g, ' '));
        const ageMatch = pHtml.match(/Age:\s*(\d+)/i);
        const heightMatch = pHtml.match(/Height:\s*(\d+)\s*cm/i);
        const cupMatch = pHtml.match(/(?:Cup|Bust):\s*([A-Z])/i);
        const raceMatch = pHtml.match(/Race:\s*([A-Za-z ]+)/i);
        age = ageMatch ? ageMatch[1] : '';
        height = heightMatch ? heightMatch[1] : '';
        cup = cupMatch ? cupMatch[1].toUpperCase() : '';
        if (raceMatch) {
          const raw = raceMatch[1].trim().toLowerCase();
          country = countryMap[raw] || (raw.charAt(0).toUpperCase() + raw.slice(1));
        }
      }
    } catch (e) { console.error(`[Marrickville] Error scraping ${profileUrl}:`, e.message); }

    const entry = {
      name, country: country ? [country] : [], age, height, cup, body: '',
      val1: '70', val2: '100', val3: '130',
      startDate: todayStr, lastRostered: todayStr, oldUrl: profileUrl,
      photos: [photo], labels: [], originalSite: 'Exists',
    };
    existing.push(entry);
    existingNames.add(name);
    addedNames.push(name);
  }

  if (addedNames.length) {
    data.girls = existing;
    data.lastGirlsSync = new Date().toISOString();
    await ghPut(env, site.jsonPath, data, sha, `[Marrickville Brothel] Auto-sync: ${addedNames.join(', ')}`);
  }
  return { added: addedNames.length, remaining: 0, names: addedNames };
}

/* ── Spring House custom scraper (WordPress, structured listing) ── */
async function syncSpringHouseGirls(env, site) {
  const { data, sha } = await loadData(env, site);
  const existing = data.girls || [];
  const existingNames = new Set(existing.map(g => g.name));

  const resp = await fetch(site.girlsUrl, { headers: { 'User-Agent': UA } });
  if (!resp.ok) return { added: 0, remaining: 0, names: [] };
  const html = await resp.text();

  // Parse girl blocks: <a href="URL" class="item ..."><div class="girl"><img src="PHOTO">...<span class="age roundBorder">Age: N</span><span class="nationality roundBorder">X</span>...<h3 class="name">Name</h3>
  const blocks = html.split('class="item col-');
  const addedNames = [];
  const todayStr = fmtDate(getAEDTDate());
  const countryMap = { china: 'Chinese', chinese: 'Chinese', singapore: 'Singaporean', singaporean: 'Singaporean', thai: 'Thai', thailand: 'Thai', japan: 'Japanese', japanese: 'Japanese', korea: 'Korean', korean: 'Korean', vietnam: 'Vietnamese', vietnamese: 'Vietnamese', taiwan: 'Taiwanese', taiwanese: 'Taiwanese', malaysia: 'Malaysian', malaysian: 'Malaysian', indonesia: 'Indonesian', indonesian: 'Indonesian', 'hong kong': 'Hong Kong' };

  // Collect profile URLs from listing
  const profileUrls = [];
  for (const block of blocks) {
    const nameMatch = block.match(/class="name">([^<]+)/);
    if (!nameMatch) continue;
    const name = nameMatch[1].trim();
    if (!name || existingNames.has(name) || !isValidGirlName(name)) continue;
    const urlMatch = block.match(/href="(https:\/\/46springhouse\.com\.au\/[^"]+)"/);
    if (urlMatch) profileUrls.push({ name, url: urlMatch[1] });
  }

  const BATCH = Math.min(20, MAX_NEW_PER_RUN);
  const toProcess = profileUrls.slice(0, BATCH);

  for (const { name, url: profileUrl } of toProcess) {
    try {
      await new Promise(r => setTimeout(r, 500));
      const pResp = await fetch(profileUrl, { headers: { 'User-Agent': UA } });
      if (!pResp.ok) continue;
      const pHtml = await pResp.text();

      // Parse profile page: <h3>Age: 24</h3> <h3>From: china</h3>
      const ageMatch = pHtml.match(/<h3>Age:\s*(\d+)/i);
      const fromMatch = pHtml.match(/<h3>From:\s*([^<]+)/i);
      let country = '';
      if (fromMatch) {
        const raw = fromMatch[1].trim().toLowerCase();
        country = countryMap[raw] || (raw.charAt(0).toUpperCase() + raw.slice(1));
      }

      // Photos: banner image + singleImage img + acf-gallery images
      const photos = [];
      const photoSet = new Set();
      const photoRe = /(?:src|data-imageUrl)="(https:\/\/46springhouse\.com\.au\/wp-content\/uploads\/[^"]+)"/gi;
      let pm;
      while ((pm = photoRe.exec(pHtml)) !== null) {
        let src = pm[1];
        if (/logo|icon|banner/i.test(src)) continue;
        src = src.replace(/-\d+x\d+(\.\w+)$/, '$1');
        if (!photoSet.has(src)) { photoSet.add(src); photos.push(src); }
      }

      // Description: <p> after <hr> that follows the schedule h4 tags
      const descMatch = pHtml.match(/<\/h4>[^<]*<hr>[^<]*<p>([^<]+)/i);
      const desc = descMatch ? descMatch[1].replace(/<[^>]+>/g, '').replace(/&#8217;/g, "'").replace(/&#8230;/g, '...').replace(/&amp;/g, '&').replace(/\s+/g, ' ').trim() : '';

      const entry = {
        name, country: country ? [country] : [], age: ageMatch ? ageMatch[1] : '',
        height: '', cup: '', body: '', desc,
        val1: '110', val2: '140', val3: '170',
        startDate: todayStr, lastRostered: todayStr, oldUrl: profileUrl,
        photos, labels: [], originalSite: 'Exists',
      };
      existing.push(entry);
      existingNames.add(name);
      addedNames.push(name);
    } catch (e) { console.error(`[Spring House] Error scraping ${profileUrl}:`, e.message); }
  }

  if (addedNames.length) {
    data.girls = existing;
    data.lastGirlsSync = new Date().toISOString();
    await ghPut(env, site.jsonPath, data, sha, `[Spring House] Auto-sync: ${addedNames.join(', ')}`);
  }
  return { added: addedNames.length, remaining: 0, names: addedNames };
}

/* ── Stiletto custom scraper (WordPress, data attributes + REST API roster) ── */
async function syncStilettoGirls(env, site) {
  const { data, sha } = await loadData(env, site);
  const existing = data.girls || [];
  const existingNames = new Set(existing.map(g => g.name));
  const existingByUrl = {};
  for (const g of existing) { if (g.oldUrl) existingByUrl[g.oldUrl] = g; }

  const resp = await fetch('https://www.stilettosydney.com/ladies-of-stiletto/', { headers: { 'User-Agent': UA } });
  if (!resp.ok) return { added: 0, remaining: 0, names: [] };
  const html = await resp.text();

  // Parse listing tiles for new girls, then fetch profile pages for age/details
  const tileRe = /<div class="worker-tile post-\d+"([^>]*)>\s*<a href="([^"]+)" class="worker-tile-img-container" title="([^"]+)">\s*<div class="worker-tile-img" style="background-image: url\('([^']+)'\)"/g;
  const addedNames = [];
  const todayStr = fmtDate(getAEDTDate());
  const newProfiles = [];
  let m;

  while ((m = tileRe.exec(html)) !== null) {
    const [, attrs, profileUrl, rawTitle, photoUrl] = m;
    let name = rawTitle.replace(/\s*\*?New\*?\s*/gi, '').trim();
    if (!name || !isValidGirlName(name)) continue;
    // Check if URL already exists (handle renames)
    if (existingByUrl[profileUrl]) {
      const g = existingByUrl[profileUrl];
      if (g.name !== name) {
        const oldName = g.name;
        if (data.calendar && data.calendar[oldName]) { data.calendar[name] = data.calendar[oldName]; delete data.calendar[oldName]; }
        existingNames.delete(oldName); existingNames.add(name);
        g.name = name;
        addedNames.push(name + ' (renamed from ' + oldName + ')');
        console.log(`[Stiletto] Renamed: ${oldName} -> ${name}`);
      }
      continue;
    }
    if (existingNames.has(name)) continue;

    const natMatch = attrs.match(/data-nationality='([^']+)'/);
    const bustSizeMatch = attrs.match(/data-bust-size='([^']+)'/);
    const servicesMatch = attrs.match(/data-services='([^']+)'/);
    const nat = natMatch ? natMatch[1].trim() : '';
    const cup = bustSizeMatch ? bustSizeMatch[1].replace(/\+/, '').trim() : '';
    const tileLabels = normalizeLabels(servicesMatch ? servicesMatch[1].replace(/&#038;/g, '&').split('::').map(s => s.trim()).filter(Boolean) : []);
    newProfiles.push({ name, profileUrl, photoUrl, nat, cup, tileLabels });
  }

  // Also backfill labels from listing tile for existing girls missing them
  const tileRe2 = /<div class="worker-tile post-\d+"([^>]*)>\s*<a href="([^"]+)" class="worker-tile-img-container" title="([^"]+)"/g;
  let m2;
  while ((m2 = tileRe2.exec(html)) !== null) {
    const [, attrs2, , rawTitle2] = m2;
    let tileName = rawTitle2.replace(/\s*\*?New\*?\s*/gi, '').trim();
    const girl = existing.find(g => g.name === tileName);
    if (girl && (!girl.labels || !girl.labels.length)) {
      const svc = attrs2.match(/data-services='([^']+)'/);
      if (svc) {
        girl.labels = svc[1].replace(/&#038;/g, '&').split('::').map(s => s.trim()).filter(Boolean);
        addedNames.push(tileName + ' (labels)');
      }
    }
  }

  // Batch fetch profile pages for age (20 per run due to subrequest limits)
  const BATCH = Math.min(20, MAX_NEW_PER_RUN);
  const toProcess = newProfiles.slice(0, BATCH);

  function parseStilettoProfile(pHtml) {
    const ageMatch = pHtml.match(/custom-field-age[^>]*>.*?<span>(\d+)<\/span>/i);
    const servicesMatch = [...pHtml.matchAll(/<li>([^<]+)<\/li>/gi)].map(m => m[1].trim());
    const labels = normalizeLabels(servicesMatch.filter(s => s.length > 1));
    // Description + body (dress size) from og:description
    const ogDesc = pHtml.match(/og:description"\s+content="([^"]+)"/i);
    let desc = '', body = '';
    if (ogDesc) {
      desc = ogDesc[1].replace(/&#8217;/g, "'").replace(/&#8220;/g, '"').replace(/&#8221;/g, '"').replace(/&hellip;/g, '...').replace(/&amp;/g, '&').replace(/About me\s*/i, '').trim();
      // Remove leading trait words like "Bold Sophisticated Brunette"
      desc = desc.replace(/^(?:Bold|Sophisticated|Elegant|Sexy|Sweet|Gorgeous|Stunning|Beautiful|Petite|Curvy|Slim|Tall|Short|Brunette|Blonde|Redhead|Asian|European|Latin|[A-Z][a-z]+)\s+(?:(?:Bold|Sophisticated|Elegant|Sexy|Sweet|Gorgeous|Stunning|Beautiful|Petite|Curvy|Slim|Tall|Short|Brunette|Blonde|Redhead|Asian|European|Latin|[A-Z][a-z]+)\s+)*(?=[A-Z])/g, '').trim();
      const sizeMatch = desc.match(/size\s+(\d+)/i);
      if (sizeMatch) body = sizeMatch[1];
    }
    // Age fallback from description if not in custom field
    let age = ageMatch ? ageMatch[1] : '';
    if (!age && desc) {
      const descAge = desc.match(/(\d{2})[- ]year[- ]old/i);
      if (descAge) age = descAge[1];
    }
    // Photos from wp-block-image figure tags
    const photos = [];
    const photoSet = new Set();
    const photoRe = /wp-block-image[^>]*>\s*<img[^>]+src="(https:\/\/www\.stilettosydney\.com\/wp-content\/uploads\/[^"]+)"/gi;
    let pm;
    while ((pm = photoRe.exec(pHtml)) !== null) {
      let src = pm[1];
      src = src.replace(/-\d+x\d+(\.\w+)$/, '$1');
      if (!photoSet.has(src)) { photoSet.add(src); photos.push(src); }
    }
    // Roster: parse availability days from profile page
    // <span class='roster-manager-post-availability-day-title-sub'>04 Apr</span>
    // <span class='roster-manager-post-availability-day-hour-start'>9:00 PM</span> ~
    // <span class='roster-manager-post-availability-day-hour-end'>5:00 AM</span>
    const roster = {};
    const dayBlocks = pHtml.split('roster-manager-post-availability-day\'');
    for (const block of dayBlocks) {
      const dateMatch = block.match(/day-title-sub'>(\d{2}) (\w{3})<\/span>/);
      const startMatch = block.match(/day-hour-start'>(\d{1,2}:\d{2} [AP]M)<\/span>/i);
      const endMatch = block.match(/day-hour-end'>(\d{1,2}:\d{2} [AP]M)<\/span>/i);
      if (!dateMatch || !startMatch || !endMatch) continue;
      // Convert "04 Apr" to "2026-04-04"
      const months = { Jan:'01', Feb:'02', Mar:'03', Apr:'04', May:'05', Jun:'06', Jul:'07', Aug:'08', Sep:'09', Oct:'10', Nov:'11', Dec:'12' };
      const mon = months[dateMatch[2]];
      if (!mon) continue;
      const year = new Date().getFullYear();
      const dateStr = year + '-' + mon + '-' + dateMatch[1];
      roster[dateStr] = { start: ampmTo24(startMatch[1].replace(' ', '')), end: ampmTo24(endMatch[1].replace(' ', '')) };
    }

    return { age, labels, photos, desc, body, roster };
  }

  for (const p of toProcess) {
    let age = '', labels = p.tileLabels || [], photos = (p.photoUrl && !/Logo|Screen-Shot/i.test(p.photoUrl)) ? [p.photoUrl] : [], desc = '', body = '', roster = {};
    try {
      await new Promise(r => setTimeout(r, 300));
      const BROWSER_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
      const pResp = await fetch(p.profileUrl, { headers: { 'User-Agent': BROWSER_UA } });
      if (pResp.ok) {
        const parsed = parseStilettoProfile(await pResp.text());
        age = parsed.age;
        labels = parsed.labels;
        desc = parsed.desc;
        body = parsed.body;
        roster = parsed.roster;
        if (parsed.photos.length) photos = parsed.photos;
        // Save roster to calendar
        if (Object.keys(roster).length) {
          if (!data.calendar) data.calendar = {};
          if (!data.calendar[p.name]) data.calendar[p.name] = {};
          Object.assign(data.calendar[p.name], roster);
        }
      }
    } catch (e) { console.error(`[Stiletto] Error fetching ${p.name}:`, e.message); }

    const entry = {
      name: p.name, country: p.nat ? [p.nat] : [], age, height: '',
      cup: p.cup, body, desc,
      val1: '390', val2: '490', val3: '590',
      startDate: (() => { for (const ph of photos) { const m = ph.match(/\/uploads\/(\d{4})\/(\d{2})\//); if (m) return m[1] + '-' + m[2] + '-01'; } return '2026-01-01'; })(),
      lastRostered: Object.keys(roster).length ? todayStr : '',
      oldUrl: p.profileUrl,
      photos, labels, originalSite: 'Exists',
    };
    existing.push(entry);
    existingNames.add(p.name);
    addedNames.push(p.name);
  }

  // Also backfill age/labels/photos/desc/body for existing girls missing them (batch of 20)
  const needBackfill = existing.filter(g => (!g.age || !g.labels || !g.labels.length || !g.desc || !g.body || g.photos.length <= 1) && g.oldUrl && g.oldUrl.includes('stilettosydney.com'));
  const backfillBatch = needBackfill.slice(0, Math.max(0, BATCH - toProcess.length));
  for (const g of backfillBatch) {
    try {
      await new Promise(r => setTimeout(r, 300));
      const BROWSER_UA2 = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
      const pResp = await fetch(g.oldUrl, { headers: { 'User-Agent': BROWSER_UA2 } });
      if (pResp.ok) {
        const parsed = parseStilettoProfile(await pResp.text());
        let updated = false;
        if (!g.age && parsed.age) { g.age = parsed.age; updated = true; }
        if ((!g.labels || !g.labels.length) && parsed.labels.length) { g.labels = parsed.labels; updated = true; }
        if (parsed.photos.length > g.photos.length) { g.photos = parsed.photos; updated = true; }
        if (!g.desc && parsed.desc) { g.desc = parsed.desc; updated = true; }
        if (!g.body && parsed.body) { g.body = parsed.body; updated = true; }
        // Save roster to calendar
        if (Object.keys(parsed.roster).length) {
          if (!data.calendar) data.calendar = {};
          if (!data.calendar[g.name]) data.calendar[g.name] = {};
          Object.assign(data.calendar[g.name], parsed.roster);
          updated = true;
        }
        if (updated) addedNames.push(g.name + ' (details)');
      }
    } catch (e) {}
  }

  if (addedNames.length) {
    data.girls = existing;
    data.lastGirlsSync = new Date().toISOString();
    await ghPut(env, site.jsonPath, data, sha, `[Stiletto] Auto-sync: ${addedNames.join(', ')}`);
  }
  return { added: addedNames.length, remaining: 0, names: addedNames };
}

async function scrapeStilettoRoster(site, env) {
  const resp = await fetch('https://www.stilettosydney.com/wp-json/roster-manager/v1/availability/current', {
    headers: { 'User-Agent': UA },
  });
  if (!resp.ok) throw new Error(`Stiletto roster API failed: ${resp.status}`);
  const entries = await resp.json();
  if (!Array.isArray(entries) || !entries.length) return {};

  const calendar = {};
  for (const entry of entries) {
    const startTime = entry.starting_time; // "2026-04-03 15:00:00"
    const endTime = entry.ending_time; // "2026-04-04 02:00:00"
    if (!startTime) continue;
    const dateStr = startTime.split(' ')[0]; // "2026-04-03"
    let name = (entry.post_title || '').replace(/\s*\*?New\*?\s*/gi, '').trim();
    if (!name) continue;
    const start = startTime.split(' ')[1]?.slice(0, 5) || '';
    const end = endTime ? endTime.split(' ')[1]?.slice(0, 5) || '' : '';
    if (!calendar[dateStr]) calendar[dateStr] = [];
    if (!calendar[dateStr].some(e => e.name === name)) calendar[dateStr].push({ name, start, end });
  }
  return calendar;
}

async function scrapeWivesOnlyRoster(site) {
  // Roster page default view shows today's roster only (other days loaded via AJAX)
  // Only assign girls to today's date
  const calendar = {};
  const BROWSER_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
  const resp = await fetch(site.rosterUrl, { headers: { 'User-Agent': BROWSER_UA } });
  if (!resp.ok) return calendar;
  const html = await resp.text();

  const todayStr = fmtDate(getAEDTDate());

  const blocks = html.split('ohoverzoom newroster');
  for (const block of blocks) {
    const nameMatch = block.match(/<h4>([^<]+)<\/h4>/);
    const timeMatch = block.match(/timeclock[\s\S]*?<span>([^<]+)<\/span>/);
    if (!nameMatch) continue;
    const name = nameMatch[1].trim();
    let start = '11:00', end = '04:00';
    if (timeMatch) {
      const parts = timeMatch[1].trim().match(/(\d{1,2}:\d{2}\s*[AP]M)\s*-\s*(\d{1,2}:\d{2}\s*[AP]M)/i);
      if (parts) {
        start = ampmTo24(parts[1].replace(/\s/g, ''));
        end = ampmTo24(parts[2].replace(/\s/g, ''));
      }
    }
    if (!calendar[todayStr]) calendar[todayStr] = [];
    if (!calendar[todayStr].some(e => e.name === name)) {
      calendar[todayStr].push({ name, start, end });
    }
  }

  return calendar;
}

async function scrapeGatewayRoster(site) {
  // Dedicated roster page: /gateway-club-roster/
  // Structure: <h4>Monday 6 April</h4> ... <h5>Morning GIRLS</h5> <h5>NAME</h5> ... <h5>Afternoon GIRLS</h5> ...
  const resp = await fetch('https://www.gatewayclub.com.au/gateway-club-roster/', { headers: { 'User-Agent': UA } });
  if (!resp.ok) throw new Error(`Gateway roster fetch failed: ${resp.status}`);
  const html = await resp.text();

  const MONTHS = { january:'01', february:'02', march:'03', april:'04', may:'05', june:'06', july:'07', august:'08', september:'09', october:'10', november:'11', december:'12' };
  const year = new Date().getFullYear();
  const calendar = {};

  // Split by day headers: <h4>Monday 6 April
  const dayBlocks = html.split(/<h4>/i).slice(1);
  for (const block of dayBlocks) {
    // Parse day header: "Monday 6 April <i>..."
    const dayMatch = block.match(/^\s*\w+\s+(\d+)\s+(\w+)/);
    if (!dayMatch) continue;
    const day = dayMatch[1].padStart(2, '0');
    const month = MONTHS[dayMatch[2].toLowerCase()];
    if (!month) continue;
    const dateStr = year + '-' + month + '-' + day;

    // Parse shift sections and girl names
    // Split by shift headers: Morning GIRLS, Afternoon GIRLS, Night GIRLS
    const sections = block.split(/<h5>\s*(Morning|Afternoon|Night)\s+GIRLS\s*<\/h5>/i);
    // sections: [before, "Morning", content, "Afternoon", content, "Night", content, ...]
    for (let i = 1; i < sections.length; i += 2) {
      const shiftType = sections[i].toLowerCase();
      const content = sections[i + 1] || '';
      const names = [...content.matchAll(/<h5>([^<]+)<\/h5>/gi)].map(m => m[1].trim()).filter(n => n.length >= 2 && !/GIRLS/i.test(n));

      let start, end;
      if (shiftType === 'morning') { start = '06:00'; end = '12:00'; }
      else if (shiftType === 'afternoon') { start = '12:00'; end = '18:00'; }
      else { start = '18:00'; end = '06:00'; }

      for (const rawName of names) {
        const name = rawName.charAt(0).toUpperCase() + rawName.slice(1).toLowerCase();
        if (!calendar[dateStr]) calendar[dateStr] = [];
        // Merge shifts for same girl on same day (extend time range)
        const existing = calendar[dateStr].find(e => e.name === name);
        if (existing) {
          // Extend: use earliest start and latest end
          if (start < existing.start) existing.start = start;
          if (shiftType === 'night') existing.end = '06:00'; // night always extends to 6am
          else if (end > existing.end && existing.end !== '06:00') existing.end = end;
        } else {
          calendar[dateStr].push({ name, start, end });
        }
      }
    }
  }

  return calendar;
}

async function scrapeSpringHouseRoster(site) {
  // /roster/ page shows all girls currently rostered (today's roster)
  const resp = await fetch(site.rosterUrl, { headers: { 'User-Agent': UA } });
  if (!resp.ok) throw new Error(`Spring House roster fetch failed: ${resp.status}`);
  const html = await resp.text();

  const todayStr = fmtDate(getAEDTDate());
  const names = [...html.matchAll(/class="name">([^<]+)/g)].map(m => m[1].trim()).filter(n => n.length >= 2);

  if (!names.length) return {};
  // All girls on the roster page are working today — default hours 10am-3am
  return { [todayStr]: names.map(name => ({ name, start: '10:00', end: '03:00' })) };
}

async function scrapeMarrickvilleRoster(site) {
  const BROWSER_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
  const resp = await fetch(site.rosterUrl, { headers: { 'User-Agent': BROWSER_UA } });
  if (!resp.ok) throw new Error(`Marrickville roster fetch failed: ${resp.status}`);
  const html = await resp.text();

  // Parse table: pairs of <td>Name</td><td>Roster</td> (skip &nbsp; cells)
  const cells = [...html.matchAll(/<td[^>]*>([^<]+)<\/td>/gi)]
    .map(m => m[1].trim())
    .filter(t => t !== '&nbsp;' && t !== '');

  const today = getAEDTDate();
  const todayDow = today.getDay(); // 0=Sun
  const calendar = {};
  const DAY_MAP = { sunday: 0, monday: 1, tuesday: 2, wednesday: 3, thursday: 4, thursay: 4, friday: 5, saturday: 6 };

  // Build dates for the next 7 days indexed by dow
  const datesByDow = {};
  for (let i = 0; i < 7; i++) {
    const d = new Date(today);
    d.setDate(today.getDate() + i);
    const dow = d.getDay();
    datesByDow[dow] = fmtDate(d);
  }

  function getDatesForRoster(rosterText) {
    const text = rosterText.toLowerCase().trim();
    if (text === 'everyday') return Object.values(datesByDow);

    // Range: "monday to friday"
    const rangeMatch = text.match(/(\w+)\s+to\s+(\w+)/);
    if (rangeMatch) {
      const startDow = DAY_MAP[rangeMatch[1]];
      const endDow = DAY_MAP[rangeMatch[2]];
      if (startDow !== undefined && endDow !== undefined) {
        const dates = [];
        for (let dow = startDow; ; dow = (dow + 1) % 7) {
          if (datesByDow[dow]) dates.push(datesByDow[dow]);
          if (dow === endDow) break;
        }
        return dates;
      }
    }

    // Specific days: "Thursday,Tuesday,Wednesday"
    const days = text.split(/[,\s]+/).map(d => d.trim());
    const dates = [];
    for (const day of days) {
      const dow = DAY_MAP[day];
      if (dow !== undefined && datesByDow[dow]) dates.push(datesByDow[dow]);
    }
    return dates;
  }

  // Process pairs: Name, Roster
  for (let i = 0; i < cells.length - 1; i += 2) {
    const name = cells[i];
    const roster = cells[i + 1];
    if (!name || !roster) continue;
    // Skip header row
    if (name === 'Image' || name === 'Name') continue;

    const dates = getDatesForRoster(roster);
    for (const dateStr of dates) {
      if (!calendar[dateStr]) calendar[dateStr] = [];
      if (!calendar[dateStr].some(e => e.name === name)) {
        calendar[dateStr].push({ name, start: '09:00', end: '00:00' });
      }
    }
  }

  return calendar;
}

async function scrapeJiniaRoster(site) {
  const BROWSER_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
  const resp = await fetch(site.rosterUrl, { headers: { 'User-Agent': BROWSER_UA } });
  if (!resp.ok) throw new Error(`Jinia roster fetch failed: ${resp.status}`);
  const html = await resp.text();

  // Homepage has 7 content sliders (avia-content-slider1 through slider7), one per day of the week:
  // Slider 1 = Monday, 2 = Tuesday, ..., 6 = Saturday, 7 = Sunday
  const today = getAEDTDate();
  const todayDow = today.getDay(); // JS: 0=Sun, 1=Mon, ..., 6=Sat
  const calendar = {};

  // Map slider number to JS day-of-week: slider1=Mon(1), slider2=Tue(2), ..., slider6=Sat(6), slider7=Sun(0)
  const sliderToDow = [0, 1, 2, 3, 4, 5, 6, 0]; // index 0 unused, slider 1-7

  for (let sliderNum = 1; sliderNum <= 7; sliderNum++) {
    const sliderDow = sliderToDow[sliderNum];
    let dayOffset = sliderDow - todayDow;
    if (dayOffset < 0) dayOffset += 7; // wrap to next week
    const targetDate = new Date(today);
    targetDate.setDate(today.getDate() + dayOffset);
    const dateStr = fmtDate(targetDate);

    // Extract section content between this slider and the next
    const marker = `avia-content-slider${sliderNum} `;
    const idx = html.indexOf(marker);
    if (idx === -1) continue;
    const nextMarker = `avia-content-slider${sliderNum + 1} `;
    const nextIdx = sliderNum < 7 ? html.indexOf(nextMarker, idx + 1) : html.length;
    const section = html.substring(idx, nextIdx > -1 ? nextIdx : html.length);

    // Parse girl names and shift times from title attributes
    const titleRe = /title='([^']+)'/g;
    const entries = [];
    let m;
    while ((m = titleRe.exec(section)) !== null) {
      const raw = m[1].trim();
      if (!raw || /Search|Menu|Facebook|Instagram|Jinia|Scroll|parking|brothel|porn|jinia|zinia|Image|YouCut|^\d+$|图片|副本/i.test(raw)) continue;

      const timeMatch = raw.match(/(\d{1,2}(?::\d{2})?\s*(?:am|pm))\s*[-–~]\s*(\d{1,2}(?::\d{2})?\s*(?:am|pm)|close)/i);
      let name = raw
        .replace(/\s*\d{1,2}(?::\d{2})?\s*(?:am|pm)\s*[-–~]?\s*(?:\d{1,2}(?::\d{2})?\s*(?:am|pm)?|close|late|open)\b.*/gi, '')
        .replace(/\d{1,2}:\d{2}\s*(?:am|pm)\s*[-–~]?\s*(?:\d{1,2}(?::\d{2})?\s*(?:am|pm)?|close|late)\b.*/gi, '')
        .replace(/\s*\(\d+yo\)/gi, '').replace(/\s*\*.*$/gi, '').trim();
      if (!name || name.length < 2 || entries.some(e => e.name === name)) continue;

      const entry = { name };
      if (timeMatch) {
        entry.start = ampmTo24(timeMatch[1]);
        entry.end = ampmTo24(timeMatch[2]);
      } else {
        entry.start = '10:30';
        entry.end = '04:00';
      }
      entries.push(entry);
    }

    if (entries.length) calendar[dateStr] = entries;
  }

  return calendar;
}

/* ── Wives Only custom scraper (Elementor, bgimage-roster) ── */
async function syncWivesOnlyGirls(env, site) {
  const { data, sha } = await loadData(env, site);
  const existing = data.girls || [];
  const existingNames = new Set(existing.map(g => g.name));

  const resp = await fetch('https://wivesonly.com.au/wives-only-ladies/', { headers: { 'User-Agent': UA } });
  if (!resp.ok) return { added: 0, remaining: 0, names: [] };
  const html = await resp.text();

  // Parse blocks: name from <a href="URL"><h4>Name</h4></a>, photo from bgimage-roster style, model-parameters for height/bust/age
  // Structure: <div class="bgimage-roster" style="background-image:url(PHOTO)">...model-parameters...Height/Bust/Age/Hair...</div>...<div class="mid-title-name"><a href="URL"><h4>Name</h4></a></div>
  const blocks = html.split('class="col-sm-3 ohoverzoom');
  const todayStr = fmtDate(getAEDTDate());

  // Collect new profiles from listing
  const newProfiles = [];
  for (const block of blocks) {
    const nameMatch = block.match(/<h4>([^<]+)<\/h4>/);
    if (!nameMatch) continue;
    const name = nameMatch[1].trim();
    if (!name || name === 'New Girl' || existingNames.has(name) || !isValidGirlName(name)) continue;
    const urlMatch = block.match(/href="(https:\/\/wivesonly\.com\.au\/[^"]+)"/);
    if (urlMatch) newProfiles.push({ name, url: urlMatch[1] });
  }

  function parseWivesOnlyProfile(pHtml) {
    // Parse h6 label/value pairs: Nationality, Age, Height (Hight), Bust, Hair, Skin
    const headings = [...pHtml.matchAll(/elementor-heading-title elementor-size-default">([^<]+)/gi)].map(m => m[1].trim());
    const fields = {};
    for (let i = 0; i < headings.length - 1; i++) {
      const key = headings[i].toLowerCase();
      if (['nationality', 'age', 'hight', 'height', 'bust', 'hair', 'skin'].includes(key)) {
        fields[key] = headings[i + 1];
      }
    }
    // Description from <p class="p1"> or first substantial <p>
    const descMatch = pHtml.match(/<p class="p1">([^<]+)/i) || pHtml.match(/elementor-widget-text-editor[\s\S]*?<p[^>]*>([^<]{20,})/i);
    const desc = descMatch ? descMatch[1].replace(/&#8217;/g, "'").replace(/&#8230;/g, '...').replace(/&amp;/g, '&').trim() : '';
    // Body (dress size) from description
    const sizeMatch = desc.match(/size\s+(\d+)/i);
    // Country from nationality field
    const country = fields.nationality || '';
    const age = fields.age || '';
    const height = fields.hight || fields.height || '';
    const cupMatch = (fields.bust || '').match(/([A-Z](?:DD)?)/i);
    const cup = cupMatch ? cupMatch[1].toUpperCase() : '';
    const body = sizeMatch ? sizeMatch[1] : '';
    // Photos
    const photos = [];
    const photoSet = new Set();
    const photoRe = /src="(https:\/\/wivesonly\.com\.au\/wp-content\/uploads\/[^"]+)"/gi;
    let pm;
    while ((pm = photoRe.exec(pHtml)) !== null) {
      let src = pm[1];
      if (/logo|icon|VIP|phn|BECOME|cropped/i.test(src)) continue;
      src = src.replace(/-\d+x\d+(\.\w+)$/, '$1');
      if (!photoSet.has(src)) { photoSet.add(src); photos.push(src); }
    }
    return { country, age, height, cup, body, desc, photos };
  }

  const BATCH = Math.min(20, MAX_NEW_PER_RUN);
  const toProcess = newProfiles.slice(0, BATCH);
  const addedNames = [];

  for (const p of toProcess) {
    let parsed = { country: '', age: '', height: '', cup: '', body: '', desc: '', photos: [] };
    try {
      await new Promise(r => setTimeout(r, 500));
      const pResp = await fetch(p.url, { headers: { 'User-Agent': UA } });
      if (pResp.ok) parsed = parseWivesOnlyProfile(await pResp.text());
    } catch (e) { console.error(`[Wives Only] Error fetching ${p.name}:`, e.message); }

    const startDate = (() => { for (const ph of parsed.photos) { const m = ph.match(/\/uploads\/(\d{4})\/(\d{2})\//); if (m) return m[1] + '-' + m[2] + '-01'; } return todayStr; })();
    const entry = {
      name: p.name, country: parsed.country ? [parsed.country] : [],
      age: parsed.age, height: parsed.height, cup: parsed.cup, body: parsed.body, desc: parsed.desc,
      val1: '250', val2: '320', val3: '400',
      startDate, lastRostered: startDate, oldUrl: p.url,
      photos: parsed.photos, labels: [], originalSite: 'Exists',
    };
    existing.push(entry);
    existingNames.add(p.name);
    addedNames.push(p.name);
  }

  // Backfill existing girls missing country/desc/photos
  const needBackfill = existing.filter(g => (!g.country || !g.country.length || !g.desc || g.photos.length <= 1) && g.oldUrl && g.oldUrl.includes('wivesonly.com.au/'));
  const backfillBatch = needBackfill.slice(0, Math.max(0, BATCH - toProcess.length));
  for (const g of backfillBatch) {
    try {
      await new Promise(r => setTimeout(r, 500));
      const pResp = await fetch(g.oldUrl, { headers: { 'User-Agent': UA } });
      if (pResp.ok) {
        const parsed = parseWivesOnlyProfile(await pResp.text());
        let updated = false;
        if ((!g.country || !g.country.length) && parsed.country) { g.country = [parsed.country]; updated = true; }
        if (!g.desc && parsed.desc) { g.desc = parsed.desc; updated = true; }
        if (!g.body && parsed.body) { g.body = parsed.body; updated = true; }
        if (!g.age && parsed.age) { g.age = parsed.age; updated = true; }
        if (!g.height && parsed.height) { g.height = parsed.height; updated = true; }
        if (!g.cup && parsed.cup) { g.cup = parsed.cup; updated = true; }
        if (parsed.photos.length > g.photos.length) { g.photos = parsed.photos; updated = true; }
        // Update startDate from photos
        if ((!g.startDate || g.startDate === todayStr) && parsed.photos.length) {
          for (const ph of parsed.photos) { const m = ph.match(/\/uploads\/(\d{4})\/(\d{2})\//); if (m) { g.startDate = m[1] + '-' + m[2] + '-01'; g.lastRostered = g.startDate; break; } }
          updated = true;
        }
        if (updated) addedNames.push(g.name + ' (details)');
      }
    } catch (e) {}
  }

  if (addedNames.length) {
    data.girls = existing;
    data.lastGirlsSync = new Date().toISOString();
    await ghPut(env, site.jsonPath, data, sha, `[Wives Only] Auto-sync: ${addedNames.join(', ')}`);
  }
  return { added: addedNames.length, remaining: newProfiles.length - toProcess.length, names: addedNames };
}

/* ── Jinia custom scraper (Enfold/Avia portfolio theme, fetches profile pages) ── */
async function syncJiniaGirls(env, site) {
  const BROWSER_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
  const { data, sha } = await loadData(env, site);
  const existing = data.girls || [];
  const existingNames = new Set(existing.map(g => g.name));
  const existingUrls = new Set(existing.map(g => g.oldUrl).filter(Boolean));

  const resp = await fetch(site.girlsUrl, { headers: { 'User-Agent': BROWSER_UA } });
  if (!resp.ok) return { added: 0, remaining: 0, names: [] };
  const html = await resp.text();

  // Collect unique profile URLs from portfolio listing
  const articles = html.split("class='slide-entry flex_column");
  const profileUrls = [];
  const seenUrls = new Set();
  const countryMap = { chinese: 'Chinese', vietnamese: 'Vietnamese', thai: 'Thai', greek: 'Greek', japanese: 'Japanese', korean: 'Korean', 'hong kong': 'Hong Kong', taiwanese: 'Taiwanese', australian: 'Australian', singaporean: 'Singaporean', malaysian: 'Malaysian', indonesian: 'Indonesian' };

  for (const article of articles) {
    const linkMatch = article.match(/href='(https:\/\/jinia\.com\.au\/[^']+)'\s+title='([^']+)'/);
    if (!linkMatch) continue;
    const [, profileUrl, rawTitle] = linkMatch;
    if (seenUrls.has(profileUrl)) continue;
    seenUrls.add(profileUrl);

    // Strip time patterns, age hints, annotations: "Nika12:30pm- close", "Whisky 11am-close", "Madoka (21yo)", "*Highly recommended"
    let name = rawTitle
      .replace(/\s*\d{1,2}(?::\d{2})?\s*(?:am|pm)\s*[-–~]?\s*(?:\d{1,2}(?::\d{2})?\s*(?:am|pm)?|close|late|open)\b.*/gi, '')
      .replace(/\d{1,2}:\d{2}\s*(?:am|pm)\s*[-–~]?\s*(?:\d{1,2}(?::\d{2})?\s*(?:am|pm)?|close|late)\b.*/gi, '')
      .replace(/\s*\(\d+yo\)/gi, '')
      .replace(/\s*\*[^)]*$/gi, '')
      .replace(/\s*\(joined.*$/gi, '')
      .replace(/\s*\([^)]*\)\s*$/gi, '')
      .trim();
    if (!name || !isValidGirlName(name)) continue;
    // Handle renames — URL exists with different name
    if (existingUrls.has(profileUrl)) {
      const g = existing.find(e => e.oldUrl === profileUrl);
      if (g && g.name !== name) {
        const oldName = g.name;
        if (data.calendar && data.calendar[oldName]) { data.calendar[name] = data.calendar[oldName]; delete data.calendar[oldName]; }
        existingNames.delete(oldName); existingNames.add(name);
        g.name = name;
        console.log(`[Jinia] Renamed: ${oldName} -> ${name}`);
      }
      continue;
    }
    if (existingNames.has(name)) continue;
    profileUrls.push({ profileUrl, name });
  }

  // Mark existing girls' originalSite status
  const activeUrls = seenUrls;
  let siteChanged = false;
  let updated = 0;
  for (const g of existing) {
    const shouldBe = activeUrls.has(g.oldUrl) ? 'Exists' : '';
    if (g.originalSite !== shouldBe) { g.originalSite = shouldBe; siteChanged = true; updated++; }
  }

  if (profileUrls.length === 0) {
    if (siteChanged) {
      data.girls = existing;
      data.lastGirlsSync = new Date().toISOString();
      await ghPut(env, site.jsonPath, data, sha, `[Jinia] Update originalSite status`);
    }
    return { added: 0, remaining: 0, names: [], updated };
  }

  // Cloudflare Workers have 50 subrequest limit; we used 1 for listing + need ~2 for GitHub
  const JINIA_BATCH = Math.min(20, MAX_NEW_PER_RUN);
  const toProcess = profileUrls.slice(0, JINIA_BATCH);
  const addedNames = [];
  const todayStr = fmtDate(getAEDTDate());

  for (const { profileUrl, name } of toProcess) {
    try {
      await new Promise(r => setTimeout(r, 1000));
      const pResp = await fetch(profileUrl, { headers: { 'User-Agent': BROWSER_UA } });
      if (!pResp.ok) continue;
      const pHtml = await pResp.text();

      // Parse profile page: <p>Nationalit: Chinese</p> <p>Age: 22yo</p> <p>Height: 160cm</p> <p>Bust Size: 36D</p>
      const natMatch = pHtml.match(/<p>Nationalit[^:]*:\s*([^<]+)/i);
      const ageMatch = pHtml.match(/<p>Age:\s*(\d+)/i);
      const heightMatch = pHtml.match(/<p>Height:\s*(\d+)\s*cm/i);
      const bustMatch = pHtml.match(/<p>Bust Size:\s*\d*([A-Z](?:DD)?)/i);

      let countries = [];
      if (natMatch) {
        // Split on ×, /, &, comma, "and", "x" (between words); strip "mixed"
        const parts = natMatch[1].trim().split(/\s*[×\/&,]\s*|\s+and\s+|\s+x\s+/i).map(s => s.trim().toLowerCase()).filter(s => s && s !== 'mixed');
        for (const part of parts) {
          const mapped = countryMap[part] || (part.charAt(0).toUpperCase() + part.slice(1));
          if (mapped) countries.push(mapped);
        }
      }

      // Fix common height typos (e.g. "60cm" should be "160cm")
      let height = heightMatch ? heightMatch[1] : '';
      if (height && parseInt(height) < 100) height = '1' + height;

      // Photos: collect from both src= and href= (masonry gallery uses href for larger versions)
      // Strip dimension suffixes to get originals, skip QR/logo/thumbnails
      const photoRe = /(?:src|href)="(https:\/\/jinia\.com\.au\/wp-content\/uploads\/[^"]+)"/gi;
      const photoSet = new Set();
      const photos = [];
      let pm;
      while ((pm = photoRe.exec(pHtml)) !== null) {
        let src = pm[1];
        if (/QR|qr|logo|QXqOMWY5vY/i.test(src)) continue;
        // Skip tiny thumbnails (80x80 etc used in sidebar)
        if (/-(80x80|36x36|120x120|180x180)\./i.test(src)) continue;
        // Strip WP dimension suffix to get original: image-529x705.jpg -> image.jpg
        src = src.replace(/-\d+x\d+(\.\w+)$/, '$1');
        // Percent-encode non-ASCII chars (e.g. Chinese 图片) so URLs work in browsers
        src = encodeURI(src);
        if (!photoSet.has(src)) { photoSet.add(src); photos.push(src); }
      }

      // Start date from datePublished meta
      const dateMatch = pHtml.match(/itemprop="datePublished"\s+datetime="(\d{4}-\d{2}-\d{2})/);
      const startDate = dateMatch ? dateMatch[1] : todayStr;

      // Pricing: parse from Service field and description text
      // Default tiers: Standard 30m=$130, Gold 30m=$170, Diamond 30m=$190 45m=$250 60m=$300
      const JINIA_STANDARD = { val1: '130', val2: '', val3: '' };
      const JINIA_GOLD = { val1: '170', val2: '', val3: '' };
      const JINIA_DIAMOND = { val1: '190', val2: '250', val3: '300' };
      // Get service/pricing text from Service field and schema description (avoid HTML class noise)
      const serviceMatch = pHtml.match(/<p>Service:\s*([^<]+)/i);
      const schemaDesc = pHtml.match(/"description":"([^"]+)"/);
      const descBlock = pHtml.match(/lady-description[\s\S]*?<\/div>/i);
      // Also capture lady-description div content
      const ladyDesc = pHtml.match(/lady-description[\s\S]*?<\/div>/i);
      // Build combined text from all sources, normalize all whitespace/newline variants
      let combined = (serviceMatch ? serviceMatch[1] : '') + ' ' + (schemaDesc ? schemaDesc[1] : '') + ' ' + (ladyDesc ? ladyDesc[0].replace(/<[^>]+>/g, ' ') : '');
      // Handle literal \r\n from JSON strings AND actual newlines
      combined = combined.split('\\r\\n').join(' ').split('\\n').join(' ').split('\\r').join(' ');
      combined = combined.replace(/\r\n|\n|\r/g, ' ').replace(/\s+/g, ' ');

      // Parse pricing: prioritise Diamond > Gold > Standard
      // Look for tier-specific pricing blocks like "Gold services(30min $170, 60min $260)"
      // or "Diamond Service Rates:\n30 mins – $190"
      let val1 = '', val2 = '', val3 = '';
      const hasDiamond = /diamond/i.test(combined);
      const hasGold = /gold(?:en)?/i.test(combined);
      const hasStandard = /standard/i.test(combined);

      // Extract prices from highest-tier block first
      function extractPrices(text) {
        // Match patterns: "30min $170", "30 mins – $200", "$170 for 30 minutes", "$200/30mins"
        const find = (dur) => {
          const patterns = [
            new RegExp(dur + '\\s*min[s]?\\s*[-–]?\\s*\\$(\\d+)', 'gi'),          // 30min $170
            new RegExp('\\$(\\d+)\\s*(?:for|per)?\\s*' + dur + '\\s*min', 'gi'),   // $170 for 30 min
            new RegExp('\\$(\\d+)\\s*/\\s*' + dur + '\\s*min', 'gi'),              // $200/30mins
          ];
          const all = patterns.flatMap(re => [...text.matchAll(re)].map(m => m[1]));
          return all.length ? all[all.length - 1] : '';
        };
        return { v1: find('30'), v2: find('45'), v3: find('60') };
      }

      // Try tier-specific sections: "Diamond ... $NNN" or "Gold services(30min $170, 60min $260)"
      const diamondBlock = combined.match(/diamond[^.]*?\$([\d, min$]+)/i);
      const goldBlock = combined.match(/gold(?:en)?[^.]*?\$([\d, min$]+)/i);

      if (hasDiamond && diamondBlock) {
        const block = combined.slice(combined.search(/diamond/i));
        const p = extractPrices(block);
        val1 = p.v1; val2 = p.v2; val3 = p.v3;
      } else if (hasGold && goldBlock) {
        const block = combined.slice(combined.search(/gold(?:en)?/i));
        const p = extractPrices(block);
        val1 = p.v1; val2 = p.v2; val3 = p.v3;
      } else if (hasStandard) {
        const block = combined.slice(combined.search(/standard/i));
        const p = extractPrices(block);
        val1 = p.v1; val2 = p.v2; val3 = p.v3;
      }

      // If no explicit prices found, use tier defaults
      if (!val1 && !val2 && !val3) {
        const tier = hasDiamond ? JINIA_DIAMOND : hasGold ? JINIA_GOLD : hasStandard ? JINIA_STANDARD : JINIA_DIAMOND;
        val1 = tier.val1; val2 = tier.val2; val3 = tier.val3;
      }

      const entry = {
        name, country: countries, age: ageMatch ? ageMatch[1] : '',
        height, cup: bustMatch ? bustMatch[1].toUpperCase() : '', body: '',
        val1, val2, val3,
        startDate, lastRostered: startDate, oldUrl: profileUrl,
        photos, labels: [], originalSite: 'Exists',
      };
      existing.push(entry);
      existingNames.add(name);
      addedNames.push(name);
    } catch (e) { console.error(`[Jinia] Error scraping ${profileUrl}:`, e.message); }
  }

  if (addedNames.length || siteChanged) {
    data.girls = existing;
    data.lastGirlsSync = new Date().toISOString();
    await ghPut(env, site.jsonPath, data, sha, `[Jinia] Auto-sync: ${addedNames.join(', ')}`);
  }
  return { added: addedNames.length, remaining: profileUrls.length - toProcess.length, names: addedNames, total: profileUrls.length, updated };
}

async function scrapePennys77Roster(site, env) {
  const BROWSER_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
  const resp = await fetch(site.rosterUrl, { headers: { 'User-Agent': BROWSER_UA } });
  if (!resp.ok) throw new Error(`Penny's 77 roster fetch failed: ${resp.status}`);
  const html = await resp.text();

  // Date from "Today's Roster (03/04/2026)"
  const todayMatch = html.match(/Today.s Roster\s*\((\d{1,2})\/(\d{1,2})\/(\d{4})\)/i);
  if (!todayMatch) return {};
  const dateStr = todayMatch[3] + '-' + String(todayMatch[2]).padStart(2, '0') + '-' + String(todayMatch[1]).padStart(2, '0');

  // Parse article blocks for profile data
  const articles = html.split('<article ').slice(1);
  const rosterGirls = [];
  const countryMap = { thai: 'Thai', thailand: 'Thai', vietnamese: 'Vietnamese', vietnam: 'Vietnamese', chinese: 'Chinese', australian: 'Australian', aussie: 'Australian', japanese: 'Japanese', korean: 'Korean', polish: 'Polish', european: 'European', indian: 'Indian', spanish: 'Spanish' };

  for (const article of articles) {
    if (!article.includes('tag-roster')) continue;
    const excerpt = article.substring(article.indexOf('post__excerpt') || 0);
    const nameMatch = excerpt.match(/Name:\s*([A-Za-z]+)/i);
    if (!nameMatch) continue;
    const name = nameMatch[1].trim();

    const ageMatch = excerpt.match(/Age\s*:?\s*(\d+)/i);
    const heightMatch = excerpt.match(/Height\s*:?\s*(\d+)\s*cm/i) || excerpt.match(/Height\s*:?\s*(\d)[''](\d+)/i);
    const cupMatch = excerpt.match(/Boobs:\s*([A-Z](?:DD)?)/i) || excerpt.match(/([A-Z](?:DD)?)\s*cup/i);
    const natMatch = excerpt.match(/Nationality:\s*[^\w]*([A-Za-z]+)/i);
    const bodyMatch = excerpt.match(/Body Size:\s*(\d+)/i) || excerpt.match(/Size:\s*(\d+)/i);

    let heightCm = '';
    if (heightMatch && heightMatch[2]) {
      heightCm = String(Math.round(parseInt(heightMatch[1]) * 30.48 + parseInt(heightMatch[2]) * 2.54));
    } else if (heightMatch) {
      heightCm = heightMatch[1];
    }

    const country = natMatch ? (countryMap[natMatch[1].toLowerCase()] || natMatch[1]) : '';

    // Get profile URL and photo from article
    const urlMatch = article.match(/href="(https:\/\/pennys77\.com\.au\/[^"]+)"/);
    const imgMatch = article.match(/src="(https:\/\/pennys77\.com\.au\/wp-content\/uploads\/[^"]+)"/);
    // Get largest image (not thumbnail)
    let photo = '';
    if (imgMatch) {
      const srcsetMatch = article.match(/srcset="([^"]+)"/);
      if (srcsetMatch) {
        const srcs = srcsetMatch[1].split(',').map(s => s.trim().split(/\s+/));
        const largest = srcs.filter(s => !/-\d+x\d+\./.test(s[0]));
        photo = largest.length ? largest[0][0] : srcs[srcs.length - 1][0];
      } else {
        photo = imgMatch[1];
      }
    }

    rosterGirls.push({
      name, age: ageMatch ? ageMatch[1] : '', height: heightCm,
      cup: cupMatch ? cupMatch[1].toUpperCase() : '', body: bodyMatch ? bodyMatch[1] : '',
      country, oldUrl: urlMatch ? urlMatch[1] : '', photo,
    });
  }

  // Create profiles for girls not already in the system
  if (env && rosterGirls.length) {
    try {
      const { data, sha } = await loadData(env, site);
      const existing = data.girls || [];
      const existingNames = new Set(existing.map(g => g.name));
      const added = [];
      for (const g of rosterGirls) {
        if (existingNames.has(g.name)) continue;
        const asianNats2 = ['Thai','Japanese','Chinese','Korean','Vietnamese','Taiwanese','Filipino','Malaysian','Indonesian','Singaporean','Cambodian','Indian','Hong Kong'];
        const isAsian2 = asianNats2.includes(g.country);
        const pr = isAsian2 ? (site.pricingByCountry?.asian || {}) : (site.pricingByCountry?.other || site.defaultPricing || {});
        existing.push({
          name: g.name, country: g.country ? [g.country] : [], age: g.age,
          height: g.height, cup: g.cup, body: g.body,
          val1: pr.val1 || '', val2: pr.val2 || '', val3: pr.val3 || '',
          startDate: dateStr, oldUrl: g.oldUrl, photos: g.photo ? [g.photo] : [],
          labels: [], originalSite: 'Exists',
        });
        existingNames.add(g.name);
        added.push(g.name);
      }
      if (added.length) {
        data.girls = existing;
        await ghPut(env, site.jsonPath, data, sha, `[Penny's 77] Auto-create from roster: ${added.join(', ')}`);
        console.log(`[Penny's 77] Created ${added.length} profiles from roster: ${added.join(', ')}`);
      }
    } catch (e) { console.error("[Penny's 77] Error creating profiles from roster:", e.message); }
  }

  const result = {};
  const names = rosterGirls.map(g => g.name);
  if (names.length) {
    result[dateStr] = names.map(name => ({ name, start: '11:00', end: '03:00' }));
  }
  console.log(`[Penny's 77] Roster scraped: ${names.length} girls for ${dateStr}`);
  return result;
}

async function syncGoldenAppleGirls(env, site) {
  const { data, sha } = await loadData(env, site);
  const existing = data.girls || [];
  const existingNames = new Set(existing.map(g => g.name));

  const resp = await fetch(site.girlsUrl, { headers: { 'User-Agent': UA } });
  if (!resp.ok) return { added: 0, remaining: 0, names: [] };
  const html = await resp.text();

  // Parse <li> items with structured spans: name, age, bust, country, dress, height, photo
  const liRe = /<li[^>]*class="mix[^"]*"[^>]*>([\s\S]*?)<\/li>/gi;
  const addedNames = [];
  const todayStr = fmtDate(getAEDTDate());
  let m;
  while ((m = liRe.exec(html)) !== null) {
    const block = m[1];
    const nameMatch = block.match(/class="name">([^<]+)/);
    if (!nameMatch) continue;
    const name = nameMatch[1].trim();
    if (existingNames.has(name)) continue;

    const ageMatch = block.match(/class="age">(\d+)/);
    const bustMatch = block.match(/class="bust">(?:\d+)?([A-Z])/i);
    const countryMatch = block.match(/class="country">([^<]+)/);
    const dressMatch = block.match(/class="dress">(\d+)/);
    const heightMatch = block.match(/class="height">(\d+)[''"](\d+)/);
    const imgRaw = block.match(/data-original="([^"]+)"/);
    const imgMatch = imgRaw && !imgRaw[1].includes('coming_soon') ? imgRaw : null;

    let heightCm = '';
    if (heightMatch) {
      heightCm = String(Math.round(parseInt(heightMatch[1]) * 30.48 + parseInt(heightMatch[2]) * 2.54));
    }

    const photos = imgMatch ? [imgMatch[1]] : [];

    existing.push({
      name, country: countryMatch ? [countryMatch[1].trim()] : [],
      age: ageMatch ? ageMatch[1] : '', height: heightCm,
      cup: bustMatch ? bustMatch[1].toUpperCase() : '', body: dressMatch ? dressMatch[1] : '',
      val1: (site.defaultPricing || {}).val1 || '', val2: (site.defaultPricing || {}).val2 || '', val3: (site.defaultPricing || {}).val3 || '',
      startDate: todayStr, oldUrl: site.girlsUrl, photos, labels: [], originalSite: 'Exists',
    });
    existingNames.add(name);
    addedNames.push(name);
  }

  if (addedNames.length) {
    data.girls = existing;
    data.lastGirlsSync = new Date().toISOString();
    await ghPut(env, site.jsonPath, data, sha, `[Golden Apple] Auto-sync: ${addedNames.join(', ')}`);
  }
  return { added: addedNames.length, remaining: 0, names: addedNames };
}

async function scrapeGoldenAppleRoster(site) {
  const resp = await fetch(site.rosterUrl, { headers: { 'User-Agent': UA } });
  if (!resp.ok) throw new Error(`Golden Apple roster fetch failed: ${resp.status}`);
  const html = await resp.text();

  const aest = getAEDTDate();
  const year = aest.getFullYear();
  const months = { january:0,february:1,march:2,april:3,may:4,june:5,july:6,august:7,september:8,october:9,november:10,december:11 };

  // Find date sections: "<h2>Friday 3 April</h2>" (no year)
  const dateRe = /<h2>\s*(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s*<\/h2>/gi;
  const sections = [];
  let dm;
  while ((dm = dateRe.exec(html)) !== null) {
    sections.push({ pos: dm.index, day: parseInt(dm[2]), month: months[dm[3].toLowerCase()], year });
  }

  const result = {};
  for (let i = 0; i < sections.length; i++) {
    const sec = sections[i];
    const sectionHtml = html.substring(sec.pos, i + 1 < sections.length ? sections[i + 1].pos : sec.pos + 10000);
    const d = new Date(sec.year, sec.month, sec.day);
    const dateStr = fmtDate(d);

    // Extract: <h4><a class="ag_inline" href="#inline">Name</a> (10am-5pm)</h4>
    // Some entries have no time: <h4><a ...>Hennessy</a></h4>
    const entryRe = /<h4><a[^>]*>([^<]+)<\/a>(?:\s*\((\d{1,2}(?:\.?\d+)?(?:am|pm))-(\d{1,2}(?:\.?\d+)?(?:am|pm))\))?<\/h4>/gi;
    let em;
    while ((em = entryRe.exec(sectionHtml)) !== null) {
      const name = em[1].trim();
      if (!name) continue;
      let startTime = '10:00', endTime = '22:00';
      if (em[2] && em[3]) {
        let startH = parseInt(em[2]);
        const startAP = em[2].replace(/[\d.]+/, '').toLowerCase();
        let endH = parseInt(em[3]);
        const endAP = em[3].replace(/[\d.]+/, '').toLowerCase();
        const start = (startAP === 'pm' && startH !== 12 ? startH + 12 : startAP === 'am' && startH === 12 ? 0 : startH);
        const end = (endAP === 'pm' && endH !== 12 ? endH + 12 : endAP === 'am' && endH === 12 ? 0 : endH);
        startTime = String(start).padStart(2, '0') + ':00';
        endTime = String(end).padStart(2, '0') + ':00';
      }
      if (!result[dateStr]) result[dateStr] = [];
      result[dateStr].push({ name, start: startTime, end: endTime });
    }
  }

  const totalEntries = Object.values(result).reduce((s, e) => s + e.length, 0);
  console.log(`[Golden Apple] Roster scraped: ${Object.keys(result).length} days, ${totalEntries} entries`);
  return result;
}

async function scrapeBlackCatRoster(site) {
  const resp = await fetch(site.rosterUrl, { headers: { 'User-Agent': UA } });
  if (!resp.ok) throw new Error(`Black Cat roster fetch failed: ${resp.status}`);
  const html = await resp.text();

  const aest = getAEDTDate();
  const dayMap = { monday: 1, tuesday: 2, wednesday: 3, thursday: 4, friday: 5, saturday: 6, sunday: 0 };
  const currentDow = aest.getDay();

  // Split by roster sections: "Thursday night time", "Friday day time" etc.
  const sectionRe = /class="roster-title">(\w+)\s+(day|night)\s+time/gi;
  const sections = [];
  let sm;
  while ((sm = sectionRe.exec(html)) !== null) {
    sections.push({ dayName: sm[1].toLowerCase(), shift: sm[2].toLowerCase(), pos: sm.index });
  }

  const result = {};
  for (let i = 0; i < sections.length; i++) {
    const sec = sections[i];
    const sectionHtml = html.substring(sec.pos, i + 1 < sections.length ? sections[i + 1].pos : html.length);

    // Calculate date for this day
    const targetDow = dayMap[sec.dayName];
    if (targetDow === undefined) continue;
    let dayOffset = targetDow - currentDow;
    if (dayOffset < 0) dayOffset += 7;
    // Night shift: same calendar day as the day name
    const targetDate = new Date(aest);
    targetDate.setDate(targetDate.getDate() + dayOffset);
    const dateStr = fmtDate(targetDate);

    const start = sec.shift === 'day' ? '06:00' : '18:00';
    const end = sec.shift === 'day' ? '18:00' : '06:00';

    // Extract girl names from this section
    const nameRe = /class="girl-name">([^<]+)/gi;
    let m;
    while ((m = nameRe.exec(sectionHtml)) !== null) {
      const name = m[1].trim();
      if (!result[dateStr]) result[dateStr] = [];
      // Check if this girl already has an entry for this date (day + night shift)
      const existing = result[dateStr].find(e => e.name === name);
      if (existing) {
        // Extend: if day shift exists and now adding night, set start to day start, end to night end
        if (sec.shift === 'night') existing.end = end;
        else existing.start = start;
      } else {
        result[dateStr].push({ name, start, end });
      }
    }
  }

  const totalEntries = Object.values(result).reduce((s, e) => s + e.length, 0);
  console.log(`[Black Cat] Roster scraped: ${Object.keys(result).length} days, ${totalEntries} entries`);
  return result;
}

async function scrapeBellevue12Roster(site) {
  const resp = await fetch(site.rosterUrl, { headers: { 'User-Agent': UA } });
  if (!resp.ok) throw new Error(`Bellevue 12 roster fetch failed: ${resp.status}`);
  const html = await resp.text();

  const aest = getAEDTDate();
  const todayStr = fmtDate(aest);

  // Bellevue 12 roster: girl descriptions contain "Name Nationality Xcm tall details"
  // Can appear in <h3>, plain text, or mixed HTML. Extract all lines with "cm tall" or nationality keywords.
  const text = html.replace(/<[^>]+>/g, '\n').replace(/&[^;]+;/g, ' ');
  const lines = text.split('\n').map(l => l.trim()).filter(l => l.length > 10);
  const entries = [];
  const natWords = /Singapore|Taiwan|Chinese|Korean|Japanese|Thai|Vietnamese|Taiwanese|Malaysian|Hong Kong|Macau|Indonesian|Chese/i;

  for (const line of lines) {
    if (!/cm\s*tall|cup|busty|service|slim|sexy|fluent/i.test(line) && !natWords.test(line)) continue;
    if (/Today|Roster|Recent|Comment|Phone|Address|Time|Rate|Booking/i.test(line)) continue;
    const nameMatch = line.match(/^([A-Z][A-Za-z]+)/);
    if (nameMatch && nameMatch[1].length > 1 && !entries.some(e => e.name === nameMatch[1])) {
      entries.push({ name: nameMatch[1], start: '10:00', end: '02:00' });
    }
  }

  const result = {};
  if (entries.length) result[todayStr] = entries;
  console.log(`[Bellevue 12] Roster scraped: ${entries.length} girls for today`);
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

/* ── Sync Log (accumulates results across daily runs) ── */

const SYNC_LOG_PATH = 'sync-log.json';

async function loadSyncLog(env) {
  try {
    const { content, sha } = await ghGet(env, SYNC_LOG_PATH);
    return { log: content, sha };
  } catch {
    return { log: { runs: [] }, sha: null };
  }
}

async function saveSyncLog(env, log, sha) {
  await ghPut(env, SYNC_LOG_PATH, log, sha, 'Update sync log');
}

/* ── SEO: Regenerate sitemap.xml ── */

async function regenerateSitemap(env) {
  const today = new Date().toISOString().split('T')[0];
  const siteList = [SITES.empire, SITES.club, SITES.kyoto206, SITES.sakura57, SITES.top127, SITES.fantasyclub35, SITES.city429, SITES.pennys77, SITES.thegoldenapple, SITES.blackcatparlour, SITES.bellevue12, SITES.thegatewayclub, SITES.marrickvillebrothel, SITES.springhouse, SITES.stiletto, SITES.wivesonly, SITES.jinia];

  let urls = [`<url><loc>https://brothelsearch.com/</loc><lastmod>${today}</lastmod><priority>1.0</priority></url>`];

  // Feature pages
  urls.push(`<url><loc>https://brothelsearch.com/profiles</loc><lastmod>${today}</lastmod><priority>0.95</priority></url>`);
  urls.push(`<url><loc>https://brothelsearch.com/working-now</loc><lastmod>${today}</lastmod><priority>0.9</priority></url>`);
  urls.push(`<url><loc>https://brothelsearch.com/compare</loc><lastmod>${today}</lastmod><priority>0.85</priority></url>`);
  urls.push(`<url><loc>https://brothelsearch.com/analytics</loc><lastmod>${today}</lastmod><priority>0.8</priority></url>`);

  // City page
  urls.push(`<url><loc>https://brothelsearch.com/sydney/</loc><lastmod>${today}</lastmod><priority>0.9</priority></url>`);

  // Region pages
  const regionsSeen = new Set();
  for (const r of Object.values(VENUE_REGION_SLUGS)) {
    if (!regionsSeen.has(r)) { regionsSeen.add(r); urls.push(`<url><loc>https://brothelsearch.com/sydney/${r}/</loc><lastmod>${today}</lastmod><priority>0.85</priority></url>`); }
  }

  // Venue pages
  for (const [venueId, site] of Object.entries(VENUE_MAP)) {
    const region = VENUE_REGION_SLUGS[venueId] || 'other';
    const suburb = VENUE_SUBURBS[venueId] || 'sydney';
    urls.push(`<url><loc>https://brothelsearch.com/sydney/${region}/${suburb}/${venueId}/</loc><lastmod>${today}</lastmod><priority>0.8</priority></url>`);
  }

  // Girl profiles
  for (const site of siteList) {
    try {
      const { data } = await loadData(env, site);
      for (const g of data.girls || []) {
        const slug = (g.name || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/-+$/g, '');
        if (!slug) continue;
        const venueId = Object.keys(SITES).find(k => SITES[k] === site);
        const id = venueId === 'city429' ? '429city' : venueId;
        const region = VENUE_REGION_SLUGS[id] || 'other';
        const suburb = VENUE_SUBURBS[id] || 'sydney';
        const country = (Array.isArray(g.country) ? g.country[0] : g.country || 'other').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/-+$/g, '');
        const lastmod = g.lastRostered || g.startDate || today;
        urls.push(`<url><loc>https://brothelsearch.com/sydney/${region}/${suburb}/${id}/${country || 'other'}/${slug}</loc><lastmod>${lastmod}</lastmod><priority>0.7</priority></url>`);
      }
    } catch (e) { console.error(`[Sitemap] Error loading ${site.name}:`, e); }
  }

  const xml = '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n' + urls.join('\n') + '\n</urlset>';

  // Load existing sitemap to get SHA
  let sha = null;
  try {
    const r = await fetch(`${GH_API}/repos/${REPO}/contents/sitemap.xml`, { headers: ghHeaders(env) });
    if (r.ok) { const d = await r.json(); sha = d.sha; }
  } catch {}

  await ghPutRaw(env, 'sitemap.xml', xml, sha, `[SEO] Auto-update sitemap.xml (${urls.length} URLs)`);
  console.log(`Sitemap updated: ${urls.length} URLs`);
  return urls.length;
}

/* ── Photo health check ── */

const MAX_PHOTO_CHECKS_PER_RUN = 20;

async function checkBrokenPhotos(env, site) {
  const { data, sha } = await loadData(env, site);
  const girls = data.girls || [];
  const calendar = data.calendar || {};

  // Find currently rostered girl names
  const rosteredNames = new Set();
  for (const [name, slots] of Object.entries(calendar)) {
    if (name === '_published') continue;
    if (slots && typeof slots === 'object') rosteredNames.add(name);
  }

  // Step 1: Flag dead profiles as deleted — oldUrl is 404 and not rostered
  const removeCandidates = girls.filter(g => g.oldUrl && !rosteredNames.has(g.name) && g.originalSite !== 'Exists' && g.deleted !== 'Yes');
  let removed = 0;
  for (const g of removeCandidates) {
    try {
      const resp = await fetch(g.oldUrl, { method: 'HEAD', headers: { 'User-Agent': UA }, redirect: 'follow' });
      if (resp.status === 404) {
        g.deleted = 'Yes';
        g.deletedAt = new Date().toISOString();
        removed++;
        console.log(`[${site.name}] Flagged dead profile: ${g.name} (404, not rostered)`);
      }
      await new Promise(r => setTimeout(r, 300));
    } catch (e) { /* keep on error */ }
  }

  // Step 2: Check photos — re-scrape to detect broken URLs and stale photos
  const girlsWithPhotos = data.girls.filter(g => g.photos && g.photos.length > 0 && g.oldUrl && g.deleted !== 'Yes');
  let checked = 0, fixed = 0;
  // Shuffle to check different girls each run
  for (let i = girlsWithPhotos.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [girlsWithPhotos[i], girlsWithPhotos[j]] = [girlsWithPhotos[j], girlsWithPhotos[i]];
  }
  const toCheck = girlsWithPhotos.slice(0, MAX_PHOTO_CHECKS_PER_RUN);

  for (const g of toCheck) {
    try {
      checked++;

      // Re-scrape the profile page to get current photos
      let newPhotos = [];
      if (site.rosterFormat === 'empire' || site.rosterFormat === 'club') {
        const idMatch = g.oldUrl.match(/\/Girls\/(\d+)/);
        if (idMatch) {
          const profile = await scrapeGirlProfile(site, idMatch[1]);
          newPhotos = profile.images || [];
        }
      } else {
        const profile = await scrapeWpProfile(site, g.oldUrl, g.name);
        newPhotos = profile.images || [];
      }

      if (newPhotos.length === 0) continue;

      // Compare with stored photos — update if different
      const oldSet = new Set(g.photos);
      const newSet = new Set(newPhotos);
      const changed = newPhotos.length !== g.photos.length || newPhotos.some(p => !oldSet.has(p));
      if (changed) {
        g.photos = newPhotos;
        fixed++;
        console.log(`[${site.name}] Updated ${g.name}: ${newPhotos.length} photos (was ${oldSet.size})`);
      }

      await new Promise(r => setTimeout(r, 500));
    } catch (e) {
      console.error(`[${site.name}] Photo check error for ${g.name}:`, e.message);
    }
  }

  if (fixed > 0 || removed > 0) {
    const parts = [];
    if (removed > 0) parts.push(`Remove ${removed} dead profile${removed > 1 ? 's' : ''}`);
    if (fixed > 0) parts.push(`Update ${fixed} stale photo${fixed > 1 ? 's' : ''}`);
    await ghPut(env, site.jsonPath, data, sha, `[${site.name}] ${parts.join(', ')}`);
  }

  console.log(`[${site.name}] Photo check: ${checked} checked, ${fixed} fixed, ${removed} removed`);
  return { checked, fixed, removed };
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
  let updated = 0;
  for (const g of existing) {
    const shouldBe = activeNames.has(g.name) ? 'Exists' : '';
    if (g.originalSite !== shouldBe) {
      g.originalSite = shouldBe;
      siteChanged = true;
      updated++;
    }
  }

  // Backfill missing fields + detect name changes from listing cards
  const cardsByUrl = {};
  for (const c of cards) cardsByUrl[`${site.girlsUrl}/${c.id}`] = c;
  const cardsByName = {};
  for (const c of cards) cardsByName[c.name] = c;
  for (const g of existing) {
    // Match by URL first (handles renames)
    const c = (g.oldUrl && cardsByUrl[g.oldUrl]) || cardsByName[g.name];
    if (!c) continue;
    let filled = false;
    // Name change detection — URL matches but name differs
    if (g.oldUrl && cardsByUrl[g.oldUrl] && cardsByUrl[g.oldUrl].name !== g.name) {
      const oldName = g.name;
      const newName = cardsByUrl[g.oldUrl].name;
      // Update calendar key too
      if (data.calendar && data.calendar[oldName]) {
        data.calendar[newName] = data.calendar[oldName];
        delete data.calendar[oldName];
      }
      g.name = newName;
      knownNames.delete(oldName);
      knownNames.add(newName);
      console.log(`[${site.name}] Renamed: ${oldName} -> ${newName}`);
      siteChanged = true; updated++;
    }
    if (!g.age && c.age) { g.age = c.age; filled = true; }
    if (!g.body && c.body) { g.body = c.body; filled = true; }
    if (!g.cup && c.cup) { g.cup = c.cup; filled = true; }
    if (!g.height && c.height) { g.height = c.height; filled = true; }
    if (filled) { siteChanged = true; updated++; }
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
    return { added: 0, remaining: 0, names: [], updated };
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

      // Photos: embed source URLs directly or upload to GitHub
      const photos = [];
      if (site.embedPhotos) {
        photos.push(...profile.images);
      } else {
        for (let i = 0; i < profile.images.length; i++) {
          try {
            const ext = (profile.images[i].match(/\.(jpe?g|png|webp|gif)$/i) || [])[1] || 'jpeg';
            const path = `${site.imgPrefix}/${card.name}/${card.name}_${i + 1}.${ext}`;
            const ghUrl = await uploadImage(env, profile.images[i], path);
            photos.push(ghUrl);
            await new Promise(r => setTimeout(r, 500));
          } catch (e) {
            console.error(`[${site.name}] Image error ${card.name} #${i + 1}: ${e.message}`);
          }
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

  return { added: addedNames.length, remaining, names: addedNames, updated };
}

/* ── Sync: Calendar ── */

async function syncCalendar(env, site) {
  const scraped = site.rosterFormat === 'kyoto206'
    ? await scrapeKyoto206Roster(site)
    : site.rosterFormat === 'top127'
    ? await scrapeTop127Roster(site)
    : site.rosterFormat === 'fantasyclub35'
    ? await scrapeFantasyClub35Roster(site)
    : site.rosterFormat === '429city'
    ? await scrape429CityRoster(site)
    : site.rosterFormat === 'pennys77'
    ? await scrapePennys77Roster(site, env)
    : site.rosterFormat === 'thegoldenapple'
    ? await scrapeGoldenAppleRoster(site)
    : site.rosterFormat === 'blackcatparlour'
    ? await scrapeBlackCatRoster(site)
    : site.rosterFormat === 'bellevue12'
    ? await scrapeBellevue12Roster(site)
    : site.rosterFormat === 'stiletto-api'
    ? await scrapeStilettoRoster(site, env)
    : site.rosterFormat === 'jinia'
    ? await scrapeJiniaRoster(site)
    : site.rosterFormat === 'marrickville'
    ? await scrapeMarrickvilleRoster(site)
    : site.rosterFormat === 'springhouse'
    ? await scrapeSpringHouseRoster(site)
    : site.rosterFormat === 'wivesonly'
    ? await scrapeWivesOnlyRoster(site)
    : site.rosterFormat === 'gateway'
    ? await scrapeGatewayRoster(site)
    : await scrapeRoster(site);
  const { data, sha } = await loadData(env, site);

  if (Object.keys(scraped).length === 0) {
    // No new scraped data, but still update lastRostered from existing calendar
    const calendar = data.calendar || {};
    const today = fmtDate(getAEDTDate());
    let changed = false;
    for (const g of (data.girls || [])) {
      const girlCal = calendar[g.name];
      if (girlCal && typeof girlCal === 'object') {
        for (const d of Object.keys(girlCal)) {
          if (d.startsWith('_')) continue;
          if (!g.lastRostered || d > g.lastRostered) { g.lastRostered = d; changed = true; }
        }
      }
      if (!g.lastRostered && g.startDate) { g.lastRostered = g.startDate; changed = true; }
    }
    if (changed) {
      data.lastCalendarSync = new Date().toISOString();
      await ghPut(env, site.jsonPath, data, sha, `[${site.name}] Update lastRostered from existing calendar`);
    }
    return changed;
  }
  const calendar = data.calendar || {};
  const validNames = new Set((data.girls || []).map(g => g.name));

  let changed = false;

  const is429City = scraped._429cityUrls;
  delete scraped._429cityUrls;

  // Auto-create profiles for unmatched rostered names by scanning listing pages
  {
    const unmatchedNames = new Set();
    if (!is429City) {
      for (const entries of Object.values(scraped)) {
        for (const { name } of entries) {
          if (!validNames.has(name)) unmatchedNames.add(name);
        }
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
            if (!pName || !unmatchedNames.has(pName) || !isValidGirlName(pName)) continue;

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
            if (site.embedPhotos) {
              entry.photos = [...profile.images];
            } else {
              for (let i = 0; i < profile.images.length; i++) {
                try {
                  const ext = (profile.images[i].match(/\.(jpe?g|png|webp|gif)$/i) || [])[1] || 'jpeg';
                  const imgPath = `${site.imgPrefix}/${pName}/${pName}_${i + 1}.${ext}`;
                  const ghUrl = await uploadImage(env, profile.images[i], imgPath);
                  entry.photos.push(ghUrl);
                  await new Promise(r => setTimeout(r, 500));
                } catch (e) { console.error(`[${site.name}] Image error ${pName}: ${e.message}`); }
              }
            }
            classifyGirl(entry);
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
            if (site.embedPhotos) {
              photos.push(...profile.images);
            } else {
              for (let i = 0; i < profile.images.length; i++) {
                try {
                  const ext = (profile.images[i].match(/\.(jpe?g|png|webp|gif)$/i) || [])[1] || 'jpeg';
                  const imgPath = `${site.imgPrefix}/${card.name}/${card.name}_${i + 1}.${ext}`;
                  const ghUrl = await uploadImage(env, profile.images[i], imgPath);
                  photos.push(ghUrl);
                  await new Promise(r => setTimeout(r, 500));
                } catch (e) { console.error(`[${site.name}] Image error ${card.name}: ${e.message}`); }
              }
            }
            entry.photos = photos;
            entry.labels = extractLabels(profile.desc);
            entry.lastModified = now;
            entry.lastRostered = '';
            classifyGirl(entry);
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
  const girlsByUrl = {};
  for (const g of (data.girls || [])) {
    girlsByName[g.name] = g;
    if (g.oldUrl) girlsByUrl[g.oldUrl.replace(/\/$/, '/').toLowerCase()] = g;
  }

  for (const [dateStr, entries] of Object.entries(scraped)) {
    if (dateStr.startsWith('_')) continue;
    for (const entry of entries) {
      let girl;
      if (is429City && entry.url) {
        // 429 City: match by URL
        girl = girlsByUrl[entry.url.replace(/\/$/, '/').toLowerCase()];
      } else {
        // Other venues: match by name
        girl = girlsByName[entry.name];
      }
      if (!girl) continue;

      const { start, end } = entry;
      if (!calendar[girl.name]) calendar[girl.name] = {};

      const existing = calendar[girl.name][dateStr];
      if (!existing || existing.start !== start || existing.end !== end) {
        calendar[girl.name][dateStr] = { start, end };
        changed = true;
      }
    }
  }

  // Remove girls from scraped dates if they're no longer in the roster (skip URL-based venues like 429 City)
  if (!is429City) {
    for (const [dateStr, entries] of Object.entries(scraped)) {
      if (dateStr.startsWith('_')) continue;
      const scrapedNames = new Set(entries.map(e => e.name));
      for (const [girlName, girlCal] of Object.entries(calendar)) {
        if (girlName.startsWith('_') || typeof girlCal !== 'object') continue;
        if (girlCal[dateStr] && !scrapedNames.has(girlName)) {
          delete girlCal[dateStr];
          changed = true;
        }
      }
    }
  }

  // Update lastRostered: use latest roster date (including future dates)
  const today = fmtDate(getAEDTDate());
  for (const [dateStr, entries] of Object.entries(scraped)) {
    if (dateStr.startsWith('_')) continue;
    for (const entry of entries) {
      let girl;
      if (is429City && entry.url) {
        girl = girlsByUrl[entry.url.replace(/\/$/, '/').toLowerCase()];
      } else {
        girl = girlsByName[entry.name];
      }
      if (girl && (!girl.lastRostered || dateStr > girl.lastRostered)) {
        girl.lastRostered = dateStr;
        changed = true;
      }
    }
  }

  // Ensure all girls have lastRostered: use latest calendar date (including future), fall back to startDate
  for (const g of (data.girls || [])) {
    const girlCal = calendar[g.name];
    if (girlCal && typeof girlCal === 'object') {
      for (const d of Object.keys(girlCal)) {
        if (d.startsWith('_')) continue;
        if (!g.lastRostered || d > g.lastRostered) {
          g.lastRostered = d;
          changed = true;
        }
      }
    }
    if (!g.lastRostered && g.startDate) {
      g.lastRostered = g.startDate;
      changed = true;
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

  // Historical roster data is retained for analytics (price trends, busiest days, girl retention)

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

/* ── Daily Digest Notifications ── */

const SB_URL = 'https://blhwekuidksxiaickeck.supabase.co';

function sbHeaders(env) {
  return { apikey: env.SUPABASE_SERVICE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`, 'Content-Type': 'application/json' };
}

async function sendDailyDigest(env) {
  const headers = sbHeaders(env);

  // Load all users with favourites
  const favRes = await fetch(`${SB_URL}/rest/v1/user_favorites?select=user_id,old_url`, { headers });
  const allFavs = await favRes.json();
  if (!allFavs.length) { console.log('[Digest] No favourites found'); return; }

  // Group favourites by user
  const userFavs = {};
  for (const f of allFavs) {
    if (!userFavs[f.user_id]) userFavs[f.user_id] = [];
    userFavs[f.user_id].push(f.old_url);
  }

  // Load all venue data + today's roster
  const siteList = [SITES.empire, SITES.club, SITES.kyoto206, SITES.sakura57, SITES.top127, SITES.fantasyclub35, SITES.city429, SITES.pennys77, SITES.thegoldenapple, SITES.blackcatparlour, SITES.bellevue12, SITES.thegatewayclub, SITES.marrickvillebrothel, SITES.springhouse, SITES.stiletto, SITES.wivesonly, SITES.jinia];
  const venueIds = ['ginzaempire', 'ginzaclub', 'kyoto206', 'sakura57', 'top127', 'fantasyclub35', '429city', 'pennys77', 'thegoldenapple', 'blackcatparlour', 'bellevue12', 'thegatewayclub', 'marrickvillebrothel', 'springhouse', 'stiletto', 'wivesonly', 'jinia'];
  const allGirls = [];
  const todayStr = fmtDate(getAEDTDate());

  for (let i = 0; i < siteList.length; i++) {
    try {
      const { data } = await loadData(env, siteList[i]);
      const calendar = data.calendar || {};
      for (const g of data.girls || []) {
        g.venue = venueIds[i];
        g.venueName = siteList[i].name;
        g.rosteredToday = !!(calendar[g.name] && calendar[g.name][todayStr]);
        allGirls.push(g);
      }
    } catch (e) { console.error(`[Digest] Error loading ${siteList[i].name}:`, e); }
  }

  // Load user preferences for match scoring
  const prefsRes = await fetch(`${SB_URL}/rest/v1/user_preferences?select=*`, { headers });
  const allPrefs = await prefsRes.json();
  const prefsMap = {};
  for (const p of allPrefs) prefsMap[p.id] = p;

  // Load user roles
  const rolesRes = await fetch(`${SB_URL}/rest/v1/user_roles?select=id,role`, { headers });
  const allRoles = await rolesRes.json();
  const roleMap = {};
  for (const r of allRoles) roleMap[r.id] = r.role;

  // Load active subscriptions
  const subsRes = await fetch(`${SB_URL}/rest/v1/user_subscriptions?status=eq.active&select=user_id`, { headers });
  const allSubs = await subsRes.json();
  const activeSubs = new Set((allSubs || []).map(s => s.user_id));

  // Load user emails — only for admins and subscribed members
  const userIds = Object.keys(userFavs);
  const userEmails = {};
  for (const uid of userIds) {
    const isAdmin = roleMap[uid] === 'admin';
    const isSubscribed = activeSubs.has(uid);
    if (!isAdmin && !isSubscribed) continue; // skip unsubscribed members
    try {
      const res = await fetch(`${SB_URL}/auth/v1/admin/users/${uid}`, { headers: { apikey: env.SUPABASE_SERVICE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}` } });
      const u = await res.json();
      if (u.email) userEmails[uid] = { email: u.email, name: u.user_metadata?.display_name || u.user_metadata?.name || u.email.split('@')[0] };
    } catch {}
  }

  // New girls (startDate in last 7 days)
  const weekAgo = new Date();
  weekAgo.setDate(weekAgo.getDate() - 7);
  const cutoffStr = weekAgo.toISOString().split('T')[0];
  const newGirls = allGirls.filter(g => g.startDate && g.startDate >= cutoffStr);

  // Process each user
  for (const [userId, favUrls] of Object.entries(userFavs)) {
    const notifications = [];
    const userInfo = userEmails[userId];
    if (!userInfo) continue;

    // All favourite girls for this user
    const allFavGirls = allGirls.filter(g => g.oldUrl && favUrls.includes(g.oldUrl));
    const favWorking = allFavGirls.filter(g => g.rosteredToday);
    const favNotWorking = allFavGirls.filter(g => !g.rosteredToday);

    // New girls matching >= 90%
    const prefs = prefsMap[userId];
    const matchesWorking = [];
    const matchesNotWorking = [];
    if (prefs && newGirls.length) {
      for (const g of newGirls) {
        const score = scoreGirlWorker(g, prefs);
        if (score >= 90) {
          const entry = { ...g, matchScore: score };
          if (g.rosteredToday) matchesWorking.push(entry);
          else matchesNotWorking.push(entry);
        }
      }
    }

    // Build single digest notification for bell UI
    const parts = [];
    if (favWorking.length) parts.push(favWorking.length + ' favourite' + (favWorking.length !== 1 ? 's' : '') + ' working today');
    if (matchesWorking.length) parts.push(matchesWorking.length + ' new match' + (matchesWorking.length !== 1 ? 'es' : '') + ' working today');
    if (matchesNotWorking.length) parts.push(matchesNotWorking.length + ' new match' + (matchesNotWorking.length !== 1 ? 'es' : ''));

    if (!parts.length && !allFavGirls.length) continue;

    // Insert single digest notification (skip if one already exists today)
    if (parts.length) {
      const dupCheck = await fetch(
        `${SB_URL}/rest/v1/notifications?user_id=eq.${userId}&title=eq.Daily%20Digest&created_at=gte.${todayStr}T00:00:00Z&select=id&limit=1`,
        { headers }
      );
      const existing = await dupCheck.json();
      if (!existing.length) {
        const digestNotif = {
          user_id: userId, type: 'favourite_rostered',
          title: 'Daily Digest',
          body: parts.join(', ') + '. Check your email for details.',
          venue: null, girl_name: null,
        };
        await fetch(`${SB_URL}/rest/v1/notifications`, { method: 'POST', headers, body: JSON.stringify(digestNotif) });
      }
    }

    // Send email via Resend (always send if user has favourites)
    if (env.RESEND_API_KEY && (favWorking.length || favNotWorking.length || matchesWorking.length || matchesNotWorking.length)) {
      const emailHtml = buildDigestEmail(userInfo.name, { favWorking, favNotWorking, matchesWorking, matchesNotWorking });
      const workingCount = favWorking.length + matchesWorking.length;
      const subject = workingCount > 0
        ? 'Daily Digest — ' + workingCount + ' working today'
        : 'Daily Digest — Your favourites update';
      try {
        await fetch('https://api.resend.com/emails', {
          method: 'POST',
          headers: { Authorization: `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ from: 'Brothel Search <info@travanixlabs.com>', to: userInfo.email, subject, html: emailHtml }),
        });
        console.log(`[Digest] Email sent to ${userInfo.email}`);
      } catch (e) { console.error(`[Digest] Email error for ${userInfo.email}:`, e); }
    }
  }

  console.log(`[Digest] Processed ${userIds.length} users`);
}

function scoreGirlWorker(girl, prefs) {
  if (!prefs) return 0;
  let score = 0, activeWeight = 0;
  if (prefs.age_min != null && prefs.age_max != null && (prefs.age_min !== 18 || prefs.age_max !== 33)) {
    activeWeight += 10;
    if (girl.age && parseInt(girl.age) >= prefs.age_min && parseInt(girl.age) <= prefs.age_max) score += 10;
  }
  if (prefs.body_min != null && prefs.body_max != null && (prefs.body_min !== 4 || prefs.body_max !== 10)) {
    activeWeight += 10;
    if (girl.body && parseInt(girl.body) >= prefs.body_min && parseInt(girl.body) <= prefs.body_max) score += 10;
  }
  if (prefs.height_min != null && prefs.height_max != null && (prefs.height_min !== 150 || prefs.height_max !== 175)) {
    activeWeight += 2;
    if (girl.height && parseInt(girl.height) >= prefs.height_min && parseInt(girl.height) <= prefs.height_max) score += 2;
  }
  if (prefs.cup_min || prefs.cup_max) {
    activeWeight += 2;
    const CUP_ORDER = ['A','B','C','D','DD','E','F','G','H'];
    const ci = CUP_ORDER.indexOf((girl.cup||'').toUpperCase());
    const mi = CUP_ORDER.indexOf((prefs.cup_min||'').toUpperCase());
    const xi = CUP_ORDER.indexOf((prefs.cup_max||'').toUpperCase());
    if (ci >= 0 && (mi < 0 || ci >= mi) && (xi < 0 || ci <= xi)) score += 2;
  }
  if (prefs.countries && prefs.countries.length > 0) {
    activeWeight += 15;
    const gc = Array.isArray(girl.country) ? girl.country : (girl.country ? [girl.country] : []);
    if (gc.length > 0) { const matched = gc.filter(c => prefs.countries.includes(c)).length; score += (matched / gc.length) * 15; }
  }
  if (activeWeight === 0) return 0;
  return Math.round((score / activeWeight) * 100);
}

function girlProfileUrl(g) {
  const region = VENUE_REGION_SLUGS[g.venue] || 'other';
  const suburb = VENUE_SUBURBS[g.venue] || 'sydney';
  const country = (Array.isArray(g.country) ? g.country[0] : g.country || 'other').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/-+$/g, '') || 'other';
  const slug = (g.name || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/-+$/g, '');
  return `https://brothelsearch.com/sydney/${region}/${suburb}/${g.venue}/${country}/${slug}`;
}

function girlCardHtml(g, statusColor, statusText, extra) {
  const photo = g.photos && g.photos[0] ? `https://wsrv.nl/?url=${encodeURIComponent(g.photos[0])}&w=80&h=106&fit=cover&output=webp&q=80` : '';
  const countries = Array.isArray(g.country) ? g.country.join(', ') : (g.country || '');
  const rates = [g.val1 ? '$' + g.val1 : '', g.val2 ? '$' + g.val2 : '', g.val3 ? '$' + g.val3 : ''].filter(Boolean).join(' / ');
  const stats = [g.age ? 'Age ' + g.age : '', g.height ? g.height + 'cm' : '', g.cup ? g.cup + ' cup' : ''].filter(Boolean).join(' \u00b7 ');
  const profileLink = girlProfileUrl(g);

  return `<tr><td style="padding:8px 0;border-bottom:1px solid #1a1a2e">
    <a href="${profileLink}" style="text-decoration:none;color:inherit;display:block">
    <table cellpadding="0" cellspacing="0" border="0" width="100%"><tr>
      ${photo ? `<td width="80" valign="top" style="padding-right:12px"><img src="${photo}" width="80" height="106" style="border-radius:8px;display:block;object-fit:cover" alt="${g.name || ''}"></td>` : ''}
      <td valign="top">
        <div style="font-size:16px;font-weight:700;color:#c9952c;margin-bottom:2px">${g.name || ''}</div>
        <div style="font-size:12px;color:#999;margin-bottom:4px">${g.venueName || ''}${countries ? ' \u00b7 ' + countries : ''}</div>
        ${stats ? `<div style="font-size:11px;color:#777;margin-bottom:4px">${stats}</div>` : ''}
        ${rates ? `<div style="font-size:12px;color:#c9952c;margin-bottom:4px">${rates}</div>` : ''}
        <div style="display:inline-block;font-size:10px;font-weight:700;color:${statusColor};background:${statusColor}15;border:1px solid ${statusColor}40;padding:2px 8px;border-radius:4px;letter-spacing:1px">${statusText}</div>
        ${extra || ''}
      </td>
    </tr></table>
    </a>
  </td></tr>`;
}

function buildDigestEmail(name, { favWorking, favNotWorking, matchesWorking, matchesNotWorking }) {
  let html = `<div style="font-family:Georgia,serif;max-width:600px;margin:0 auto;background:#0e0e16;color:#e0d6c8;padding:32px;border-radius:12px">`;
  html += `<div style="text-align:center;margin-bottom:24px"><span style="font-size:24px;font-weight:700;color:#c9952c;letter-spacing:2px">BROTHEL SEARCH</span></div>`;
  html += `<p style="font-size:16px;margin-bottom:24px">Hi ${name},</p>`;

  const hasWorking = favWorking.length || matchesWorking.length;
  const hasNotWorking = favNotWorking.length || matchesNotWorking.length;

  // ── WORKING TODAY ──
  if (hasWorking) {
    html += `<div style="font-size:13px;font-weight:700;color:#00c864;text-transform:uppercase;letter-spacing:2px;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid #1a1a2e">&#9679; Working Today</div>`;
    html += `<table cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-bottom:24px">`;
    for (const g of favWorking) {
      html += girlCardHtml(g, '#c9952c', 'FAVOURITE');
    }
    for (const g of matchesWorking) {
      html += girlCardHtml(g, '#00c864', g.matchScore + '% MATCH', `<span style="font-size:10px;color:#00c864;margin-left:6px">NEW</span>`);
    }
    html += `</table>`;
  }

  // ── NOT WORKING TODAY ──
  if (hasNotWorking) {
    html += `<div style="font-size:13px;font-weight:700;color:#666;text-transform:uppercase;letter-spacing:2px;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid #1a1a2e">Not Working Today</div>`;
    html += `<table cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-bottom:24px">`;
    for (const g of favNotWorking) {
      html += girlCardHtml(g, '#555', 'FAVOURITE');
    }
    for (const g of matchesNotWorking) {
      html += girlCardHtml(g, '#3c78ff', g.matchScore + '% MATCH', `<span style="font-size:10px;color:#3c78ff;margin-left:6px">NEW</span>`);
    }
    html += `</table>`;
  }

  html += `<div style="text-align:center;margin-top:24px"><a href="https://brothelsearch.com/working-now" style="display:inline-block;padding:12px 32px;background:#c9952c;color:#0e0e16;text-decoration:none;border-radius:8px;font-weight:700;letter-spacing:1px;font-size:14px">See Who's Working Now</a></div>`;
  html += `<p style="font-size:11px;color:#555;margin-top:24px;text-align:center">You're receiving this because you have favourites on Brothel Search.</p>`;
  html += `</div>`;
  return html;
}

/* ── Social bot pre-rendering ── */

const BOT_UA = /facebookexternalhit|twitterbot|linkedinbot|slackbot|whatsapp|telegrambot|discordbot|pinterest|snapchat/i;

const VENUE_MAP = {
  ginzaempire: SITES.empire, ginzaclub: SITES.club, kyoto206: SITES.kyoto206,
  sakura57: SITES.sakura57, top127: SITES.top127, fantasyclub35: SITES.fantasyclub35,
  '429city': SITES.city429,
  pennys77: SITES.pennys77, thegoldenapple: SITES.thegoldenapple,
  blackcatparlour: SITES.blackcatparlour, bellevue12: SITES.bellevue12,
  thegatewayclub: SITES.thegatewayclub, marrickvillebrothel: SITES.marrickvillebrothel,
  springhouse: SITES.springhouse, stiletto: SITES.stiletto,
  wivesonly: SITES.wivesonly, jinia: SITES.jinia,
};

const VENUE_SUBURBS = {
  ginzaempire: 'surryhills', ginzaclub: 'surryhills', kyoto206: 'surryhills',
  sakura57: 'surryhills', top127: 'chippendale', fantasyclub35: 'annandale', '429city': 'haymarket',
  pennys77: 'newtown', thegoldenapple: 'surryhills', blackcatparlour: 'surryhills', bellevue12: 'surryhills',
  thegatewayclub: 'petersham', marrickvillebrothel: 'marrickville', springhouse: 'marrickville',
  stiletto: 'camperdown', wivesonly: 'stpeters', jinia: 'strathfieldsouth',
};
const VENUE_REGION_SLUGS = {
  ginzaempire: 'cbdandcentral', ginzaclub: 'cbdandcentral', kyoto206: 'cbdandcentral',
  sakura57: 'cbdandcentral', top127: 'cbdandcentral', fantasyclub35: 'innerwest', '429city': 'cbdandcentral',
  pennys77: 'innerwest', thegoldenapple: 'cbdandcentral', blackcatparlour: 'cbdandcentral', bellevue12: 'cbdandcentral',
  thegatewayclub: 'innerwest', marrickvillebrothel: 'innerwest', springhouse: 'innerwest',
  stiletto: 'innerwest', wivesonly: 'innerwest', jinia: 'westernsuburbs',
};

const VENUE_NAMES = {
  ginzaempire: 'Ginza Empire', ginzaclub: 'Ginza Club', kyoto206: 'Kyoto 206',
  sakura57: 'Sakura 57', top127: 'Top 127', fantasyclub35: 'Fantasy Club 35', '429city': '429 City',
  pennys77: "Penny's 77", thegoldenapple: 'The Golden Apple', blackcatparlour: 'Black Cat Parlour', bellevue12: 'Bellevue 12',
  thegatewayclub: 'The Gateway Club', marrickvillebrothel: 'Marrickville Brothel', springhouse: 'Spring House',
  stiletto: 'Stiletto', wivesonly: 'Wives Only', jinia: 'Jinia',
};
const SUBURB_NAMES = { surryhills: 'Surry Hills', chippendale: 'Chippendale', annandale: 'Annandale', haymarket: 'Haymarket', newtown: 'Newtown', petersham: 'Petersham', marrickville: 'Marrickville', camperdown: 'Camperdown', stpeters: 'St Peters', strathfieldsouth: 'Strathfield South' };
const REGION_NAMES_WORKER = { cbdandcentral: 'CBD & Central', innerwest: 'Inner West', westernsuburbs: 'Western Suburbs' };

function botHtml(title, desc, url, jsonLd) {
  return `<!DOCTYPE html>
<html><head>
<meta charset="UTF-8">
<title>${title}</title>
<meta name="description" content="${desc}">
<meta property="og:type" content="website">
<meta property="og:title" content="${title}">
<meta property="og:description" content="${desc}">
<meta property="og:url" content="${url}">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="${title}">
<meta name="twitter:description" content="${desc}">
<script type="application/ld+json">${JSON.stringify(jsonLd)}</script>
<meta http-equiv="refresh" content="0;url=${url}">
</head><body></body></html>`;
}

async function serveBotLanding(env, pathname) {
  const parts = pathname.replace(/^\//, '').replace(/\/$/, '').split('/');

  // /profiles
  if (parts.length === 1 && parts[0] === 'profiles') {
    const title = 'Browse All Profiles \u2013 Rosters Included | Brothel Search';
    const desc = 'Browse all girl profiles across Australian brothels. Filter by venue, country, availability, pricing and preferences. Photos, rosters and reviews.';
    return new Response(botHtml(title, desc, 'https://brothelsearch.com/profiles', { '@context': 'https://schema.org', '@type': 'WebPage', name: title, description: desc }), { headers: { 'Content-Type': 'text/html; charset=UTF-8' } });
  }

  // /working-now
  if (parts.length === 1 && parts[0] === 'working-now') {
    const title = 'Who\u2019s Working Now \u2013 Live Roster | Brothel Search';
    const desc = 'See which girls are available right now across Sydney brothels. Live roster updated daily.';
    return new Response(botHtml(title, desc, 'https://brothelsearch.com/working-now', { '@context': 'https://schema.org', '@type': 'WebPage', name: title, description: desc }), { headers: { 'Content-Type': 'text/html; charset=UTF-8' } });
  }

  // /compare
  if (parts.length === 1 && parts[0] === 'compare') {
    const title = 'Compare Brothels in Sydney | Brothel Search';
    const desc = 'Compare 7 Sydney brothels side-by-side. Rankings by preference match, pricing, girl count, countries and availability. Ginza Empire, Ginza Club, Kyoto 206, Sakura 57, Top 127, Fantasy Club 35, 429 City.';
    return new Response(botHtml(title, desc, 'https://brothelsearch.com/compare', { '@context': 'https://schema.org', '@type': 'WebPage', name: title, description: desc }), { headers: { 'Content-Type': 'text/html; charset=UTF-8' } });
  }

  // /analytics
  if (parts.length === 1 && parts[0] === 'analytics') {
    const title = 'Analytics \u2013 Data Insights | Brothel Search';
    const desc = 'Data insights across Sydney brothels. Busiest days, country breakdown and roster trends. Members-only analytics.';
    return new Response(botHtml(title, desc, 'https://brothelsearch.com/analytics', { '@context': 'https://schema.org', '@type': 'WebPage', name: title, description: desc }), { headers: { 'Content-Type': 'text/html; charset=UTF-8' } });
  }

  // /roadmap
  if (parts.length === 1 && parts[0] === 'roadmap') {
    const title = 'Roadmap \u2013 Development Timeline | Brothel Search';
    const desc = 'Upcoming features, fixes and improvements. Track the development progress of Brothel Search.';
    return new Response(botHtml(title, desc, 'https://brothelsearch.com/roadmap', { '@context': 'https://schema.org', '@type': 'WebPage', name: title, description: desc }), { headers: { 'Content-Type': 'text/html; charset=UTF-8' } });
  }

  // /sydney — city page
  if (parts.length === 1 && parts[0] === 'sydney') {
    const title = 'Brothels in Sydney \u2013 Browse All Venues | Brothel Search';
    const desc = 'Browse 7 brothels across Sydney. Compare venues in Surry Hills, Chippendale, Haymarket and Annandale.';
    const url = 'https://brothelsearch.com/sydney/';
    return new Response(botHtml(title, desc, url, { '@context': 'https://schema.org', '@type': 'ItemList', name: 'Brothels in Sydney', numberOfItems: 7 }), { headers: { 'Content-Type': 'text/html; charset=UTF-8' } });
  }

  // /sydney/{region} — region page
  if (parts.length === 2 && parts[0] === 'sydney' && REGION_NAMES_WORKER[parts[1]]) {
    const regionName = REGION_NAMES_WORKER[parts[1]];
    const venueCount = Object.entries(VENUE_REGION_SLUGS).filter(([k, v]) => v === parts[1]).length;
    const title = 'Brothels in ' + regionName + ', Sydney | Brothel Search';
    const desc = 'Browse ' + venueCount + ' brothels in ' + regionName + ', Sydney. Compare venues, pricing and profiles.';
    const url = 'https://brothelsearch.com/sydney/' + parts[1] + '/';
    return new Response(botHtml(title, desc, url, { '@context': 'https://schema.org', '@type': 'ItemList', name: 'Brothels in ' + regionName + ', Sydney', numberOfItems: venueCount }), { headers: { 'Content-Type': 'text/html; charset=UTF-8' } });
  }

  // /sydney/{region}/{suburb}/{venue} — venue page
  if (parts.length === 4 && parts[0] === 'sydney' && VENUE_NAMES[parts[3]]) {
    const venueId = parts[3];
    const venueName = VENUE_NAMES[venueId];
    const suburbName = SUBURB_NAMES[parts[2]] || parts[2];
    const site = VENUE_MAP[venueId];
    let girlCount = 0;
    try { const { data } = await loadData(env, site); girlCount = (data.girls || []).length; } catch {}
    const title = venueName + ' \u2013 ' + suburbName + ', Sydney | Brothel Search';
    const desc = venueName + ' in ' + suburbName + ', Sydney. ' + girlCount + ' girls available. Browse profiles, photos and rosters.';
    const url = 'https://brothelsearch.com/sydney/' + parts[1] + '/' + parts[2] + '/' + venueId + '/';
    return new Response(botHtml(title, desc, url, { '@context': 'https://schema.org', '@type': 'LocalBusiness', name: venueName, address: { '@type': 'PostalAddress', addressLocality: suburbName, addressRegion: 'NSW', addressCountry: 'AU' } }), { headers: { 'Content-Type': 'text/html; charset=UTF-8' } });
  }

  return null;
}

async function serveBotMeta(env, pathname) {
  const parts = pathname.replace(/^\//, '').split('/');
  let venueId, slug;
  // New format: /sydney/{region}/{suburb}/{venue}/{country}/{name}
  if (parts.length === 6 && parts[0] === 'sydney') { venueId = parts[3]; slug = parts[5]; }
  // Previous format: /sydney/{suburb}/{venue}/{country}/{name}
  else if (parts.length === 5 && parts[0] === 'sydney') { venueId = parts[2]; slug = parts[4]; }
  // Previous format: /sydney/{suburb}/{venue}/{name}
  else if (parts.length === 4 && parts[0] === 'sydney') { venueId = parts[2]; slug = parts[3]; }
  // Legacy format: /{venue}/{name}
  else if (parts.length === 2) { venueId = parts[0]; slug = parts[1]; }
  else return null;
  const site = VENUE_MAP[venueId];
  if (!site) return null;

  try {
    const { data } = await loadData(env, site);
    const girl = (data.girls || []).find(g => {
      const s = (g.name || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/-+$/g, '');
      return s === slug;
    });
    if (!girl) return null;

    const name = girl.name || '';
    const venue = site.name || '';
    const photo = (girl.photos && girl.photos[0]) || '';
    const countriesDisplay = Array.isArray(girl.country) ? girl.country.join(', ') : (girl.country || '');
    const region = VENUE_REGION_SLUGS[venueId] || 'other';
    const suburb = VENUE_SUBURBS[venueId] || 'sydney';
    const suburbNames = { surryhills: 'Surry Hills', chippendale: 'Chippendale', annandale: 'Annandale', haymarket: 'Haymarket' };
    const suburbName = suburbNames[suburb] || 'Sydney';
    const location = `${suburbName}, Sydney`;
    const firstCountry = Array.isArray(girl.country) ? girl.country[0] : (girl.country || 'other');
    const countrySlug = firstCountry.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/-+$/g, '') || 'other';
    const pageUrl = `https://brothelsearch.com/sydney/${region}/${suburb}/${venueId}/${countrySlug}/${slug}`;
    const title = `${name} – ${venue} ${location} | Brothel Search`;
    const desc = `${name} at ${venue}, ${location}. ${[girl.age ? 'Age ' + girl.age : '', countriesDisplay].filter(Boolean).join(', ')}. Browse profile, photos and availability.`;

    const html = `<!DOCTYPE html>
<html><head>
<meta charset="UTF-8">
<title>${title}</title>
<meta name="description" content="${desc}">
<meta property="og:type" content="profile">
<meta property="og:title" content="${title}">
<meta property="og:description" content="${desc}">
<meta property="og:url" content="${pageUrl}">
${photo ? `<meta property="og:image" content="${photo}">` : ''}
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="${title}">
<meta name="twitter:description" content="${desc}">
${photo ? `<meta name="twitter:image" content="${photo}">` : ''}
<script type="application/ld+json">
${JSON.stringify({ '@context': 'https://schema.org', '@type': 'Person', name, description: desc, url: pageUrl, image: photo || undefined, worksFor: { '@type': 'LocalBusiness', name: venue, address: { '@type': 'PostalAddress', addressLocality: suburbName, addressRegion: 'NSW', addressCountry: 'AU' } } })}
</script>
<meta http-equiv="refresh" content="0;url=${pageUrl}">
</head><body></body></html>`;

    return new Response(html, { headers: { 'Content-Type': 'text/html; charset=UTF-8' } });
  } catch (e) {
    console.error('[Bot meta] Error:', e);
    return null;
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const json = h => new Response(JSON.stringify(h), { headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });

    // Social bot pre-rendering — serve meta tags for crawlers
    const ua = request.headers.get('user-agent') || '';
    if (BOT_UA.test(ua) && url.pathname !== '/' && !url.pathname.startsWith('/sync') && !url.pathname.startsWith('/check') && !url.pathname.startsWith('/regenerate')) {
      const landingResponse = await serveBotLanding(env, url.pathname);
      if (landingResponse) return landingResponse;
      const botResponse = await serveBotMeta(env, url.pathname);
      if (botResponse) return botResponse;
    }

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

    // 429 City endpoints
    if (url.pathname === '/sync-429city-girls' && request.method === 'POST') {
      try { return json(await syncWpGirls(env, SITES.city429)); }
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

    // 429 City endpoints
    if (url.pathname === '/sync-429city-calendar' && request.method === 'POST') {
      try { return json({ success: await syncCalendar(env, SITES.city429) }); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-pennys77-girls' && request.method === 'POST') {
      try { return json(await syncPennys77Girls(env, SITES.pennys77)); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-pennys77-calendar' && request.method === 'POST') {
      try { return json({ success: await syncCalendar(env, SITES.pennys77) }); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-thegoldenapple-girls' && request.method === 'POST') {
      try { return json(await syncGoldenAppleGirls(env, SITES.thegoldenapple)); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-thegoldenapple-calendar' && request.method === 'POST') {
      try { return json({ success: await syncCalendar(env, SITES.thegoldenapple) }); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-blackcatparlour-girls' && request.method === 'POST') {
      try { return json(await syncBlackCatGirls(env, SITES.blackcatparlour)); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-blackcatparlour-calendar' && request.method === 'POST') {
      try { return json({ success: await syncCalendar(env, SITES.blackcatparlour) }); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-bellevue12-girls' && request.method === 'POST') {
      try { return json(await syncBellevue12Girls(env, SITES.bellevue12)); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-bellevue12-calendar' && request.method === 'POST') {
      try { return json({ success: await syncCalendar(env, SITES.bellevue12) }); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-thegatewayclub-girls' && request.method === 'POST') {
      try { return json(await syncGatewayClubGirls(env, SITES.thegatewayclub)); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-thegatewayclub-calendar' && request.method === 'POST') {
      try { return json({ success: await syncCalendar(env, SITES.thegatewayclub) }); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-marrickvillebrothel-girls' && request.method === 'POST') {
      try { return json(await syncMarrickvilleBrothelGirls(env, SITES.marrickvillebrothel)); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-marrickvillebrothel-calendar' && request.method === 'POST') {
      try { return json({ success: await syncCalendar(env, SITES.marrickvillebrothel) }); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-springhouse-girls' && request.method === 'POST') {
      try { return json(await syncSpringHouseGirls(env, SITES.springhouse)); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-springhouse-calendar' && request.method === 'POST') {
      try { return json({ success: await syncCalendar(env, SITES.springhouse) }); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-stiletto-girls' && request.method === 'POST') {
      try { return json(await syncStilettoGirls(env, SITES.stiletto)); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-stiletto-calendar' && request.method === 'POST') {
      try { return json({ success: await syncCalendar(env, SITES.stiletto) }); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-wivesonly-girls' && request.method === 'POST') {
      try { return json(await syncWivesOnlyGirls(env, SITES.wivesonly)); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-wivesonly-calendar' && request.method === 'POST') {
      try { return json({ success: await syncCalendar(env, SITES.wivesonly) }); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-jinia-girls' && request.method === 'POST') {
      try { return json(await syncJiniaGirls(env, SITES.jinia)); }
      catch (e) { return json({ error: e.message }); }
    }
    if (url.pathname === '/sync-jinia-calendar' && request.method === 'POST') {
      try { return json({ success: await syncCalendar(env, SITES.jinia) }); }
      catch (e) { return json({ error: e.message }); }
    }

    // ── Photo health check endpoints ──
    if (url.pathname === '/check-photos' && request.method === 'POST') {
      try {
        const results = await Promise.all([
          checkBrokenPhotos(env, SITES.empire), checkBrokenPhotos(env, SITES.club),
          checkBrokenPhotos(env, SITES.kyoto206), checkBrokenPhotos(env, SITES.sakura57),
          checkBrokenPhotos(env, SITES.top127), checkBrokenPhotos(env, SITES.fantasyclub35),
          checkBrokenPhotos(env, SITES.city429),
          checkBrokenPhotos(env, SITES.pennys77), checkBrokenPhotos(env, SITES.thegoldenapple),
          checkBrokenPhotos(env, SITES.blackcatparlour), checkBrokenPhotos(env, SITES.bellevue12),
          checkBrokenPhotos(env, SITES.thegatewayclub), checkBrokenPhotos(env, SITES.marrickvillebrothel),
          checkBrokenPhotos(env, SITES.springhouse), checkBrokenPhotos(env, SITES.stiletto),
          checkBrokenPhotos(env, SITES.wivesonly), checkBrokenPhotos(env, SITES.jinia),
        ]);
        return json({ results });
      } catch (e) { return json({ error: e.message }); }
    }

    // ── Digest endpoint ──
    if (url.pathname === '/send-digest' && request.method === 'POST') {
      try { await sendDailyDigest(env); return json({ success: true }); }
      catch (e) { return json({ error: e.message }); }
    }

    // ── SEO endpoints ──
    if (url.pathname === '/regenerate-sitemap' && request.method === 'POST') {
      try { const count = await regenerateSitemap(env); return json({ success: true, urls: count }); }
      catch (e) { return json({ error: e.message }); }
    }

    // ── Stripe / Subscription endpoints ──

    const SUPABASE_URL = 'https://blhwekuidksxiaickeck.supabase.co';
    const STRIPE_API = 'https://api.stripe.com/v1';
    const PRICE_IDS = {
      trial: 'price_1TGIVSQjtcp0NkpMvkCpbeK7',
      recurring: 'price_1TGITsQjtcp0NkpMQAXCH9mE',
      'one-time': 'price_1TGISrQjtcp0NkpMCSTjTFFJ',
    };

    const cors = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type,Authorization',
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: cors });
    }

    // Activate free trial
    if (url.pathname === '/activate-trial' && request.method === 'POST') {
      try {
        const { userId } = await request.json();
        if (!userId) return json({ error: 'No userId' });

        const SUPABASE_URL = 'https://blhwekuidksxiaickeck.supabase.co';
        const sbH = { apikey: env.SUPABASE_SERVICE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`, 'Content-Type': 'application/json' };

        // Check if trial already used
        const subRes = await fetch(`${SUPABASE_URL}/rest/v1/user_subscriptions?user_id=eq.${userId}&select=trial_used`, { headers: sbH });
        const subs = await subRes.json();
        if (subs.length && subs[0].trial_used) return json({ error: 'Free trial already used' });

        // Create 3-day subscription
        const now = new Date();
        const periodEnd = new Date(now.getTime() + 3 * 24 * 60 * 60 * 1000);
        await fetch(`${SUPABASE_URL}/rest/v1/user_subscriptions`, {
          method: 'POST',
          headers: { ...sbH, Prefer: 'resolution=merge-duplicates' },
          body: JSON.stringify({
            user_id: userId, plan: 'trial', status: 'active', trial_used: true,
            current_period_start: now.toISOString(), current_period_end: periodEnd.toISOString(),
            updated_at: now.toISOString(),
          }),
        });

        return json({ success: true });
      } catch (e) { return json({ error: e.message }); }
    }

    // Create Stripe Checkout Session
    if (url.pathname === '/create-checkout' && request.method === 'POST') {
      try {
        const { plan, userId, email, returnUrl } = await request.json();
        if (!PRICE_IDS[plan]) return new Response(JSON.stringify({ error: 'Invalid plan' }), { status: 400, headers: { ...cors, 'Content-Type': 'application/json' } });

        // Check trial eligibility
        if (plan === 'trial') {
          const subRes = await fetch(`${SUPABASE_URL}/rest/v1/user_subscriptions?user_id=eq.${userId}&select=trial_used`, {
            headers: { apikey: env.SUPABASE_SERVICE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}` },
          });
          const subs = await subRes.json();
          if (subs.length && subs[0].trial_used) {
            return new Response(JSON.stringify({ error: 'Trial already used' }), { status: 400, headers: { ...cors, 'Content-Type': 'application/json' } });
          }
        }

        // Create Stripe Checkout Session
        const isRecurring = plan === 'recurring';
        // Create or find Stripe customer
        const stripeAuth = { Authorization: `Basic ${btoa(env.STRIPE_SECRET_KEY + ':')}`, 'Content-Type': 'application/x-www-form-urlencoded' };
        let customerId;
        const custSearch = await fetch(`${STRIPE_API}/customers?email=${encodeURIComponent(email)}&limit=1`, { headers: stripeAuth });
        const custData = await custSearch.json();
        if (custData.data && custData.data.length > 0) {
          customerId = custData.data[0].id;
        } else {
          const custCreate = await fetch(`${STRIPE_API}/customers`, { method: 'POST', headers: stripeAuth, body: new URLSearchParams({ email, 'metadata[user_id]': userId }).toString() });
          const newCust = await custCreate.json();
          customerId = newCust.id;
        }

        const body = new URLSearchParams({
          'line_items[0][price]': PRICE_IDS[plan],
          'line_items[0][quantity]': '1',
          mode: isRecurring ? 'subscription' : 'payment',
          success_url: returnUrl || 'https://travanixlabs.github.io/brothel-search/?payment=success',
          cancel_url: returnUrl || 'https://travanixlabs.github.io/brothel-search/?payment=cancelled',
          customer: customerId,
          'metadata[user_id]': userId,
          'metadata[plan]': plan,
        });

        const stripeRes = await fetch(`${STRIPE_API}/checkout/sessions`, {
          method: 'POST',
          headers: {
            Authorization: `Basic ${btoa(env.STRIPE_SECRET_KEY + ':')}`,
            'Content-Type': 'application/x-www-form-urlencoded',
          },
          body: body.toString(),
        });
        const session = await stripeRes.json();
        if (session.error) return new Response(JSON.stringify({ error: session.error.message }), { status: 400, headers: { ...cors, 'Content-Type': 'application/json' } });

        return new Response(JSON.stringify({ sessionUrl: session.url, sessionId: session.id }), { headers: { ...cors, 'Content-Type': 'application/json' } });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: { ...cors, 'Content-Type': 'application/json' } });
      }
    }

    // Stripe Customer Portal
    if (url.pathname === '/create-portal-session' && request.method === 'POST') {
      try {
        const { userId } = await request.json();
        // Get stripe_customer_id from Supabase
        const subRes = await fetch(`${SUPABASE_URL}/rest/v1/user_subscriptions?user_id=eq.${userId}&select=stripe_customer_id`, {
          headers: { apikey: env.SUPABASE_SERVICE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}` },
        });
        const subs = await subRes.json();
        if (!subs.length || !subs[0].stripe_customer_id) {
          return new Response(JSON.stringify({ error: 'No subscription found' }), { status: 404, headers: { ...cors, 'Content-Type': 'application/json' } });
        }
        const body = new URLSearchParams({
          customer: subs[0].stripe_customer_id,
          return_url: 'https://travanixlabs.github.io/brothel-search/',
        });
        const portalRes = await fetch(`${STRIPE_API}/billing_portal/sessions`, {
          method: 'POST',
          headers: {
            Authorization: `Basic ${btoa(env.STRIPE_SECRET_KEY + ':')}`,
            'Content-Type': 'application/x-www-form-urlencoded',
          },
          body: body.toString(),
        });
        const session = await portalRes.json();
        if (session.error) return new Response(JSON.stringify({ error: session.error.message }), { status: 400, headers: { ...cors, 'Content-Type': 'application/json' } });
        return new Response(JSON.stringify({ portalUrl: session.url }), { headers: { ...cors, 'Content-Type': 'application/json' } });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: { ...cors, 'Content-Type': 'application/json' } });
      }
    }

    // Stripe Webhook
    if (url.pathname === '/stripe-webhook' && request.method === 'POST') {
      try {
        const body = await request.text();
        const sig = request.headers.get('stripe-signature');

        // Verify webhook signature
        if (env.STRIPE_WEBHOOK_SECRET && sig) {
          const verified = await verifyStripeSignature(body, sig, env.STRIPE_WEBHOOK_SECRET);
          if (!verified) return new Response('Invalid signature', { status: 400 });
        }

        const event = JSON.parse(body);
        const supabaseHeaders = {
          apikey: env.SUPABASE_SERVICE_KEY,
          Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
          'Content-Type': 'application/json',
          Prefer: 'resolution=merge-duplicates',
        };

        if (event.type === 'checkout.session.completed') {
          const session = event.data.object;
          const userId = session.metadata?.user_id;
          const plan = session.metadata?.plan;
          if (!userId) return new Response('No user_id', { status: 400 });

          const now = new Date();
          let periodEnd;
          if (plan === 'trial') {
            periodEnd = new Date(now.getTime() + 3 * 24 * 60 * 60 * 1000); // 3 days
          } else {
            periodEnd = new Date(now.getTime() + 30 * 24 * 60 * 60 * 1000); // ~1 month
          }

          const upsertData = {
            user_id: userId,
            stripe_customer_id: session.customer || null,
            stripe_subscription_id: session.subscription || null,
            plan: plan,
            status: 'active',
            trial_used: plan === 'trial' ? true : undefined,
            current_period_start: now.toISOString(),
            current_period_end: periodEnd.toISOString(),
            updated_at: now.toISOString(),
          };
          // Remove undefined
          Object.keys(upsertData).forEach(k => upsertData[k] === undefined && delete upsertData[k]);

          // Upsert into user_subscriptions
          await fetch(`${SUPABASE_URL}/rest/v1/user_subscriptions`, {
            method: 'POST',
            headers: { ...supabaseHeaders, Prefer: 'resolution=merge-duplicates' },
            body: JSON.stringify(upsertData),
          });

          // Process referral reward — only for non-trial plans
          if (plan !== 'trial') try {
            // Get referee's referral code from user metadata
            const userRes = await fetch(`${SUPABASE_URL}/auth/v1/admin/users/${userId}`, {
              headers: { apikey: env.SUPABASE_SERVICE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}` }
            });
            const userData = await userRes.json();
            const refCode = userData.user_metadata?.referral_code;
            if (refCode) {
              // Find referrer
              const codeRes = await fetch(`${SUPABASE_URL}/rest/v1/user_referral_codes?code=eq.${encodeURIComponent(refCode)}&select=user_id`, {
                headers: supabaseHeaders
              });
              const codeData = await codeRes.json();
              if (codeData.length && codeData[0].user_id !== userId) {
                const referrerId = codeData[0].user_id;
                // Check if referral already completed for this referee
                const existingRes = await fetch(`${SUPABASE_URL}/rest/v1/referrals?referee_id=eq.${userId}&status=eq.completed&select=id`, {
                  headers: supabaseHeaders
                });
                const existing = await existingRes.json();
                if (!existing.length) {
                  // Record referral
                  await fetch(`${SUPABASE_URL}/rest/v1/referrals`, {
                    method: 'POST', headers: supabaseHeaders,
                    body: JSON.stringify({ referrer_id: referrerId, referee_id: userId, code: refCode, status: 'completed', completed_at: new Date().toISOString() })
                  });
                  // Add 7 days to referrer's subscription
                  const refSubRes = await fetch(`${SUPABASE_URL}/rest/v1/user_subscriptions?user_id=eq.${referrerId}&select=current_period_end`, {
                    headers: supabaseHeaders
                  });
                  const refSub = await refSubRes.json();
                  if (refSub.length) {
                    const currentEnd = new Date(refSub[0].current_period_end);
                    currentEnd.setDate(currentEnd.getDate() + 7);
                    await fetch(`${SUPABASE_URL}/rest/v1/user_subscriptions?user_id=eq.${referrerId}`, {
                      method: 'PATCH', headers: supabaseHeaders,
                      body: JSON.stringify({ current_period_end: currentEnd.toISOString(), updated_at: new Date().toISOString() })
                    });
                  }
                  // Add 5 bonus days to referee's subscription
                  const refeeSubRes = await fetch(`${SUPABASE_URL}/rest/v1/user_subscriptions?user_id=eq.${userId}&select=current_period_end`, {
                    headers: supabaseHeaders
                  });
                  const refeeSub = await refeeSubRes.json();
                  if (refeeSub.length) {
                    const refeeEnd = new Date(refeeSub[0].current_period_end);
                    refeeEnd.setDate(refeeEnd.getDate() + 5);
                    await fetch(`${SUPABASE_URL}/rest/v1/user_subscriptions?user_id=eq.${userId}`, {
                      method: 'PATCH', headers: supabaseHeaders,
                      body: JSON.stringify({ current_period_end: refeeEnd.toISOString(), updated_at: new Date().toISOString() })
                    });
                  }
                  console.log(`[Referral] ${refCode}: referrer +7 days, referee +5 days`);
                }
              }
            }
          } catch (e) { console.error('[Referral] Error:', e); }

          // If trial, mark trial_used even if upgrading later
          if (plan === 'trial') {
            await fetch(`${SUPABASE_URL}/rest/v1/user_subscriptions?user_id=eq.${userId}`, {
              method: 'PATCH',
              headers: supabaseHeaders,
              body: JSON.stringify({ trial_used: true }),
            });
          }
        }

        if (event.type === 'invoice.paid') {
          const invoice = event.data.object;
          const subId = invoice.subscription;
          if (subId) {
            // Fetch subscription from Stripe to get current_period_end
            const subRes = await fetch(`${STRIPE_API}/subscriptions/${subId}`, {
              headers: { Authorization: `Basic ${btoa(env.STRIPE_SECRET_KEY + ':')}` },
            });
            const sub = await subRes.json();
            const userId = sub.metadata?.user_id;
            if (userId) {
              await fetch(`${SUPABASE_URL}/rest/v1/user_subscriptions?user_id=eq.${userId}`, {
                method: 'PATCH',
                headers: supabaseHeaders,
                body: JSON.stringify({
                  status: 'active',
                  current_period_end: new Date(sub.current_period_end * 1000).toISOString(),
                  updated_at: new Date().toISOString(),
                }),
              });
            }
          }
        }

        if (event.type === 'customer.subscription.deleted') {
          const sub = event.data.object;
          const userId = sub.metadata?.user_id;
          if (userId) {
            await fetch(`${SUPABASE_URL}/rest/v1/user_subscriptions?user_id=eq.${userId}`, {
              method: 'PATCH',
              headers: supabaseHeaders,
              body: JSON.stringify({ status: 'inactive', updated_at: new Date().toISOString() }),
            });
          }
        }

        if (event.type === 'invoice.payment_failed') {
          const invoice = event.data.object;
          const subId = invoice.subscription;
          if (subId) {
            const subRes = await fetch(`${STRIPE_API}/subscriptions/${subId}`, {
              headers: { Authorization: `Basic ${btoa(env.STRIPE_SECRET_KEY + ':')}` },
            });
            const sub = await subRes.json();
            const userId = sub.metadata?.user_id;
            if (userId) {
              await fetch(`${SUPABASE_URL}/rest/v1/user_subscriptions?user_id=eq.${userId}`, {
                method: 'PATCH',
                headers: supabaseHeaders,
                body: JSON.stringify({ status: 'past_due', updated_at: new Date().toISOString() }),
              });
            }
          }
        }

        return new Response('ok', { status: 200 });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), { status: 500 });
      }
    }

    // Subscription status check
    if (url.pathname === '/subscription-status' && request.method === 'GET') {
      try {
        const authHeader = request.headers.get('Authorization');
        if (!authHeader) return new Response(JSON.stringify({ error: 'No auth' }), { status: 401, headers: { ...cors, 'Content-Type': 'application/json' } });

        // Verify JWT and get user
        const userRes = await fetch(`${SUPABASE_URL}/auth/v1/user`, {
          headers: { apikey: env.SUPABASE_SERVICE_KEY, Authorization: authHeader },
        });
        const user = await userRes.json();
        if (!user.id) return new Response(JSON.stringify({ error: 'Invalid token' }), { status: 401, headers: { ...cors, 'Content-Type': 'application/json' } });

        // Check role
        const roleRes = await fetch(`${SUPABASE_URL}/rest/v1/user_roles?id=eq.${user.id}&select=role`, {
          headers: { apikey: env.SUPABASE_SERVICE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}` },
        });
        const roles = await roleRes.json();
        const isAdmin = roles.length && roles[0].role === 'admin';

        if (isAdmin) {
          return new Response(JSON.stringify({ status: 'active', plan: 'admin', expiresAt: null, trialUsed: false, isAdmin: true }), { headers: { ...cors, 'Content-Type': 'application/json' } });
        }

        // Get subscription
        const subRes = await fetch(`${SUPABASE_URL}/rest/v1/user_subscriptions?user_id=eq.${user.id}&select=*`, {
          headers: { apikey: env.SUPABASE_SERVICE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}` },
        });
        const subs = await subRes.json();
        const sub = subs[0];

        if (!sub) {
          return new Response(JSON.stringify({ status: 'none', plan: 'none', expiresAt: null, trialUsed: false }), { headers: { ...cors, 'Content-Type': 'application/json' } });
        }

        // Check if expired
        const isActive = sub.status === 'active' && sub.current_period_end && new Date(sub.current_period_end) > new Date();

        return new Response(JSON.stringify({
          status: isActive ? 'active' : 'expired',
          plan: sub.plan,
          expiresAt: sub.current_period_end,
          trialUsed: sub.trial_used || false,
        }), { headers: { ...cors, 'Content-Type': 'application/json' } });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: { ...cors, 'Content-Type': 'application/json' } });
      }
    }

    // POST /api/notify-reply — send email when someone replies to a review
    if (url.pathname === '/api/notify-reply' && request.method === 'POST') {
      try {
        const body = await request.json();
        const { review_user_id, review_user_name, girl_name, venue, reply_user_name, reply_comment } = body;
        if (!review_user_id || !reply_user_name) return new Response(JSON.stringify({ error: 'Missing fields' }), { status: 400, headers: { ...cors, 'Content-Type': 'application/json' } });

        // Look up the review author's email
        const headers = { apikey: env.SUPABASE_SERVICE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`, 'Content-Type': 'application/json' };
        const userRes = await fetch(`${SB_URL}/auth/v1/admin/users/${review_user_id}`, { headers });
        const userData = await userRes.json();
        if (!userData.email) return new Response(JSON.stringify({ error: 'User email not found' }), { status: 404, headers: { ...cors, 'Content-Type': 'application/json' } });

        const emailHtml = `
<div style="font-family:Arial,sans-serif;background:#0f0f1e;color:#e0e0e0;padding:28px;border-radius:10px;max-width:500px;margin:0 auto">
  <h2 style="color:#d4af37;margin:0 0 16px 0;font-size:18px">New Reply to Your Review</h2>
  <p style="color:#aaa;margin:0 0 16px 0"><strong style="color:#d4af37">${reply_user_name}</strong> replied to your review of <strong style="color:#d4af37">${girl_name}</strong>:</p>
  <div style="background:#1a1a2e;border-left:3px solid #d4af37;padding:12px 16px;border-radius:6px;margin-bottom:16px">
    <p style="color:#e0e0e0;margin:0;font-size:14px;line-height:1.6">${reply_comment.replace(/</g, '&lt;').replace(/>/g, '&gt;')}</p>
  </div>
  <a href="https://brothelsearch.com/" style="display:inline-block;background:#d4af37;color:#0f0f1e;padding:10px 24px;border-radius:8px;text-decoration:none;font-weight:700;font-size:13px">View on Brothel Search</a>
  <p style="color:#555;font-size:10px;margin:20px 0 0 0">Brothel Search</p>
</div>`;

        await fetch('https://api.resend.com/emails', {
          method: 'POST',
          headers: { Authorization: `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            from: 'Brothel Search <info@travanixlabs.com>',
            to: [userData.email],
            subject: reply_user_name + ' replied to your review of ' + girl_name,
            html: emailHtml,
          }),
        });

        return new Response(JSON.stringify({ ok: true }), { headers: { ...cors, 'Content-Type': 'application/json' } });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: { ...cors, 'Content-Type': 'application/json' } });
      }
    }

    // GET /api/test-sync-email — trigger the daily summary email manually
    if (url.pathname === '/api/test-sync-email') {
      const allSites = [SITES.empire, SITES.club, SITES.kyoto206, SITES.sakura57, SITES.top127, SITES.fantasyclub35, SITES.city429, SITES.pennys77, SITES.thegoldenapple, SITES.blackcatparlour, SITES.bellevue12, SITES.thegatewayclub, SITES.marrickvillebrothel, SITES.springhouse, SITES.stiletto, SITES.wivesonly, SITES.jinia];
      try {
        const { log } = await loadSyncLog(env);

        // Read roster data
        const now = new Date();
        const aestNow = new Date(now.getTime() + 10 * 60 * 60 * 1000);
        const todayStr = aestNow.toISOString().split('T')[0];

        // Filter runs to today only
        const allRuns = (log.runs || []).filter(r => r.date === todayStr);

        // Aggregate per venue
        const agg = {};
        for (const s of allSites) agg[s.name] = { added: 0, updated: 0, removed: 0, errors: [] };
        for (const run of allRuns) {
          for (const [name, v] of Object.entries(run.venues || {})) {
            if (!agg[name]) agg[name] = { added: 0, updated: 0, removed: 0, errors: [] };
            agg[name].added += v.added || 0;
            agg[name].updated += v.updated || 0;
            agg[name].removed += v.removed || 0;
            if (v.error) agg[name].errors.push(`${run.time}: ${v.error}`);
          }
        }
        const tomorrow = new Date(aestNow); tomorrow.setDate(tomorrow.getDate() + 1);
        const tomorrowStr = tomorrow.toISOString().split('T')[0];

        const rosterCounts = {};
        await Promise.all(allSites.map(async (site) => {
          try {
            const { data } = await loadData(env, site);
            const cal = data.calendar || {};
            let today = 0, tmr = 0;
            for (const [name, slots] of Object.entries(cal)) {
              if (name === '_published') continue;
              if (slots && slots[todayStr]) today++;
              if (slots && slots[tomorrowStr]) tmr++;
            }
            rosterCounts[site.name] = { today, tomorrow: tmr };
          } catch(e) {
            rosterCounts[site.name] = { today: 0, tomorrow: 0 };
          }
        }));

        const timeLabel = now.toLocaleString('en-AU', { timeZone: 'Australia/Sydney', dateStyle: 'medium', timeStyle: 'short' });
        const runLabels = allRuns.length > 0 ? allRuns.map(r => r.time).join(', ') : 'none yet today';
        const totalAdded = Object.values(agg).reduce((s, v) => s + v.added, 0);
        const totalRemoved = Object.values(agg).reduce((s, v) => s + v.removed, 0);
        const totalUpdated = Object.values(agg).reduce((s, v) => s + v.updated, 0);
        const totalErrors = Object.values(agg).reduce((s, v) => s + v.errors.length, 0);
        const totalRosterToday = Object.values(rosterCounts).reduce((s, v) => s + v.today, 0);
        const totalRosterTmr = Object.values(rosterCounts).reduce((s, v) => s + v.tomorrow, 0);

        const rows = allSites.map(s => {
          const a = agg[s.name];
          const rc = rosterCounts[s.name] || { today: 0, tomorrow: 0 };
          const hasErr = a.errors.length > 0;
          const errIcon = hasErr ? ' <span style="color:#e74c3c" title="' + a.errors.join('; ').replace(/"/g, '&quot;') + '">⚠</span>' : '';
          const addedStyle = a.added > 0 ? 'color:#2ecc71;font-weight:bold' : 'color:#888';
          const removedStyle = a.removed > 0 ? 'color:#e74c3c;font-weight:bold' : 'color:#888';
          const updatedStyle = a.updated > 0 ? 'color:#3498db;font-weight:bold' : 'color:#888';
          const rosterTodayStyle = rc.today > 0 ? 'color:#2ecc71' : 'color:#888';
          const rosterTmrStyle = rc.tomorrow > 0 ? 'color:#2ecc71' : 'color:#888';
          const td = 'padding:6px 10px;border-bottom:1px solid #2a2a3e;';
          return `<tr>
            <td style="${td}white-space:nowrap">${s.name}${errIcon}</td>
            <td style="${td}text-align:center;${addedStyle}">${a.added}</td>
            <td style="${td}text-align:center;${removedStyle}">${a.removed}</td>
            <td style="${td}text-align:center;${updatedStyle}">${a.updated}</td>
            <td style="${td}text-align:center;${rosterTodayStyle}">${rc.today}</td>
            <td style="${td}text-align:center;${rosterTmrStyle}">${rc.tomorrow}</td>
          </tr>`;
        }).join('');

        const subjectStatus = totalErrors > 0 ? `⚠️ ${totalErrors} error(s)` : '✅ All OK';

        const emailHtml = `
<div style="font-family:Arial,sans-serif;background:#0f0f1e;color:#e0e0e0;padding:28px;border-radius:10px;max-width:720px;margin:0 auto">
  <h2 style="color:#d4af37;margin:0 0 4px 0;font-size:20px">Brothel Search — Daily Sync Report</h2>
  <p style="color:#888;margin:0 0 20px 0;font-size:13px">${timeLabel} &nbsp;|&nbsp; Runs included: ${runLabels}</p>

  <div style="display:flex;gap:16px;margin-bottom:20px;flex-wrap:wrap">
    <div style="background:#1a1a2e;padding:12px 18px;border-radius:8px;flex:1;min-width:100px;text-align:center">
      <div style="color:#2ecc71;font-size:22px;font-weight:bold">${totalAdded}</div>
      <div style="color:#888;font-size:11px">New Profiles</div>
    </div>
    <div style="background:#1a1a2e;padding:12px 18px;border-radius:8px;flex:1;min-width:100px;text-align:center">
      <div style="color:#e74c3c;font-size:22px;font-weight:bold">${totalRemoved}</div>
      <div style="color:#888;font-size:11px">Removed</div>
    </div>
    <div style="background:#1a1a2e;padding:12px 18px;border-radius:8px;flex:1;min-width:100px;text-align:center">
      <div style="color:#3498db;font-size:22px;font-weight:bold">${totalUpdated}</div>
      <div style="color:#888;font-size:11px">Updated</div>
    </div>
    <div style="background:#1a1a2e;padding:12px 18px;border-radius:8px;flex:1;min-width:100px;text-align:center">
      <div style="color:#d4af37;font-size:22px;font-weight:bold">${totalRosterToday}</div>
      <div style="color:#888;font-size:11px">Roster Today</div>
    </div>
    <div style="background:#1a1a2e;padding:12px 18px;border-radius:8px;flex:1;min-width:100px;text-align:center">
      <div style="color:#d4af37;font-size:22px;font-weight:bold">${totalRosterTmr}</div>
      <div style="color:#888;font-size:11px">Roster Tomorrow</div>
    </div>
  </div>

  <table style="width:100%;border-collapse:collapse;font-size:12px;background:#1a1a2e;border-radius:8px;overflow:hidden">
    <thead>
      <tr style="background:#252540">
        <th style="text-align:left;padding:8px 10px;color:#d4af37;font-size:11px">Venue</th>
        <th style="text-align:center;padding:8px 10px;color:#d4af37;font-size:11px">New Profiles</th>
        <th style="text-align:center;padding:8px 10px;color:#d4af37;font-size:11px">Removed</th>
        <th style="text-align:center;padding:8px 10px;color:#d4af37;font-size:11px">Updated</th>
        <th style="text-align:center;padding:8px 10px;color:#d4af37;font-size:11px">Roster Today</th>
        <th style="text-align:center;padding:8px 10px;color:#d4af37;font-size:11px">Roster Tomorrow</th>
      </tr>
    </thead>
    <tbody>
      ${rows}
      <tr style="background:#252540;font-weight:bold">
        <td style="padding:8px 10px;color:#d4af37">Total</td>
        <td style="padding:8px 10px;text-align:center;color:#2ecc71">${totalAdded}</td>
        <td style="padding:8px 10px;text-align:center;color:#e74c3c">${totalRemoved}</td>
        <td style="padding:8px 10px;text-align:center;color:#3498db">${totalUpdated}</td>
        <td style="padding:8px 10px;text-align:center;color:#d4af37">${totalRosterToday}</td>
        <td style="padding:8px 10px;text-align:center;color:#d4af37">${totalRosterTmr}</td>
      </tr>
    </tbody>
  </table>

  <p style="color:#555;font-size:10px;margin:16px 0 0 0;text-align:center">Brothel Search Worker &nbsp;•&nbsp; ${todayStr}</p>
</div>`;

        await fetch('https://api.resend.com/emails', {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            from: 'Brothel Search <info@travanixlabs.com>',
            to: ['info@travanixlabs.com'],
            subject: `Daily Sync Report — ${subjectStatus} — +${totalAdded} new, ${totalRosterToday} rostered today`,
            html: emailHtml,
          }),
        });

        return new Response(JSON.stringify({ ok: true, runs: allRuns.length, totalAdded, totalRemoved, totalUpdated, rosterToday: totalRosterToday, rosterTomorrow: totalRosterTmr }), { headers: { ...cors, 'Content-Type': 'application/json' } });
      } catch(e) {
        return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: { ...cors, 'Content-Type': 'application/json' } });
      }
    }

    return new Response('Not found', { status: 404 });
  },

  async scheduled(event, env, ctx) {
    const hour = new Date(event.scheduledTime).getUTCHours();

    ctx.waitUntil((async () => {
      // 23:00 UTC (9am AEST) — Daily digest only
      if (hour === 23) {
        console.log('9am AEDT — Daily digest');
        await sendDailyDigest(env).catch(e => console.error('[Digest] Error:', e));
        return;
      }

      // Sync hours: 10(8pm AEST), 22(8am), 4(2pm), 16(2am)
      // Run girl sync and roster sync SEQUENTIALLY in small batches to avoid subrequest limits
      const aest = { 10: '8pm', 22: '8am', 4: '2pm', 16: '2am' };
      if (aest[hour]) {
        console.log(aest[hour] + ' AEST — Sync starting');

        const allSites = [SITES.empire, SITES.club, SITES.kyoto206, SITES.sakura57, SITES.top127, SITES.fantasyclub35, SITES.city429, SITES.pennys77, SITES.thegoldenapple, SITES.blackcatparlour, SITES.bellevue12, SITES.thegatewayclub, SITES.marrickvillebrothel, SITES.springhouse, SITES.stiletto, SITES.wivesonly, SITES.jinia];

        const girlResults = {};
        const photoResults = {};
        const rosterResults = {};

        async function syncAllGirls(fn, site) {
          let totalAdded = 0, totalUpdated = 0;
          let result;
          do {
            result = await fn(env, site).catch(e => { console.error(`[${site.name}] Girls sync error:`, e); return { remaining: 0, error: e.message }; });
            totalAdded += result.added || 0;
            totalUpdated += result.updated || 0;
            console.log(`[${site.name}] Girls batch: added=${result.added || 0}, remaining=${result.remaining || 0}`);
          } while (result.remaining > 0);
          girlResults[site.name] = { added: totalAdded, updated: totalUpdated, error: result.error || null };
        }
        async function trackRoster(site) {
          try { await syncCalendar(env, site); rosterResults[site.name] = { ok: true, error: null }; }
          catch(e) { console.error(`[${site.name}] Calendar sync error:`, e); rosterResults[site.name] = { ok: false, error: e.message }; }
        }

        // Girl sync — 3 sequential batches of ~6 to stay within subrequest limits
        const girlSyncTasks = [
          () => syncAllGirls(syncGirls, SITES.empire),
          () => syncAllGirls(syncGirls, SITES.club),
          () => syncAllGirls(syncWpGirls, SITES.kyoto206),
          () => syncAllGirls(syncWpGirls, SITES.sakura57),
          () => syncAllGirls(syncWpGirls, SITES.top127),
          () => syncAllGirls(syncWpGirls, SITES.fantasyclub35),
          () => syncAllGirls(syncWpGirls, SITES.city429),
          () => syncAllGirls(syncPennys77Girls, SITES.pennys77),
          () => syncAllGirls(syncGoldenAppleGirls, SITES.thegoldenapple),
          () => syncAllGirls(syncBlackCatGirls, SITES.blackcatparlour),
          () => syncAllGirls(syncBellevue12Girls, SITES.bellevue12),
          () => syncAllGirls(syncGatewayClubGirls, SITES.thegatewayclub),
          () => syncAllGirls(syncMarrickvilleBrothelGirls, SITES.marrickvillebrothel),
          () => syncAllGirls(syncSpringHouseGirls, SITES.springhouse),
          () => syncAllGirls(syncStilettoGirls, SITES.stiletto),
          () => syncAllGirls(syncWivesOnlyGirls, SITES.wivesonly),
          () => syncAllGirls(syncJiniaGirls, SITES.jinia),
        ];
        // Run in sequential batches of 6
        for (let i = 0; i < girlSyncTasks.length; i += 6) {
          await Promise.all(girlSyncTasks.slice(i, i + 6).map(fn => fn()));
          console.log(`Girls batch ${Math.floor(i/6)+1} complete.`);
        }
        console.log('All girls syncs complete.');

        // Roster sync — sequential batches of 6
        for (let i = 0; i < allSites.length; i += 6) {
          await Promise.all(allSites.slice(i, i + 6).map(s => trackRoster(s)));
          console.log(`Roster batch ${Math.floor(i/6)+1} complete.`);
        }
        console.log('All roster syncs complete.');

        // Photo checks + sitemap at 2am AEST only
        if (hour === 16) {
          for (const site of allSites) {
            try {
              const r = await checkBrokenPhotos(env, site);
              photoResults[site.name] = { removed: r.removed || 0, fixed: r.fixed || 0 };
            } catch(e) { photoResults[site.name] = { removed: 0, fixed: 0 }; }
          }
          await regenerateSitemap(env).catch(e => console.error('[SEO] Sitemap error:', e));
          console.log('Photo checks + sitemap complete.');
        }

        // Save run results to sync log
        const syncNow = new Date();
        const syncAest = new Date(syncNow.getTime() + 10 * 60 * 60 * 1000);
        const runData = { time: aest[hour], date: syncAest.toISOString().split('T')[0], venues: {} };
        for (const s of allSites) {
          runData.venues[s.name] = {
            added: (girlResults[s.name] || {}).added || 0,
            updated: (girlResults[s.name] || {}).updated || 0,
            removed: (photoResults[s.name] || {}).removed || 0,
            rosterOk: (rosterResults[s.name] || {}).ok || false,
            error: (girlResults[s.name] || {}).error || (rosterResults[s.name] || {}).error || null,
          };
        }
        try {
          const { log, sha } = await loadSyncLog(env);
          log.runs.push(runData);
          await saveSyncLog(env, log, sha);
          console.log('Saved run results to sync log.');
        } catch(e) { console.error('[SyncLog] Save error:', e); }

        // 8pm — send daily email
        if (hour === 10) {
          try {
            const { log, sha: logSha } = await loadSyncLog(env);

            // Read roster data (today + tomorrow) from profile JSONs
            const now = new Date(event.scheduledTime);
            const aestNow = new Date(now.getTime() + 10 * 60 * 60 * 1000);
            const todayStr = aestNow.toISOString().split('T')[0];

            // Filter runs to today only
            const allRuns = (log.runs || []).filter(r => r.date === todayStr);

            // Aggregate per venue across today's runs
            const agg = {};
            for (const s of allSites) agg[s.name] = { added: 0, updated: 0, removed: 0, errors: [] };
            for (const run of allRuns) {
              for (const [name, v] of Object.entries(run.venues || {})) {
                if (!agg[name]) agg[name] = { added: 0, updated: 0, removed: 0, errors: [] };
                agg[name].added += v.added || 0;
                agg[name].updated += v.updated || 0;
                agg[name].removed += v.removed || 0;
                if (v.error) agg[name].errors.push(`${run.time}: ${v.error}`);
              }
            }
            const tomorrow = new Date(aestNow); tomorrow.setDate(tomorrow.getDate() + 1);
            const tomorrowStr = tomorrow.toISOString().split('T')[0];

            const rosterCounts = {};
            await Promise.all(allSites.map(async (site) => {
              try {
                const { data } = await loadData(env, site);
                const cal = data.calendar || {};
                let today = 0, tmr = 0;
                for (const [name, slots] of Object.entries(cal)) {
                  if (name === '_published') continue;
                  if (slots && slots[todayStr]) today++;
                  if (slots && slots[tomorrowStr]) tmr++;
                }
                rosterCounts[site.name] = { today, tomorrow: tmr };
              } catch(e) {
                rosterCounts[site.name] = { today: 0, tomorrow: 0 };
              }
            }));

            // Build email
            const timeLabel = now.toLocaleString('en-AU', { timeZone: 'Australia/Sydney', dateStyle: 'medium', timeStyle: 'short' });
            const runLabels = allRuns.map(r => r.time).join(', ');
            const totalAdded = Object.values(agg).reduce((s, v) => s + v.added, 0);
            const totalRemoved = Object.values(agg).reduce((s, v) => s + v.removed, 0);
            const totalUpdated = Object.values(agg).reduce((s, v) => s + v.updated, 0);
            const totalErrors = Object.values(agg).reduce((s, v) => s + v.errors.length, 0);
            const totalRosterToday = Object.values(rosterCounts).reduce((s, v) => s + v.today, 0);
            const totalRosterTmr = Object.values(rosterCounts).reduce((s, v) => s + v.tomorrow, 0);

            const rows = allSites.map(s => {
              const a = agg[s.name];
              const rc = rosterCounts[s.name] || { today: 0, tomorrow: 0 };
              const hasErr = a.errors.length > 0;
              const errIcon = hasErr ? ' <span style="color:#e74c3c" title="' + a.errors.join('; ').replace(/"/g, '&quot;') + '">⚠</span>' : '';
              const addedStyle = a.added > 0 ? 'color:#2ecc71;font-weight:bold' : 'color:#888';
              const removedStyle = a.removed > 0 ? 'color:#e74c3c;font-weight:bold' : 'color:#888';
              const updatedStyle = a.updated > 0 ? 'color:#3498db;font-weight:bold' : 'color:#888';
              const rosterTodayStyle = rc.today > 0 ? 'color:#2ecc71' : 'color:#888';
              const rosterTmrStyle = rc.tomorrow > 0 ? 'color:#2ecc71' : 'color:#888';
              const td = 'padding:6px 10px;border-bottom:1px solid #2a2a3e;';
              return `<tr>
                <td style="${td}white-space:nowrap">${s.name}${errIcon}</td>
                <td style="${td}text-align:center;${addedStyle}">${a.added}</td>
                <td style="${td}text-align:center;${removedStyle}">${a.removed}</td>
                <td style="${td}text-align:center;${updatedStyle}">${a.updated}</td>
                <td style="${td}text-align:center;${rosterTodayStyle}">${rc.today}</td>
                <td style="${td}text-align:center;${rosterTmrStyle}">${rc.tomorrow}</td>
              </tr>`;
            }).join('');

            const subjectStatus = totalErrors > 0 ? `⚠️ ${totalErrors} error(s)` : '✅ All OK';

            const html = `
<div style="font-family:Arial,sans-serif;background:#0f0f1e;color:#e0e0e0;padding:28px;border-radius:10px;max-width:720px;margin:0 auto">
  <h2 style="color:#d4af37;margin:0 0 4px 0;font-size:20px">Brothel Search — Daily Sync Report</h2>
  <p style="color:#888;margin:0 0 20px 0;font-size:13px">${timeLabel} &nbsp;|&nbsp; Runs included: ${runLabels}</p>

  <div style="display:flex;gap:16px;margin-bottom:20px;flex-wrap:wrap">
    <div style="background:#1a1a2e;padding:12px 18px;border-radius:8px;flex:1;min-width:100px;text-align:center">
      <div style="color:#2ecc71;font-size:22px;font-weight:bold">${totalAdded}</div>
      <div style="color:#888;font-size:11px">New Profiles</div>
    </div>
    <div style="background:#1a1a2e;padding:12px 18px;border-radius:8px;flex:1;min-width:100px;text-align:center">
      <div style="color:#e74c3c;font-size:22px;font-weight:bold">${totalRemoved}</div>
      <div style="color:#888;font-size:11px">Removed</div>
    </div>
    <div style="background:#1a1a2e;padding:12px 18px;border-radius:8px;flex:1;min-width:100px;text-align:center">
      <div style="color:#3498db;font-size:22px;font-weight:bold">${totalUpdated}</div>
      <div style="color:#888;font-size:11px">Updated</div>
    </div>
    <div style="background:#1a1a2e;padding:12px 18px;border-radius:8px;flex:1;min-width:100px;text-align:center">
      <div style="color:#d4af37;font-size:22px;font-weight:bold">${totalRosterToday}</div>
      <div style="color:#888;font-size:11px">Roster Today</div>
    </div>
    <div style="background:#1a1a2e;padding:12px 18px;border-radius:8px;flex:1;min-width:100px;text-align:center">
      <div style="color:#d4af37;font-size:22px;font-weight:bold">${totalRosterTmr}</div>
      <div style="color:#888;font-size:11px">Roster Tomorrow</div>
    </div>
  </div>

  <table style="width:100%;border-collapse:collapse;font-size:12px;background:#1a1a2e;border-radius:8px;overflow:hidden">
    <thead>
      <tr style="background:#252540">
        <th style="text-align:left;padding:8px 10px;color:#d4af37;font-size:11px">Venue</th>
        <th style="text-align:center;padding:8px 10px;color:#d4af37;font-size:11px">New Profiles</th>
        <th style="text-align:center;padding:8px 10px;color:#d4af37;font-size:11px">Removed</th>
        <th style="text-align:center;padding:8px 10px;color:#d4af37;font-size:11px">Updated</th>
        <th style="text-align:center;padding:8px 10px;color:#d4af37;font-size:11px">Roster Today</th>
        <th style="text-align:center;padding:8px 10px;color:#d4af37;font-size:11px">Roster Tomorrow</th>
      </tr>
    </thead>
    <tbody>
      ${rows}
      <tr style="background:#252540;font-weight:bold">
        <td style="padding:8px 10px;color:#d4af37">Total</td>
        <td style="padding:8px 10px;text-align:center;color:#2ecc71">${totalAdded}</td>
        <td style="padding:8px 10px;text-align:center;color:#e74c3c">${totalRemoved}</td>
        <td style="padding:8px 10px;text-align:center;color:#3498db">${totalUpdated}</td>
        <td style="padding:8px 10px;text-align:center;color:#d4af37">${totalRosterToday}</td>
        <td style="padding:8px 10px;text-align:center;color:#d4af37">${totalRosterTmr}</td>
      </tr>
    </tbody>
  </table>

  <p style="color:#555;font-size:10px;margin:16px 0 0 0;text-align:center">Brothel Search Worker &nbsp;•&nbsp; ${todayStr}</p>
</div>`;

            await fetch('https://api.resend.com/emails', {
              method: 'POST',
              headers: { 'Authorization': `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
              body: JSON.stringify({
                from: 'Brothel Search <info@travanixlabs.com>',
                to: ['info@travanixlabs.com'],
                subject: `Daily Sync Report — ${subjectStatus} — +${totalAdded} new, ${totalRosterToday} rostered today`,
                html,
              }),
            });
            console.log('Daily sync summary email sent.');

            // Prune runs older than today (keep today's for the test endpoint)
            try {
              log.runs = (log.runs || []).filter(r => r.date === todayStr);
              await saveSyncLog(env, log, logSha);
            } catch(e) { console.error('[SyncLog] Prune error:', e); }
          } catch(e) {
            console.error('[Email] Daily sync summary error:', e);
          }
        }
      }
    })());
  },
};
