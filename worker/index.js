/*
 * Cloudflare Worker — Brothel Search Data Sync
 *
 * Scrapes 479ginza.com.au directly and maintains a combined JSON file
 * at profiles/Ginza Empire/ginzaempire.json in the brothel-search repo.
 *
 * Cron schedule:
 *   9:00 UTC  (7pm AEST) — sync girls (new profiles)
 *   10:00 UTC (8pm AEST) — sync calendar (roster)
 *
 * Secrets required (set via Cloudflare dashboard or `wrangler secret put`):
 *   GITHUB_TOKEN — GitHub personal access token (contents read/write scope)
 */

const REPO = 'travanixlabs/brothel-search';
const GH_API = 'https://api.github.com';
const COMBINED_PATH = 'profiles/Ginza Empire/ginzaempire.json';

const GIRLS_URL = 'https://479ginza.com.au/Girls';
const ROSTER_URL = 'https://479ginza.com.au/Roster';

const UA = 'Mozilla/5.0 (compatible; BrothelSearchBot/1.0)';

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
  const m = timeStr.match(/^(\d{1,2})(?::(\d{2}))?([ap]m)$/i);
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

/* ── Girls scraping ── */

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
};

const LABEL_PATTERNS = [
  ['Double Lesbian', /\blesbian\s*double\b/i],
  ['Shower Together', /\bshower\s*together\b/i],
  ['Pussy Slide', /\bpussy\s*slide\b/i],
  ['DFK', /\bDFK\b/],
  ['BBBJ', /\bBBBJ\b/],
  ['DATY', /\bDATY\b/],
  ['69', /\b69\b/],
  ['CIM', /\bCIM\b/],
  ['COB', /\bCOB\b/],
  ['COF', /\bCOF\b/],
  ['Rimming', /\brimming\b/i],
  ['Anal', /\ban[- ]?al\b/i],
  ['Double', /\bdouble\b/i],
  ['Swallow', /\bswallow\b/i],
  ['Filming', /\bfilming\b/i],
  ['GFE', /\bGFE\b/],
  ['PSE', /\bPSE\b/],
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

async function scrapeGirlsListing() {
  const resp = await fetch(GIRLS_URL, { headers: { 'User-Agent': UA } });
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
    const cup    = (cardHtml.match(/Cup Size:([\w\-]+)/) || [])[1] || '';
    const height = (cardHtml.match(/Height:(\d+)/)       || [])[1] || '';

    cards.push({ id, ...parsed, age, body, cup, height });
  }
  return cards;
}

async function scrapeGirlProfile(id) {
  const resp = await fetch(`${GIRLS_URL}/${id}`, { headers: { 'User-Agent': UA } });
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

  // Images: source URLs from 479ginza.com.au
  const imgRe = /<a[^>]+href="(\/data\/upload\/[^"]+\.\w+)"[^>]*>/gi;
  const images = [];
  let im;
  while ((im = imgRe.exec(html)) !== null) {
    const src = im[1];
    if (/s\.\w+$/i.test(src)) continue;
    if (/\.(jpe?g|png|webp)$/i.test(src)) {
      images.push('https://479ginza.com.au' + src);
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

  return { val1, val2, val3, images, desc, profileHeight, profileType, profileLang, profileExp };
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

async function scrapeRoster() {
  const resp = await fetch(ROSTER_URL, { headers: { 'User-Agent': UA } });
  if (!resp.ok) throw new Error(`Roster fetch failed: ${resp.status}`);
  const html = await resp.text();

  const text = html
    .replace(/<[^>]*>/g, '')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&#\d+;/g, '')
    .replace(/&[a-z]+;/g, '');

  const lines = text.split('\n').map(l => l.trim()).filter(Boolean);

  const dayHeaderRe = /Happy\s+\w+\s+(\d+)\w*\s+of\s+(\w+)/i;
  const entryRe = /(\w[\w .]*?)\s+(\d{1,2}(?::\d{2})?[ap]m)-(\d{1,2}(?::\d{2})?[ap]m)/i;

  const result = {};
  let currentDate = null;

  for (const line of lines) {
    const dayMatch = line.match(dayHeaderRe);
    if (dayMatch) {
      currentDate = resolveDate(parseInt(dayMatch[1], 10), dayMatch[2]);
      continue;
    }
    if (!currentDate) continue;

    const entryMatch = line.match(entryRe);
    if (entryMatch) {
      const rawName = entryMatch[1].trim();
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

/* ── Load existing combined.json (or default) ── */

async function loadCombined(env) {
  try {
    const { content, sha } = await ghGet(env, COMBINED_PATH);
    return { combined: content, sha };
  } catch {
    return {
      combined: { girls: [], calendar: {}, lastGirlsSync: null, lastCalendarSync: null },
      sha: null,
    };
  }
}

/* ── Sync: Girls ── */

// Cloudflare Workers free tier: 50 subrequests per invocation.
// Each girl needs ~1 profile fetch + N image uploads + SHA checks.
// Process max 2 new girls per run to stay within limits.
const MAX_NEW_PER_RUN = 2;

async function syncGirls(env) {
  const { combined, sha } = await loadCombined(env);
  const existing = combined.girls || [];
  const knownNames = new Set(existing.map(g => g.name));
  const knownUrls = new Set(existing.map(g => g.oldUrl).filter(Boolean));

  const cards = await scrapeGirlsListing();
  const allNew = cards.filter(c => {
    const url = `https://479ginza.com.au/Girls/${c.id}`;
    return !knownNames.has(c.name) && !knownUrls.has(url);
  });

  if (allNew.length === 0) {
    console.log('Girls sync: no new profiles');
    return { added: 0, remaining: 0, names: [] };
  }

  // Batch: only process up to MAX_NEW_PER_RUN per invocation
  const newCards = allNew.slice(0, MAX_NEW_PER_RUN);
  const remaining = allNew.length - newCards.length;

  console.log(`Girls sync: ${allNew.length} new, processing ${newCards.length} this run (${remaining} remaining)`);
  const now = new Date().toISOString();
  const todayStr = now.split('T')[0];
  const addedNames = [];

  for (const card of newCards) {
    try {
      await new Promise(r => setTimeout(r, 1000)); // polite delay
      const profile = await scrapeGirlProfile(card.id);

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
      entry.startDate = todayStr;
      entry.lang = profile.profileLang || (card.country.length ? LANG_FROM_COUNTRY[card.country[0]] || '' : '');
      entry.oldUrl = `https://479ginza.com.au/Girls/${card.id}`;
      entry.type = profile.profileType || '';
      entry.desc = profile.desc || '';
      // Download & upload images to profiles/{Name}/{Name}_1.jpeg, etc.
      const photos = [];
      for (let i = 0; i < profile.images.length; i++) {
        try {
          const ext = (profile.images[i].match(/\.(jpe?g|png|webp)$/i) || [])[1] || 'jpeg';
          const path = `profiles/Ginza Empire/${card.name}/${card.name}_${i + 1}.${ext}`;
          const ghUrl = await uploadImage(env, profile.images[i], path);
          photos.push(ghUrl);
          await new Promise(r => setTimeout(r, 500)); // delay between uploads
        } catch (e) {
          console.error(`Image error ${card.name} #${i + 1}: ${e.message}`);
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
      console.log(`Girls sync: added ${card.name} (${profile.images.length} photos)`);
    } catch (e) {
      console.error(`Failed to process ${card.name}: ${e.message}`);
    }
  }

  if (addedNames.length > 0) {
    combined.girls = existing;
    combined.lastGirlsSync = now;
    await ghPut(env, COMBINED_PATH, combined, sha,
      `Auto-sync new girls: ${addedNames.join(', ')}`);
    console.log(`Girls sync: combined.json updated with ${addedNames.length} new profile(s)`);
  }

  return { added: addedNames.length, remaining, names: addedNames };
}

/* ── Sync: Calendar ── */

async function syncCalendar(env) {
  const scraped = await scrapeRoster();
  if (Object.keys(scraped).length === 0) {
    console.log('Roster scrape: no data found');
    return false;
  }

  const { combined, sha } = await loadCombined(env);
  const calendar = combined.calendar || {};
  const validNames = new Set((combined.girls || []).map(g => g.name));

  let changed = false;

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

  // Remove old dates from each girl's schedule
  for (const key of Object.keys(calendar)) {
    if (key.startsWith('_')) continue; // skip _published, _bookings
    const sched = calendar[key];
    if (typeof sched !== 'object') continue;
    for (const dateStr of Object.keys(sched)) {
      if (dateStr < cutoff) {
        delete sched[dateStr];
        changed = true;
      }
    }
  }

  // Remove old dates from _published
  const before = calendar._published.length;
  calendar._published = calendar._published.filter(d => d >= cutoff);
  if (calendar._published.length !== before) changed = true;

  if (!changed) {
    console.log('Calendar sync: no changes needed');
    return true;
  }

  const now = new Date().toISOString();
  combined.calendar = calendar;
  combined.lastCalendarSync = now;

  await ghPut(env, COMBINED_PATH, combined, sha,
    'Auto-sync roster from 479ginza.com.au');

  console.log('Calendar sync: combined.json updated');
  return true;
}

/* ── Export ── */

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Manual triggers: POST /sync-girls, POST /sync-calendar, POST /sync-all
    // POST /sync-girls — sync one batch of new girls
    if (url.pathname === '/sync-girls' && request.method === 'POST') {
      try {
        const result = await syncGirls(env);
        return new Response(JSON.stringify(result), {
          headers: { 'Content-Type': 'application/json' },
        });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), { status: 500 });
      }
    }

    // POST /sync-calendar — sync roster
    if (url.pathname === '/sync-calendar' && request.method === 'POST') {
      try {
        const result = await syncCalendar(env);
        return new Response(JSON.stringify({ success: result }), {
          headers: { 'Content-Type': 'application/json' },
        });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), { status: 500 });
      }
    }

    return new Response('Not found', { status: 404 });
  },

  async scheduled(event, env, ctx) {
    const hour = new Date(event.scheduledTime).getUTCHours();

    // 9:00 UTC = 7pm AEST — girls sync (new profiles)
    if (hour === 9) {
      ctx.waitUntil(
        syncGirls(env).catch(e => console.error('Girls sync error:', e))
      );
    }

    // 10:00 UTC = 8pm AEST — calendar sync
    if (hour === 10) {
      ctx.waitUntil(
        syncCalendar(env).catch(e => console.error('Calendar sync error:', e))
      );
    }
  },
};
