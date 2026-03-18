const https = require('https');
const fs = require('fs');

function fetchUrl(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'Mozilla/5.0' } }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400) {
        let loc = res.headers.location;
        if (loc && !loc.startsWith('http')) loc = new URL(loc, url).href;
        return fetchUrl(loc).then(resolve).catch(reject);
      }
      let data = ''; res.on('data', c => data += c);
      res.on('end', () => resolve(data));
    }).on('error', reject);
  });
}

function t12to24(t) {
  t = t.trim().toLowerCase();
  if (t === 'close') return '05:00';
  const m = t.match(/^(\d{1,2})(?::(\d{2}))?([ap]m)$/);
  if (!m) return null;
  let h = parseInt(m[1]);
  const min = m[2] ? parseInt(m[2]) : 0;
  if (m[3] === 'pm' && h !== 12) h += 12;
  if (m[3] === 'am' && h === 12) h = 0;
  return String(h).padStart(2, '0') + ':' + String(min).padStart(2, '0');
}

const now = new Date();
const today = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0') + '-' + String(now.getDate()).padStart(2, '0');
console.log('Today:', today);

async function syncFC35() {
  console.log('\n=== Fantasy Club 35 ===');
  const html = await fetchUrl('https://fantasyclub35.com.au/roster/');
  const d = JSON.parse(fs.readFileSync('profiles/fantasyclub35.json', 'utf8'));
  const cal = d.calendar || { _published: [] };
  const validNames = new Set(d.girls.map(g => g.name));
  const girlsByName = {};
  d.girls.forEach(g => girlsByName[g.name] = g);

  const weekMatch = html.match(/Week\s+(\d{1,2})\/(\d{1,2})\/(\d{4})\s+to\s+(\d{1,2})\/(\d{1,2})\/(\d{4})/i);
  if (!weekMatch) { console.log('No week dates found'); return; }
  const startDate = new Date(parseInt(weekMatch[3]), parseInt(weekMatch[2]) - 1, parseInt(weekMatch[1]));
  console.log('Week:', weekMatch[0]);

  // Tabs use kt-inner-tab-1 through kt-inner-tab-7 (Mon=1, Sun=7)
  for (let tabNum = 1; tabNum <= 7; tabNum++) {
    const tabMarker = 'kt-inner-tab-' + tabNum;
    const tabStart = html.indexOf(tabMarker);
    if (tabStart === -1) continue;
    const nextTab = html.indexOf('kt-inner-tab-' + (tabNum + 1), tabStart);
    const section = html.substring(tabStart, nextTab > tabStart ? nextTab : tabStart + 5000);
    const text = section.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ');

    const dayDate = new Date(startDate);
    dayDate.setDate(dayDate.getDate() + (tabNum - 1));
    const dateStr = dayDate.getFullYear() + '-' + String(dayDate.getMonth() + 1).padStart(2, '0') + '-' + String(dayDate.getDate()).padStart(2, '0');
    const dayName = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][tabNum - 1];

    const entryRe = /([A-Za-z]+)\s*[\(\uff08]\s*(HK|CN|JP|VN|TH|SG|TW)\s*[\)\uff09]\s*(?:NEW\s+)?(\d{1,2}[ap]m)\s*-\s*(\d{1,2}[ap]m)/gi;
    let em, count = 0;
    while ((em = entryRe.exec(text)) !== null) {
      const name = em[1].trim();
      const start = t12to24(em[3]);
      const end = t12to24(em[4]);
      if (!start || !end) continue;
      if (validNames.has(name)) {
        if (!cal[name]) cal[name] = {};
        cal[name][dateStr] = { start, end };
        if (girlsByName[name] && (!girlsByName[name].lastRostered || dateStr > girlsByName[name].lastRostered))
          girlsByName[name].lastRostered = dateStr;
        count++;
      }
    }
    if (!cal._published.includes(dateStr)) cal._published.push(dateStr);
    if (count) console.log('  ' + dateStr + ' (' + dayName + '): ' + count + ' girls');
  }
  cal._published.sort();
  d.calendar = cal;
  d.lastCalendarSync = new Date().toISOString();
  fs.writeFileSync('profiles/fantasyclub35.json', JSON.stringify(d, null, 2));

  const todayCount = Object.keys(cal).filter(k => !k.startsWith('_') && cal[k][today]).length;
  console.log('Total rostered today: ' + todayCount);
}

async function sync429() {
  console.log('\n=== 429 City ===');
  const html = await fetchUrl('https://www.429city.com/roster/');
  const d = JSON.parse(fs.readFileSync('profiles/429city.json', 'utf8'));
  const cal = d.calendar || { _published: [] };
  const validUrls = {};
  d.girls.forEach(g => { if (g.oldUrl) validUrls[g.oldUrl.replace(/\/$/, '/').toLowerCase()] = g.name; });
  const girlsByName = {};
  d.girls.forEach(g => girlsByName[g.name] = g);

  const re = /href=["']?(https?:\/\/www\.429city\.com\/[a-z0-9%\-]+\/?)["']?/gi;
  const links = new Set();
  let m;
  while ((m = re.exec(html)) !== null) {
    const url = m[1].replace(/\/$/, '/');
    const path = url.replace('https://www.429city.com/', '').replace(/\/$/, '');
    if (path && !['ladies', 'roster', 'contact', 'rate', 'escort', 'work-for-us', 'wp-content', 'feed'].some(x => path.includes(x))) {
      links.add(url);
    }
  }

  let matched = 0, unmatched = [];
  for (const url of links) {
    const name = validUrls[url.toLowerCase()];
    if (name) {
      if (!cal[name]) cal[name] = {};
      cal[name][today] = { start: '10:00', end: '05:00' };
      if (girlsByName[name] && (!girlsByName[name].lastRostered || today > girlsByName[name].lastRostered))
        girlsByName[name].lastRostered = today;
      matched++;
    } else {
      unmatched.push(url);
    }
  }
  if (!cal._published.includes(today)) cal._published.push(today);
  cal._published.sort();
  d.calendar = cal;
  d.lastCalendarSync = new Date().toISOString();
  fs.writeFileSync('profiles/429city.json', JSON.stringify(d, null, 2));
  console.log(matched + ' matched from ' + links.size + ' links');
  if (unmatched.length) console.log('Unmatched:', unmatched);
}

(async () => {
  await syncFC35();
  await sync429();
  console.log('\nDone!');
})();
