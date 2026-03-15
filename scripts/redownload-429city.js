/*
 * Re-download ALL 429 City images from each profile's oldUrl
 */
const https = require('https');
const fs = require('fs');
const path = require('path');

const REPO = 'travanixlabs/brothel-search';
const BASE = 'profiles/429city';
const JSON_PATH = BASE + '/429city.json';

function fetchUrl(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' } }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetchUrl(res.headers.location).then(resolve).catch(reject);
      }
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ status: res.statusCode, data }));
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

const BANNER_KEYWORDS = ['sydney-brothel', 'qr-code', '429-city-brothel', 'for-tonight'];

function getImages(html) {
  const re = /(https?:\/\/www\.429city\.com\/wp-content\/uploads\/[^\s"']+\.(?:jpe?g|png|webp|gif))/gi;
  const all = [];
  let m;
  while ((m = re.exec(html)) !== null) all.push(m[1]);

  const groups = {};
  for (const u of all) {
    const fn = decodeURIComponent(u.split('/').pop()).toLowerCase();
    // Skip banners
    if (BANNER_KEYWORDS.some(k => fn.includes(k))) continue;
    // Skip tiny thumbs
    if (/-\d+x\d+\./.test(u)) {
      const r = u.match(/-(\d+)x(\d+)\./);
      if (r && parseInt(r[1]) <= 300 && parseInt(r[2]) <= 300) continue;
    }
    // Skip sidebar thumbs (80x80)
    if (/-80x80\./.test(u)) continue;

    const base = u.replace(/-scaled\.(jpe?g|png|webp|gif)$/i, '.$1').replace(/-\d+x\d+\.(jpe?g|png|webp|gif)$/i, '.$1');
    if (!groups[base]) groups[base] = { scaled: null, res: null, orig: null, px: 0 };
    if (/-scaled\./i.test(u)) groups[base].scaled = u;
    else if (/-\d+x\d+\./i.test(u)) {
      const r = u.match(/-(\d+)x(\d+)\./);
      const px = r ? parseInt(r[1]) * parseInt(r[2]) : 0;
      if (px > groups[base].px) { groups[base].res = u; groups[base].px = px; }
    } else groups[base].orig = u;
  }
  return Object.values(groups).map(g => g.scaled || g.orig || g.res).filter(Boolean);
}

async function main() {
  const d = JSON.parse(fs.readFileSync(JSON_PATH, 'utf8'));
  let total = 0, errors = 0, noImages = 0;

  for (let idx = 0; idx < d.girls.length; idx++) {
    const g = d.girls[idx];
    const label = `[${idx + 1}/${d.girls.length}] ${g.name}`;

    if (!g.oldUrl) {
      console.log(`${label}: no oldUrl, skipping`);
      continue;
    }

    try {
      const { status, data: html } = await fetchUrl(g.oldUrl);
      if (status === 404) {
        console.log(`${label}: 404, skipping`);
        g.photos = [];
        continue;
      }

      const images = getImages(html);
      if (images.length === 0) {
        console.log(`${label}: 0 images found`);
        g.photos = [];
        noImages++;
        continue;
      }

      const dir = path.join(BASE, g.name);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

      const photos = [];
      for (let i = 0; i < images.length; i++) {
        const ext = (images[i].match(/\.(jpe?g|png|webp|gif)$/i) || [])[1] || 'jpg';
        const fname = `${g.name}_${i + 1}.${ext}`;
        try {
          await download(images[i], path.join(dir, fname));
          photos.push(`https://raw.githubusercontent.com/${REPO}/main/${BASE}/${encodeURIComponent(g.name)}/${encodeURIComponent(fname)}`);
          process.stdout.write('.');
        } catch (e) {
          process.stdout.write('x');
        }
      }
      g.photos = photos;
      total += photos.length;
      console.log(` ${label}: ${photos.length} photos`);

      // Save every 20
      if ((idx + 1) % 20 === 0) {
        fs.writeFileSync(JSON_PATH, JSON.stringify(d, null, 2));
        console.log(`  [Saved at ${idx + 1}]`);
      }

      await sleep(300);
    } catch (e) {
      console.log(`${label}: ERROR ${e.message}`);
      errors++;
    }
  }

  fs.writeFileSync(JSON_PATH, JSON.stringify(d, null, 2));
  console.log(`\nDone! ${total} images across ${d.girls.length} profiles. Errors: ${errors}. No images: ${noImages}`);
}

main().catch(console.error);
