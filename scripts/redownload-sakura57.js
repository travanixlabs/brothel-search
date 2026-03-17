const https = require('https');
const fs = require('fs');
const path = require('path');
const REPO = 'travanixlabs/brothel-search';
const BASE = 'profiles/sakura57';

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

function download(url, dest) {
  return new Promise((resolve, reject) => {
    const dir = path.dirname(dest);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    https.get(url, { headers: { 'User-Agent': 'Mozilla/5.0' } }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400) {
        let loc = res.headers.location;
        if (loc && !loc.startsWith('http')) loc = new URL(loc, url).href;
        return download(loc, dest).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) { res.resume(); return reject(new Error('HTTP ' + res.statusCode)); }
      const file = fs.createWriteStream(dest);
      res.pipe(file);
      file.on('finish', () => { file.close(); resolve(); });
    }).on('error', reject);
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function getImages(rawHtml) {
  // Decode HTML entities
  let html = rawHtml.replace(/&#038;/g, '&').replace(/&amp;/g, '&');
  // Cut at "In Portfolios" to exclude gallery of other girls
  const portIdx = html.indexOf('In Portfolios');
  let contentArea = portIdx > 0 ? html.substring(0, portIdx) : html;

  const re = /(https?:\/\/www\.surryhillsbrothel\.com\.au\/wp-content\/uploads\/[^\s"'<>]+\.(?:jpe?g|png|webp))/gi;
  const all = []; let m;
  while ((m = re.exec(contentArea)) !== null) all.push(m[1]);

  // Group by base, prefer scaled > orig > highest res
  const groups = {};
  for (const u of all) {
    const fn = decodeURIComponent(u.split('/').pop()).toLowerCase();
    if (fn.includes('logo') || fn.includes('qr') || fn.includes('new57') || fn.includes('wechat')) continue;
    if (/-\d+x\d+\./.test(u)) {
      const r = u.match(/-(\d+)x(\d+)\./);
      if (r && parseInt(r[1]) <= 160 && parseInt(r[2]) <= 160) continue;
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
  return Object.values(groups).map(g => g.scaled || g.orig || g.res).filter(Boolean);
}

(async () => {
  const d = JSON.parse(fs.readFileSync(BASE + '/sakura57.json', 'utf8'));
  let totalImages = 0;
  let errors = 0;

  for (let idx = 0; idx < d.girls.length; idx++) {
    const g = d.girls[idx];
    if (!g.oldUrl) { errors++; continue; }

    process.stdout.write(`[${idx + 1}/${d.girls.length}] ${g.name}: `);

    try {
      const html = await fetchUrl(g.oldUrl);
      const images = getImages(html);

      // Clear old folder
      const dir = path.join(BASE, g.name);
      if (fs.existsSync(dir)) {
        fs.readdirSync(dir).forEach(f => fs.unlinkSync(path.join(dir, f)));
      }

      const photos = [];
      for (let i = 0; i < images.length; i++) {
        const ext = (images[i].match(/\.(jpe?g|png|webp)$/i) || [])[1] || 'jpeg';
        const fname = g.name + '_' + (i + 1) + '.' + ext;
        try {
          await download(images[i], path.join(dir, fname));
          photos.push(`https://raw.githubusercontent.com/${REPO}/main/${BASE}/${encodeURIComponent(g.name)}/${encodeURIComponent(fname)}`);
          process.stdout.write('.');
        } catch (e) { process.stdout.write('x'); }
      }
      g.photos = photos;
      totalImages += photos.length;
      console.log(` ${photos.length} photos`);

      // Save progress every 50
      if ((idx + 1) % 50 === 0) {
        fs.writeFileSync(BASE + '/sakura57.json', JSON.stringify(d, null, 2));
        console.log(`  [Saved progress: ${idx + 1} profiles]`);
      }

      await sleep(300);
    } catch (e) {
      console.log(` ERROR: ${e.message}`);
      errors++;
    }
  }

  fs.writeFileSync(BASE + '/sakura57.json', JSON.stringify(d, null, 2));
  console.log(`\nDone! ${totalImages} images across ${d.girls.length} profiles. Errors: ${errors}`);
})();
