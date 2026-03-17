const https = require('https');
const fs = require('fs');
const path = require('path');

const REPO = 'travanixlabs/brothel-search';
const BASE = 'profiles/fantasyclub35';

function fetchUrl(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'Mozilla/5.0' } }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400) {
        let loc = res.headers.location;
        if (loc && !loc.startsWith('http')) loc = new URL(loc, url).href;
        return fetchUrl(loc).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) { res.resume(); return reject(new Error('HTTP ' + res.statusCode)); }
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

function getProfileImages(html) {
  // Extract from Elementor gallery hrefs (most reliable)
  const galleryRe = /e-gallery-item[^>]*href="([^"]+)"/gi;
  const images = [];
  let m;
  while ((m = galleryRe.exec(html)) !== null) {
    const u = m[1];
    if (/\.(?:jpe?g|png|webp)$/i.test(u)) images.push(u);
  }
  if (images.length > 0) return images;

  // Fallback for pages without Elementor gallery: find profile images in uploads
  const skipRe = /(?:icon|logo|bullet|diamond_bullet|out-0|florid|qr|wechat)/i;
  const imgRe = /(https?:\/\/fantasyclub35\.com\.au\/wp-content\/uploads\/[^\s"']+\.(?:jpe?g|png|webp))/gi;
  const seen = new Set();
  while ((m = imgRe.exec(html)) !== null) {
    const u = m[1];
    const fn = decodeURIComponent(u.split('/').pop());
    if (skipRe.test(fn)) continue;
    if (/-\d+x\d+\./.test(u)) continue; // skip resized variants
    if (/-scaled\./i.test(u)) continue;
    if (!seen.has(u)) { seen.add(u); images.push(u); }
  }
  return images;
}

(async () => {
  const d = JSON.parse(fs.readFileSync(BASE + '/fantasyclub35.json', 'utf8'));
  let totalImages = 0;
  let errors = 0;

  for (let gi = 0; gi < d.girls.length; gi++) {
    const g = d.girls[gi];
    if (!g.oldUrl) continue;

    process.stdout.write(`[${gi + 1}/${d.girls.length}] ${g.name}: `);

    try {
      const html = await fetchUrl(g.oldUrl);
      const images = getProfileImages(html);

      // Use source URLs directly (no download)
      g.photos = images;
      totalImages += images.length;
      console.log(images.length + ' photos');

      // Delete local image folder if it exists
      const dir = path.join(BASE, g.name);
      if (fs.existsSync(dir)) {
        fs.readdirSync(dir).forEach(f => fs.unlinkSync(path.join(dir, f)));
        fs.rmdirSync(dir);
      }

      // Save progress every 10 profiles
      if ((gi + 1) % 10 === 0) {
        fs.writeFileSync(BASE + '/fantasyclub35.json', JSON.stringify(d, null, 2));
      }

      await sleep(500);
    } catch (e) {
      console.log('ERROR: ' + e.message);
      errors++;
    }
  }

  fs.writeFileSync(BASE + '/fantasyclub35.json', JSON.stringify(d, null, 2));
  console.log(`\nDone! ${totalImages} images across ${d.girls.length} profiles. Errors: ${errors}`);
})();
