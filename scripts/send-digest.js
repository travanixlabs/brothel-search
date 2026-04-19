// Daily digest runner — standalone Node script.
// Runs as a GitHub Action so we don't depend on Cloudflare Workers.
// Reads profile JSONs from the repo checkout, talks to Supabase + Resend via fetch.

const fs = require('fs');
const path = require('path');

const SB_URL = process.env.SUPABASE_URL || 'https://blhwekuidksxiaickeck.supabase.co';
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const RESEND_API_KEY = process.env.RESEND_API_KEY;

if (!SUPABASE_SERVICE_KEY) { console.error('Missing SUPABASE_SERVICE_KEY'); process.exit(1); }

const headers = {
  apikey: SUPABASE_SERVICE_KEY,
  Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
  'Content-Type': 'application/json',
};

const VENUES = [
  { id: 'ginzaempire', name: 'Ginza Empire', file: 'ginzaempire.json' },
  { id: 'ginzaclub', name: 'Ginza Club', file: 'ginzaclub.json' },
  { id: 'kyoto206', name: 'Kyoto 206', file: 'kyoto206.json' },
  { id: 'sakura57', name: 'Sakura 57', file: 'sakura57.json' },
  { id: 'top127', name: 'Top 127', file: 'top127.json' },
  { id: 'fantasyclub35', name: 'Fantasy Club 35', file: 'fantasyclub35.json' },
  { id: '429city', name: '429 City', file: '429city.json' },
  { id: 'pennys77', name: "Penny's 77", file: 'pennys77.json' },
  { id: 'thegoldenapple', name: 'The Golden Apple', file: 'thegoldenapple.json' },
  { id: 'blackcatparlour', name: 'Black Cat Parlour', file: 'blackcatparlour.json' },
  { id: 'bellevue12', name: 'Bellevue 12', file: 'bellevue12.json' },
  { id: 'thegatewayclub', name: 'The Gateway Club', file: 'thegatewayclub.json' },
  { id: 'marrickvillebrothel', name: 'Marrickville Brothel', file: 'marrickvillebrothel.json' },
  { id: 'springhouse', name: 'Spring House', file: 'springhouse.json' },
  { id: 'stiletto', name: 'Stiletto', file: 'stiletto.json' },
  { id: 'wivesonly', name: 'Wives Only', file: 'wivesonly.json' },
  { id: 'jinia', name: 'Jinia', file: 'jinia.json' },
];

const VENUE_REGION_SLUGS = {
  ginzaempire: 'cbdandcentral', ginzaclub: 'cbdandcentral', kyoto206: 'cbdandcentral',
  sakura57: 'cbdandcentral', top127: 'cbdandcentral', fantasyclub35: 'innerwest', '429city': 'cbdandcentral',
  pennys77: 'innerwest', thegoldenapple: 'cbdandcentral', blackcatparlour: 'cbdandcentral', bellevue12: 'cbdandcentral',
  thegatewayclub: 'innerwest', marrickvillebrothel: 'innerwest', springhouse: 'innerwest',
  stiletto: 'innerwest', wivesonly: 'innerwest', jinia: 'westernsuburbs',
};
const VENUE_SUBURBS = {
  ginzaempire: 'surryhills', ginzaclub: 'surryhills', kyoto206: 'surryhills',
  sakura57: 'surryhills', top127: 'chippendale', fantasyclub35: 'annandale', '429city': 'haymarket',
  pennys77: 'newtown', thegoldenapple: 'surryhills', blackcatparlour: 'surryhills', bellevue12: 'surryhills',
  thegatewayclub: 'petersham', marrickvillebrothel: 'marrickville', springhouse: 'marrickville',
  stiletto: 'camperdown', wivesonly: 'stpeters', jinia: 'strathfieldsouth',
};

function getAEDTDate() { return new Date(Date.now() + 10 * 60 * 60 * 1000); }
function fmtDate(d) { return d.toISOString().split('T')[0]; }

function scoreGirl(girl, prefs) {
  if (!prefs) return 0;
  let score = 0, weight = 0;
  if (prefs.age_min != null && prefs.age_max != null && (prefs.age_min !== 18 || prefs.age_max !== 33)) {
    weight += 10;
    if (girl.age && parseInt(girl.age) >= prefs.age_min && parseInt(girl.age) <= prefs.age_max) score += 10;
  }
  if (prefs.body_min != null && prefs.body_max != null && (prefs.body_min !== 4 || prefs.body_max !== 10)) {
    weight += 10;
    if (girl.body && parseInt(girl.body) >= prefs.body_min && parseInt(girl.body) <= prefs.body_max) score += 10;
  }
  if (prefs.height_min != null && prefs.height_max != null && (prefs.height_min !== 150 || prefs.height_max !== 175)) {
    weight += 2;
    if (girl.height && parseInt(girl.height) >= prefs.height_min && parseInt(girl.height) <= prefs.height_max) score += 2;
  }
  if (prefs.cup_min || prefs.cup_max) {
    weight += 2;
    const CUP = ['A','B','C','D','DD','E','F','G','H'];
    const ci = CUP.indexOf((girl.cup || '').toUpperCase());
    const mi = CUP.indexOf((prefs.cup_min || '').toUpperCase());
    const xi = CUP.indexOf((prefs.cup_max || '').toUpperCase());
    if (ci >= 0 && (mi < 0 || ci >= mi) && (xi < 0 || ci <= xi)) score += 2;
  }
  if (prefs.countries && prefs.countries.length > 0) {
    weight += 15;
    const gc = Array.isArray(girl.country) ? girl.country : (girl.country ? [girl.country] : []);
    if (gc.length > 0) { const matched = gc.filter(c => prefs.countries.includes(c)).length; score += (matched / gc.length) * 15; }
  }
  if (weight === 0) return 0;
  return Math.round((score / weight) * 100);
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

function buildDigestEmail(name, { favWorking, favNotWorking, matchesWorking, matchesNotWorking, backOnRoster, dontMissPicks }) {
  let html = `<div style="font-family:Georgia,serif;max-width:600px;margin:0 auto;background:#0e0e16;color:#e0d6c8;padding:32px;border-radius:12px">`;
  html += `<div style="text-align:center;margin-bottom:24px"><span style="font-size:24px;font-weight:700;color:#c9952c;letter-spacing:2px">BROTHEL SEARCH</span></div>`;
  html += `<p style="font-size:16px;margin-bottom:24px">Hi ${name},</p>`;

  const hasWorking = favWorking.length || matchesWorking.length;
  const hasNotWorking = favNotWorking.length || matchesNotWorking.length;

  if (hasWorking) {
    html += `<div style="font-size:13px;font-weight:700;color:#00c864;text-transform:uppercase;letter-spacing:2px;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid #1a1a2e">&#9679; Working Today</div>`;
    html += `<table cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-bottom:24px">`;
    for (const g of favWorking) html += girlCardHtml(g, '#c9952c', 'FAVOURITE');
    for (const g of matchesWorking) html += girlCardHtml(g, '#00c864', g.matchScore + '% MATCH', `<span style="font-size:10px;color:#00c864;margin-left:6px">NEW</span>`);
    html += `</table>`;
  }
  if (backOnRoster && backOnRoster.length) {
    html += `<div style="font-size:13px;font-weight:700;color:#4a9eff;text-transform:uppercase;letter-spacing:2px;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid #1a1a2e">&#9679; Back on Roster</div>`;
    html += `<table cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-bottom:24px">`;
    for (const g of backOnRoster) html += girlCardHtml(g, '#4a9eff', 'BACK AFTER ' + g.rosterGapDays + ' DAYS');
    html += `</table>`;
  }
  if (hasNotWorking) {
    html += `<div style="font-size:13px;font-weight:700;color:#666;text-transform:uppercase;letter-spacing:2px;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid #1a1a2e">Not Working Today</div>`;
    html += `<table cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-bottom:24px">`;
    for (const g of favNotWorking) html += girlCardHtml(g, '#555', 'FAVOURITE');
    for (const g of matchesNotWorking) html += girlCardHtml(g, '#3c78ff', g.matchScore + '% MATCH', `<span style="font-size:10px;color:#3c78ff;margin-left:6px">NEW</span>`);
    html += `</table>`;
  }
  if (dontMissPicks && dontMissPicks.length) {
    html += `<div style="font-size:13px;font-weight:700;color:#c9952c;text-transform:uppercase;letter-spacing:2px;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid #1a1a2e">\u2605 Don't Miss This Week</div>`;
    html += `<div style="font-size:12px;color:#888;margin-bottom:12px;font-style:italic">Hand-picked recommendations based on your preferences.</div>`;
    html += `<table cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-bottom:24px">`;
    for (const g of dontMissPicks) {
      const badge = g.matchScore ? g.matchScore + '% MATCH' : 'PICK FOR YOU';
      html += girlCardHtml(g, '#c9952c', badge);
    }
    html += `</table>`;
  }
  html += `<div style="text-align:center;margin-top:24px"><a href="https://brothelsearch.com/working-now" style="display:inline-block;padding:12px 32px;background:#c9952c;color:#0e0e16;text-decoration:none;border-radius:8px;font-weight:700;letter-spacing:1px;font-size:14px">See Who's Working Now</a></div>`;
  html += `<p style="font-size:11px;color:#555;margin-top:24px;text-align:center">You're receiving this because you have favourites on Brothel Search.</p>`;
  html += `</div>`;
  return html;
}

async function main() {
  console.log('[Digest] Starting...');

  // 1. Favourites
  const allFavs = await fetch(`${SB_URL}/rest/v1/user_favorites?select=user_id,old_url`, { headers }).then(r => r.json());
  if (!allFavs.length) { console.log('[Digest] No favourites found'); return; }
  const userFavs = {};
  for (const f of allFavs) { (userFavs[f.user_id] ||= []).push(f.old_url); }

  // 2. Load all venue data from local filesystem (checked-out repo)
  const todayStr = fmtDate(getAEDTDate());
  const allGirls = [];
  for (const v of VENUES) {
    try {
      const filePath = path.join(__dirname, '..', 'profiles', v.file);
      const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      const calendar = data.calendar || {};
      for (const g of data.girls || []) {
        if (g.deleted === 'Yes') continue;
        g.venue = v.id;
        g.venueName = v.name;
        g.rosteredToday = !!(calendar[g.name] && calendar[g.name][todayStr]);
        if (g.rosteredToday && calendar[g.name]) {
          const prev = Object.keys(calendar[g.name]).filter(d => !d.startsWith('_') && d < todayStr).sort().reverse();
          if (prev.length) {
            g.rosterGapDays = Math.round((new Date(todayStr + 'T00:00:00') - new Date(prev[0] + 'T00:00:00')) / 86400000);
          } else {
            g.rosterGapDays = 999;
          }
        }
        allGirls.push(g);
      }
    } catch (e) { console.error(`[Digest] Error loading ${v.name}:`, e.message); }
  }

  // 3. Preferences, roles, subscriptions
  const allPrefs = await fetch(`${SB_URL}/rest/v1/user_preferences?select=*`, { headers }).then(r => r.json());
  const prefsMap = {}; for (const p of allPrefs) prefsMap[p.id] = p;
  const allRoles = await fetch(`${SB_URL}/rest/v1/user_roles?select=id,role`, { headers }).then(r => r.json());
  const roleMap = {}; for (const r of allRoles) roleMap[r.id] = r.role;
  const allSubs = await fetch(`${SB_URL}/rest/v1/user_subscriptions?status=eq.active&select=user_id`, { headers }).then(r => r.json());
  const activeSubs = new Set((allSubs || []).map(s => s.user_id));

  // 4. User emails (only for admins + subscribed)
  const userIds = Object.keys(userFavs);
  const userEmails = {};
  for (const uid of userIds) {
    const isAdmin = roleMap[uid] === 'admin';
    const isSub = activeSubs.has(uid);
    if (!isAdmin && !isSub) continue;
    try {
      const res = await fetch(`${SB_URL}/auth/v1/admin/users/${uid}`, { headers: { apikey: SUPABASE_SERVICE_KEY, Authorization: `Bearer ${SUPABASE_SERVICE_KEY}` } });
      const u = await res.json();
      if (u.email) userEmails[uid] = { email: u.email, name: u.user_metadata?.display_name || u.user_metadata?.name || u.email.split('@')[0] };
    } catch {}
  }

  // 5. New girls (last 7 days)
  const weekAgo = new Date(); weekAgo.setDate(weekAgo.getDate() - 7);
  const cutoffStr = weekAgo.toISOString().split('T')[0];
  const newGirls = allGirls.filter(g => g.startDate && g.startDate >= cutoffStr);

  const isMonday = getAEDTDate().getDay() === 1;
  let emailsSent = 0;

  // 6. Per-user processing
  for (const [userId, favUrls] of Object.entries(userFavs)) {
    const userInfo = userEmails[userId];
    if (!userInfo) continue;

    const favGirls = allGirls.filter(g => g.oldUrl && favUrls.includes(g.oldUrl));
    const favWorking = favGirls.filter(g => g.rosteredToday);
    const favNotWorking = favGirls.filter(g => !g.rosteredToday);
    const backOnRoster = favGirls.filter(g => g.rosteredToday && g.rosterGapDays >= 7 && g.rosterGapDays < 999);

    // Back on Roster bell notifications (deduped per girl per day)
    for (const g of backOnRoster) {
      try {
        const dup = await fetch(
          `${SB_URL}/rest/v1/notifications?user_id=eq.${userId}&title=eq.Back%20on%20Roster&girl_name=eq.${encodeURIComponent(g.name)}&created_at=gte.${todayStr}T00:00:00Z&select=id&limit=1`,
          { headers }
        ).then(r => r.json());
        if (!dup.length) {
          await fetch(`${SB_URL}/rest/v1/notifications`, { method: 'POST', headers, body: JSON.stringify({
            user_id: userId, type: 'back_on_roster', title: 'Back on Roster',
            body: `${g.name} at ${g.venueName} is back after ${g.rosterGapDays} days away!`,
            venue: g.venue, girl_name: g.name,
          })});
        }
      } catch (e) { console.error('[Digest] Back-on-roster error:', e.message); }
    }

    // Smart Alerts
    try {
      const presets = await fetch(`${SB_URL}/rest/v1/user_filter_presets?user_id=eq.${userId}&select=name,filters`, { headers }).then(r => r.json());
      for (const preset of (presets || [])) {
        if (!preset.filters || !preset.filters.notifyEnabled) continue;
        const f = preset.filters;
        const matchingNew = newGirls.filter(g => {
          if (f.activeVenue && f.activeVenue.include && f.activeVenue.include.length && !f.activeVenue.include.includes(g.venue)) return false;
          if (f.activeVenue && f.activeVenue.exclude && f.activeVenue.exclude.includes(g.venue)) return false;
          if (f.activeCountry && f.activeCountry.include && f.activeCountry.include.length) {
            const gc = Array.isArray(g.country) ? g.country : [g.country || ''];
            if (!gc.some(c => f.activeCountry.include.includes(c))) return false;
          }
          return true;
        });
        if (matchingNew.length) {
          const dup = await fetch(
            `${SB_URL}/rest/v1/notifications?user_id=eq.${userId}&title=eq.Smart%20Alert&body=like.*${encodeURIComponent(preset.name)}*&created_at=gte.${todayStr}T00:00:00Z&select=id&limit=1`,
            { headers }
          ).then(r => r.json());
          if (!dup.length) {
            await fetch(`${SB_URL}/rest/v1/notifications`, { method: 'POST', headers, body: JSON.stringify({
              user_id: userId, type: 'smart_alert', title: 'Smart Alert',
              body: `${matchingNew.length} new girl${matchingNew.length !== 1 ? 's' : ''} matching "${preset.name}": ${matchingNew.slice(0, 3).map(g => g.name).join(', ')}${matchingNew.length > 3 ? ' +' + (matchingNew.length - 3) + ' more' : ''}`,
              venue: null, girl_name: null,
            })});
          }
        }
      }
    } catch (e) { console.error(`[Digest] Smart alert error for ${userId}:`, e.message); }

    // 90%+ new match scoring
    const prefs = prefsMap[userId];
    const matchesWorking = [], matchesNotWorking = [];
    if (prefs) {
      for (const g of newGirls) {
        const score = scoreGirl(g, prefs);
        if (score >= 90) {
          const entry = { ...g, matchScore: score };
          (g.rosteredToday ? matchesWorking : matchesNotWorking).push(entry);
        }
      }
    }

    // Don't Miss — Monday picks
    let dontMissPicks = [];
    if (isMonday) {
      const thirtyAgo = new Date(); thirtyAgo.setDate(thirtyAgo.getDate() - 30);
      const thirtyStr = thirtyAgo.toISOString().split('T')[0];
      const pool = allGirls.filter(g => g.oldUrl && !favUrls.includes(g.oldUrl) && g.photos && g.photos.length && g.lastRostered && g.lastRostered >= thirtyStr);
      if (prefs) {
        dontMissPicks = pool.map(g => ({ ...g, matchScore: scoreGirl(g, prefs) })).filter(g => g.matchScore >= 80).sort((a, b) => b.matchScore - a.matchScore).slice(0, 5);
      } else {
        dontMissPicks = pool.sort(() => Math.random() - 0.5).slice(0, 5);
      }
    }

    // Send email (EMAIL ONLY — no digest bell notification)
    const hasContent = favWorking.length || favNotWorking.length || matchesWorking.length || matchesNotWorking.length || backOnRoster.length || dontMissPicks.length;
    if (RESEND_API_KEY && hasContent) {
      const html = buildDigestEmail(userInfo.name, { favWorking, favNotWorking, matchesWorking, matchesNotWorking, backOnRoster, dontMissPicks });
      const workingCount = favWorking.length + matchesWorking.length;
      const subject = workingCount > 0
        ? `Daily Digest — ${workingCount} working today`
        : dontMissPicks.length && isMonday
        ? `Don't Miss — ${dontMissPicks.length} picks for you this week`
        : 'Daily Digest — Your favourites update';
      try {
        await fetch('https://api.resend.com/emails', {
          method: 'POST',
          headers: { Authorization: `Bearer ${RESEND_API_KEY}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ from: 'Brothel Search <info@travanixlabs.com>', to: userInfo.email, subject, html }),
        });
        emailsSent++;
        console.log(`[Digest] Email sent to ${userInfo.email}`);
      } catch (e) { console.error(`[Digest] Email error for ${userInfo.email}:`, e.message); }
    }
  }

  console.log(`[Digest] Done. ${emailsSent} emails sent across ${userIds.length} users.`);
}

main().catch(e => { console.error('[Digest] Fatal error:', e); process.exit(1); });
