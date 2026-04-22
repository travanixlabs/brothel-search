// Daily sync summary email — runs after the 8pm AEST girl sync.
// Reads git commits since midnight AEST to count today's added/updated/removed
// profiles, then reads local JSONs for today/tomorrow roster counts.

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const RESEND_API_KEY = process.env.RESEND_API_KEY;
if (!RESEND_API_KEY) { console.error('Missing RESEND_API_KEY'); process.exit(1); }

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

function getAEDTDate() { return new Date(Date.now() + 10 * 60 * 60 * 1000); }
function fmtDate(d) { return d.toISOString().split('T')[0]; }

// Parse git commits from today (AEST) for sync activity
function gitCommits() {
  // Start of today in AEST = today 00:00 AEST = yesterday 14:00 UTC
  const now = new Date();
  const aest = getAEDTDate();
  const startOfTodayAEST = new Date(Date.UTC(aest.getUTCFullYear(), aest.getUTCMonth(), aest.getUTCDate(), 0, 0, 0));
  const startUTC = new Date(startOfTodayAEST.getTime() - 10 * 60 * 60 * 1000);
  const sinceIso = startUTC.toISOString();
  try {
    const out = execSync(`git log --since="${sinceIso}" --pretty=format:"%H%x09%s"`, { encoding: 'utf8' });
    return out.trim().split('\n').filter(Boolean).map(line => {
      const [hash, ...rest] = line.split('\t');
      return { hash, subject: rest.join('\t') };
    });
  } catch (e) {
    console.error('git log failed:', e.message);
    return [];
  }
}

function parseSyncCommits() {
  const commits = gitCommits();
  const stats = {};
  for (const v of VENUES) stats[v.id] = { added: 0, updated: 0, removed: 0, rosterSynced: false, errors: [] };

  for (const c of commits) {
    const s = c.subject;
    // Find which venue this commit belongs to
    let venueId = null;
    for (const v of VENUES) {
      const bracketName = `[${v.name}]`;
      if (s.startsWith(bracketName)) { venueId = v.id; break; }
    }
    if (!venueId) continue;

    // Classify commit
    // - "Auto-sync roster" or "Update lastRostered from existing calendar" → roster sync
    // - "Auto-sync: Name1, Name2" → new profiles
    // - "Auto-sync: Name (details)" → updates
    // - "Update X stale photo sets" → photo updates (updated)
    // - "Flag X dead profiles" → removed
    // - "Remove X dead profiles" → removed
    if (/Auto-sync roster/i.test(s) || /lastRostered/i.test(s)) {
      stats[venueId].rosterSynced = true;
      continue;
    }
    const autoSyncMatch = s.match(/Auto-sync:?\s*(.*?)(?:\s+\(|$)/i);
    if (autoSyncMatch) {
      // Extract names — comma separated. Look for "(details)" suffix for updates.
      const namesList = s.replace(/^\[[^\]]+\]\s*(?:Auto-sync(?: new girls)?:?\s*)?/i, '');
      // Count (details) vs plain names
      const detailParts = namesList.split(',').map(x => x.trim()).filter(Boolean);
      for (const p of detailParts) {
        if (/\(details\)/i.test(p)) stats[venueId].updated++;
        else if (p) stats[venueId].added++;
      }
      continue;
    }
    // Photo/dead flags
    const photoMatch = s.match(/(\d+)\s+(?:stale|photo|broken)\s*photo/i);
    if (photoMatch) stats[venueId].updated += parseInt(photoMatch[1]);
    const deadMatch = s.match(/(?:Flag|Remove)\s+(\d+)\s+dead/i);
    if (deadMatch) stats[venueId].removed += parseInt(deadMatch[1]);
  }
  return stats;
}

function loadRosterCounts() {
  const todayStr = fmtDate(getAEDTDate());
  const tomorrow = new Date(getAEDTDate()); tomorrow.setDate(tomorrow.getDate() + 1);
  const tomorrowStr = fmtDate(tomorrow);
  const counts = {};
  for (const v of VENUES) {
    try {
      const data = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'profiles', v.file), 'utf8'));
      const cal = data.calendar || {};
      let today = 0, tmr = 0;
      for (const [name, slots] of Object.entries(cal)) {
        if (name === '_published') continue;
        if (slots && slots[todayStr]) today++;
        if (slots && slots[tomorrowStr]) tmr++;
      }
      counts[v.id] = { today, tomorrow: tmr };
    } catch (e) {
      counts[v.id] = { today: 0, tomorrow: 0 };
    }
  }
  return counts;
}

function buildEmail(syncStats, rosterCounts) {
  const totalAdded = VENUES.reduce((s, v) => s + syncStats[v.id].added, 0);
  const totalUpdated = VENUES.reduce((s, v) => s + syncStats[v.id].updated, 0);
  const totalRemoved = VENUES.reduce((s, v) => s + syncStats[v.id].removed, 0);
  const totalRosterToday = VENUES.reduce((s, v) => s + rosterCounts[v.id].today, 0);
  const totalRosterTmr = VENUES.reduce((s, v) => s + rosterCounts[v.id].tomorrow, 0);
  const runLabel = new Date().toLocaleString('en-AU', { timeZone: 'Australia/Sydney', dateStyle: 'medium', timeStyle: 'short' });

  let html = `<div style="font-family:Arial,sans-serif;max-width:800px;margin:0 auto;background:#0e0e16;color:#e0d6c8;padding:32px;border-radius:12px">`;
  html += `<h1 style="color:#c9952c;margin:0 0 8px">Brothel Search — Daily Sync Report</h1>`;
  html += `<div style="color:#888;font-size:13px;margin-bottom:24px">${runLabel}</div>`;

  // Summary tiles
  html += `<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:24px">`;
  const tile = (value, label, color) => `<div style="flex:1;min-width:120px;padding:16px;background:#1a1a2e;border-radius:8px;border:1px solid ${color}22;text-align:center"><div style="font-size:28px;font-weight:700;color:${color}">${value}</div><div style="font-size:11px;color:#888;text-transform:uppercase;letter-spacing:1px">${label}</div></div>`;
  html += tile(totalAdded, 'New Profiles', '#00c864');
  html += tile(totalRemoved, 'Removed', '#e74c3c');
  html += tile(totalUpdated, 'Updated', '#4a9eff');
  html += tile(totalRosterToday, 'Roster Today', '#c9952c');
  html += tile(totalRosterTmr, 'Roster Tomorrow', '#c9952c');
  html += `</div>`;

  // Per-venue table
  html += `<table cellpadding="8" cellspacing="0" border="0" style="width:100%;border-collapse:collapse;font-size:13px">`;
  html += `<thead><tr style="background:#1a1a2e;color:#c9952c"><th style="text-align:left">Venue</th><th>New Profiles</th><th>Removed</th><th>Updated</th><th>Roster Today</th><th>Roster Tomorrow</th></tr></thead><tbody>`;
  for (const v of VENUES) {
    const s = syncStats[v.id];
    const r = rosterCounts[v.id];
    const color = n => n > 0 ? '#00c864' : '#555';
    html += `<tr style="border-bottom:1px solid #1a1a2e">`;
    html += `<td style="color:#e0d6c8">${v.name}</td>`;
    html += `<td style="text-align:center;color:${color(s.added)}">${s.added}</td>`;
    html += `<td style="text-align:center;color:${s.removed > 0 ? '#e74c3c' : '#555'}">${s.removed}</td>`;
    html += `<td style="text-align:center;color:${s.updated > 0 ? '#4a9eff' : '#555'}">${s.updated}</td>`;
    html += `<td style="text-align:center;color:${color(r.today)}">${r.today}</td>`;
    html += `<td style="text-align:center;color:${color(r.tomorrow)}">${r.tomorrow}</td>`;
    html += `</tr>`;
  }
  html += `</tbody></table>`;
  html += `<p style="font-size:11px;color:#555;margin-top:24px">Counts derived from today's git commit history (AEST).</p>`;
  html += `</div>`;
  return { html, totalAdded, totalRosterToday };
}

async function main() {
  console.log('[SyncSummary] Parsing commit history...');
  const syncStats = parseSyncCommits();
  console.log('[SyncSummary] Reading roster counts...');
  const rosterCounts = loadRosterCounts();

  const { html, totalAdded, totalRosterToday } = buildEmail(syncStats, rosterCounts);
  const subject = `Daily Sync Report — +${totalAdded} new, ${totalRosterToday} rostered today`;

  try {
    const resp = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { Authorization: `Bearer ${RESEND_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        from: 'Brothel Search <info@travanixlabs.com>',
        to: ['info@travanixlabs.com'],
        subject,
        html,
      }),
    });
    if (!resp.ok) {
      const t = await resp.text();
      console.error('[SyncSummary] Resend error:', resp.status, t);
      process.exit(1);
    }
    console.log('[SyncSummary] Email sent.');
  } catch (e) {
    console.error('[SyncSummary] Email error:', e.message);
    process.exit(1);
  }
}

main();
