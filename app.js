// SPA redirect from 404.html — restore original path
(function() {
  const redirect = sessionStorage.getItem('spa-redirect');
  if (redirect) {
    sessionStorage.removeItem('spa-redirect');
    history.replaceState(null, '', redirect);
  }
})();

// Supabase Auth
const SUPABASE_URL = 'https://blhwekuidksxiaickeck.supabase.co';
const SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJsaHdla3VpZGtzeGlhaWNrZWNrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQwMzMxODEsImV4cCI6MjA4OTYwOTE4MX0.dx8_2UHRJqCJ5aOf2O9ogSYDHY3hUKyGPRJjJiT4ghE';
const sbClient = window.supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

let authMode = 'signin'; // 'signin' or 'signup'
let userRole = 'member'; // 'admin' or 'member'
let userFavorites = []; // array of oldUrl strings
function getMaxFavorites() { return userRole === 'admin' ? Infinity : 10; }

async function fetchUserRole() {
  const { data, error } = await sbClient.from('user_roles').select('role').single();
  if (data) userRole = data.role;
  else userRole = 'member';
  document.body.classList.toggle('is-admin', userRole === 'admin');
  // Update menu display
  const { data: { user } } = await sbClient.auth.getUser();
  if (user) document.getElementById('userMenuEmail').textContent = user.user_metadata?.display_name || user.email;
  document.getElementById('userMenuRole').textContent = userRole;
}

let subscriptionStatus = null;
const WORKER_URL = 'https://brothel-search-sync.travanixlabs.workers.dev';
const STRIPE_PK = 'pk_test_51TDeqBHn68lZzkHWFu0CfjzwjgnfhBWVB1LSf5R7q5JcQLXHJ6euyTI1sZjePJeml0dsMddMyfLFVmFFHwoqpwmL00jd1XcTrc';

async function checkSubscription() {
  try {
    const { data: { session } } = await sbClient.auth.getSession();
    if (!session) return null;
    const res = await fetch(`${WORKER_URL}/subscription-status`, {
      headers: { Authorization: `Bearer ${session.access_token}` },
    });
    const data = await res.json();
    subscriptionStatus = data;
    return data;
  } catch (e) { console.error('Subscription check failed:', e); return null; }
}

function showPaywall() {
  document.getElementById('paywallOverlay').style.display = 'flex';
  document.body.style.overflow = 'hidden';
  if (window.location.hash !== '#subscribe') window.history.replaceState(null, '', '#subscribe');
  const trialBtn = document.getElementById('paywallTrialBtn');
  if (subscriptionStatus && subscriptionStatus.trialUsed) {
    trialBtn.style.opacity = '0.4';
    trialBtn.style.pointerEvents = 'none';
    trialBtn.querySelector('.paywall-plan-note').textContent = 'Already used';
  }
}

function hidePaywall() {
  document.getElementById('paywallOverlay').style.display = 'none';
  document.body.style.overflow = '';
  if (window.location.hash === '#subscribe') window.history.replaceState(null, '', window.location.pathname);
}

async function selectPlan(plan) {
  const { data: { session } } = await sbClient.auth.getSession();
  if (!session) return;
  const btn = document.querySelector(`[data-plan="${plan}"]`);
  if (btn) btn.textContent = 'Redirecting...';
  try {
    const res = await fetch(`${WORKER_URL}/create-checkout`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        plan,
        userId: session.user.id,
        email: session.user.email,
        returnUrl: window.location.origin + window.location.pathname,
      }),
    });
    const data = await res.json();
    if (data.error) { alert(data.error); if (btn) btn.textContent = 'Select'; return; }
    if (data.sessionUrl) window.location.href = data.sessionUrl;
  } catch (e) { alert('Payment error: ' + e.message); if (btn) btn.textContent = 'Select'; }
}

async function checkAuth() {
  const { data: { session } } = await sbClient.auth.getSession();
  if (session) {
    document.getElementById('authOverlay').style.display = 'none'; document.body.style.overflow = '';
    document.getElementById('userMenu').style.display = '';
    await fetchUserRole();
    await loadFavorites();
    return true;
  }
  document.getElementById('authOverlay').style.display = 'flex'; document.body.style.overflow = 'hidden';
  document.getElementById('userMenu').style.display = 'none';
  return false;
}

async function handleAuth() {
  const email = document.getElementById('authEmail').value.trim();
  const password = document.getElementById('authPassword').value;
  const errorEl = document.getElementById('authError');
  const btn = document.getElementById('authBtn');
  errorEl.textContent = '';

  const profileName = document.getElementById('authName').value.trim();
  const confirmPassword = authMode === 'signup' ? document.getElementById('authPasswordConfirm').value : '';

  // Clear previous red borders
  ['authName', 'authEmail', 'authPassword', 'authPasswordConfirm'].forEach(id => document.getElementById(id).style.borderColor = '');

  // Check empty fields
  const missing = [];
  if (authMode === 'signup' && !profileName) missing.push('authName');
  if (!email) missing.push('authEmail');
  if (!password) missing.push('authPassword');
  if (authMode === 'signup' && !confirmPassword) missing.push('authPasswordConfirm');
  if (missing.length) {
    missing.forEach(id => document.getElementById(id).style.borderColor = '#ff4444');
    errorEl.textContent = 'Please fill in the highlighted fields';
    return;
  }

  if (authMode === 'signup') {
    if (password.length < 8) { errorEl.textContent = 'Password must be at least 8 characters'; return; }
    if (!/[a-z]/.test(password)) { errorEl.textContent = 'Password must contain a lowercase letter'; return; }
    if (!/[A-Z]/.test(password)) { errorEl.textContent = 'Password must contain an uppercase letter'; return; }
    if (!/[0-9]/.test(password)) { errorEl.textContent = 'Password must contain a number'; return; }
    if (!/[^a-zA-Z0-9]/.test(password)) { errorEl.textContent = 'Password must contain a special character'; return; }
    if (password !== confirmPassword) { errorEl.textContent = 'Passwords do not match'; return; }
  }

  btn.disabled = true;
  btn.textContent = authMode === 'signin' ? 'Signing in...' : 'Signing up...';

  let result;
  if (authMode === 'signin') {
    result = await sbClient.auth.signInWithPassword({ email, password });
  } else {
    result = await sbClient.auth.signUp({ email, password, options: { data: { display_name: profileName } } });
  }

  btn.disabled = false;
  btn.textContent = authMode === 'signin' ? 'Sign In' : 'Sign Up';

  if (result.error) {
    errorEl.textContent = result.error.message;
    return;
  }

  if (authMode === 'signup' && result.data?.user?.identities?.length === 0) {
    errorEl.textContent = 'An account with this email already exists';
    return;
  }

  if (authMode === 'signup' && !result.data.session) {
    errorEl.style.color = '#00c864';
    errorEl.textContent = 'Check your email to confirm your account';
    return;
  }

  document.getElementById('authOverlay').style.display = 'none'; document.body.style.overflow = '';
  document.getElementById('userMenu').style.display = '';
  await fetchUserRole();
  await loadFavorites();
  loadProfiles();
}

function toggleAuthMode() {
  authMode = authMode === 'signin' ? 'signup' : 'signin';
  const isSignup = authMode === 'signup';
  document.getElementById('authBtn').textContent = isSignup ? 'Create Account' : 'Sign In';
  document.getElementById('authSubtitle').textContent = isSignup ? 'Create a new account' : 'Sign in to continue';
  document.getElementById('authToggle').textContent = isSignup ? 'Already have an account? Sign in' : "Don't have an account? Sign up";
  document.getElementById('authName').style.display = isSignup ? '' : 'none';
  document.getElementById('authName').value = '';
  document.getElementById('authPassword').setAttribute('autocomplete', isSignup ? 'new-password' : 'current-password');
  document.getElementById('authPassword').setAttribute('placeholder', isSignup ? 'Password' : 'Password');
  document.getElementById('authPasswordConfirm').style.display = isSignup ? '' : 'none';
  document.getElementById('authPasswordReqs').style.display = isSignup ? '' : 'none';
  document.getElementById('authPasswordConfirm').value = '';
  document.getElementById('authError').textContent = '';
  document.getElementById('authResetPw').style.display = isSignup ? 'none' : '';
}

async function signOut() {
  await sbClient.auth.signOut();
  userRole = 'member';
  document.body.classList.remove('is-admin');
  hidePaywall();
  document.getElementById('authOverlay').style.display = 'flex'; document.body.style.overflow = 'hidden';
  document.getElementById('userMenu').style.display = 'none';
}

async function resetPassword() {
  const email = document.getElementById('authEmail').value.trim();
  const errorEl = document.getElementById('authError');
  if (!email) {
    errorEl.textContent = 'Please enter your email address';
    errorEl.style.color = '#ff4444';
    return;
  }
  const { error } = await sbClient.auth.resetPasswordForEmail(email, {
    redirectTo: window.location.origin + window.location.pathname
  });
  if (error) {
    errorEl.textContent = error.message;
    errorEl.style.color = '#ff4444';
  } else {
    errorEl.textContent = 'Password reset link sent to your email';
    errorEl.style.color = '#00c864';
  }
}

function checkPasswordReqs() {
  const pw = document.getElementById('authPassword').value;
  const checks = [
    { id: 'reqLen', pass: pw.length >= 8 },
    { id: 'reqUpper', pass: /[A-Z]/.test(pw) },
    { id: 'reqLower', pass: /[a-z]/.test(pw) },
    { id: 'reqNum', pass: /[0-9]/.test(pw) },
    { id: 'reqSpecial', pass: /[^a-zA-Z0-9]/.test(pw) },
  ];
  checks.forEach(c => {
    const el = document.getElementById(c.id);
    el.style.color = pw ? (c.pass ? '#00c864' : '#ff4444') : '#dbb550';
  });
}

function toggleUserMenu() {
  document.getElementById('userMenuDropdown').classList.toggle('open');
}

function showProfileSettings() {
  document.getElementById('userMenuDropdown').classList.remove('open');
  sbClient.auth.getUser().then(({ data }) => {
    if (data?.user) {
      document.getElementById('settingsName').textContent = data.user.user_metadata?.display_name || '';
      document.getElementById('settingsEmail').textContent = data.user.email;
    }
  });
  document.getElementById('settingsNewPw').value = '';
  document.getElementById('settingsConfirmPw').value = '';
  document.getElementById('settingsConfirmPw').style.borderColor = '';
  document.getElementById('settingsPwMsg').textContent = '';
  document.getElementById('settingsOverlay').classList.add('open');
  document.body.style.overflow = 'hidden';
  loadSubscriptionInfo();
}

function closeSettings() {
  document.getElementById('settingsOverlay').classList.remove('open');
  document.body.style.overflow = '';
  if (window.location.hash.includes('profile/')) history.replaceState(null, '', window.location.pathname);
}

async function loadSubscriptionInfo() {
  const info = document.getElementById('subscriptionInfo');
  const btn = document.getElementById('manageSubscriptionBtn');
  if (!info) return;
  try {
    const { data: { session } } = await sbClient.auth.getSession();
    if (!session) { info.textContent = 'Not signed in'; btn.style.display = 'none'; return; }
    const res = await fetch(WORKER_URL + '/subscription-status', { headers: { Authorization: 'Bearer ' + session.access_token } });
    const sub = await res.json();
    if (sub.status === 'active' && sub.expiresAt) {
      const days = Math.ceil((new Date(sub.expiresAt) - new Date()) / (1000 * 60 * 60 * 24));
      info.textContent = sub.plan.charAt(0).toUpperCase() + sub.plan.slice(1) + ' plan — ' + days + ' days remaining';
      btn.style.display = '';
    } else {
      info.textContent = 'No active subscription';
      btn.style.display = 'none';
    }
  } catch (e) { info.textContent = 'Unable to load subscription info'; btn.style.display = 'none'; }
}

async function openPortal() {
  const { data: { session } } = await sbClient.auth.getSession();
  if (!session) return;
  const btn = document.getElementById('manageSubscriptionBtn');
  btn.textContent = 'Loading...';
  try {
    const res = await fetch(WORKER_URL + '/create-portal-session', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId: session.user.id }),
    });
    const data = await res.json();
    if (data.portalUrl) window.location.href = data.portalUrl;
    else { btn.textContent = 'Manage Subscription'; alert(data.error || 'Failed to open portal'); }
  } catch (e) { btn.textContent = 'Manage Subscription'; alert('Error: ' + e.message); }
}

function showPreferences() {
  document.getElementById('userMenuDropdown').classList.remove('open');
  document.getElementById('prefMsg').textContent = '';
  populatePrefCheckboxes();
  initPrefSliders();
  clearPrefsForm();
  loadPreferences();
  document.getElementById('preferencesOverlay').classList.add('open');
  document.body.style.overflow = 'hidden';
}

function closePreferences() {
  document.getElementById('preferencesOverlay').classList.remove('open');
  document.body.style.overflow = '';
  if (window.location.hash.includes('profile/')) history.replaceState(null, '', window.location.pathname);
}

// Favorites
async function loadFavorites() {
  const { data } = await sbClient.from('user_favorites').select('old_url');
  userFavorites = data ? data.map(r => r.old_url) : [];
}

async function toggleFavorite(oldUrl, e) {
  if (e) e.stopPropagation();
  if (!oldUrl) return;
  const idx = userFavorites.indexOf(oldUrl);
  if (idx > -1) {
    // Remove
    userFavorites.splice(idx, 1);
    await sbClient.from('user_favorites').delete().eq('old_url', oldUrl);
  } else {
    if (userFavorites.length >= getMaxFavorites()) {
      alert('Maximum ' + getMaxFavorites() + ' favourites. Remove one first.');
      return;
    }
    userFavorites.push(oldUrl);
    const { data: { user } } = await sbClient.auth.getUser();
    await sbClient.from('user_favorites').insert({ user_id: user.id, old_url: oldUrl });
  }
  renderGrid();
  // Update heart and panel glow in profile detail if open
  const detailHeart = document.getElementById('profileFavHeart');
  if (detailHeart) detailHeart.classList.toggle('active', userFavorites.includes(oldUrl));
  const panel = document.getElementById('profilePanel');
  if (panel && detailHeart) panel.classList.toggle('favorited', userFavorites.includes(oldUrl));
}

function isFavorite(g) {
  return g.oldUrl && userFavorites.includes(g.oldUrl);
}

function showFavorites() {
  document.getElementById('userMenuDropdown').classList.remove('open');
  const overlay = document.getElementById('favoritesOverlay');
  const grid = document.getElementById('favGrid');
  const empty = document.getElementById('favEmpty');
  const count = document.getElementById('favCount');

  const favGirls = allGirls.filter(g => isFavorite(g)).sort((a, b) => {
    const sa = matchScores.get(a.venue + ':' + a.name) || 0;
    const sb = matchScores.get(b.venue + ':' + b.name) || 0;
    return sb - sa;
  });
  const max = getMaxFavorites();
  count.textContent = favGirls.length + (max === Infinity ? '' : ' / ' + max) + ' favourites';

  if (favGirls.length === 0) {
    grid.style.display = 'none';
    empty.style.display = '';
  } else {
    grid.style.display = '';
    empty.style.display = 'none';
    grid.innerHTML = '';
    favGirls.forEach(g => {
      renderCard(g, grid);
    });
  }
  renderFavRoster(favGirls);
  overlay.classList.add('open');
  document.body.style.overflow = 'hidden';
}

let favRosterSelectedDay = 0;
function setFavRosterDay(i) { favRosterSelectedDay = i; const favGirls = allGirls.filter(g => isFavorite(g)); renderFavRoster(favGirls); }

function renderFavRoster(favGirls) {
  const container = document.getElementById('favRoster');
  if (!favGirls.length) { container.innerHTML = ''; return; }

  const now = new Date();
  const rosterNow = new Date(now);
  if (rosterNow.getHours() < 6) rosterNow.setDate(rosterNow.getDate() - 1);
  const todayStr = rosterNow.getFullYear() + '-' + String(rosterNow.getMonth() + 1).padStart(2, '0') + '-' + String(rosterNow.getDate()).padStart(2, '0');
  const dayNamesShort = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

  const favKeys = new Set(favGirls.map(g => (g.venue || '') + ':' + g.name));

  const dates = [];
  for (let i = 0; i < 7; i++) {
    const d = new Date(rosterNow); d.setDate(d.getDate() + i);
    dates.push({ str: d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0'), date: d });
  }

  const rosterDays = [];
  for (const { str: dateStr, date } of dates) {
    const entries = [];
    for (const [key, cal] of Object.entries(calendarData)) {
      if (!cal[dateStr] || !favKeys.has(key)) continue;
      const g = favGirls.find(g => (g.venue || '') + ':' + g.name === key);
      if (!g) continue;
      if (dateStr === todayStr) {
        const slot = cal[dateStr];
        const [sh, sm] = slot.start.split(':').map(Number);
        const [eh, em] = slot.end.split(':').map(Number);
        let nowMins = now.getHours() * 60 + now.getMinutes();
        if (now.getHours() < 6 && rosterNow.getDate() !== now.getDate()) nowMins += 24 * 60;
        const endMins = eh * 60 + em;
        const startMins = sh * 60 + sm;
        const effectiveEnd = endMins <= startMins ? 24 * 60 + endMins : endMins;
        if (nowMins >= effectiveEnd) continue;
      }
      entries.push({ girl: g, slot: cal[dateStr] });
    }
    if (entries.length) rosterDays.push({ dateStr, date, entries });
  }

  if (!rosterDays.length) {
    container.innerHTML = '<div style="padding:20px 0;color:var(--text-dim);font-family:Orbitron,sans-serif;font-size:11px;letter-spacing:2px">No roster data for favorites.</div>';
    return;
  }

  if (favRosterSelectedDay >= rosterDays.length) favRosterSelectedDay = 0;

  const TIMELINE_START = 6, TIMELINE_HOURS = 24;
  const hours = [];
  for (let i = 0; i <= TIMELINE_HOURS; i += 2) { const h = (TIMELINE_START + i) % 24; hours.push(fmt24to12(String(h).padStart(2, '0') + ':00')); }

  let html = '<div style="border-top:1px solid rgba(201,149,44,0.15);padding-top:20px;margin-top:8px">';
  const totalGirls = rosterDays[favRosterSelectedDay].entries.length;
  html += '<div class="roster-day-count">' + totalGirls + ' girl' + (totalGirls !== 1 ? 's' : '') + ' rostered</div>';
  html += '<div class="roster-day-tabs">';
  rosterDays.forEach((day, i) => {
    const isToday = day.dateStr === todayStr;
    const label = isToday ? 'Today' : dayNamesShort[day.date.getDay()];
    const dateLabel = day.date.getDate() + ' ' + monthNames[day.date.getMonth()];
    html += '<button class="roster-day-tab' + (i === favRosterSelectedDay ? ' active' : '') + '" onclick="setFavRosterDay(' + i + ')">' + label + ' <span style="opacity:0.7">' + dateLabel + '</span><span class="tab-count">' + day.entries.length + '</span></button>';
  });
  html += '</div>';

  const day = rosterDays[favRosterSelectedDay];
  const isToday = day.dateStr === todayStr;
  html += '<div class="roster-day"><div class="roster-timeline"><div class="roster-timeline-header"><div></div><div class="roster-timeline-hours">' + hours.map(h => '<span>' + h + '</span>').join('') + '</div></div>';

  for (const { girl: g, slot } of day.entries) {
    const thumb = g.photos && g.photos.length ? imgProxy(g.photos[0], 72) : '';
    const [sh, sm] = slot.start.split(':').map(Number);
    const [eh, em] = slot.end.split(':').map(Number);
    let startOffset = (sh - TIMELINE_START) * 60 + sm; if (startOffset < 0) startOffset += 24 * 60;
    let endOffset = (eh - TIMELINE_START) * 60 + em; if (endOffset < 0) endOffset += 24 * 60;
    if (endOffset <= startOffset) endOffset += 24 * 60;
    const totalMins = TIMELINE_HOURS * 60;
    const leftPct = Math.max(0, (startOffset / totalMins) * 100);
    const widthPct = Math.min(100 - leftPct, ((endOffset - startOffset) / totalMins) * 100);
    let barClass = 'future';
    if (isToday) {
      let nowOffset = (now.getHours() - TIMELINE_START) * 60 + now.getMinutes();
      if (nowOffset < 0) nowOffset += 24 * 60;
      if (now.getHours() < 6 && rosterNow.getDate() !== now.getDate()) nowOffset += 24 * 60;
      if (nowOffset >= startOffset && nowOffset < endOffset) barClass = 'now';
      else if (nowOffset < startOffset) barClass = 'later';
    }
    const timeStr = fmt24to12(slot.start) + ' - ' + fmt24to12(slot.end);
    const priceStr = (g.val1 || g.val2 || g.val3) ? [g.val1 ? '$' + g.val1 : '', g.val2 ? '$' + g.val2 : '', g.val3 ? '$' + g.val3 : ''].filter(Boolean).join(' / ') : g.venueName;
    html += '<div class="roster-entry" onclick="closeFavorites();showProfile(allGirls.find(gg=>gg.venue===\'' + g.venue + '\'&&gg.name===\'' + g.name.replace(/'/g, "\\'") + '\'))">' +
      '<div class="roster-entry-info">' +
      (thumb ? '<img class="roster-entry-thumb" src="' + thumb + '" alt="">' : '<div class="roster-entry-thumb" style="background:rgba(255,255,255,0.06)"></div>') +
      '<div><div class="roster-entry-name">' + g.name + '</div><div class="roster-entry-venue">' + priceStr + '</div></div></div>' +
      '<div class="roster-entry-bar-container"><div class="roster-entry-bar ' + barClass + '" style="left:' + leftPct + '%;width:' + widthPct + '%" title="' + timeStr + '"><span>' + timeStr + '</span></div></div></div>';
  }

  html += '</div></div>';

  // Now line
  if (isToday) {
    let nowOffset = (now.getHours() - TIMELINE_START) * 60 + now.getMinutes();
    if (nowOffset < 0) nowOffset += 24 * 60;
    if (now.getHours() < 6 && rosterNow.getDate() !== now.getDate()) nowOffset += 24 * 60;
    const nowPct = (nowOffset / (TIMELINE_HOURS * 60)) * 100;
    if (nowPct >= 0 && nowPct <= 100) {
      html += '<style>.fav-roster-now{position:absolute;top:0;bottom:0;width:2px;background:rgba(255,80,80,0.7);z-index:2;pointer-events:none;left:' + nowPct + '%}</style>';
    }
  }

  html += '</div>';
  container.innerHTML = html;

  // Insert now lines
  if (isToday) {
    let nowOffset = (now.getHours() - TIMELINE_START) * 60 + now.getMinutes();
    if (nowOffset < 0) nowOffset += 24 * 60;
    if (now.getHours() < 6 && rosterNow.getDate() !== now.getDate()) nowOffset += 24 * 60;
    const nowPct = (nowOffset / (TIMELINE_HOURS * 60)) * 100;
    if (nowPct >= 0 && nowPct <= 100) {
      container.querySelectorAll('.roster-entry-bar-container').forEach(bc => {
        const line = document.createElement('div');
        line.className = 'roster-now-line';
        line.style.left = nowPct + '%';
        const label = document.createElement('div');
        label.className = 'roster-now-label';
        label.textContent = 'Now';
        line.appendChild(label);
        bc.appendChild(line);
      });
    }
  }
}

function closeFavorites() {
  document.getElementById('favoritesOverlay').classList.remove('open');
  document.body.style.overflow = '';
  if (window.location.hash.includes('profile/')) history.replaceState(null, '', window.location.pathname);
}

async function changePassword() {
  const newPw = document.getElementById('settingsNewPw').value;
  const confirmPw = document.getElementById('settingsConfirmPw').value;
  const msg = document.getElementById('settingsPwMsg');
  const btn = document.getElementById('settingsChangePwBtn');
  msg.textContent = '';
  msg.style.color = '';

  if (!newPw) { msg.style.color = '#ff4444'; msg.textContent = 'Please enter a new password'; return; }
  if (newPw.length < 8) { msg.style.color = '#ff4444'; msg.textContent = 'Password must be at least 8 characters'; return; }
  if (!/[a-z]/.test(newPw)) { msg.style.color = '#ff4444'; msg.textContent = 'Must contain a lowercase letter'; return; }
  if (!/[A-Z]/.test(newPw)) { msg.style.color = '#ff4444'; msg.textContent = 'Must contain an uppercase letter'; return; }
  if (!/[0-9]/.test(newPw)) { msg.style.color = '#ff4444'; msg.textContent = 'Must contain a number'; return; }
  if (!/[^a-zA-Z0-9]/.test(newPw)) { msg.style.color = '#ff4444'; msg.textContent = 'Must contain a special character'; return; }
  if (newPw !== confirmPw) { msg.style.color = '#ff4444'; msg.textContent = 'Passwords do not match'; return; }

  btn.disabled = true;
  btn.textContent = 'Updating...';
  const { error } = await sbClient.auth.updateUser({ password: newPw });
  btn.disabled = false;
  btn.textContent = 'Update Password';

  if (error) { msg.style.color = '#ff4444'; msg.textContent = error.message; }
  else {
    msg.style.color = '#00c864'; msg.textContent = 'Password updated successfully';
    document.getElementById('settingsNewPw').value = '';
    document.getElementById('settingsConfirmPw').value = '';
    document.getElementById('settingsConfirmPw').style.borderColor = '';
  }
}

function checkPasswordMatch() {
  const confirm = document.getElementById('authPasswordConfirm');
  if (authMode !== 'signup' || !confirm.value) { confirm.style.borderColor = ''; return; }
  const match = document.getElementById('authPassword').value === confirm.value;
  confirm.style.borderColor = match ? '#00c864' : '#ff4444';
}

// Listen for auth state changes (e.g. email confirmation redirect)
// Hash routing
function navigateTo(route) {
  window.location.hash = route;
}

function handleRoute() {
  const hash = window.location.hash.replace('#', '');
  // Close all overlays first
  ['settingsOverlay', 'preferencesOverlay', 'favoritesOverlay'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.classList.remove('open');
  });
  document.body.style.overflow = '';

  if (hash === 'profile/settings') { showProfileSettings(); return true; }
  if (hash === 'profile/preferences') { showPreferences(); return true; }
  if (hash === 'profile/favourites') { showFavorites(); return true; }
  if (hash === 'subscribe') { return true; }
  return false;
}

window.addEventListener('hashchange', () => {
  const hash = window.location.hash.replace('#', '');
  if (hash === 'subscribe') {
    if (userRole !== 'admin') showPaywall();
  } else {
    hidePaywall();
    handleRoute();
  }
});

sbClient.auth.onAuthStateChange((event, session) => {
  if (event === 'PASSWORD_RECOVERY') {
    document.getElementById('authOverlay').style.display = 'none'; document.body.style.overflow = '';
    document.getElementById('userMenu').style.display = '';
    fetchUserRole();
    setTimeout(() => {
      navigateTo('profile/settings');
    }, 500);
    return;
  }
  if (session) {
    document.getElementById('authOverlay').style.display = 'none'; document.body.style.overflow = '';
    document.getElementById('userMenu').style.display = '';
    fetchUserRole().then(() => {
      loadPreferences().then(() => {
        if (userPreferences) { computeMatchScores(); renderGrid(); }
      });
      // Paywall check 2s after login
      setTimeout(async () => {
        if (userRole === 'admin') return;
        const sub = await checkSubscription();
        if (!sub || sub.status !== 'active') showPaywall();
      }, 2000);
    });
  }
});

// Enter key to submit
document.getElementById('userMenuBtn').addEventListener('click', toggleUserMenu);
document.getElementById('settingsBack').addEventListener('click', closeSettings);
document.getElementById('settingsOverlay').addEventListener('click', e => { if (e.target === e.currentTarget) closeSettings(); });
document.getElementById('prefBack').addEventListener('click', closePreferences);
document.getElementById('preferencesOverlay').addEventListener('click', e => { if (e.target === e.currentTarget) closePreferences(); });
document.getElementById('favBack').addEventListener('click', closeFavorites);
document.getElementById('favoritesOverlay').addEventListener('click', e => { if (e.target === e.currentTarget) closeFavorites(); });
document.getElementById('settingsChangePwBtn').addEventListener('click', changePassword);
document.getElementById('settingsNewPw').addEventListener('input', () => {
  const pw = document.getElementById('settingsNewPw').value;
  const checks = [
    { id: 'sReqLen', pass: pw.length >= 8 },
    { id: 'sReqUpper', pass: /[A-Z]/.test(pw) },
    { id: 'sReqLower', pass: /[a-z]/.test(pw) },
    { id: 'sReqNum', pass: /[0-9]/.test(pw) },
    { id: 'sReqSpecial', pass: /[^a-zA-Z0-9]/.test(pw) },
  ];
  checks.forEach(c => { document.getElementById(c.id).style.color = pw ? (c.pass ? '#00c864' : '#ff4444') : '#dbb550'; });
  // Also update confirm match
  const confirm = document.getElementById('settingsConfirmPw');
  if (confirm.value) confirm.style.borderColor = pw === confirm.value ? '#00c864' : '#ff4444';
});
document.getElementById('settingsConfirmPw').addEventListener('input', () => {
  const el = document.getElementById('settingsConfirmPw');
  if (!el.value) { el.style.borderColor = ''; return; }
  el.style.borderColor = el.value === document.getElementById('settingsNewPw').value ? '#00c864' : '#ff4444';
});
document.getElementById('settingsConfirmPw').addEventListener('keydown', e => { if (e.key === 'Enter') changePassword(); });
document.addEventListener('click', e => { if (!e.target.closest('.user-menu')) document.getElementById('userMenuDropdown').classList.remove('open'); });
document.getElementById('authBtn').addEventListener('click', handleAuth);
document.getElementById('authToggle').addEventListener('click', toggleAuthMode);
document.getElementById('authPassword').addEventListener('input', () => { checkPasswordReqs(); checkPasswordMatch(); });
document.getElementById('authPasswordConfirm').addEventListener('input', checkPasswordMatch);
document.getElementById('authName').addEventListener('keydown', e => { if (e.key === 'Enter') document.getElementById('authEmail').focus(); });
document.getElementById('authEmail').addEventListener('keydown', e => { if (e.key === 'Enter') { if (authMode === 'signup') document.getElementById('authPassword').focus(); else document.getElementById('authPassword').focus(); } });
document.getElementById('authPassword').addEventListener('keydown', e => { if (e.key === 'Enter') { if (authMode === 'signup') document.getElementById('authPasswordConfirm').focus(); else handleAuth(); } });
document.getElementById('authPasswordConfirm').addEventListener('keydown', e => { if (e.key === 'Enter') handleAuth(); });

// ── Preferences & Scoring Engine ──

const CUP_MAP = { A:1, B:2, C:3, D:4, DD:5, E:6, F:7, G:8 };

function parseCup(cupStr) {
  if (!cupStr) return null;
  const s = cupStr.toUpperCase().replace(/[^A-Z]/g, '');
  // handle DD specifically
  if (s.includes('DD')) return 5;
  // try each cup letter
  const vals = [];
  for (const [k,v] of Object.entries(CUP_MAP)) {
    if (s.includes(k) && k !== 'DD') vals.push(v);
  }
  if (s.includes('DD')) vals.push(5);
  if (!vals.length) return null;
  return vals;
}

function cupInRange(cupStr, minCup, maxCup) {
  if (!minCup && !maxCup) return null; // not set
  const minVal = CUP_MAP[minCup] || 0;
  const maxVal = CUP_MAP[maxCup] || 99;
  const parsed = parseCup(cupStr);
  if (!parsed) return null; // no data
  const arr = Array.isArray(parsed) ? parsed : [parsed];
  return arr.some(v => v >= minVal && v <= maxVal);
}

const PREF_WEIGHTS = {
  age: 10, body: 10, height: 2, cup: 2,
  countries: 15, services: 12, experience: 10, language: 10, av: 5,
  price30: 2, price45: 2, price60: 5,
  photos: 5, lastRosterDays: 5, dateStartedDays: 5
};

function scoreGirl(girl, prefs) {
  if (!prefs) return 0;
  let score = 0;
  let activeWeight = 0;

  // Age (10%) - in range = 10%, else 0%
  if (prefs.age_min != null && prefs.age_max != null && (prefs.age_min !== 18 || prefs.age_max !== 33)) {
    activeWeight += 10;
    if (girl.age && parseInt(girl.age) >= prefs.age_min && parseInt(girl.age) <= prefs.age_max) score += 10;
  }

  // Body (10%) - in range = 10%, else 0%
  if (prefs.body_min != null && prefs.body_max != null && (prefs.body_min !== 4 || prefs.body_max !== 10)) {
    activeWeight += 10;
    if (girl.body && parseInt(girl.body) >= prefs.body_min && parseInt(girl.body) <= prefs.body_max) score += 10;
  }

  // Height (2%) - in range = 2%, else 0%
  if (prefs.height_min != null && prefs.height_max != null && (prefs.height_min !== 150 || prefs.height_max !== 175)) {
    activeWeight += 2;
    if (girl.height && parseInt(girl.height) >= prefs.height_min && parseInt(girl.height) <= prefs.height_max) score += 2;
  }

  // Cup (2%) - in range = 2%, else 0%
  if (prefs.cup_min || prefs.cup_max) {
    activeWeight += 2;
    const result = cupInRange(girl.cup, prefs.cup_min, prefs.cup_max);
    if (result) score += 2;
  }

  // Country (15%) - proportional by how many of girl's countries match
  if (prefs.countries && prefs.countries.length > 0) {
    activeWeight += 15;
    const gc = Array.isArray(girl.country) ? girl.country : (girl.country ? [girl.country] : []);
    if (gc.length > 0) {
      const matched = gc.filter(c => prefs.countries.includes(c)).length;
      score += (matched / gc.length) * 15;
    }
  }

  // Services (12%) - proportional by how many selected labels the girl has
  if (prefs.services && prefs.services.length > 0) {
    activeWeight += 12;
    const gl = Array.isArray(girl.labels) ? girl.labels : [];
    const matched = prefs.services.filter(s => gl.includes(s)).length;
    score += (matched / prefs.services.length) * 12;
  }

  // Experience (10%) - at least one match = 10%, else 0%
  if (prefs.experience && prefs.experience.length > 0) {
    activeWeight += 10;
    const gExp = girl.experienceLevel || '';
    if (prefs.experience.includes(gExp)) score += 10;
  }

  // Language (10%) - at least one match = 10%, else 0%
  if (prefs.language && prefs.language.length > 0) {
    activeWeight += 10;
    const gLang = girl.englishLevel || '';
    if (prefs.language.includes(gLang)) score += 10;
  }

  // AV/Pornstar (5%) - at least one match = 5%, else 0%
  if (prefs.av && prefs.av.length > 0) {
    activeWeight += 5;
    const gAV = girl.pornstar ? 'Pornstar' : '';
    if (prefs.av.includes(gAV)) score += 5;
  }

  // 30 Min Price (2%) - in range = 2%, else 0%
  if (prefs.price30_min != null && prefs.price30_max != null && (prefs.price30_min !== 150 || prefs.price30_max !== 250)) {
    activeWeight += 2;
    if (girl.val1 && parseInt(girl.val1) >= prefs.price30_min && parseInt(girl.val1) <= prefs.price30_max) score += 2;
  }

  // 45 Min Price (2%) - in range = 2%, else 0%
  if (prefs.price45_min != null && prefs.price45_max != null && (prefs.price45_min !== 200 || prefs.price45_max !== 310)) {
    activeWeight += 2;
    if (girl.val2 && parseInt(girl.val2) >= prefs.price45_min && parseInt(girl.val2) <= prefs.price45_max) score += 2;
  }

  // 60 Min Price (5%) - in range = 5%, else 0%
  if (prefs.price60_min != null && prefs.price60_max != null && (prefs.price60_min !== 250 || prefs.price60_max !== 380)) {
    activeWeight += 5;
    if (girl.val3 && parseInt(girl.val3) >= prefs.price60_min && parseInt(girl.val3) <= prefs.price60_max) score += 5;
  }

  // Photos (5%) - in range = 5%, else 0%
  if (prefs.photos_min != null && prefs.photos_max != null && (prefs.photos_min !== 0 || prefs.photos_max !== 13)) {
    activeWeight += 5;
    const pc = (girl.photos && girl.photos.length) || 0;
    if (pc >= prefs.photos_min && pc <= prefs.photos_max) score += 5;
  }

  // Last Roster Days (5%) - within days = 5%, else 0%
  if (prefs.last_roster_days != null && prefs.last_roster_days > 0) {
    activeWeight += 5;
    if (girl.lastRostered) {
      const today = new Date(); today.setHours(0,0,0,0);
      const rd = new Date(girl.lastRostered + 'T00:00:00');
      const diff = Math.round((today - rd) / 86400000);
      if (diff <= prefs.last_roster_days) score += 5;
    }
  }

  // Date Started Days (5%) - within days = 5%, else 0%
  if (prefs.date_started_days != null && prefs.date_started_days > 0) {
    activeWeight += 5;
    if (girl.startDate) {
      const today = new Date(); today.setHours(0,0,0,0);
      const sd = new Date(girl.startDate + 'T00:00:00');
      const diff = Math.round((today - sd) / 86400000);
      if (diff <= prefs.date_started_days) score += 5;
    }
  }

  // No active preferences = 0
  if (activeWeight === 0) return 0;
  // Normalize: score is out of activeWeight, scale to 100
  return Math.round((score / activeWeight) * 100);
}

function computeMatchScores() {
  matchScores.clear();
  matchThreshold = 0;
  if (!userPreferences || !allGirls.length) return;

  const scores = [];
  allGirls.forEach(g => {
    const key = g.venue + ':' + g.name;
    const s = scoreGirl(g, userPreferences);
    matchScores.set(key, s);
    if (s > 0) scores.push(s);
  });

  if (!scores.length) return;
  scores.sort((a, b) => b - a);
  const top20idx = Math.max(0, Math.floor(scores.length * 0.2) - 1);
  matchThreshold = scores[top20idx] || 0;
  // Ensure threshold is at least 1 so we don't show 0% badges
  if (matchThreshold < 1) matchThreshold = 1;
}

function initPrefSliders() {
  document.querySelectorAll('.pref-range-slider').forEach(container => {
    const inputs = container.querySelectorAll('input[type=range]');
    const fill = container.querySelector('.range-slider-fill');
    const minSpan = container.querySelector('.pref-range-min');
    const maxSpan = container.querySelector('.pref-range-max');
    const minInput = container.querySelector('[data-handle=min]');
    const maxInput = container.querySelector('[data-handle=max]');

    const isCup = container.hasAttribute('data-cup');
    function update() {
      const lo = parseInt(minInput.value);
      const hi = parseInt(maxInput.value);
      const rangeMin = parseInt(minInput.min);
      const rangeMax = parseInt(minInput.max);
      const pctL = ((lo - rangeMin) / (rangeMax - rangeMin)) * 100;
      const pctR = 100 - ((hi - rangeMin) / (rangeMax - rangeMin)) * 100;
      fill.style.left = pctL + '%';
      fill.style.right = pctR + '%';
      minSpan.textContent = isCup ? (CUP_ORDER[lo] || lo) : lo;
      maxSpan.textContent = isCup ? (CUP_ORDER[hi] || hi) : hi;
    }

    minInput.addEventListener('input', () => {
      if (parseInt(minInput.value) > parseInt(maxInput.value)) minInput.value = maxInput.value;
      update();
    });
    maxInput.addEventListener('input', () => {
      if (parseInt(maxInput.value) < parseInt(minInput.value)) maxInput.value = minInput.value;
      update();
    });
    update();
  });
}

function populatePrefCheckboxes() {
  // Countries
  const countriesSet = new Set();
  allGirls.forEach(g => {
    (Array.isArray(g.country) ? g.country : (g.country ? [g.country] : [])).forEach(c => { if (c) countriesSet.add(c); });
  });
  const countriesEl = document.getElementById('prefCountries');
  countriesEl.innerHTML = [...countriesSet].sort().map(c =>
    `<label class="pref-cb"><input type="checkbox" value="${c.replace(/"/g, '&quot;')}"><span>${c}</span></label>`
  ).join('');

  // Services / Labels
  const labelsSet = new Set();
  allGirls.forEach(g => {
    (Array.isArray(g.labels) ? g.labels : []).forEach(l => { if (l) labelsSet.add(l); });
  });
  const servicesEl = document.getElementById('prefServices');
  servicesEl.innerHTML = [...labelsSet].sort().map(l =>
    `<label class="pref-cb"><input type="checkbox" value="${l.replace(/"/g, '&quot;')}"><span>${l}</span></label>`
  ).join('');
}

function readPrefsFromForm() {
  const getSlider = (id) => {
    const c = document.getElementById(id);
    return {
      min: parseInt(c.querySelector('[data-handle=min]').value),
      max: parseInt(c.querySelector('[data-handle=max]').value)
    };
  };
  const getChecked = (id) => {
    return [...document.querySelectorAll('#' + id + ' input[type=checkbox]:checked')].map(cb => cb.value);
  };

  const age = getSlider('prefAge');
  const body = getSlider('prefBody');
  const height = getSlider('prefHeight');
  const p30 = getSlider('prefPrice30');
  const p45 = getSlider('prefPrice45');
  const p60 = getSlider('prefPrice60');
  const photos = getSlider('prefPhotos');

  return {
    age_min: age.min, age_max: age.max,
    body_min: body.min, body_max: body.max,
    height_min: height.min, height_max: height.max,
    cup_min: CUP_ORDER[parseInt(document.getElementById('prefCup').querySelector('[data-handle=min]').value)] || null,
    cup_max: CUP_ORDER[parseInt(document.getElementById('prefCup').querySelector('[data-handle=max]').value)] || null,
    countries: getChecked('prefCountries'),
    services: getChecked('prefServices'),
    experience: getChecked('prefExperience'),
    language: getChecked('prefLanguage'),
    av: getChecked('prefAV'),
    price30_min: p30.min, price30_max: p30.max,
    price45_min: p45.min, price45_max: p45.max,
    price60_min: p60.min, price60_max: p60.max,
    photos_min: photos.min, photos_max: photos.max,
    last_roster_days: parseInt(document.getElementById('prefLastRosterDays').value) || null,
    date_started_days: parseInt(document.getElementById('prefDateStartedDays').value) || null
  };
}

function writePrefsToForm(p) {
  if (!p) return;
  const setSlider = (id, min, max) => {
    const c = document.getElementById(id);
    const minI = c.querySelector('[data-handle=min]');
    const maxI = c.querySelector('[data-handle=max]');
    if (min != null) minI.value = min;
    if (max != null) maxI.value = max;
    // trigger update
    minI.dispatchEvent(new Event('input'));
  };
  const setChecked = (id, vals) => {
    if (!vals || !vals.length) return;
    document.querySelectorAll('#' + id + ' input[type=checkbox]').forEach(cb => {
      cb.checked = vals.includes(cb.value);
    });
  };

  setSlider('prefAge', p.age_min, p.age_max);
  setSlider('prefBody', p.body_min, p.body_max);
  setSlider('prefHeight', p.height_min, p.height_max);
  setSlider('prefPrice30', p.price30_min, p.price30_max);
  setSlider('prefPrice45', p.price45_min, p.price45_max);
  setSlider('prefPrice60', p.price60_min, p.price60_max);
  setSlider('prefPhotos', p.photos_min, p.photos_max);

  if (p.cup_min || p.cup_max) {
    const cupC = document.getElementById('prefCup');
    const cupMinI = cupC.querySelector('[data-handle=min]');
    const cupMaxI = cupC.querySelector('[data-handle=max]');
    if (p.cup_min) cupMinI.value = cupToNum(p.cup_min);
    if (p.cup_max) cupMaxI.value = cupToNum(p.cup_max);
    cupMinI.dispatchEvent(new Event('input'));
  }

  setChecked('prefCountries', p.countries);
  setChecked('prefServices', p.services);
  setChecked('prefExperience', p.experience);
  setChecked('prefLanguage', p.language);
  setChecked('prefAV', p.av);

  if (p.last_roster_days) document.getElementById('prefLastRosterDays').value = p.last_roster_days;
  if (p.date_started_days) document.getElementById('prefDateStartedDays').value = p.date_started_days;
}

function clearPrefsForm() {
  document.querySelectorAll('.pref-range-slider').forEach(c => {
    const minI = c.querySelector('[data-handle=min]');
    const maxI = c.querySelector('[data-handle=max]');
    maxI.value = maxI.max;
    minI.value = minI.min;
    minI.dispatchEvent(new Event('input'));
  });
  document.querySelectorAll('#settingsOverlay .pref-checkboxes input[type=checkbox]').forEach(cb => cb.checked = false);
  document.getElementById('prefLastRosterDays').value = '';
  document.getElementById('prefDateStartedDays').value = '';
}

async function loadPreferences() {
  try {
    const { data: { user } } = await sbClient.auth.getUser();
    if (!user) return;
    const { data, error } = await sbClient.from('user_preferences').select('*').eq('id', user.id).single();
    if (data && !error) {
      userPreferences = data;
      writePrefsToForm(data);
    }
  } catch (e) { /* no prefs yet */ }
}

async function savePreferences() {
  const msg = document.getElementById('prefMsg');
  const btn = document.getElementById('prefSaveBtn');
  msg.textContent = '';
  msg.style.color = '';

  const { data: { user } } = await sbClient.auth.getUser();
  if (!user) { msg.style.color = '#ff4444'; msg.textContent = 'Not logged in'; return; }

  const prefs = readPrefsFromForm();
  prefs.id = user.id;
  prefs.updated_at = new Date().toISOString();

  btn.disabled = true;
  btn.textContent = 'Saving...';

  const { error } = await sbClient.from('user_preferences').upsert(prefs, { onConflict: 'id' });

  btn.disabled = false;
  btn.textContent = 'Save Preferences';

  if (error) {
    msg.style.color = '#ff4444';
    msg.textContent = error.message;
  } else {
    msg.style.color = '#00c864';
    msg.textContent = 'Preferences saved';
    userPreferences = prefs;
    computeMatchScores();
    renderGrid();
  }
}

function clearPreferences() {
  clearPrefsForm();
  const msg = document.getElementById('prefMsg');
  msg.style.color = '#dbb550';
  msg.textContent = 'Preferences cleared — press Save to apply';
}

document.getElementById('prefSaveBtn').addEventListener('click', savePreferences);
document.getElementById('prefClearBtn').addEventListener('click', e => { e.preventDefault(); clearPreferences(); });

// ── End Preferences ──

const PROFILES_BASE = 'https://raw.githubusercontent.com/travanixlabs/brothel-search/main/profiles';
const VENUES = [
  { id: 'ginzaempire', name: 'Ginza Empire', file: 'ginzaempire.json' },
  { id: 'ginzaclub', name: 'Ginza Club', file: 'ginzaclub.json' },
  { id: 'kyoto206', name: 'Kyoto 206', file: 'kyoto206.json' },
  { id: 'sakura57', name: 'Sakura 57', file: 'sakura57.json' },
  { id: 'top127', name: 'Top 127', file: 'top127.json' },
  { id: 'fantasyclub35', name: 'Fantasy Club 35', file: 'fantasyclub35.json' },
  { id: '429city', name: '429 City', file: '429city.json' }
];

let allGirls = [];
let userPreferences = null;
let matchScores = new Map(); // girl key -> score 0-100
let matchThreshold = 0; // top 20% cutoff

let activeVenue = { include: [], exclude: [] };
let activeCountry = { include: [], exclude: [] };
let activeLabels = { include: [], exclude: [] };
let rangeFilters = {};
let rangeDefaults = {};
let activeSort = 'preference';
let sortDir = 'asc';
let searchTimer;
let calendarData = {};
let activeAV = { include: [], exclude: [] };
let activeAvailability = { include: [], exclude: [] };
let activePhotos = { include: [], exclude: [] };
let activeFavFilter = { include: [], exclude: [] };
let activeDateTime = '';
let dtEnabled = false;
let dtPendingMonth = '';
let dtPendingDay = '';
const textFilters = { name: '', exp: '', special: '', lang: '', type: '', desc: '' };

const RANGE_DEFS = [
  { key: 'age', label: 'Age', field: 'age', type: 'num' },
  { key: 'body', label: 'Body', field: 'body', type: 'num' },
  { key: 'height', label: 'Height', field: 'height', type: 'num', suffix: 'cm' },
  { key: 'cup', label: 'Cup', field: 'cup', type: 'cup' },
  { key: 'val1', label: '30 Min', field: 'val1', type: 'num', prefix: '$' },
  { key: 'val2', label: '45 Min', field: 'val2', type: 'num', prefix: '$' },
  { key: 'val3', label: '60 Min', field: 'val3', type: 'num', prefix: '$' },
  { key: 'startDate', label: 'Start Date', field: 'startDate', type: 'date' },
  { key: 'lastRostered', label: 'Last Available', field: 'lastRostered', type: 'date' }
];
const CUP_ORDER = ['A','B','C','D','DD','E','F','G','H'];
function cupToNum(c) { const i = CUP_ORDER.indexOf((c||'').toUpperCase()); return i >= 0 ? i : -1; }
function numToCup(n) { return CUP_ORDER[n] || ''; }
function dateToNum(d) { return d ? new Date(d + 'T00:00:00').getTime() : NaN; }
function numToDate(n) { const d = new Date(n); return d.toISOString().slice(0,10); }
function numToDateShort(n) { const d = new Date(n); return d.toISOString().slice(2,10); }
function isNewProfile(g) { if (!g.startDate) return false; const diff = (Date.now() - new Date(g.startDate + 'T00:00:00').getTime()) / 86400000; return diff <= 30; }
function imgProxy(url, w = 300) { if (!url) return ''; return `https://wsrv.nl/?url=${encodeURIComponent(url)}&w=${w}&output=webp&q=80`; }

function fmt24to12(t) {
  const [h, m] = t.split(':').map(Number);
  const suffix = h >= 12 ? 'pm' : 'am';
  const h12 = h === 0 ? 12 : h > 12 ? h - 12 : h;
  return m === 0 ? h12 + suffix : h12 + ':' + String(m).padStart(2, '0') + suffix;
}

function getAvailabilityText(g) {
  const cal = calendarData[(g.venue || '') + ':' + g.name];
  if (!cal) return null;
  const now = new Date();
  const nowMins = now.getHours() * 60 + now.getMinutes();
  const today = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0') + '-' + String(now.getDate()).padStart(2, '0');

  // Before 6am, check if yesterday's shift is still active (wraps past midnight)
  if (now.getHours() < 6) {
    const yesterday = new Date(now);
    yesterday.setDate(yesterday.getDate() - 1);
    const yesterdayStr = yesterday.getFullYear() + '-' + String(yesterday.getMonth() + 1).padStart(2, '0') + '-' + String(yesterday.getDate()).padStart(2, '0');
    const ySlot = cal[yesterdayStr];
    if (ySlot) {
      const [ysh, ysm] = ySlot.start.split(':').map(Number);
      const [yeh, yem] = ySlot.end.split(':').map(Number);
      const yStartMins = ysh * 60 + ysm;
      const yEndMins = yeh * 60 + yem;
      // If end < start, shift wraps past midnight
      if (yEndMins <= yStartMins && nowMins < yEndMins) {
        const timeStr = fmt24to12(ySlot.start) + ' - ' + fmt24to12(ySlot.end);
        return 'Available Now (' + timeStr + ')';
      }
    }
  }

  const slot = cal[today];
  if (slot) {
    const [sh, sm] = slot.start.split(':').map(Number);
    const [eh, em] = slot.end.split(':').map(Number);
    const startMins = sh * 60 + sm;
    const endMins = eh * 60 + em;
    const effectiveEnd = endMins <= startMins ? 24 * 60 + endMins : endMins;
    const timeStr = fmt24to12(slot.start) + ' - ' + fmt24to12(slot.end);
    if (nowMins >= startMins && nowMins < effectiveEnd) return 'Available Now (' + timeStr + ')';
    if (nowMins < startMins) return 'Available Later Today (' + timeStr + ')';
  }
  // Check future dates
  const futureDates = Object.keys(cal).filter(d => d > today && !d.startsWith('_'));
  if (futureDates.length) {
    const next = futureDates.sort()[0];
    const fSlot = cal[next];
    const fTimeStr = fmt24to12(fSlot.start) + ' - ' + fmt24to12(fSlot.end);
    const dObj = new Date(next + 'T00:00:00');
    const dayNames = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
    return 'Available Future: ' + dayNames[dObj.getDay()] + ' ' + next.slice(5) + ' (' + fTimeStr + ')';
  }
  return 'ended';
}

function isAvailableAt(g, dateStr, timeMins) {
  const cal = calendarData[(g.venue || '') + ':' + g.name];
  if (!cal) return false;
  const slot = cal[dateStr];
  if (!slot) return false;
  const [sh, sm] = slot.start.split(':').map(Number);
  const [eh, em] = slot.end.split(':').map(Number);
  const startMins = sh * 60 + sm;
  const endMins = eh * 60 + em;
  const effectiveEnd = endMins <= startMins ? 24 * 60 + endMins : endMins;
  return timeMins >= startMins && timeMins < effectiveEnd;
}

function getAvailabilityStatus(g) {
  const cal = calendarData[(g.venue || '') + ':' + g.name];
  if (!cal) return 'Unavailable';
  const now = new Date();
  const todayStr = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0') + '-' + String(now.getDate()).padStart(2, '0');
  const slot = cal[todayStr];
  if (slot) {
    const [sh, sm] = slot.start.split(':').map(Number);
    const [eh, em] = slot.end.split(':').map(Number);
    const nowMins = now.getHours() * 60 + now.getMinutes();
    const startMins = sh * 60 + sm;
    const endMins = eh * 60 + em;
    const effectiveEnd = endMins <= startMins ? 24 * 60 + endMins : endMins;
    if (nowMins >= startMins && nowMins < effectiveEnd) return 'Available Now';
    if (nowMins < startMins) return 'Available Later Today';
  }
  // Check future dates
  const dates = Object.keys(cal).filter(d => d > todayStr);
  if (dates.length) return 'Available Future Date';
  return 'Unavailable';
}

const LABEL_RULES = [
  { keywords: ['lesbian'], label: 'Double Lesbian' },
  { keywords: ['shower together', 'shower'], label: 'Shower Together' },
  { keywords: ['pussy slide', 'pussy sliding'], label: 'Pussy Slide' },
  { keywords: ['dfk'], label: 'DFK' },
  { keywords: ['bbbj'], label: 'BBBJ' },
  { keywords: ['69'], label: '69' },
  { keywords: ['cim'], label: 'CIM' },
  { keywords: ['rimming', 'rimmimg', 'rimimg'], label: 'Rimming' },
  { keywords: ['daty', 'dining'], label: 'DATY' },
  { keywords: ['gfe'], label: 'GFE' },
  { keywords: ['double'], label: 'Double' },
  { keywords: ['boob slide', 'boobs slide'], label: 'Boob Slide' },
  { keywords: ['couple'], label: 'Couple' },
  { keywords: ['2 men', '2men', '2 man'], label: '2 Men' },
  { keywords: ['outcall'], label: 'Outcall' },
  { keywords: ['swallow'], label: 'Swallow' },
  { keywords: ['an-al', 'anal'], label: 'Anal' },
  { keywords: ['cof'], label: 'COF' },
  { keywords: ['filming', 'filmming', 'video'], label: 'Filming' },
  { keywords: ['pse'], label: 'PSE' },
];

function autoExtractLabels(girls) {
  girls.forEach(g => {
    const desc = (g.desc || '').toLowerCase();
    if (!desc) return;
    if (!g.labels) g.labels = [];
    const existing = new Set(g.labels.map(l => l.toLowerCase()));
    LABEL_RULES.forEach(rule => {
      if (!existing.has(rule.label.toLowerCase()) && rule.keywords.some(kw => desc.includes(kw))) {
        g.labels.push(rule.label);
        existing.add(rule.label.toLowerCase());
      }
    });
  });
}

function showSkeletonGrid() {
  const grid = document.getElementById('girlsGrid');
  grid.innerHTML = '';
  for (let i = 0; i < PAGE_SIZE; i++) {
    const el = document.createElement('div');
    el.className = 'skeleton-card';
    el.style.animationDelay = (i * 0.05) + 's';
    el.innerHTML = '<div class="skeleton-img"></div><div class="skeleton-info"><div class="skeleton-line"></div><div class="skeleton-line"></div><div class="skeleton-line"></div></div>';
    grid.appendChild(el);
  }
}

async function loadProfiles() {
  showSkeletonGrid();
  const results = await Promise.allSettled(
    VENUES.map(async v => {
      const r = await fetch(`${PROFILES_BASE}/${v.file}`);
      if (!r.ok) return { girls: [], calendar: {} };
      const data = await r.json();
      return {
        girls: (data.girls || []).map(g => ({ ...g, venue: v.id, venueName: v.name })),
        calendar: data.calendar || {}
      };
    })
  );
  allGirls = [];
  calendarData = {};
  results.forEach((r, i) => {
    if (r.status !== 'fulfilled') return;
    const venueId = VENUES[i].id;
    allGirls.push(...r.value.girls);
    const cal = r.value.calendar;
    for (const key of Object.keys(cal)) {
      if (!key.startsWith('_')) calendarData[venueId + ':' + key] = cal[key];
    }
  });
  autoExtractLabels(allGirls);
  computeMatchScores();
  renderFilters();
  renderRangeFilters();
  renderGrid();
}

function closeAllDropdowns(except) {
  document.querySelectorAll('.filter-dropdown-btn.open').forEach(b => { if (b !== except) b.classList.remove('open') });
  document.querySelectorAll('.filter-dropdown-panel.open').forEach(p => { if (p.previousElementSibling !== except) p.classList.remove('open') });
}
document.addEventListener('click', e => { if (!e.target.closest('.filter-dropdown') && !e.target.closest('.sort-dir-btn')) closeAllDropdowns() });

function hasAnyFilter() {
  const hasRangeActive = Object.keys(rangeFilters).some(k => { const d = rangeDefaults[k]; return d && (rangeFilters[k].min > d.min || rangeFilters[k].max < d.max); });
  const hasTextFilter = Object.values(textFilters).some(v => v);
  return activeVenue.include.length || activeVenue.exclude.length || activeCountry.include.length || activeCountry.exclude.length || activeLabels.include.length || activeLabels.exclude.length || activeAV.include.length || activeAV.exclude.length || activeAvailability.include.length || activeAvailability.exclude.length || activePhotos.include.length || activePhotos.exclude.length || activeFavFilter.include.length || activeFavFilter.exclude.length || activeDateTime || hasRangeActive || hasTextFilter;
}

function updateMoreFiltersCount() {
  const count = Object.values(textFilters).filter(v => v).length +
    Object.keys(rangeFilters).filter(k => { const d = rangeDefaults[k]; return d && (rangeFilters[k].min > d.min || rangeFilters[k].max < d.max); }).length +
    (activeDateTime ? 1 : 0);
  const badge = document.getElementById('moreFiltersBadge');
  if (badge) { badge.textContent = count || ''; badge.style.display = count ? 'inline-flex' : 'none'; }
}

function updateClearBtn() {
  const fr = document.getElementById('filterRow');
  const existing = document.getElementById('clearAllBtn');
  if (hasAnyFilter() && !existing) {
    const btn = document.createElement('button');
    btn.className = 'clear-all-btn';
    btn.id = 'clearAllBtn';
    btn.textContent = 'Clear All';
    btn.onclick = () => { activeVenue.include.length = 0; activeVenue.exclude.length = 0; activeCountry.include.length = 0; activeCountry.exclude.length = 0; activeLabels.include.length = 0; activeLabels.exclude.length = 0; activeAV.include.length = 0; activeAV.exclude.length = 0; activeAvailability.include.length = 0; activeAvailability.exclude.length = 0; activePhotos.include.length = 0; activePhotos.exclude.length = 0; activeFavFilter.include.length = 0; activeFavFilter.exclude.length = 0; activeDateTime = ''; rangeFilters = {}; Object.keys(textFilters).forEach(k => textFilters[k] = ''); renderFilters(); renderRangeFilters(); renderGrid(); };
    fr.appendChild(btn);
  } else if (!hasAnyFilter() && existing) {
    existing.remove();
  }
}

function buildDropdown(id, label, options, selected) {
  const hasSel = selected.length > 0 && selected[0] !== '';
  let selLabel = label;
  let countHtml = hasSel ? '<div class="filter-btn-count">+' + selected.length + '</div>' : '';
  let html = `<div class="filter-dropdown" id="${id}">`;
  html += `<button class="filter-dropdown-btn${hasSel ? ' has-selection' : ''}"><span class="filter-btn-label">${label}${countHtml}</span><svg viewBox="0 0 24 24"><path d="M7.41 8.59L12 13.17l4.59-4.58L18 10l-6 6-6-6z"/></svg></button>`;
  html += `<div class="filter-dropdown-panel">`;
  options.forEach(o => {
    const active = selected.includes(o.value);
    html += `<button class="filter-option${active ? ' active' : ''}" data-value="${o.value}"><span class="filter-checkbox"></span>${o.label}${o.count != null ? '<span class="filter-option-count">' + o.count + '</span>' : ''}</button>`;
  });
  html += `</div></div>`;
  return html;
}

function buildLabelDropdown(id, label, options, includeList, excludeList) {
  const hasSel = includeList.length > 0 || excludeList.length > 0;
  let selLabel = label;
  let countHtml = '';
  if (hasSel) {
    const parts = [];
    if (includeList.length) parts.push('+' + includeList.length);
    if (excludeList.length) parts.push('-' + excludeList.length);
    countHtml = '<div class="filter-btn-count">' + parts.join(' ') + '</div>';
  }
  let html = `<div class="filter-dropdown" id="${id}">`;
  html += `<button class="filter-dropdown-btn${hasSel ? ' has-selection' : ''}"><span class="filter-btn-label">${label}${countHtml}</span><svg viewBox="0 0 24 24"><path d="M7.41 8.59L12 13.17l4.59-4.58L18 10l-6 6-6-6z"/></svg></button>`;
  html += `<div class="filter-dropdown-panel">`;
  options.forEach(o => {
    const isIncluded = includeList.includes(o.value);
    const isExcluded = excludeList.includes(o.value);
    const cls = isIncluded ? ' included' : (isExcluded ? ' excluded' : '');
    html += `<div class="filter-option${cls}" data-value="${o.value}">${o.label}<div class="label-toggle"><button class="label-toggle-btn${isIncluded ? ' active-include' : ''}" data-action="include" data-label="${o.value}">+</button><button class="label-toggle-btn${isExcluded ? ' active-exclude' : ''}" data-action="exclude" data-label="${o.value}">&minus;</button></div>${o.count != null ? '<span class="filter-option-count">' + o.count + '</span>' : ''}</div>`;
  });
  html += `</div></div>`;
  return html;
}

function getRosteredDates() {
  const dates = new Set();
  Object.values(calendarData).forEach(cal => {
    Object.keys(cal).forEach(k => { if (/^\d{4}-\d{2}-\d{2}$/.test(k)) dates.add(k); });
  });
  return [...dates].sort();
}

function buildDateTimePicker() {
  const now = new Date();
  const curYear = String(now.getFullYear());
  const curMonth = String(now.getMonth() + 1).padStart(2, '0');
  const curDay = String(now.getDate()).padStart(2, '0');
  const rosteredDates = getRosteredDates();
  const todayStr = curYear + '-' + curMonth + '-' + curDay;

  // Default: round up to next 30-min boundary, then add 30 mins
  // e.g. 12:01-12:30 -> 13:00, 12:31-13:00 -> 13:30
  const totalMins = now.getHours() * 60 + now.getMinutes();
  const nextBoundary = Math.ceil(totalMins / 30) * 30 + 30;
  let defHour = Math.floor(nextBoundary / 60) % 24;
  let defMin = nextBoundary % 60;
  const defHourStr = String(defHour).padStart(2, '0');
  const defMinStr = String(defMin).padStart(2, '0');

  // Parse current selection (default to today + next slot)
  let selYear = curYear, selMonth = curMonth, selDay = curDay, selHour = defHourStr, selMin = defMinStr;
  if (activeDateTime) {
    const parts = activeDateTime.match(/(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})/);
    if (parts) { selYear = parts[1]; selMonth = parts[2]; selDay = parts[3]; selHour = parts[4]; selMin = parts[5]; }
  }
  if (dtPendingMonth) selMonth = dtPendingMonth;
  if (dtPendingDay) selDay = dtPendingDay;

  const monthNames = ['','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

  // Available years: current + any rostered years
  const years = new Set([curYear]);
  rosteredDates.forEach(d => years.add(d.slice(0, 4)));
  const sortedYears = [...years].sort();

  // Available months for selected year
  const months = new Set();
  if (selYear === curYear) months.add(curMonth);
  rosteredDates.forEach(d => {
    if (d.slice(0, 4) === selYear && d >= todayStr) months.add(d.slice(5, 7));
  });
  const sortedMonths = [...months].sort();
  // Clamp selMonth if not available
  if (!sortedMonths.includes(selMonth) && sortedMonths.length) selMonth = sortedMonths[0];

  // Available days for selected year+month
  const days = new Set();
  if (selYear === curYear && selMonth === curMonth) days.add(curDay);
  const minDay = (selYear === curYear && selMonth === curMonth) ? curDay : '01';
  rosteredDates.forEach(d => {
    if (d.slice(0, 4) === selYear && d.slice(5, 7) === selMonth && d.slice(8) >= minDay) days.add(d.slice(8));
  });
  const sortedDays = [...days].sort();
  // Clamp selDay if not available
  if (!sortedDays.includes(selDay) && sortedDays.length) selDay = sortedDays[0];

  let html = '<div class="dt-picker' + (dtEnabled ? ' active' : '') + '">';

  function dtField(label, id, options, selected) {
    let h = '<div class="dt-field"><span class="dt-label">' + label + '</span><select id="' + id + '">';
    options.forEach(o => {
      h += '<option value="' + o.value + '"' + (o.value === selected ? ' selected' : '') + '>' + o.label + '</option>';
    });
    h += '</select></div>';
    return h;
  }

  const selDate = selYear + '-' + selMonth + '-' + selDay;
  html += '<div class="dt-field"><span class="dt-label">Date</span><input type="date" id="dtDate" class="range-date-input" value="' + selDate + '"></div>';

  const hourOpts = [];
  for (let h = 0; h < 24; h++) { const s = String(h).padStart(2, '0'); hourOpts.push({ value: s, label: s }); }
  html += dtField('Hour', 'dtHour', hourOpts, selHour);
  html += dtField('Min', 'dtMinute', [{ value: '00', label: '00' }, { value: '30', label: '30' }], selMin);

  // Toggle button
  const btnCls = dtEnabled ? 'dt-toggle-active' : 'dt-toggle-inactive';
  html += '<div class="dt-field"><span class="dt-label">&nbsp;</span><button class="' + btnCls + '" id="dtToggle">' + (dtEnabled ? 'ON' : 'OFF') + '</button></div>';

  html += '</div>';
  return html;
}

function buildSortDropdown(id, options, activeField, activeDir) {
  const active = options.find(o => o.value === activeField);
  const dirIcon = activeDir === 'asc' ? '&#x25B2;' : '&#x25BC;';
  const selLabel = active ? active.label + ' ' + dirIcon : 'Sort';
  let html = `<div class="filter-dropdown" id="${id}">`;
  html += `<button class="filter-dropdown-btn has-selection">${selLabel}<svg viewBox="0 0 24 24"><path d="M7.41 8.59L12 13.17l4.59-4.58L18 10l-6 6-6-6z"/></svg></button>`;
  html += `<div class="filter-dropdown-panel">`;
  options.forEach(o => {
    const isAsc = activeField === o.value && activeDir === 'asc';
    const isDesc = activeField === o.value && activeDir === 'desc';
    const cls = isAsc || isDesc ? (isAsc ? ' included' : ' excluded') : '';
    html += `<div class="filter-option${cls}" data-value="${o.value}">${o.label}<div class="label-toggle"><button class="sort-dir-toggle${isAsc ? ' active-include' : ''}" data-action="asc" data-sort="${o.value}">&#x25B2;</button><button class="sort-dir-toggle${isDesc ? ' active-exclude' : ''}" data-action="desc" data-sort="${o.value}">&#x25BC;</button></div></div>`;
  });
  html += `</div></div>`;
  return html;
}

function renderFilters() {
  const fr = document.getElementById('filterRow');
  const base = allGirls;

  // Venue options
  const venueOpts = [];
  VENUES.forEach(v => { venueOpts.push({ value: v.id, label: v.name, count: allGirls.filter(g => g.venue === v.id).length }) });
  venueOpts.sort((a, b) => {
    const aActive = activeVenue.include.includes(a.value) || activeVenue.exclude.includes(a.value);
    const bActive = activeVenue.include.includes(b.value) || activeVenue.exclude.includes(b.value);
    if (aActive && !bActive) return -1;
    if (!aActive && bActive) return 1;
    return a.label.localeCompare(b.label);
  });

  // Country options
  const countryCounts = {};
  base.forEach(g => {
    (Array.isArray(g.country) ? g.country : (g.country ? [g.country] : [])).forEach(c => { countryCounts[c] = (countryCounts[c] || 0) + 1 });
  });
  const countryOpts = Object.keys(countryCounts).sort((a, b) => {
    const aActive = activeCountry.include.includes(a) || activeCountry.exclude.includes(a);
    const bActive = activeCountry.include.includes(b) || activeCountry.exclude.includes(b);
    if (aActive && !bActive) return -1;
    if (!aActive && bActive) return 1;
    return a.localeCompare(b);
  }).map(c => ({ value: c, label: c, count: countryCounts[c] }));

  // Label options
  const labelCounts = {};
  base.forEach(g => { (g.labels || []).forEach(l => { labelCounts[l] = (labelCounts[l] || 0) + 1 }) });
  const labelOpts = Object.keys(labelCounts).sort((a, b) => {
    const aActive = activeLabels.include.includes(a) || activeLabels.exclude.includes(a);
    const bActive = activeLabels.include.includes(b) || activeLabels.exclude.includes(b);
    if (aActive && !bActive) return -1;
    if (!aActive && bActive) return 1;
    return a.localeCompare(b);
  }).map(l => ({ value: l, label: l, count: labelCounts[l] }));

  // Availability options
  const availStatuses = ['Available Now', 'Available Later Today', 'Available Future Date', 'Unavailable'];
  const availCounts = {};
  availStatuses.forEach(s => { availCounts[s] = 0 });
  base.forEach(g => { const s = getAvailabilityStatus(g); availCounts[s] = (availCounts[s] || 0) + 1; });
  const availOpts = availStatuses.sort((a, b) => {
    const aActive = activeAvailability.include.includes(a) || activeAvailability.exclude.includes(a);
    const bActive = activeAvailability.include.includes(b) || activeAvailability.exclude.includes(b);
    if (aActive && !bActive) return -1;
    if (!aActive && bActive) return 1;
    return 0;
  }).map(s => ({ value: s, label: s, count: availCounts[s] }));

  // Photos options
  const photosOpts = [
    { value: 'Yes', label: 'Yes', count: base.filter(g => g.photos && g.photos.length > 0).length },
    { value: 'No', label: 'No', count: base.filter(g => !g.photos || g.photos.length === 0).length }
  ];

  fr.innerHTML = buildLabelDropdown('ddVenue', 'Venue', venueOpts, activeVenue.include, activeVenue.exclude)
    + buildLabelDropdown('ddCountry', 'Country', countryOpts, activeCountry.include, activeCountry.exclude)
    + buildLabelDropdown('ddLabels', 'Services', labelOpts, activeLabels.include, activeLabels.exclude)
    + buildLabelDropdown('ddAV', 'AV', [{value:'Yes',label:'Yes',count:allGirls.filter(g=>g.pornstar).length},{value:'No',label:'No',count:allGirls.filter(g=>!g.pornstar).length}], activeAV.include, activeAV.exclude)
    + buildLabelDropdown('ddPhotos', 'Photos', photosOpts, activePhotos.include, activePhotos.exclude)
    + buildLabelDropdown('ddFav', 'Favourites', [{value:'Yes',label:'Yes',count:allGirls.filter(g=>isFavorite(g)).length},{value:'No',label:'No',count:allGirls.filter(g=>!isFavorite(g)).length}], activeFavFilter.include, activeFavFilter.exclude)
    + buildLabelDropdown('ddAvailability', 'Availability', availOpts, activeAvailability.include, activeAvailability.exclude)
    + (hasAnyFilter() ? '<button class="clear-all-btn" id="clearAllBtn">Clear All</button>' : '');

  // Date-time picker hidden
  document.getElementById('dtPickerRow').innerHTML = '';

  // Text filter inputs in More Filters panel
  const mfd = document.getElementById('moreFiltersDropdowns');
  const tfDefs = [
    { key: 'name', placeholder: 'Name', maxlength: 15 },
    { key: 'exp', placeholder: 'Experience', maxlength: 15 },
    { key: 'special', placeholder: 'Special', maxlength: 15 },
    { key: 'lang', placeholder: 'Language', maxlength: 15 },
    { key: 'type', placeholder: 'Type', maxlength: 15 },
    { key: 'desc', placeholder: 'Description', maxlength: 15 }
  ];
  mfd.innerHTML = tfDefs.map(d => '<input type="text" class="search-input" data-tf="' + d.key + '" placeholder="' + d.placeholder + '..." autocomplete="off"' + (d.maxlength ? ' maxlength="' + d.maxlength + '"' : '') + (d.key === 'cup' ? ' style="text-transform:uppercase"' : '') + ' value="' + (textFilters[d.key] || '').replace(/"/g, '&quot;') + '">').join('');
  mfd.querySelectorAll('.search-input').forEach(inp => {
    inp.addEventListener('input', e => {
      clearTimeout(searchTimer);
      searchTimer = setTimeout(() => { textFilters[inp.dataset.tf] = e.target.value.trim(); renderGrid(); updateClearBtn(); }, 200);
    });
  });

  // Bind dropdown toggles
  fr.querySelectorAll('.filter-dropdown-btn').forEach(btn => {
    btn.onclick = e => { e.stopPropagation(); closeAllDropdowns(btn); btn.classList.toggle('open'); btn.nextElementSibling.classList.toggle('open') };
  });

  // Bind +/- toggle for all label dropdowns
  const toggleMappings = [
    { sel: '#ddVenue', state: activeVenue },
    { sel: '#ddCountry', state: activeCountry },
    { sel: '#ddLabels', state: activeLabels },
    { sel: '#ddAV', state: activeAV },
    { sel: '#ddAvailability', state: activeAvailability },
    { sel: '#ddPhotos', state: activePhotos },
    { sel: '#ddFav', state: activeFavFilter },
  ];
  toggleMappings.forEach(({ sel, state }) => {
    document.querySelectorAll(sel + ' .label-toggle-btn').forEach(btn => {
      btn.onclick = e => {
        e.stopPropagation();
        const label = btn.dataset.label;
        const action = btn.dataset.action;
        const incIdx = state.include.indexOf(label);
        const excIdx = state.exclude.indexOf(label);
        if (incIdx >= 0) state.include.splice(incIdx, 1);
        if (excIdx >= 0) state.exclude.splice(excIdx, 1);
        if (action === 'include' && incIdx < 0) state.include.push(label);
        else if (action === 'exclude' && excIdx < 0) state.exclude.push(label);
        renderFilters(); renderGrid();
      };
    });
  });

  // DateTime filter dropdowns
  const dtDate = document.getElementById('dtDate');
  const dtHour = document.getElementById('dtHour');
  const dtMinute = document.getElementById('dtMinute');
  const dtToggle = document.getElementById('dtToggle');
  function syncDateTime() {
    if (dtEnabled && dtDate && dtHour && dtMinute && dtDate.value) {
      activeDateTime = dtDate.value + 'T' + dtHour.value + ':' + dtMinute.value;
    } else {
      activeDateTime = '';
    }
  }
  function dtChanged() { syncDateTime(); renderFilters(); renderGrid(); }
  if (dtDate) dtDate.onchange = () => {
    if (dtDate.value) {
      const parts = dtDate.value.split('-');
      dtPendingMonth = parts[1]; dtPendingDay = parts[2];
    }
    dtChanged();
  };
  if (dtHour) dtHour.onchange = dtChanged;
  if (dtMinute) dtMinute.onchange = dtChanged;
  if (dtToggle) dtToggle.onclick = () => {
    dtEnabled = !dtEnabled;
    syncDateTime(); renderFilters(); renderGrid();
  };

  // Clear all filters
  const clearBtn = document.getElementById('clearAllBtn');
  if (clearBtn) {
    clearBtn.onclick = () => { activeVenue.include.length = 0; activeVenue.exclude.length = 0; activeCountry.include.length = 0; activeCountry.exclude.length = 0; activeLabels.include.length = 0; activeLabels.exclude.length = 0; activeAV.include.length = 0; activeAV.exclude.length = 0; activeAvailability.include.length = 0; activeAvailability.exclude.length = 0; activePhotos.include.length = 0; activePhotos.exclude.length = 0; activeFavFilter.include.length = 0; activeFavFilter.exclude.length = 0; activeDateTime = ''; dtEnabled = false; dtPendingMonth = ''; dtPendingDay = ''; rangeFilters = {}; Object.keys(textFilters).forEach(k => textFilters[k] = ''); renderFilters(); renderRangeFilters(); renderGrid(); };
  }

  // Sort row (dropdown + direction toggle)
  const sr = document.getElementById('sortRow');
  const sorts = [
    { id: 'venue', label: 'Venue' },
    { id: 'name', label: 'Name' },
    { id: 'age', label: 'Age' },
    { id: 'body', label: 'Body' },
    { id: 'height', label: 'Height' },
    { id: 'val1', label: '30 Min' },
    { id: 'val2', label: '45 Min' },
    { id: 'val3', label: '60 Min' },
    { id: 'preference', label: 'Preference' }
  ];
  const sortOpts = sorts.map(s => ({ value: s.id, label: s.label }));
  sr.innerHTML = buildSortDropdown('ddSort', sortOpts, activeSort, sortDir);

  // Sort dropdown toggle
  const sortBtn = sr.querySelector('.filter-dropdown-btn');
  sortBtn.onclick = e => { e.stopPropagation(); closeAllDropdowns(sortBtn); sortBtn.classList.toggle('open'); sortBtn.nextElementSibling.classList.toggle('open') };

  // Sort options with direction
  document.querySelectorAll('#ddSort .sort-dir-toggle').forEach(btn => {
    btn.onclick = e => {
      e.stopPropagation();
      const field = btn.dataset.sort;
      const dir = btn.dataset.action;
      if (activeSort === field && sortDir === dir) return;
      activeSort = field;
      sortDir = dir;
      renderFilters(); renderGrid();
    };
  });
}

function renderRangeFilters() {
  const grid = document.getElementById('rangeFiltersGrid');
  grid.innerHTML = '';
  RANGE_DEFS.forEach(def => {
    let vals = [];
    allGirls.forEach(g => {
      const raw = g[def.field];
      if (!raw && raw !== 0) return;
      if (def.type === 'num') { const n = parseFloat(raw); if (!isNaN(n)) vals.push(n); }
      else if (def.type === 'cup') { const n = cupToNum(raw); if (n >= 0) vals.push(n); }
      else if (def.type === 'date') { const n = dateToNum(raw); if (!isNaN(n)) vals.push(n); }
    });
    if (vals.length < 2) return;
    const dataMin = Math.min(...vals), dataMax = Math.max(...vals);
    if (dataMin === dataMax) return;
    rangeDefaults[def.key] = { min: dataMin, max: dataMax };
    const sevenDaysAgo = new Date(); sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
    const defaultMin = def.key === 'lastRostered' ? Math.max(dataMin, sevenDaysAgo.getTime()) : dataMin;
    if (!rangeFilters[def.key] && defaultMin > dataMin) rangeFilters[def.key] = { min: defaultMin, max: dataMax };
    const rf = rangeFilters[def.key] || { min: defaultMin, max: dataMax };
    if (rf.min < dataMin) rf.min = dataMin;
    if (rf.max > dataMax) rf.max = dataMax;

    const fmt = v => {
      if (def.type === 'cup') return numToCup(v);
      if (def.type === 'date') return numToDateShort(v);
      return (def.prefix || '') + Math.round(v) + (def.suffix || '');
    };

    const isActive = rf.min > dataMin || rf.max < dataMax;
    const wrap = document.createElement('div');
    wrap.className = 'range-filter';
    if (def.type === 'date') {
      wrap.innerHTML = `
        <div class="range-filter-label"><span>${def.label}</span><span><span class="range-date-val" data-handle="min" data-key="${def.key}">${fmt(rf.min)}</span><span class="range-date-sep"> – </span><span class="range-date-val" data-handle="max" data-key="${def.key}">${fmt(rf.max)}</span></span></div>
        <div class="range-slider">
          <div class="range-slider-track"></div>
          <div class="range-slider-fill" id="fill_${def.key}"></div>
          <input type="range" min="${dataMin}" max="${dataMax}" value="${rf.min}" step="86400000" data-handle="min" data-key="${def.key}">
          <input type="range" min="${dataMin}" max="${dataMax}" value="${rf.max}" step="86400000" data-handle="max" data-key="${def.key}">
        </div>`;
    } else {
      wrap.innerHTML = `
        <div class="range-filter-label"><span>${def.label}</span><span>${fmt(rf.min)} – ${fmt(rf.max)}</span></div>
        <div class="range-slider">
          <div class="range-slider-track"></div>
          <div class="range-slider-fill" id="fill_${def.key}"></div>
          <input type="range" min="${dataMin}" max="${dataMax}" value="${rf.min}" step="1" data-handle="min" data-key="${def.key}">
          <input type="range" min="${dataMin}" max="${dataMax}" value="${rf.max}" step="1" data-handle="max" data-key="${def.key}">
        </div>`;
    }
    grid.appendChild(wrap);

    // Position fill bar
    const fillPct = v => ((v - dataMin) / (dataMax - dataMin)) * 100;
    const fill = wrap.querySelector(`#fill_${def.key}`);
    fill.style.left = fillPct(rf.min) + '%';
    fill.style.right = (100 - fillPct(rf.max)) + '%';

    // Update label helper
    const updateLabel = () => {
      const rf2 = rangeFilters[def.key] || { min: dataMin, max: dataMax };
      if (def.type === 'date') {
        const minEl = wrap.querySelector('.range-date-val[data-handle="min"]');
        const maxEl = wrap.querySelector('.range-date-val[data-handle="max"]');
        if (minEl) minEl.textContent = fmt(rf2.min);
        if (maxEl) maxEl.textContent = fmt(rf2.max);
      } else {
        wrap.querySelector('.range-filter-label span:last-child').textContent = fmt(rf2.min) + ' \u2013 ' + fmt(rf2.max);
      }
    };

    // Bind range inputs
    wrap.querySelectorAll('input[type=range]').forEach(inp => {
      const update = () => {
        const handle = inp.dataset.handle;
        let v = parseFloat(inp.value);
        const other = wrap.querySelector(`input[type=range][data-handle="${handle === 'min' ? 'max' : 'min'}"]`);
        if (handle === 'min' && v > parseFloat(other.value)) { v = parseFloat(other.value); inp.value = v; }
        if (handle === 'max' && v < parseFloat(other.value)) { v = parseFloat(other.value); inp.value = v; }
        if (!rangeFilters[def.key]) rangeFilters[def.key] = { min: dataMin, max: dataMax };
        rangeFilters[def.key][handle] = v;
        fill.style.left = fillPct(rangeFilters[def.key].min) + '%';
        fill.style.right = (100 - fillPct(rangeFilters[def.key].max)) + '%';
        updateLabel();
      };
      inp.addEventListener('input', update);
      inp.addEventListener('change', () => { update(); renderFilters(); renderGrid(); });
    });

    // Date picker on click for date-type filters (event delegation)
    if (def.type === 'date') {
      wrap.addEventListener('click', e => {
        const el = e.target.closest('.range-date-val');
        if (!el || wrap.querySelector('.range-date-input')) return;
        const handle = el.dataset.handle;
        const dateInput = document.createElement('input');
        dateInput.type = 'date';
        dateInput.className = 'range-date-input';
        dateInput.value = fmt(rangeFilters[def.key]?.[handle] ?? (handle === 'min' ? dataMin : dataMax));
        dateInput.min = numToDate(dataMin);
        dateInput.max = numToDate(dataMax);
        dateInput.addEventListener('keydown', ev => ev.preventDefault());
        el.style.display = 'none';
        el.after(dateInput);
        dateInput.focus();
        let committed = false;
        const commit = () => {
          if (committed) return;
          committed = true;
          const v = dateToNum(dateInput.value);
          if (!isNaN(v) && v >= dataMin && v <= dataMax) {
            if (!rangeFilters[def.key]) rangeFilters[def.key] = { min: dataMin, max: dataMax };
            rangeFilters[def.key][handle] = v;
            const slider = wrap.querySelector(`input[type=range][data-handle="${handle}"]`);
            if (slider) slider.value = v;
            fill.style.left = fillPct(rangeFilters[def.key].min) + '%';
            fill.style.right = (100 - fillPct(rangeFilters[def.key].max)) + '%';
          }
          dateInput.remove();
          el.style.display = '';
          updateLabel();
          renderFilters(); renderGrid();
        };
        dateInput.addEventListener('change', commit);
        dateInput.addEventListener('blur', commit);
      });
    }
  });
}

function getFiltered() {
  let list = [...allGirls];
  // Venue filter
  if (activeVenue.include.length) list = list.filter(g => activeVenue.include.includes(g.venue));
  if (activeVenue.exclude.length) list = list.filter(g => !activeVenue.exclude.includes(g.venue));
  // Country filter
  if (activeCountry.include.length) list = list.filter(g => {
    const countries = Array.isArray(g.country) ? g.country : (g.country ? [g.country] : []);
    return activeCountry.include.every(c => countries.includes(c));
  });
  if (activeCountry.exclude.length) list = list.filter(g => {
    const countries = Array.isArray(g.country) ? g.country : (g.country ? [g.country] : []);
    return activeCountry.exclude.every(c => !countries.includes(c));
  });
  // Labels filter
  if (activeLabels.include.length) list = list.filter(g => {
    const gl = g.labels || [];
    return activeLabels.include.every(l => gl.some(lb => lb.toLowerCase() === l.toLowerCase()));
  });
  if (activeLabels.exclude.length) list = list.filter(g => {
    const gl = g.labels || [];
    return activeLabels.exclude.every(l => !gl.some(lb => lb.toLowerCase() === l.toLowerCase()));
  });
  // AV filter
  if (activeAV.include.length) list = list.filter(g => {
    const isAV = g.pornstar ? 'Yes' : 'No';
    return activeAV.include.includes(isAV);
  });
  if (activeAV.exclude.length) list = list.filter(g => {
    const isAV = g.pornstar ? 'Yes' : 'No';
    return !activeAV.exclude.includes(isAV);
  });
  // Availability filter
  if (activeAvailability.include.length) list = list.filter(g => activeAvailability.include.includes(getAvailabilityStatus(g)));
  if (activeAvailability.exclude.length) list = list.filter(g => !activeAvailability.exclude.includes(getAvailabilityStatus(g)));
  // Photos filter
  if (activePhotos.include.length) list = list.filter(g => {
    const hasPhotos = g.photos && g.photos.length > 0;
    return activePhotos.include.includes(hasPhotos ? 'Yes' : 'No');
  });
  if (activePhotos.exclude.length) list = list.filter(g => {
    const hasPhotos = g.photos && g.photos.length > 0;
    return !activePhotos.exclude.includes(hasPhotos ? 'Yes' : 'No');
  });
  // Favourites filter
  if (activeFavFilter.include.length) list = list.filter(g => {
    const fav = isFavorite(g) ? 'Yes' : 'No';
    return activeFavFilter.include.includes(fav);
  });
  if (activeFavFilter.exclude.length) list = list.filter(g => {
    const fav = isFavorite(g) ? 'Yes' : 'No';
    return !activeFavFilter.exclude.includes(fav);
  });
  // DateTime filter
  if (activeDateTime) {
    const dt = new Date(activeDateTime);
    const dateStr = dt.getFullYear() + '-' + String(dt.getMonth() + 1).padStart(2, '0') + '-' + String(dt.getDate()).padStart(2, '0');
    const timeMins = dt.getHours() * 60 + dt.getMinutes();
    list = list.filter(g => isAvailableAt(g, dateStr, timeMins));
  }
  // Range filters
  RANGE_DEFS.forEach(def => {
    const rf = rangeFilters[def.key];
    if (!rf) return;
    list = list.filter(g => {
      const raw = g[def.field];
      if (!raw && raw !== 0) return true; // no data = pass through
      let v;
      if (def.type === 'num') v = parseFloat(raw);
      else if (def.type === 'cup') v = cupToNum(raw);
      else if (def.type === 'date') v = dateToNum(raw);
      if (isNaN(v) || v < 0) return true;
      return v >= rf.min && v <= rf.max;
    });
  });
  // Text filters
  const tfMap = { name: 'name', cup: 'cup', exp: 'exp', special: 'special', lang: 'lang', type: 'type', desc: 'desc' };
  Object.entries(tfMap).forEach(([key, field]) => {
    const q = (textFilters[key] || '').toLowerCase();
    if (!q) return;
    list = list.filter(g => (g[field] || '').toLowerCase().includes(q));
  });
  // Sort
  const dir = sortDir === 'desc' ? -1 : 1;
  const emptyLast = (a, b, cmp) => {
    const an = (a.name || '').trim(), bn = (b.name || '').trim();
    if (!an && !bn) return 0; if (!an) return 1; if (!bn) return -1;
    return cmp(a, b) * dir;
  };
  if (activeSort === 'venue') list.sort((a, b) => (a.venueName || '').localeCompare(b.venueName || ''));
  else if (activeSort === 'age') list.sort((a, b) => emptyLast(a, b, () => (parseFloat(a.age) || 999) - (parseFloat(b.age) || 999)));
  else if (activeSort === 'body') list.sort((a, b) => emptyLast(a, b, () => (parseFloat(a.body) || 999) - (parseFloat(b.body) || 999)));
  else if (activeSort === 'height') list.sort((a, b) => emptyLast(a, b, () => (parseFloat(a.height) || 999) - (parseFloat(b.height) || 999)));
  else if (activeSort === 'cup') list.sort((a, b) => emptyLast(a, b, () => (a.cup || '').toLowerCase().localeCompare((b.cup || '').toLowerCase())));
  else if (activeSort === 'val1') list.sort((a, b) => emptyLast(a, b, () => (parseFloat(a.val1) || 999) - (parseFloat(b.val1) || 999)));
  else if (activeSort === 'val2') list.sort((a, b) => emptyLast(a, b, () => (parseFloat(a.val2) || 999) - (parseFloat(b.val2) || 999)));
  else if (activeSort === 'val3') list.sort((a, b) => emptyLast(a, b, () => (parseFloat(a.val3) || 999) - (parseFloat(b.val3) || 999)));
  else if (activeSort === 'startDate') list.sort((a, b) => emptyLast(a, b, () => (a.startDate || '').localeCompare(b.startDate || '')));
  else if (activeSort === 'lastRostered') list.sort((a, b) => emptyLast(a, b, () => (a.lastRostered || '').localeCompare(b.lastRostered || '')));
  else if (activeSort === 'preference') list.sort((a, b) => {
    const sa = matchScores.get(a.venue + ':' + a.name) || 0;
    const sb = matchScores.get(b.venue + ':' + b.name) || 0;
    return sb - sa;
  });
  else list.sort((a, b) => emptyLast(a, b, () => (a.name || '').toLowerCase().localeCompare((b.name || '').toLowerCase())));
  return list;
}

const PAGE_SIZE = 12;
let currentFiltered = [];
let currentPage = 0;
let loadingMore = false;

function renderCard(g, grid) {
    const el = document.createElement('div');
    el.className = 'girl-card' + (isFavorite(g) ? ' favorited' : '');
    const img = g.photos && g.photos.length
      ? `<img class="card-thumb" src="${imgProxy(g.photos[0])}" alt="${(g.name || '').replace(/"/g, '&quot;')} – ${(g.venueName || '').replace(/"/g, '&quot;')} ${(VENUE_SUBURB_NAMES[g.venue] || '').replace(/"/g, '&quot;')}, Sydney" loading="lazy">`
      : '<div class="silhouette"></div>';
    const countries = Array.isArray(g.country) ? g.country.join(', ') : (g.country || '');

    const lastRostered = (() => {
      const avail = getAvailabilityText(g);
      if (avail && avail !== 'ended') return avail;
      if (!g.lastRostered) return '';
      const today = new Date(); today.setHours(0,0,0,0);
      const rd = new Date(g.lastRostered + 'T00:00:00');
      if (rd > today) return '';
      const diff = Math.round((today - rd) / 86400000);
      if (diff === 0) return 'Last available: Today';
      if (diff === 1) return 'Last available: Yesterday';
      return 'Last available: ' + diff + ' days ago';
    })();

    const girlKey = g.venue + ':' + g.name;
    const girlScore = matchScores.get(girlKey) || 0;
    const showBadge = userPreferences && girlScore > 0;

    const heartSvg = '<svg viewBox="0 0 24 24"><path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"/></svg>';
    const favActive = isFavorite(g) ? ' active' : '';

    el.innerHTML = `
      <div class="fav-heart${favActive}" data-url="${(g.oldUrl||'').replace(/"/g,'&quot;')}">${heartSvg}</div>
      <div class="card-badges">${'<span class="country-badge">' + g.venueName + '</span>'}${showBadge ? '<div class="match-badge">' + girlScore + '%</div>' : ''}${isNewProfile(g) ? '<span class="new-badge">New</span>' : ''}${g.pornstar ? '<span class="av-badge">AV</span>' : ''}</div>
      <div class="card-img">${img}</div>
      <div class="card-info">
        <div class="card-name">${g.name || ''}</div>
        <div class="card-country">${countries}</div>
        <div class="card-stats">
          ${g.age ? '<span>Age ' + g.age + '</span>' : ''}
          ${g.body ? '<span>Body ' + g.body + '</span>' : ''}
          ${g.height ? '<span>' + g.height + 'cm</span>' : ''}
          ${g.cup ? '<span>' + g.cup + ' cup</span>' : ''}
        </div>
        ${(g.val1 || g.val2 || g.val3) ? '<div class="card-rates">' + [g.val1 ? '$'+g.val1 : '', g.val2 ? '$'+g.val2 : '', g.val3 ? '$'+g.val3 : ''].filter(Boolean).join(' / ') + '</div>' : ''}
        ${lastRostered ? '<div class="card-last-rostered' + (lastRostered.startsWith('Available Now') ? ' available-now' : lastRostered.startsWith('Available Later') ? ' available-later' : lastRostered.startsWith('Available Future') ? ' available-future' : '') + '">' + lastRostered + '</div>' : ''}
        <div class="card-hover-line"></div>
      </div>`;
    el.querySelector('.fav-heart').addEventListener('click', (e) => toggleFavorite(g.oldUrl, e));
    el.onclick = (e) => { closeFavorites(); spawnParticles(e); showProfile(g); };
    grid.appendChild(el);
    cardObserver.observe(el);
}

// Staggered card entrance via IntersectionObserver
const cardObserver = new IntersectionObserver((entries) => {
  entries.forEach((entry, i) => {
    if (entry.isIntersecting) {
      const el = entry.target;
      const delay = Array.from(el.parentElement.children).indexOf(el) % PAGE_SIZE * 40;
      setTimeout(() => {
        el.classList.add('card-visible');
        // After entrance animation, switch to settled state so tab-switch doesn't replay
        setTimeout(() => { el.classList.remove('card-visible'); el.classList.add('card-settled'); }, 600);
      }, delay);
      cardObserver.unobserve(el);
    }
  });
}, { threshold: 0.05 });

// Gold particle burst on card click
function spawnParticles(e) {
  let canvas = document.querySelector('.particle-canvas');
  if (!canvas) {
    canvas = document.createElement('canvas');
    canvas.className = 'particle-canvas';
    document.body.appendChild(canvas);
  }
  canvas.width = window.innerWidth;
  canvas.height = window.innerHeight;
  const ctx = canvas.getContext('2d');
  const cx = e.clientX, cy = e.clientY;
  const particles = [];
  for (let i = 0; i < 28; i++) {
    const angle = Math.random() * Math.PI * 2;
    const speed = 1.5 + Math.random() * 4;
    particles.push({ x: cx, y: cy, vx: Math.cos(angle) * speed, vy: Math.sin(angle) * speed, life: 1, size: 1.5 + Math.random() * 2.5, color: ['#c9952c','#f5e6a3','#e1b97e','#f5d78e'][Math.floor(Math.random()*4)] });
  }
  function tick() {
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    let alive = false;
    for (const p of particles) {
      p.x += p.vx; p.y += p.vy; p.vy += 0.08; p.life -= 0.02;
      if (p.life <= 0) continue;
      alive = true;
      ctx.globalAlpha = p.life;
      ctx.fillStyle = p.color;
      ctx.beginPath();
      ctx.arc(p.x, p.y, p.size * p.life, 0, Math.PI * 2);
      ctx.fill();
    }
    if (alive) requestAnimationFrame(tick);
    else ctx.clearRect(0, 0, canvas.width, canvas.height);
  }
  requestAnimationFrame(tick);
}

function loadMore() {
  if (loadingMore) return;
  const grid = document.getElementById('girlsGrid');
  const start = currentPage * PAGE_SIZE;
  const end = Math.min(start + PAGE_SIZE, currentFiltered.length);
  if (start >= currentFiltered.length) return;
  loadingMore = true;
  for (let i = start; i < end; i++) renderCard(currentFiltered[i], grid);
  currentPage++;
  loadingMore = false;
}


function renderGrid() {
  const grid = document.getElementById('girlsGrid');
  currentFiltered = getFiltered();
  currentPage = 0;

  document.getElementById('resultCount').textContent = currentFiltered.length + ' girl' + (currentFiltered.length !== 1 ? 's' : '') + ' found';
  updateMoreFiltersCount();

  if (!currentFiltered.length) {
    grid.innerHTML = '<div class="empty-msg"><svg width="80" height="80" viewBox="0 0 80 80" fill="none" style="margin-bottom:20px"><circle cx="40" cy="40" r="38" stroke="rgba(201,149,44,0.25)" stroke-width="1.5"/><circle cx="40" cy="40" r="28" stroke="rgba(201,149,44,0.15)" stroke-width="1"/><path d="M30 45c0-5.5 4.5-10 10-10s10 4.5 10 10" stroke="rgba(201,149,44,0.3)" stroke-width="1.5" stroke-linecap="round" fill="none" transform="rotate(180 40 40)"/><circle cx="33" cy="35" r="2" fill="rgba(201,149,44,0.3)"/><circle cx="47" cy="35" r="2" fill="rgba(201,149,44,0.3)"/></svg><div>No girls match your filters</div></div>';
    return;
  }

  grid.innerHTML = '';
  loadMore();
  fillViewport();
  if (rosterViewActive) renderRoster();
}

function fillViewport() {
  requestAnimationFrame(() => {
    while (currentPage * PAGE_SIZE < currentFiltered.length && document.body.offsetHeight <= window.innerHeight + 400) {
      loadMore();
    }
  });
}

window.addEventListener('scroll', () => {
  if (currentPage * PAGE_SIZE >= currentFiltered.length) return;
  if (window.innerHeight + window.scrollY >= document.body.offsetHeight - 400) loadMore();
});

/* Roster Calendar View */
let rosterViewActive = false;
let rosterVenueFilter = 'all';
let rosterSelectedDay = 0; // index into rosterDays

function toggleRosterView() {
  rosterViewActive = !rosterViewActive;
  const btn = document.getElementById('rosterToggle');
  const grid = document.getElementById('girlsGrid');
  const roster = document.getElementById('rosterView');
  btn.classList.toggle('active', rosterViewActive);
  grid.style.display = rosterViewActive ? 'none' : '';
  roster.classList.toggle('active', rosterViewActive);
  document.getElementById('resultCount').style.display = rosterViewActive ? 'none' : '';

  if (rosterViewActive) renderRoster();
}

function renderRoster() {
  const container = document.getElementById('rosterView');
  const now = new Date();
  // Before 6am, the roster "today" is still yesterday (timeline runs 6am-6am)
  const rosterNow = new Date(now);
  if (rosterNow.getHours() < 6) rosterNow.setDate(rosterNow.getDate() - 1);
  const todayStr = rosterNow.getFullYear() + '-' + String(rosterNow.getMonth() + 1).padStart(2, '0') + '-' + String(rosterNow.getDate()).padStart(2, '0');
  const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
  const dayNamesShort = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

  // Collect all dates with roster entries (today + next 7 days max)
  const dates = [];
  for (let i = 0; i < 7; i++) {
    const d = new Date(rosterNow); d.setDate(d.getDate() + i);
    const ds = d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
    dates.push({ str: ds, date: d });
  }

  // Build per-date roster entries, respecting all active filters + sort
  const filtered = getFiltered();
  const filteredOrder = new Map();
  filtered.forEach((g, i) => { filteredOrder.set((g.venue || '') + ':' + g.name, i); });

  const rosterDays = [];
  for (const { str: dateStr, date } of dates) {
    const entries = [];
    for (const [key, cal] of Object.entries(calendarData)) {
      if (!cal[dateStr]) continue;
      if (!filteredOrder.has(key)) continue;
      const g = filtered.find(g => (g.venue || '') + ':' + g.name === key);
      if (!g) continue;
      // For today, skip entries whose shift has ended
      if (dateStr === todayStr) {
        const slot = cal[dateStr];
        const [sh, sm] = slot.start.split(':').map(Number);
        const [eh, em] = slot.end.split(':').map(Number);
        // If we're before 6am and todayStr is yesterday, add 24h to nowMins
        let nowMins = now.getHours() * 60 + now.getMinutes();
        if (now.getHours() < 6 && rosterNow.getDate() !== now.getDate()) nowMins += 24 * 60;
        const startMins = sh * 60 + sm;
        const endMins = eh * 60 + em;
        const effectiveEnd = endMins <= startMins ? 24 * 60 + endMins : endMins;
        if (nowMins >= effectiveEnd) continue;
      }
      entries.push({ girl: g, slot: cal[dateStr], order: filteredOrder.get(key) });
    }
    if (entries.length > 0) {
      entries.sort((a, b) => a.order - b.order);
      rosterDays.push({ dateStr, date, entries });
    }
  }

  const venueFilterHtml = '';

  if (!rosterDays.length) {
    container.innerHTML = venueFilterHtml + '<div class="empty-msg">No roster data available for the next 7 days.</div>';
    return;
  }

  // If date/time filter is active, only show the matching day
  if (activeDateTime) {
    const dt = new Date(activeDateTime);
    const dtDateStr = dt.getFullYear() + '-' + String(dt.getMonth() + 1).padStart(2, '0') + '-' + String(dt.getDate()).padStart(2, '0');
    const matchIdx = rosterDays.findIndex(d => d.dateStr === dtDateStr);
    if (matchIdx >= 0) rosterSelectedDay = matchIdx;
    else {
      container.innerHTML = '<div class="empty-msg">No roster data for ' + dtDateStr + '.</div>';
      return;
    }
  }

  // Clamp selected day index
  if (rosterSelectedDay >= rosterDays.length) rosterSelectedDay = 0;

  // Timeline: 6am to 6am next day (24h range)
  const TIMELINE_START = 6; // 6am
  const TIMELINE_HOURS = 24;
  const hours = [];
  for (let i = 0; i <= TIMELINE_HOURS; i += 2) {
    const h = (TIMELINE_START + i) % 24;
    hours.push(fmt24to12(String(h).padStart(2, '0') + ':00'));
  }

  // Girl count above tabs
  let html = venueFilterHtml;
  const totalGirls = rosterDays[rosterSelectedDay].entries.length;
  html += '<div class="roster-day-count">' + totalGirls + ' girl' + (totalGirls !== 1 ? 's' : '') + ' found</div>';

  // Day tabs (hidden when date/time filter locks to a specific day)
  if (!activeDateTime) {
    html += '<div class="roster-day-tabs">';
    rosterDays.forEach((day, i) => {
      const isToday = day.dateStr === todayStr;
      const label = isToday ? 'Today' : dayNamesShort[day.date.getDay()];
      const dateLabel = day.date.getDate() + ' ' + monthNames[day.date.getMonth()];
      html += `<button class="roster-day-tab${i === rosterSelectedDay ? ' active' : ''}" onclick="setRosterDay(${i})">${label} <span style="opacity:0.7">${dateLabel}</span><span class="tab-count">${day.entries.length}</span></button>`;
    });
    html += '</div>';
  }

  // Render only selected day
  const day = rosterDays[rosterSelectedDay];
  {
    const isToday = day.dateStr === todayStr;
    const dayName = dayNames[day.date.getDay()];
    const dateLabel = day.date.getDate() + ' ' + monthNames[day.date.getMonth()];

    html += `<div class="roster-day">
      <div class="roster-timeline">
        <div class="roster-timeline-header">
          <div></div>
          <div class="roster-timeline-hours">${hours.map(h => `<span>${h}</span>`).join('')}</div>
        </div>`;

    for (const { girl: g, slot } of day.entries) {
      const thumb = g.photos && g.photos.length ? imgProxy(g.photos[0], 72) : '';
      const [sh, sm] = slot.start.split(':').map(Number);
      const [eh, em] = slot.end.split(':').map(Number);

      // Convert to position within timeline (6am = 0%, 6am+24h = 100%)
      let startOffset = (sh - TIMELINE_START) * 60 + sm;
      if (startOffset < 0) startOffset += 24 * 60;
      let endOffset = (eh - TIMELINE_START) * 60 + em;
      if (endOffset < 0) endOffset += 24 * 60;
      if (endOffset <= startOffset) endOffset += 24 * 60;

      const totalMins = TIMELINE_HOURS * 60;
      const leftPct = Math.max(0, (startOffset / totalMins) * 100);
      const widthPct = Math.min(100 - leftPct, ((endOffset - startOffset) / totalMins) * 100);

      // Determine bar state
      let barClass = 'future';
      if (isToday) {
        const nowMins = now.getHours() * 60 + now.getMinutes();
        let nowOffset = (now.getHours() - TIMELINE_START) * 60 + now.getMinutes();
        if (nowOffset < 0) nowOffset += 24 * 60;
        if (nowOffset >= startOffset && nowOffset < endOffset) barClass = 'now';
        else if (nowOffset < startOffset) barClass = 'later';
      }

      const timeStr = fmt24to12(slot.start) + ' - ' + fmt24to12(slot.end);

      html += `<div class="roster-entry" onclick="showProfile(allGirls.find(g=>g.venue==='${g.venue}'&&g.name==='${g.name.replace(/'/g, "\\'")}'))">
        <div class="roster-entry-info">
          ${thumb ? `<img class="roster-entry-thumb" src="${thumb}" alt="">` : '<div class="roster-entry-thumb" style="background:rgba(255,255,255,0.06)"></div>'}
          <div>
            <div class="roster-entry-name">${g.name}</div>
            <div class="roster-entry-venue">${(g.val1 || g.val2 || g.val3) ? [g.val1 ? '$'+g.val1 : '', g.val2 ? '$'+g.val2 : '', g.val3 ? '$'+g.val3 : ''].filter(Boolean).join(' / ') : g.venueName}</div>
          </div>
        </div>
        <div class="roster-entry-bar-container">
          <div class="roster-entry-bar ${barClass}" style="left:${leftPct}%;width:${widthPct}%" title="${timeStr}">
            <span>${timeStr}</span>
          </div>
        </div>
      </div>`;
    }

    // Now line for today
    if (isToday) {
      let nowOffset = (now.getHours() - TIMELINE_START) * 60 + now.getMinutes();
      if (nowOffset < 0) nowOffset += 24 * 60;
      const nowPct = (nowOffset / (TIMELINE_HOURS * 60)) * 100;
      if (nowPct >= 0 && nowPct <= 100) {
        // Insert now line via JS after render
      }
    }

    html += `</div></div>`;
  }

  container.innerHTML = html;

  // Add vertical indicator lines to the timeline
  const timeline = container.querySelector('.roster-timeline');
  if (timeline) {
    const nameColWidth = 180; // 160px col + 12px gap + 8px padding

    // Now line (red) if selected day is today
    if (day.dateStr === todayStr) {
      let nowOffset = (now.getHours() - TIMELINE_START) * 60 + now.getMinutes();
      if (nowOffset < 0) nowOffset += 24 * 60;
      const nowPct = (nowOffset / (TIMELINE_HOURS * 60)) * 100;
      if (nowPct >= 0 && nowPct <= 100) {
        const line = document.createElement('div');
        line.className = 'roster-timeline-line now';
        line.style.left = `calc(${nameColWidth}px + (100% - ${nameColWidth}px) * ${nowPct / 100})`;
        const label = document.createElement('div');
        label.className = 'roster-timeline-line-label';
        label.textContent = 'Now';
        line.appendChild(label);
        timeline.appendChild(line);
      }
    }

    // Filter line (purple) for activeDateTime
    if (activeDateTime) {
      const dt = new Date(activeDateTime);
      const dtDateStr = dt.getFullYear() + '-' + String(dt.getMonth() + 1).padStart(2, '0') + '-' + String(dt.getDate()).padStart(2, '0');
      if (day.dateStr === dtDateStr) {
        let filterOffset = (dt.getHours() - TIMELINE_START) * 60 + dt.getMinutes();
        if (filterOffset < 0) filterOffset += 24 * 60;
        const filterPct = (filterOffset / (TIMELINE_HOURS * 60)) * 100;
        if (filterPct >= 0 && filterPct <= 100) {
          const line = document.createElement('div');
          line.className = 'roster-timeline-line filter';
          line.style.left = `calc(${nameColWidth}px + (100% - ${nameColWidth}px) * ${filterPct / 100})`;
          const label = document.createElement('div');
          label.className = 'roster-timeline-line-label';
          label.textContent = 'Selected';
          line.appendChild(label);
          timeline.appendChild(line);
        }
      }
    }
  }
}

function setRosterDay(index) {
  rosterSelectedDay = index;
  renderRoster();
}

function setRosterVenue(venueId) {
  rosterVenueFilter = venueId;
  renderRoster();
}

document.getElementById('rosterToggle').addEventListener('click', toggleRosterView);

function buildProfileCalendar(g) {
  const cal = calendarData[(g.venue || '') + ':' + g.name];
  if (!cal) return '';
  const now = new Date();
  const rosterNow = new Date(now);
  if (rosterNow.getHours() < 6) rosterNow.setDate(rosterNow.getDate() - 1);
  const todayStr = rosterNow.getFullYear() + '-' + String(rosterNow.getMonth() + 1).padStart(2, '0') + '-' + String(rosterNow.getDate()).padStart(2, '0');
  const TIMELINE_START = 6, TIMELINE_HOURS = 24;
  const dayNames = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

  // Collect upcoming days with slots
  const days = [];
  for (let i = 0; i < 7; i++) {
    const d = new Date(rosterNow); d.setDate(d.getDate() + i);
    const ds = d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
    const slot = cal[ds];
    if (slot && slot.start && slot.end) {
      days.push({ date: d, dateStr: ds, slot, isToday: ds === todayStr });
    }
  }
  if (!days.length) return '';

  const hours = [];
  for (let i = 0; i <= TIMELINE_HOURS; i += 3) {
    const h = (TIMELINE_START + i) % 24;
    hours.push(fmt24to12(String(h).padStart(2, '0') + ':00'));
  }

  let html = '<div class="profile-calendar" style="margin-top:20px;border-top:1px solid rgba(201,149,44,0.15);padding-top:16px">';
  html += '<div style="font-family:Orbitron,sans-serif;font-size:12px;letter-spacing:3px;text-transform:uppercase;margin-bottom:12px;color:var(--gold)">Schedule</div>';

  for (const day of days) {
    const [sh, sm] = day.slot.start.split(':').map(Number);
    const [eh, em] = day.slot.end.split(':').map(Number);
    let startOffset = (sh - TIMELINE_START) * 60 + sm;
    if (startOffset < 0) startOffset += 24 * 60;
    let endOffset = (eh - TIMELINE_START) * 60 + em;
    if (endOffset < 0) endOffset += 24 * 60;
    if (endOffset <= startOffset) endOffset += 24 * 60;
    const totalMins = TIMELINE_HOURS * 60;
    const leftPct = Math.max(0, (startOffset / totalMins) * 100);
    const widthPct = Math.min(100 - leftPct, ((endOffset - startOffset) / totalMins) * 100);
    const timeStr = fmt24to12(day.slot.start) + ' - ' + fmt24to12(day.slot.end);
    const label = day.isToday ? 'Today' : dayNames[day.date.getDay()];
    const dateLabel = day.date.getDate() + ' ' + monthNames[day.date.getMonth()];

    // Bar class
    let barClass = 'future';
    if (day.isToday) {
      let nowMins = (now.getHours() - TIMELINE_START) * 60 + now.getMinutes();
      if (nowMins < 0) nowMins += 24 * 60;
      if (now.getHours() < 6 && rosterNow.getDate() !== now.getDate()) nowMins += 24 * 60;
      if (nowMins >= startOffset && nowMins < endOffset) barClass = 'now';
      else if (nowMins < startOffset) barClass = 'later';
    }

    html += '<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">';
    html += '<div style="min-width:70px;font-size:11px;letter-spacing:1px;color:var(--gold)">' + label + ' <span style="opacity:0.6">' + dateLabel + '</span></div>';
    html += '<div style="flex:1;position:relative;height:22px;background:rgba(255,255,255,0.03);border-radius:4px;overflow:visible">';
    html += '<div class="roster-entry-bar ' + barClass + '" style="position:absolute;top:0;bottom:0;left:' + leftPct + '%;width:' + widthPct + '%;border-radius:4px;display:flex;align-items:center;justify-content:center;font-size:9px;letter-spacing:1px"><span>' + timeStr + '</span></div>';

    // Now line (red) for today
    if (day.isToday) {
      let nowMinsLine = (now.getHours() - TIMELINE_START) * 60 + now.getMinutes();
      if (nowMinsLine < 0) nowMinsLine += 24 * 60;
      if (now.getHours() < 6 && rosterNow.getDate() !== now.getDate()) nowMinsLine += 24 * 60;
      const nowPct = (nowMinsLine / totalMins) * 100;
      if (nowPct >= 0 && nowPct <= 100) {
        html += '<div class="roster-timeline-line now" style="position:absolute;top:0;bottom:0;left:' + nowPct + '%"><div class="roster-timeline-line-label">Now</div></div>';
      }
    }

    // Selected line (gold) for activeDateTime
    if (activeDateTime) {
      const dt = new Date(activeDateTime);
      const dtDateStr = dt.getFullYear() + '-' + String(dt.getMonth() + 1).padStart(2, '0') + '-' + String(dt.getDate()).padStart(2, '0');
      if (day.dateStr === dtDateStr) {
        let filterOffset = (dt.getHours() - TIMELINE_START) * 60 + dt.getMinutes();
        if (filterOffset < 0) filterOffset += 24 * 60;
        const filterPct = (filterOffset / totalMins) * 100;
        if (filterPct >= 0 && filterPct <= 100) {
          html += '<div class="roster-timeline-line filter" style="position:absolute;top:0;bottom:0;left:' + filterPct + '%"><div class="roster-timeline-line-label">Selected</div></div>';
        }
      }
    }

    html += '</div></div>';
  }

  html += '</div>';
  return html;
}

const VENUE_SUBURBS = {
  ginzaempire: 'surryhills', ginzaclub: 'surryhills', kyoto206: 'surryhills',
  sakura57: 'surryhills', top127: 'chippendale', fantasyclub35: 'annandale', '429city': 'haymarket'
};
const VENUE_SUBURB_NAMES = {
  ginzaempire: 'Surry Hills', ginzaclub: 'Surry Hills', kyoto206: 'Surry Hills',
  sakura57: 'Surry Hills', top127: 'Chippendale', fantasyclub35: 'Annandale', '429city': 'Haymarket'
};

function slugify(s) { return (s || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/-+$/, ''); }

function profilePath(g) {
  const name = slugify(g.name);
  const suburb = VENUE_SUBURBS[g.venue] || 'sydney';
  const country = slugify(Array.isArray(g.country) ? g.country[0] : g.country) || 'other';
  return '/sydney/' + suburb + '/' + g.venue + '/' + country + '/' + name;
}

function findGirlByPath(path) {
  const parts = path.replace(/^\//, '').split('/');
  // New format: /sydney/{suburb}/{venue}/{country}/{name}
  if (parts.length === 5 && parts[0] === 'sydney') {
    const venue = parts[2];
    const slug = parts[4];
    return allGirls.find(g => g.venue === venue && slugify(g.name) === slug);
  }
  // Previous format: /sydney/{suburb}/{venue}/{name}
  if (parts.length === 4 && parts[0] === 'sydney') {
    const venue = parts[2];
    const slug = parts[3];
    return allGirls.find(g => g.venue === venue && slugify(g.name) === slug);
  }
  // Legacy format: /{venue}/{name}
  if (parts.length === 2) {
    const venue = parts[0];
    const slug = parts[1];
    return allGirls.find(g => g.venue === venue && slugify(g.name) === slug);
  }
  return null;
}

function updateMeta(title, desc, image, url, jsonLd) {
  document.title = title;
  const set = (sel, val) => { const el = document.querySelector(sel); if (el) el.setAttribute('content', val); };
  set('meta[name="description"]', desc);
  set('meta[property="og:title"]', title);
  set('meta[property="og:description"]', desc);
  set('meta[property="og:url"]', url);
  if (image) { set('meta[property="og:image"]', image); set('meta[name="twitter:image"]', image); }
  set('meta[name="twitter:title"]', title);
  set('meta[name="twitter:description"]', desc);
  const canonical = document.querySelector('link[rel="canonical"]');
  if (canonical) canonical.setAttribute('href', url);
  // Update JSON-LD structured data
  let ld = document.getElementById('profileJsonLd');
  if (jsonLd) {
    if (!ld) { ld = document.createElement('script'); ld.type = 'application/ld+json'; ld.id = 'profileJsonLd'; document.head.appendChild(ld); }
    ld.textContent = JSON.stringify(jsonLd);
  } else if (ld) { ld.remove(); }
}

function showProfile(g) {
  if (!g) return;
  const path = profilePath(g);
  if (window.location.pathname !== path) history.pushState({ profile: true }, '', path);
  const suburbName = VENUE_SUBURB_NAMES[g.venue] || '';
  const location = suburbName ? suburbName + ', Sydney' : 'Sydney';
  const title = (g.name || '') + ' \u2013 ' + (g.venueName || '') + ' ' + location + ' | Brothel Search';
  const desc = (g.name || '') + ' at ' + (g.venueName || '') + ', ' + location + '. ' + [g.age ? 'Age ' + g.age : '', g.country ? (Array.isArray(g.country) ? g.country.join(', ') : g.country) : ''].filter(Boolean).join(', ') + '. Browse profile, photos and availability.';
  const image = g.photos && g.photos[0] ? g.photos[0] : '';
  updateMeta(title, desc, image, 'https://brothelsearch.com' + path, {
    '@context': 'https://schema.org', '@type': 'Person',
    name: g.name || '', description: desc, url: 'https://brothelsearch.com' + path,
    image: image || undefined,
    worksFor: { '@type': 'LocalBusiness', name: g.venueName || '', address: { '@type': 'PostalAddress', addressLocality: suburbName, addressRegion: 'NSW', addressCountry: 'AU' } }
  });
  const overlay = document.getElementById('profileOverlay');
  const panel = document.getElementById('profilePanel');
  const countries = Array.isArray(g.country) ? g.country.join(', ') : (g.country || '');
  const photos = g.photos || [];
  const mainImg = photos.length ? photos[0] : '';

  panel.classList.toggle('favorited', isFavorite(g));
  panel.innerHTML = `
    <button class="profile-close" onclick="closeProfile()">&times;</button>
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:12px;flex-wrap:wrap">
      <div class="fav-heart${isFavorite(g) ? ' active' : ''}" id="profileFavHeart" data-url="${(g.oldUrl||'').replace(/"/g,'&quot;')}" onclick="toggleFavorite('${(g.oldUrl||'').replace(/'/g,"\\'")}',event)" style="position:relative;top:auto;left:auto"><svg viewBox="0 0 24 24"><path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"/></svg></div>
      <div class="country-badge">${g.venueName}</div>
      ${(() => { const k = g.venue + ':' + g.name; const s = matchScores.get(k) || 0; return userPreferences && s > 0 ? '<div class="match-badge" style="position:static;pointer-events:auto">' + s + '%</div>' : ''; })()}
      ${isNewProfile(g) ? '<span class="new-badge">New</span>' : ''}
      ${g.pornstar ? '<span class="av-badge">AV</span>' : ''}
    </div>
    <div class="profile-name">${g.name || ''}</div>
    <div class="profile-layout">
      <div class="profile-gallery">
        <div class="profile-main-wrap">
          <img id="profileMainImg" src="${mainImg}" alt="${(g.name || '').replace(/"/g, '&quot;')}" style="${!mainImg ? 'display:none' : ''}">
          ${photos.length > 1 ? '<div class="photo-counter" id="photoCounter">1 / ' + photos.length + '</div>' : ''}
        </div>
        <div class="profile-thumbs">
          ${photos.map((p, i) => `<img src="${imgProxy(p, 120)}" alt="${(g.name || '')} photo ${i + 1} of ${photos.length}" class="${i === 0 ? 'active' : ''}" onclick="selectProfilePhoto(${i})">`).join('')}
        </div>
      </div>
      <div>
        <div class="profile-detail">
          ${detailRow('Country', countries)}
          ${detailRow('Age', g.age)}
          ${detailRow('Body', g.body)}
          ${detailRow('Height', g.height ? g.height + ' cm' : '')}
          ${detailRow('Cup', g.cup)}
          ${detailRow('30 min', g.val1 ? '$' + g.val1 : '')}
          ${detailRow('45 min', g.val2 ? '$' + g.val2 : '')}
          ${detailRow('60 min', g.val3 ? '$' + g.val3 : '')}
          ${detailRow('Start Date', g.startDate)}
          ${detailRow('Last Available', g.lastRostered && new Date(g.lastRostered + 'T00:00:00') <= new Date(new Date().toDateString()) ? g.lastRostered : '')}
          ${(() => { const avail = getAvailabilityText(g); return avail && avail !== 'ended' ? '<div class="profile-detail-row"><span>Availability</span><span class="' + (avail.startsWith('Available Now') ? 'available-now' : avail.startsWith('Available Later') ? 'available-later' : avail.startsWith('Available Future') ? 'available-future' : '') + '">' + avail + '</span></div>' : ''; })()}
          ${detailRow('Experience', g.exp)}
          ${detailRow('Special', g.special)}
          ${detailRow('Language', g.lang)}
          ${detailRow('Type', g.type)}
          ${g.oldUrl ? '<div class="profile-detail-row"><span>Reference</span><span><a href="' + g.oldUrl + '" target="_blank" rel="noopener" style="color:var(--accent);text-decoration:none">' + g.oldUrl + '</a></span></div>' : ''}
        </div>
        ${g.desc ? '<div class="profile-desc">' + g.desc + '</div>' : ''}
        ${g.labels && g.labels.length ? '<div class="card-labels">' + g.labels.map(l => '<span class="card-label">' + l + '</span>').join('') + '</div>' : ''}
      </div>
    </div>
    ${buildProfileCalendar(g)}`;
  // Cinematic open: show overlay then trigger transition
  overlay.style.display = 'flex';
  requestAnimationFrame(() => requestAnimationFrame(() => overlay.classList.add('active')));
  document.body.style.overflow = 'hidden';

  // Auto-rotate profile photos with crossfade
  clearInterval(window._profileRotate);
  window._profilePhotos = photos;
  window._profilePhotoIdx = 0;
  if (photos.length > 1) {
    window._profileRotate = setInterval(() => {
      const nextIdx = (window._profilePhotoIdx + 1) % photos.length;
      crossfadeProfilePhoto(nextIdx);
    }, 5000);
  }
}

function crossfadeProfilePhoto(idx) {
  const photos = window._profilePhotos || [];
  if (!photos[idx]) return;
  window._profilePhotoIdx = idx;
  const wrap = document.querySelector('.profile-main-wrap');
  const current = document.getElementById('profileMainImg');
  if (!wrap || !current) return;
  const next = document.createElement('img');
  next.src = photos[idx];
  next.alt = current.alt;
  next.id = 'profileMainImg';
  next.style.opacity = '0';
  next.style.transition = 'opacity .5s ease';
  next.style.position = 'absolute';
  next.style.inset = '0';
  next.style.width = '100%';
  next.style.aspectRatio = '3/4';
  next.style.objectFit = 'cover';
  wrap.style.position = 'relative';
  wrap.appendChild(next);
  requestAnimationFrame(() => {
    next.style.opacity = '1';
    current.style.opacity = '0';
  });
  setTimeout(() => {
    current.remove();
    next.style.position = '';
    next.style.inset = '';
  }, 550);
  document.querySelectorAll('.profile-thumbs img').forEach((t, i) => t.classList.toggle('active', i === idx));
  const counter = document.getElementById('photoCounter');
  if (counter) counter.textContent = (idx + 1) + ' / ' + photos.length;
}

// Arrow key navigation for profile photos
document.addEventListener('keydown', function(e) {
  if (!document.getElementById('profileOverlay')?.style.display ||
      document.getElementById('profileOverlay').style.display === 'none') return;
  if (!window._profilePhotos || window._profilePhotos.length <= 1) return;

  let newIdx = null;
  if (e.key === 'ArrowRight') {
    newIdx = (window._profilePhotoIdx + 1) % window._profilePhotos.length;
  } else if (e.key === 'ArrowLeft') {
    newIdx = (window._profilePhotoIdx - 1 + window._profilePhotos.length) % window._profilePhotos.length;
  }
  if (newIdx !== null) {
    crossfadeProfilePhoto(newIdx);
    // Reset auto-rotate timer
    clearInterval(window._profileRotate);
    window._profileRotate = setInterval(() => {
      const nextIdx = (window._profilePhotoIdx + 1) % window._profilePhotos.length;
      crossfadeProfilePhoto(nextIdx);
    }, 5000);
  }
});

function detailRow(label, value) {
  if (!value) return '';
  return `<div class="profile-detail-row"><span>${label}</span><span>${value}</span></div>`;
}

function selectProfilePhoto(idx) {
  crossfadeProfilePhoto(idx);
  // Reset auto-rotate timer
  clearInterval(window._profileRotate);
  if (window._profilePhotos && window._profilePhotos.length > 1) {
    window._profileRotate = setInterval(() => {
      const nextIdx = (window._profilePhotoIdx + 1) % window._profilePhotos.length;
      crossfadeProfilePhoto(nextIdx);
    }, 5000);
  }
}

function closeProfile() {
  clearInterval(window._profileRotate);
  const overlay = document.getElementById('profileOverlay');
  overlay.classList.remove('active');
  document.body.style.overflow = '';
  setTimeout(() => { if (!overlay.classList.contains('active')) overlay.style.display = 'none'; }, 500);
  if (window.location.pathname !== '/') history.pushState(null, '', '/');
  updateMeta('Brothel Search \u2013 Girls, Rosters & Profiles', 'Browse profiles, rosters and availability across Sydney\'s top brothels.', 'https://brothelsearch.com/og-preview.png', 'https://brothelsearch.com/', null);
}

// Close profile on Escape
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeProfile() });

// Browser back/forward navigation
window.addEventListener('popstate', () => {
  const path = window.location.pathname;
  if (path === '/' || path === '/index.html') {
    clearInterval(window._profileRotate);
    const overlay = document.getElementById('profileOverlay');
    overlay.classList.remove('active');
    document.body.style.overflow = '';
    setTimeout(() => { if (!overlay.classList.contains('active')) overlay.style.display = 'none'; }, 500);
    showMainSection();
    updateMeta('Brothel Search \u2013 Girls, Rosters & Profiles', 'Browse profiles, rosters and availability across Sydney\'s top brothels.', 'https://brothelsearch.com/og-preview.png', 'https://brothelsearch.com/', null);
  } else if (path.match(/^\/sydney\/([\w]+\/?){0,2}$/) && !findGirlByPath(path)) {
    closeProfile();
    handleLandingRoute(path);
  } else {
    const g = findGirlByPath(path);
    if (g) showProfile(g);
  }
});
document.getElementById('profileOverlay').addEventListener('click', e => {
  if (e.target === e.currentTarget) closeProfile();
});


// Back to top
const btt = document.getElementById('backToTop');
window.addEventListener('scroll', () => {
  btt.classList.toggle('visible', window.scrollY > 400);
});
btt.onclick = () => window.scrollTo({ top: 0, behavior: 'smooth' });

// More filters toggle
document.getElementById('moreFiltersToggle').onclick = function() {
  this.classList.toggle('open');
  document.getElementById('moreFiltersPanel').classList.toggle('open');
};

// Handle payment return
if (new URLSearchParams(window.location.search).get('payment') === 'success') {
  window.history.replaceState({}, '', window.location.pathname);
}

// Init - always load profiles for background preview, gate interactions behind auth
let profilesLoaded = false;
let lastVisibleTime = Date.now();
const STALE_THRESHOLD = 10 * 60 * 1000; // 10 minutes

loadProfiles().then(() => {
  profilesLoaded = true;
  lastVisibleTime = Date.now();
  // Handle URL path on load
  const path = window.location.pathname;
  if (path !== '/' && path !== '/index.html') {
    const g = findGirlByPath(path);
    if (g) { showProfile(g); }
    else if (path.startsWith('/sydney')) { handleLandingRoute(path); }
  }
});
checkAuth().then(() => {
  loadPreferences().then(() => {
    if (userPreferences) { computeMatchScores(); renderGrid(); }
  });
  // Handle hash routes on page load
  const hash = window.location.hash.replace('#', '');
  if (hash === 'subscribe') {
    document.getElementById('paywallOverlay').style.display = 'flex';
    document.body.style.overflow = 'hidden';
  } else if (hash.startsWith('profile/')) {
    handleRoute();
  }
});

// pageshow: skip re-init when restored from bfcache
window.addEventListener('pageshow', (e) => {
  if (e.persisted && profilesLoaded) {
    // Page restored from bfcache — data is already in memory, no reload needed
    lastVisibleTime = Date.now();
    return;
  }
});

// visibilitychange: pause animations when hidden, refresh stale data when visible
document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'visible') {
    document.documentElement.style.animationPlayState = '';
    if (profilesLoaded) {
      const hiddenFor = Date.now() - lastVisibleTime;
      if (hiddenFor > STALE_THRESHOLD) {
        console.log(`Tab hidden for ${Math.round(hiddenFor / 60000)}m — refreshing profiles`);
        loadProfiles().then(() => { lastVisibleTime = Date.now(); });
      } else {
        lastVisibleTime = Date.now();
      }
    }
  } else if (document.visibilityState === 'hidden') {
    lastVisibleTime = Date.now();
    document.documentElement.style.animationPlayState = 'paused';
  }
});

// Delayed paywall check — runs 3 seconds after page load, never blocks anything
setTimeout(async () => {
  try {
    const { data: { session } } = await sbClient.auth.getSession();
    if (!session) return;
    if (userRole === 'admin') return;
    // Show paywall if unsubscribed OR if URL has #subscribe
    const sub = await checkSubscription();
    if (!sub || sub.status !== 'active') showPaywall();
    else if (window.location.hash === '#subscribe') hidePaywall(); // subscribed user visiting #subscribe
  } catch(e) { console.error('Paywall check:', e); }
}, 2000);

// hashchange handled above in unified listener

// ── Landing Pages (City / Suburb / Venue) ──

const VENUE_DATA = {
  ginzaempire: { name: 'Ginza Empire', suburb: 'Surry Hills', suburbSlug: 'surryhills', url: 'https://479ginza.com.au/', address: '479 Elizabeth St, Surry Hills NSW 2010', lat: -33.88698124490204, lng: 151.20805761312394 },
  ginzaclub: { name: 'Ginza Club', suburb: 'Surry Hills', suburbSlug: 'surryhills', url: 'https://www.ginzaclub.com.au/', address: '10 Cleveland St, Surry Hills NSW 2010', lat: -33.88993022667204, lng: 151.20912609962915 },
  kyoto206: { name: 'Kyoto 206', suburb: 'Surry Hills', suburbSlug: 'surryhills', url: 'https://citybrothel.com.au/', address: '206 Commonwealth St, Surry Hills NSW 2010', lat: -33.88317474375967, lng: 151.2108589818634 },
  sakura57: { name: 'Sakura 57', suburb: 'Surry Hills', suburbSlug: 'surryhills', url: 'https://www.surryhillsbrothel.com.au/', address: '2/57 Reservoir St, Surry Hills NSW 2010', lat: -33.8812693750108, lng: 151.21102289535847 },
  top127: { name: 'Top 127', suburb: 'Chippendale', suburbSlug: 'chippendale', url: 'https://127city.com/', address: '127 Regent St, Chippendale NSW 2008', lat: -33.887895846811354, lng: 151.20126815545416 },
  fantasyclub35: { name: 'Fantasy Club 35', suburb: 'Annandale', suburbSlug: 'annandale', url: 'https://fantasyclub35.com.au/', address: '33/35 Parramatta Rd, Annandale NSW 2038', lat: -33.88719005098213, lng: 151.1706113116501 },
  '429city': { name: '429 City', suburb: 'Haymarket', suburbSlug: 'haymarket', url: 'https://www.429city.com/', address: '429A Pitt St, Haymarket NSW 2000', lat: -33.87874734224782, lng: 151.20694241127885 },
};

function getSuburbs() {
  const map = {};
  for (const [id, v] of Object.entries(VENUE_DATA)) {
    if (!map[v.suburbSlug]) map[v.suburbSlug] = { name: v.suburb, slug: v.suburbSlug, venues: [] };
    map[v.suburbSlug].venues.push({ id, ...v });
  }
  return Object.values(map);
}

function venueGirlCount(venueId) {
  return allGirls.filter(g => g.venue === venueId).length;
}

function venuePriceRange(venueId) {
  const girls = allGirls.filter(g => g.venue === venueId && g.val1);
  if (!girls.length) return '';
  const prices = girls.map(g => parseInt(g.val1)).filter(p => p > 0);
  if (!prices.length) return '';
  return '$' + Math.min(...prices) + ' – $' + Math.max(...prices);
}

function renderCityPage() {
  const suburbs = getSuburbs();
  const totalVenues = Object.keys(VENUE_DATA).length;
  const totalGirls = allGirls.length;

  updateMeta(
    'Brothels in Sydney \u2013 Browse All Venues | Brothel Search',
    'Browse ' + totalVenues + ' brothels across Sydney with ' + totalGirls + '+ girls. Compare venues in Surry Hills, Chippendale, Haymarket and Annandale.',
    'https://brothelsearch.com/og-preview.png',
    'https://brothelsearch.com/sydney/',
    { '@context': 'https://schema.org', '@type': 'ItemList', name: 'Brothels in Sydney', numberOfItems: totalVenues, itemListElement: suburbs.flatMap(s => s.venues.map((v, i) => ({ '@type': 'ListItem', position: i + 1, item: { '@type': 'LocalBusiness', name: v.name, address: v.address } }))) }
  );

  let html = '<div class="landing-map-container"><div id="venueMap"></div></div>';
  html += '<div class="landing-page">';
  html += '<div class="landing-breadcrumb"><a href="/">Home</a> / <span>Sydney</span></div>';
  html += '<h1 class="landing-title">Brothels in Sydney</h1>';
  html += '<p class="landing-desc">' + totalVenues + ' venues across ' + suburbs.length + ' suburbs with ' + totalGirls + '+ girls available.</p>';
  html += '<div class="landing-grid">';

  for (const suburb of suburbs) {
    const girlCount = suburb.venues.reduce((sum, v) => sum + venueGirlCount(v.id), 0);
    html += '<a href="/sydney/' + suburb.slug + '/" class="landing-card" onclick="event.preventDefault();navigateToLanding(\'/sydney/' + suburb.slug + '/\')">';
    html += '<h2 class="landing-card-title">' + suburb.name + '</h2>';
    html += '<div class="landing-card-stat">' + suburb.venues.length + ' venue' + (suburb.venues.length !== 1 ? 's' : '') + '</div>';
    html += '<div class="landing-card-stat">' + girlCount + ' girls</div>';
    html += '</a>';
  }

  html += '</div></div>';
  return html;
}

function initVenueMap() {
  const mapEl = document.getElementById('venueMap');
  if (!mapEl || typeof L === 'undefined') return;
  if (window._venueMap) { window._venueMap.remove(); window._venueMap = null; }

  const filterSuburb = mapEl.dataset.suburb || null;

  const map = L.map('venueMap', { zoomControl: true, scrollWheelZoom: true }).setView([-33.883, 151.207], 14);
  window._venueMap = map;

  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; <a href="https://carto.com/">CARTO</a>',
    maxZoom: 19,
  }).addTo(map);

  const clusters = L.markerClusterGroup({
    maxClusterRadius: 60,
    iconCreateFunction: function(cluster) {
      return L.divIcon({ html: '<div class="venue-cluster">' + cluster.getChildCount() + '</div>', className: 'venue-cluster-icon', iconSize: [44, 44] });
    },
    spiderfyOnMaxZoom: true,
    showCoverageOnHover: false,
  });

  const venues = Object.entries(VENUE_DATA).filter(([id, v]) => !filterSuburb || v.suburbSlug === filterSuburb);

  for (const [id, v] of venues) {
    const count = venueGirlCount(id);
    const marker = L.marker([v.lat, v.lng], {
      icon: L.divIcon({ html: '<div class="venue-marker">' + v.name + '</div>', className: 'venue-marker-icon', iconSize: null, iconAnchor: [60, 40] }),
    });
    marker.on('click', function() { navigateToLanding('/sydney/' + v.suburbSlug + '/' + id + '/'); });
    marker.bindTooltip('<strong>' + v.name + '</strong><br>' + v.address + '<br>' + count + ' girls', { className: 'venue-tooltip', direction: 'top', offset: [0, -20] });
    clusters.addLayer(marker);
  }

  map.addLayer(clusters);
  const bounds = L.latLngBounds(venues.map(([id, v]) => [v.lat, v.lng]));
  map.fitBounds(bounds.pad(filterSuburb ? 0.5 : 0.3));
}

function renderSuburbPage(suburbSlug) {
  const suburbs = getSuburbs();
  const suburb = suburbs.find(s => s.slug === suburbSlug);
  if (!suburb) return null;

  const girlCount = suburb.venues.reduce((sum, v) => sum + venueGirlCount(v.id), 0);

  updateMeta(
    'Brothels in ' + suburb.name + ', Sydney | Brothel Search',
    'Browse ' + suburb.venues.length + ' brothels in ' + suburb.name + ', Sydney. ' + girlCount + ' girls available. Compare venues, pricing and profiles.',
    'https://brothelsearch.com/og-preview.png',
    'https://brothelsearch.com/sydney/' + suburbSlug + '/',
    { '@context': 'https://schema.org', '@type': 'ItemList', name: 'Brothels in ' + suburb.name + ', Sydney', numberOfItems: suburb.venues.length }
  );

  let html = '<div class="landing-map-container"><div id="venueMap" data-suburb="' + suburbSlug + '"></div></div>';
  html += '<div class="landing-page">';
  html += '<div class="landing-breadcrumb"><a href="/">Home</a> / <a href="/sydney/" onclick="event.preventDefault();navigateToLanding(\'/sydney/\')">Sydney</a> / <span>' + suburb.name + '</span></div>';
  html += '<h1 class="landing-title">Brothels in ' + suburb.name + '</h1>';
  html += '<p class="landing-desc">' + suburb.venues.length + ' venues with ' + girlCount + ' girls in ' + suburb.name + ', Sydney.</p>';
  html += '<div class="landing-grid">';

  for (const v of suburb.venues) {
    const count = venueGirlCount(v.id);
    const priceRange = venuePriceRange(v.id);
    html += '<a href="/sydney/' + suburbSlug + '/' + v.id + '/" class="landing-card" onclick="event.preventDefault();navigateToLanding(\'/sydney/' + suburbSlug + '/' + v.id + '/\')">';
    html += '<h2 class="landing-card-title">' + v.name + '</h2>';
    html += '<div class="landing-card-address">' + v.address + '</div>';
    html += '<div class="landing-card-stat">' + count + ' girls</div>';
    if (priceRange) html += '<div class="landing-card-stat">From ' + priceRange + ' (30min)</div>';
    html += '<div class="landing-card-link">View profiles \u2192</div>';
    html += '</a>';
  }

  html += '</div></div>';
  return html;
}

function renderVenuePage(suburbSlug, venueId) {
  const v = VENUE_DATA[venueId];
  if (!v || v.suburbSlug !== suburbSlug) return null;

  const girls = allGirls.filter(g => g.venue === venueId);
  const priceRange = venuePriceRange(venueId);

  updateMeta(
    v.name + ' \u2013 ' + v.suburb + ', Sydney | Brothel Search',
    v.name + ' at ' + v.address + '. ' + girls.length + ' girls available.' + (priceRange ? ' Prices from ' + priceRange + '.' : '') + ' Browse profiles, photos and rosters.',
    'https://brothelsearch.com/og-preview.png',
    'https://brothelsearch.com/sydney/' + suburbSlug + '/' + venueId + '/',
    { '@context': 'https://schema.org', '@type': 'LocalBusiness', name: v.name, url: v.url, address: { '@type': 'PostalAddress', streetAddress: v.address.split(',')[0], addressLocality: v.suburb, addressRegion: 'NSW', addressCountry: 'AU' } }
  );

  let html = '<div class="landing-page">';
  html += '<div class="landing-breadcrumb"><a href="/">Home</a> / <a href="/sydney/" onclick="event.preventDefault();navigateToLanding(\'/sydney/\')">Sydney</a> / <a href="/sydney/' + suburbSlug + '/" onclick="event.preventDefault();navigateToLanding(\'/sydney/' + suburbSlug + '/\')">' + v.suburb + '</a> / <span>' + v.name + '</span></div>';
  html += '<h1 class="landing-title">' + v.name + '</h1>';
  html += '<div class="landing-venue-meta">';
  html += '<div class="landing-card-address">' + v.address + '</div>';
  html += '<a href="' + v.url + '" target="_blank" rel="noopener" class="landing-venue-link">' + v.url.replace(/^https?:\/\//, '').replace(/\/$/, '') + '</a>';
  html += '</div>';
  html += '<p class="landing-desc">' + girls.length + ' girls available.' + (priceRange ? ' Prices from ' + priceRange + ' for 30 minutes.' : '') + '</p>';
  html += '<hr class="gold-divider">';
  html += '<div class="girls-grid" style="margin-top:16px">';

  for (const g of girls) {
    const countries = Array.isArray(g.country) ? g.country.join(', ') : (g.country || '');
    const img = g.photos && g.photos.length
      ? '<img class="card-thumb" src="' + imgProxy(g.photos[0]) + '" alt="' + (g.name || '').replace(/"/g, '&quot;') + ' \u2013 ' + v.name + ' ' + v.suburb + ', Sydney" loading="lazy">'
      : '<div class="silhouette"></div>';
    html += '<div class="girl-card card-settled" onclick="showProfile(allGirls.find(gg=>gg.venue===\'' + venueId + '\'&&gg.name===\'' + (g.name || '').replace(/'/g, "\\'") + '\'))">';
    html += '<div class="card-img">' + img + '</div>';
    html += '<div class="card-info">';
    html += '<div class="card-name">' + (g.name || '') + '</div>';
    html += '<div class="card-country">' + countries + '</div>';
    html += '</div></div>';
  }

  html += '</div></div>';
  return html;
}

function navigateToLanding(path) {
  const dd = document.getElementById('navBrothelsDropdown');
  if (dd) dd.classList.remove('open');
  history.pushState({ landing: true }, '', path);
  handleLandingRoute(path);
}

function handleLandingRoute(path) {
  const parts = path.replace(/^\//, '').replace(/\/$/, '').split('/');
  const landingEl = document.getElementById('landingPage');
  const mainSection = document.querySelector('section.section');

  let html = null;

  if (parts.length === 1 && parts[0] === 'sydney') {
    html = renderCityPage();
  } else if (parts.length === 2 && parts[0] === 'sydney') {
    html = renderSuburbPage(parts[1]);
  } else if (parts.length === 3 && parts[0] === 'sydney') {
    html = renderVenuePage(parts[1], parts[2]);
  }

  if (html) {
    landingEl.innerHTML = html;
    landingEl.style.display = '';
    mainSection.style.display = 'none';
    document.querySelectorAll('.nav-link').forEach(l => l.classList.toggle('active', l.id === 'navBrothels'));
    window.scrollTo({ top: 0 });
    // Init map if on city page
    if (document.getElementById('venueMap')) setTimeout(initVenueMap, 50);
    return true;
  }
  return false;
}

function showMainSection() {
  const landingEl = document.getElementById('landingPage');
  const mainSection = document.querySelector('section.section');
  landingEl.style.display = 'none';
  landingEl.innerHTML = '';
  mainSection.style.display = '';
  document.querySelectorAll('.nav-link').forEach(l => l.classList.toggle('active', l.id === 'navProfiles'));
}

// Nav link click handlers
document.getElementById('navProfiles').addEventListener('click', function(e) {
  e.preventDefault();
  history.pushState(null, '', '/');
  showMainSection();
  updateMeta('Brothel Search \u2013 Girls, Rosters & Profiles', 'Browse profiles, rosters and availability across Sydney\'s top brothels.', 'https://brothelsearch.com/og-preview.png', 'https://brothelsearch.com/', null);
});

document.getElementById('navBrothels').addEventListener('click', function(e) {
  e.preventDefault();
  e.stopPropagation();
  document.getElementById('navBrothelsDropdown').classList.toggle('open');
});

document.addEventListener('click', function(e) {
  const dd = document.getElementById('navBrothelsDropdown');
  if (dd && !dd.contains(e.target)) dd.classList.remove('open');
});

// Background particles
(function(){
  const tpl = document.getElementById('bgMiniLogo').content;
  const container = document.getElementById('bgParticles');
  const positions = [
    {l:5,t:10,d:0,dur:14},{l:18,t:70,d:1.5,dur:18},{l:10,t:25,d:3,dur:12},
    {l:80,t:75,d:0.5,dur:16},{l:75,t:8,d:2,dur:20},{l:90,t:20,d:4,dur:15},
    {l:12,t:85,d:5.5,dur:17},{l:93,t:55,d:3.5,dur:13},{l:25,t:92,d:6,dur:19},
    {l:85,t:90,d:7,dur:11},{l:3,t:50,d:8,dur:22},{l:95,t:40,d:9,dur:16},
    {l:20,t:5,d:10,dur:21},{l:88,t:80,d:11,dur:14},{l:8,t:65,d:12,dur:25}
  ];
  positions.forEach(function(p){
    var span = document.createElement('span');
    span.style.left = p.l + '%';
    span.style.top = p.t + '%';
    span.appendChild(tpl.cloneNode(true));
    container.appendChild(span);
  });
})();