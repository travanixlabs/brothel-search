// SPA redirect from 404.html — restore original path
(function() {
  const redirect = sessionStorage.getItem('spa-redirect');
  if (redirect) {
    sessionStorage.removeItem('spa-redirect');
    history.replaceState(null, '', redirect);
  }
})();

// Burn Link — 4-hour expiring share link. If expired, strip the param and reload URL.
const BURN_WINDOW_MS = 4 * 60 * 60 * 1000;
window._burnLinkActive = false;
(function() {
  const params = new URLSearchParams(window.location.search);
  const burn = params.get('burn');
  if (!burn) return;
  const ts = parseInt(burn, 10);
  if (!isNaN(ts) && Date.now() - ts < BURN_WINDOW_MS) {
    window._burnLinkActive = true;
  } else {
    // Expired — strip the param and update URL
    params.delete('burn');
    const qs = params.toString();
    history.replaceState(null, '', window.location.pathname + (qs ? '?' + qs : '') + window.location.hash);
  }
})();

function generateBurnLink(btn) {
  const url = new URL(window.location.href);
  url.searchParams.set('burn', Date.now().toString());
  navigator.clipboard.writeText(url.toString()).then(() => {
    const original = btn.textContent;
    btn.textContent = 'Copied! (4hr link)';
    setTimeout(() => { btn.textContent = original; }, 2000);
  });
}

// Capture referral code from URL param
(function() {
  const params = new URLSearchParams(window.location.search);
  const ref = params.get('ref');
  if (ref) {
    sessionStorage.setItem('pending-referral', ref.toUpperCase());
    history.replaceState(null, '', window.location.pathname);
  }
})();

function applyPendingReferral() {
  const pendingRef = sessionStorage.getItem('pending-referral');
  if (!pendingRef) return;
  // Switch to signup mode if in login mode
  const nameField = document.getElementById('authName');
  if (nameField && nameField.style.display === 'none') toggleAuthMode();
  // Fill the referral code and trigger validation
  const refInput = document.getElementById('authReferralCode');
  if (refInput) {
    refInput.value = pendingRef;
    refInput.dispatchEvent(new Event('input'));
  }
}

// Country flag emoji mapping
const COUNTRY_FLAGS = {
  'Japanese': '\ud83c\uddef\ud83c\uddf5', 'Korean': '\ud83c\uddf0\ud83c\uddf7', 'Chinese': '\ud83c\udde8\ud83c\uddf3',
  'Thailandese': '\ud83c\uddf9\ud83c\udded', 'Vietnamese': '\ud83c\uddfb\ud83c\uddf3', 'Taiwanese': '\ud83c\uddf9\ud83c\uddfc',
  'Sri Lankan': '\ud83c\uddf1\ud83c\uddf0',
  'Filipino': '\ud83c\uddf5\ud83c\udded', 'Malaysian': '\ud83c\uddf2\ud83c\uddfe', 'Indonesian': '\ud83c\uddee\ud83c\udde9',
  'Indian': '\ud83c\uddee\ud83c\uddf3', 'Singaporean': '\ud83c\uddf8\ud83c\uddec', 'Hong Kong': '\ud83c\udded\ud83c\uddf0',
  'Australian': '\ud83c\udde6\ud83c\uddfa', 'European': '\ud83c\uddea\ud83c\uddfa', 'Brazilian': '\ud83c\udde7\ud83c\uddf7',
  'Colombian': '\ud83c\udde8\ud83c\uddf4', 'Russian': '\ud83c\uddf7\ud83c\uddfa', 'African': '\ud83c\udf0d',
  'Mongolian': '\ud83c\uddf2\ud83c\uddf3', 'Cambodian': '\ud83c\uddf0\ud83c\udded', 'Nepalese': '\ud83c\uddf3\ud83c\uddf5',
  'Myanmar': '\ud83c\uddf2\ud83c\uddf2', 'Laos': '\ud83c\uddf1\ud83c\udde6',
};
function countryFlag(country) { return COUNTRY_FLAGS[country] || ''; }
function countriesWithFlags(countries) {
  const cs = Array.isArray(countries) ? countries : [countries || ''];
  return cs.join(', ');
}

// Supabase Auth
const SUPABASE_URL = 'https://blhwekuidksxiaickeck.supabase.co';
const SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJsaHdla3VpZGtzeGlhaWNrZWNrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQwMzMxODEsImV4cCI6MjA4OTYwOTE4MX0.dx8_2UHRJqCJ5aOf2O9ogSYDHY3hUKyGPRJjJiT4ghE';
const sbClient = window.supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

let authMode = 'signin'; // 'signin' or 'signup'
let userRole = 'member'; // 'admin' or 'member'
let userFavorites = []; // array of oldUrl strings
let userHidden = []; // array of oldUrl strings
let userFilterPresets = []; // array of { id, name, filters, is_active }
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
const STRIPE_PK = 'pk_live_51TDepzQjtcp0NkpMMSRDq1MrNlo6HvR72TSbZjHKtjga9xHFzsKFvAtrPyrHioNispZd6DCNO8kvUePXlffB4b4s006XsKxD0X';

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
  // Redirect to home first if not already there
  const path = window.location.pathname;
  if (path !== '/' && path !== '/index.html') {
    history.replaceState(null, '', '/');
    handleLandingRoute('/');
  }
  document.getElementById('paywallOverlay').style.display = 'flex';
  document.body.style.overflow = 'hidden';
}

function hidePaywall() {
  document.getElementById('paywallOverlay').style.display = 'none';
  document.body.style.overflow = '';
}

async function selectPlan(plan) {
  const { data: { session } } = await sbClient.auth.getSession();
  if (!session) return;
  const btn = document.querySelector(`[data-plan="${plan}"]`);
  if (btn) btn.textContent = plan === 'trial' ? 'Activating...' : 'Redirecting...';
  try {
    // Free trial — create subscription directly without Stripe
    if (plan === 'trial') {
      const res = await fetch(`${WORKER_URL}/activate-trial`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + session.access_token },
        body: JSON.stringify({ userId: session.user.id }),
      });
      const data = await res.json();
      if (data.error) { alert(data.error); if (btn) btn.textContent = 'Select'; return; }
      isSubscribed = true;
      hidePaywall();
      return;
    }
    // Paid plans — Stripe checkout
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
  } catch (e) { alert('Error: ' + e.message); if (btn) btn.textContent = 'Select'; }
}

async function checkAuth() {
  const { data: { session } } = await sbClient.auth.getSession();
  if (session) {
    document.getElementById('authOverlay').style.display = 'none'; document.body.style.overflow = '';
    document.getElementById('userMenu').style.display = ''; document.getElementById('loginBtn').style.display = 'none';
    document.getElementById('notifBell').style.display = 'flex';
    loadNotifications();
    await fetchUserRole();
    await loadFavorites();
    return true;
  }
  // Don't block home or roadmap pages
  const path = window.location.pathname;
  if (path === '/' || path === '/index.html' || path === '/roadmap') {
    document.getElementById('userMenu').style.display = 'none';
    return false;
  }
  // Active burn link — bypass auth overlay so recipient can view profile
  if (window._burnLinkActive) {
    document.getElementById('userMenu').style.display = 'none';
    return false;
  }
  document.getElementById('authOverlay').style.display = 'flex'; document.body.style.overflow = 'hidden';
  document.getElementById('userMenu').style.display = 'none';
  applyPendingReferral();
  return false;
}

function requireLogin() {
  return true; // Access open to all users
}
function requireLoginReal() {
  if (document.getElementById('userMenu').style.display !== 'none') return true;
  document.getElementById('authOverlay').style.display = 'flex';
  document.body.style.overflow = 'hidden';
  applyPendingReferral();
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
    const refCode = document.getElementById('authReferralCode').value.trim().toUpperCase();
    result = await sbClient.auth.signUp({ email, password, options: { data: { display_name: profileName, referral_code: refCode || undefined } } });
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
  document.getElementById('userMenu').style.display = ''; document.getElementById('loginBtn').style.display = 'none';
  document.getElementById('notifBell').style.display = 'flex';
  loadNotifications();
  await fetchUserRole();
  await loadFavorites();

  // Auto-activate trial on signup
  if (authMode === 'signup' && result.data.session) {
    try {
      await fetch(`${WORKER_URL}/activate-trial`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + result.data.session.access_token },
        body: JSON.stringify({ userId: result.data.session.user.id }),
      });
      isSubscribed = true;
    } catch (e) { console.error('Auto-trial failed:', e); }
  }

  loadProfiles();
  // Navigate to home after login
  history.replaceState(null, '', '/');
  handleLandingRoute('/');

  // Prompt to set preferences if none exist
  await loadPreferences();
  if (!hasPreferencesSet(userPreferences)) {
    showPrefsPrompt();
  } else {
    computeMatchScores(); renderGrid();
  }
}

function toggleAuthMode() {
  authMode = authMode === 'signin' ? 'signup' : 'signin';
  const isSignup = authMode === 'signup';
  document.getElementById('authBtn').textContent = isSignup ? 'Create Account' : 'Sign In';
  document.getElementById('authSubtitle').textContent = isSignup ? 'Create a new account' : 'Sign in to continue';
  document.getElementById('authToggle').textContent = isSignup ? 'Already have an account? Sign in' : "Don't have an account? Sign up for free to access all features.";
  document.getElementById('authName').style.display = isSignup ? '' : 'none';
  document.getElementById('authReferralHint').style.display = isSignup ? '' : 'none';
  document.getElementById('authReferralCode').style.display = isSignup ? '' : 'none';
  if (isSignup) {
    const pendingRef = sessionStorage.getItem('pending-referral');
    if (pendingRef) document.getElementById('authReferralCode').value = pendingRef;
  }
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
  // Close all overlays
  ['settingsOverlay', 'preferencesOverlay', 'favoritesOverlay'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.classList.remove('open');
  });
  ['prefsPromptOverlay', 'inactivityOverlay'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.style.display = 'none';
  });
  document.getElementById('authOverlay').style.display = 'none'; document.body.style.overflow = '';
  document.getElementById('userMenu').style.display = 'none';
  document.getElementById('loginBtn').style.display = '';
  document.getElementById('notifBell').style.display = 'none';
  window.location.hash = '';
  history.replaceState(null, '', '/');
  handleLandingRoute('/');
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
  // Load referral info
  getOrCreateReferralCode().then(code => {
    const refEl = document.getElementById('settingsReferral');
    if (refEl && code) {
      getReferralStats().then(stats => {
        const refLink = 'https://brothelsearch.com/?ref=' + code;
        refEl.innerHTML = '<div class="settings-subtitle" style="margin-top:24px">Referral Program</div>' +
          '<div class="referral-code-label">Your referral code</div>' +
          '<div class="referral-code-box" style="margin-bottom:12px"><span>' + code + '</span><button onclick="navigator.clipboard.writeText(\'' + code + '\').then(()=>{this.textContent=\'Copied!\';setTimeout(()=>this.textContent=\'Copy\',1500)})">Copy</button></div>' +
          '<div class="referral-code-label">Or share this link</div>' +
          '<div class="referral-code-box" style="margin-bottom:12px"><span style="font-size:11px;letter-spacing:1px">' + refLink + '</span><button onclick="navigator.clipboard.writeText(\'' + refLink + '\').then(()=>{this.textContent=\'Copied!\';setTimeout(()=>this.textContent=\'Copy\',1500)})">Copy</button></div>' +
          '<div style="font-size:13px;color:var(--text-dim)">' + stats.completed + ' successful referral' + (stats.completed !== 1 ? 's' : '') + ' \u00b7 ' + stats.daysEarned + ' bonus days earned</div>';
      });
    }
  });
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

function hasPreferencesSet(p) {
  if (!p) return false;
  // Preferences row exists but check if anything meaningful was chosen
  const arrays = [p.countries, p.services, p.experience, p.language, p.av];
  if (arrays.some(a => a && a.length > 0)) return true;
  if (p.last_roster_days || p.date_started_days) return true;
  return false;
}

function showPrefsPrompt() {
  const overlay = document.getElementById('prefsPromptOverlay');
  overlay.style.display = 'flex';
  document.body.style.overflow = 'hidden';
}

document.getElementById('prefsPromptOpenBtn').addEventListener('click', () => {
  document.getElementById('prefsPromptOverlay').style.display = 'none';
  document.body.style.overflow = '';
  navigateTo('profile/preferences');
});
document.getElementById('prefsPromptDismissBtn').addEventListener('click', () => {
  document.getElementById('prefsPromptOverlay').style.display = 'none';
  document.body.style.overflow = '';
});

function showPreferences() {
  document.getElementById('userMenuDropdown').classList.remove('open');
  document.getElementById('prefMsg').textContent = '';
  populatePrefCheckboxes();
  initPrefSliders();
  clearPrefsForm();
  loadPreferences().then(() => colorPrefLabels());
  document.getElementById('preferencesOverlay').classList.add('open');
  document.body.style.overflow = 'hidden';
  // Live-update label colors on interaction
  setTimeout(() => {
    document.querySelectorAll('#preferencesOverlay input[type=range]').forEach(inp => {
      inp.addEventListener('input', colorPrefLabels);
    });
    document.querySelectorAll('#preferencesOverlay input[type=checkbox]').forEach(cb => {
      cb.addEventListener('change', colorPrefLabels);
    });
    document.querySelectorAll('#preferencesOverlay input[type=number]').forEach(inp => {
      inp.addEventListener('input', colorPrefLabels);
    });
    document.querySelectorAll('#preferencesOverlay .pref-select').forEach(sel => {
      sel.addEventListener('change', colorPrefLabels);
    });
  }, 100);
}

function closePreferences() {
  document.getElementById('preferencesOverlay').classList.remove('open');
  document.body.style.overflow = '';
  if (window.location.hash.includes('profile/')) history.replaceState(null, '', window.location.pathname);
}

// Favorites & Hidden
async function loadFavorites() {
  const { data } = await sbClient.from('user_favorites').select('old_url');
  userFavorites = data ? data.map(r => r.old_url) : [];
  const { data: hData } = await sbClient.from('user_hidden').select('old_url');
  userHidden = hData ? hData.map(r => r.old_url) : [];
}

async function toggleFavorite(oldUrl, e) {
  if (e) e.stopPropagation();
  if (!oldUrl) return;
  const { data: { user } } = await sbClient.auth.getUser();
  if (!user) return;

  const idx = userFavorites.indexOf(oldUrl);
  if (idx > -1) {
    userFavorites.splice(idx, 1);
    await sbClient.from('user_favorites').delete().eq('old_url', oldUrl);
  } else {
    if (userFavorites.length >= getMaxFavorites()) {
      alert('Maximum ' + getMaxFavorites() + ' favourites. Remove one first.');
      return;
    }
    // Remove from hidden if it was hidden
    const hidIdx = userHidden.indexOf(oldUrl);
    if (hidIdx > -1) { userHidden.splice(hidIdx, 1); await sbClient.from('user_hidden').delete().eq('old_url', oldUrl); }
    userFavorites.push(oldUrl);
    await sbClient.from('user_favorites').insert({ user_id: user.id, old_url: oldUrl });
  }
  updateHeartStates(oldUrl);
  renderGrid();
}

async function toggleHidden(oldUrl, e) {
  if (e) e.stopPropagation();
  if (!oldUrl) return;
  const { data: { user } } = await sbClient.auth.getUser();
  if (!user) return;

  const idx = userHidden.indexOf(oldUrl);
  if (idx > -1) {
    userHidden.splice(idx, 1);
    await sbClient.from('user_hidden').delete().eq('old_url', oldUrl);
  } else {
    // Remove from favourites if it was favourited
    const favIdx = userFavorites.indexOf(oldUrl);
    if (favIdx > -1) { userFavorites.splice(favIdx, 1); await sbClient.from('user_favorites').delete().eq('old_url', oldUrl); }
    userHidden.push(oldUrl);
    await sbClient.from('user_hidden').insert({ user_id: user.id, old_url: oldUrl });
  }
  updateHeartStates(oldUrl);
  renderGrid();
}

function updateHeartStates(oldUrl) {
  // Update card icons
  document.querySelectorAll('.fav-heart[data-url="' + oldUrl.replace(/"/g, '\\"') + '"]').forEach(el => {
    el.classList.toggle('active', userFavorites.includes(oldUrl));
  });
  document.querySelectorAll('.hide-btn[data-url="' + oldUrl.replace(/"/g, '\\"') + '"]').forEach(el => {
    el.classList.toggle('active', userHidden.includes(oldUrl));
  });
  // Update profile detail
  const detailFav = document.getElementById('profileFavHeart');
  if (detailFav) detailFav.classList.toggle('active', userFavorites.includes(oldUrl));
  const detailHide = document.getElementById('profileHideBtn');
  if (detailHide) detailHide.classList.toggle('active', userHidden.includes(oldUrl));
  const panel = document.getElementById('profilePanel');
  if (panel) panel.classList.toggle('favorited', userFavorites.includes(oldUrl));
}

function isFavorite(g) {
  return g.oldUrl && userFavorites.includes(g.oldUrl);
}

function isHidden(g) {
  return g.oldUrl && userHidden.includes(g.oldUrl);
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
      if (dateStr === todayStr && validSlot(cal[dateStr])) {
        const slot = cal[dateStr];
        const startMins = slotMins(slot.start);
        const endMins = slotMins(slot.end);
        let nowMins = now.getHours() * 60 + now.getMinutes();
        if (now.getHours() < 6 && rosterNow.getDate() !== now.getDate()) nowMins += 24 * 60;
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
    const sh = validSlot(slot) ? slotMins(slot.start) : 0;
    const eh = validSlot(slot) ? slotMins(slot.end) : 0;
    let startOffset = sh - TIMELINE_START * 60; if (startOffset < 0) startOffset += 24 * 60;
    let endOffset = eh - TIMELINE_START * 60; if (endOffset < 0) endOffset += 24 * 60;
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
    const timeStr = validSlot(slot) ? fmt24to12(slot.start) + ' - ' + fmt24to12(slot.end) : 'Rostered';
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
  if (event === 'SIGNED_OUT') {
    userRole = 'member';
    document.body.classList.remove('is-admin');
    hidePaywall();
    ['settingsOverlay', 'preferencesOverlay', 'favoritesOverlay'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.classList.remove('open');
    });
    ['prefsPromptOverlay', 'inactivityOverlay'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.style.display = 'none';
    });
    document.getElementById('authOverlay').style.display = 'none';
    document.body.style.overflow = '';
    const um = document.getElementById('userMenu'); if (um) um.style.display = 'none';
    const lb = document.getElementById('loginBtn'); if (lb) lb.style.display = '';
    const nb = document.getElementById('notifBell'); if (nb) nb.style.display = 'none';
    return;
  }
  if (event === 'PASSWORD_RECOVERY') {
    document.getElementById('authOverlay').style.display = 'none'; document.body.style.overflow = '';
    document.getElementById('userMenu').style.display = ''; document.getElementById('loginBtn').style.display = 'none';
    document.getElementById('notifBell').style.display = 'flex';
    loadNotifications();
    fetchUserRole();
    setTimeout(() => {
      navigateTo('profile/settings');
    }, 500);
    return;
  }
  if (session) {
    document.getElementById('authOverlay').style.display = 'none'; document.body.style.overflow = '';
    document.getElementById('userMenu').style.display = ''; document.getElementById('loginBtn').style.display = 'none';
    document.getElementById('notifBell').style.display = 'flex';
    // Skip redundant work on token refresh or session restore — only run full init on actual sign-in
    if (event === 'TOKEN_REFRESHED' || event === 'INITIAL_SESSION') return;
    loadNotifications();
    // Navigate to home only on actual login from auth overlay, not session restore
    const authOverlay = document.getElementById('authOverlay');
    if (event === 'SIGNED_IN' && authOverlay && authOverlay.style.display === 'flex') {
      history.replaceState(null, '', '/');
      handleLandingRoute('/');
    }
    fetchUserRole().then(() => {
      loadPreferences().then(() => {
        if (hasPreferencesSet(userPreferences)) {
          computeMatchScores(); renderGrid();
          const p = window.location.pathname;
          if (p === '/' || p === '/index.html') handleLandingRoute('/');
        } else {
          showPrefsPrompt();
        }
      });
      loadFilterPresets().then(() => {
        const active = userFilterPresets.find(p => p.is_active);
        if (active) applyFilterState(active.filters);
      });
      // Subscription check (no paywall enforcement)
      setTimeout(async () => {
        if (userRole === 'admin') { isSubscribed = true; return; }
        const sub = await checkSubscription();
        isSubscribed = sub && sub.status === 'active';
      }, 2000);
    });
  }
});

// Enter key to submit
document.getElementById('userMenuBtn').addEventListener('click', toggleUserMenu);

// Notification bell toggle
document.getElementById('notifBellBtn').addEventListener('click', function(e) {
  e.stopPropagation();
  document.getElementById('notifBell').classList.toggle('open');
  document.getElementById('userMenuDropdown').classList.remove('open');
});

document.getElementById('notifMarkAllRead').addEventListener('click', function(e) {
  e.stopPropagation();
  markAllNotificationsRead();
});

document.getElementById('settingsBack').addEventListener('click', closeSettings);
document.getElementById('settingsOverlay').addEventListener('click', e => { if (e.target === e.currentTarget) closeSettings(); });
document.getElementById('prefBack').addEventListener('click', closePreferences);
document.getElementById('preferencesOverlay').addEventListener('click', e => { if (e.target === e.currentTarget) closePreferences(); });
document.getElementById('favBack').addEventListener('click', closeFavorites);
document.getElementById('favoritesOverlay').addEventListener('click', e => { if (e.target === e.currentTarget) closeFavorites(); });
document.getElementById('presetsBack').addEventListener('click', closePresetsOverlay);
document.getElementById('presetsOverlay').addEventListener('click', e => { if (e.target === e.currentTarget) closePresetsOverlay(); });
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
document.addEventListener('click', e => {
  if (!e.target.closest('.user-menu')) document.getElementById('userMenuDropdown').classList.remove('open');
  const bell = document.getElementById('notifBell');
  if (bell && !bell.contains(e.target)) bell.classList.remove('open');
});
document.getElementById('authBtn').addEventListener('click', handleAuth);
document.getElementById('authToggle').addEventListener('click', toggleAuthMode);
document.getElementById('authPassword').addEventListener('input', () => { checkPasswordReqs(); checkPasswordMatch(); });
document.getElementById('authPasswordConfirm').addEventListener('input', checkPasswordMatch);
document.getElementById('authName').addEventListener('keydown', e => { if (e.key === 'Enter') document.getElementById('authEmail').focus(); });
document.getElementById('authEmail').addEventListener('keydown', e => { if (e.key === 'Enter') { if (authMode === 'signup') document.getElementById('authPassword').focus(); else document.getElementById('authPassword').focus(); } });
document.getElementById('authPassword').addEventListener('keydown', e => { if (e.key === 'Enter') { if (authMode === 'signup') document.getElementById('authPasswordConfirm').focus(); else handleAuth(); } });
document.getElementById('authPasswordConfirm').addEventListener('keydown', e => { if (e.key === 'Enter') handleAuth(); });

// Referral code validation
let refValidateTimer;
document.getElementById('authReferralCode').addEventListener('input', function() {
  const input = this;
  const code = input.value.trim().toUpperCase();
  clearTimeout(refValidateTimer);
  if (!code) { input.style.borderColor = ''; return; }
  refValidateTimer = setTimeout(async () => {
    const valid = await validateReferralCode(code);
    input.style.borderColor = valid ? '#00c864' : '#ff4444';
  }, 500);
});

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

function colorPrefLabels() {
  // Color pref-labels, sliders, checkboxes, selects, inputs green if active, red if not
  document.querySelectorAll('#preferencesOverlay .pref-group').forEach(group => {
    const label = group.querySelector('.pref-label');
    if (!label) return;
    let active = false;

    // Check sliders: active if min > slider.min or max < slider.max
    const slider = group.querySelector('.pref-range-slider');
    if (slider) {
      const minI = slider.querySelector('[data-handle=min]');
      const maxI = slider.querySelector('[data-handle=max]');
      if (minI && maxI) active = parseInt(minI.value) > parseInt(minI.min) || parseInt(maxI.value) < parseInt(maxI.max);
      // Color the slider fill
      const fill = slider.querySelector('.range-slider-fill');
      if (fill) fill.style.background = active ? '#00c864' : '#ff4444';
      // Color the range values
      const vals = slider.querySelector('.pref-range-values');
      if (vals) vals.style.color = active ? '#00c864' : '#ff4444';
      // Color the thumb circles
      slider.querySelectorAll('input[type=range]').forEach(inp => {
        inp.classList.toggle('pref-slider-active', active);
        inp.classList.toggle('pref-slider-inactive', !active);
      });
    }
    // Check checkboxes: active if any checked
    const cbs = group.querySelectorAll('input[type=checkbox]');
    const anyCbChecked = [...cbs].some(cb => cb.checked);
    if (anyCbChecked) active = true;
    // Color individual checkbox labels green/red and sort checked to top
    const cbWrap = group.querySelector('.pref-checkboxes');
    cbs.forEach(cb => {
      const lbl = cb.closest('.pref-cb');
      if (lbl) {
        const span = lbl.querySelector('span');
        if (cb.checked) {
          lbl.style.borderColor = 'rgba(0,200,100,0.5)';
          lbl.style.background = 'rgba(0,200,100,0.06)';
          if (span) span.style.color = '#00c864';
          cb.style.borderColor = '#00c864';
          cb.style.background = 'rgba(0,200,100,0.2)';
          cb.classList.add('pref-active');
        } else {
          lbl.style.borderColor = 'rgba(255,68,68,0.3)';
          lbl.style.background = '';
          if (span) span.style.color = '#ff4444';
          cb.style.borderColor = 'rgba(255,68,68,0.3)';
          cb.style.background = 'rgba(255,68,68,0.04)';
          cb.classList.remove('pref-active');
        }
      }
    });
    // Move checked to top of list
    if (cbWrap) {
      const labels = [...cbWrap.querySelectorAll('.pref-cb')];
      labels.sort((a, b) => {
        const ac = a.querySelector('input').checked ? 0 : 1;
        const bc = b.querySelector('input').checked ? 0 : 1;
        return ac - bc;
      });
      labels.forEach(l => cbWrap.appendChild(l));
    }
    // Check selects: active if value is set
    const sel = group.querySelector('.pref-select');
    if (sel && sel.value) active = true;
    if (sel) {
      sel.style.borderColor = (sel.value ? 'rgba(0,200,100,0.5)' : 'rgba(255,68,68,0.3)');
      sel.style.color = sel.value ? '#00c864' : '#ff4444';
    }
    // Check text/number inputs
    const inp = group.querySelector('input[type=number]');
    if (inp && inp.value && parseInt(inp.value) > 0) active = true;
    if (inp) {
      inp.style.borderColor = (inp.value && parseInt(inp.value) > 0) ? 'rgba(0,200,100,0.5)' : 'rgba(255,68,68,0.3)';
      inp.style.color = (inp.value && parseInt(inp.value) > 0) ? '#00c864' : '#ff4444';
    }

    label.style.color = active ? '#00c864' : '#ff4444';
  });
}

function clearPrefsForm() {
  document.querySelectorAll('.pref-range-slider').forEach(c => {
    const minI = c.querySelector('[data-handle=min]');
    const maxI = c.querySelector('[data-handle=max]');
    maxI.value = maxI.max;
    minI.value = minI.min;
    minI.dispatchEvent(new Event('input'));
  });
  document.querySelectorAll('#preferencesOverlay .pref-checkboxes input[type=checkbox]').forEach(cb => cb.checked = false);
  document.querySelectorAll('#preferencesOverlay .pref-select').forEach(sel => sel.value = '');
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

// ── Filter Presets ──

function captureFilterState() {
  return {
    activeRegion: { include: [...activeRegion.include], exclude: [...activeRegion.exclude] },
    activeVenue: { include: [...activeVenue.include], exclude: [...activeVenue.exclude] },
    activeCountry: { include: [...activeCountry.include], exclude: [...activeCountry.exclude] },
    activeLabels: { include: [...activeLabels.include], exclude: [...activeLabels.exclude] },
    activeAV: { include: [...activeAV.include], exclude: [...activeAV.exclude] },
    activeAvailability: { include: [...activeAvailability.include], exclude: [...activeAvailability.exclude] },
    activePhotos: { include: [...activePhotos.include], exclude: [...activePhotos.exclude] },
    activeFavFilter: { include: [...activeFavFilter.include], exclude: [...activeFavFilter.exclude] },
    rangeFilters: JSON.parse(JSON.stringify(rangeFilters)),
    activeSort, sortDir,
    textFilters: { ...textFilters },
  };
}

function applyFilterState(f) {
  if (!f) return;
  const apply = (target, src) => { target.include.length = 0; target.exclude.length = 0; (src.include || []).forEach(v => target.include.push(v)); (src.exclude || []).forEach(v => target.exclude.push(v)); };
  apply(activeRegion, f.activeRegion || {});
  apply(activeVenue, f.activeVenue || {});
  apply(activeCountry, f.activeCountry || {});
  apply(activeLabels, f.activeLabels || {});
  apply(activeAV, f.activeAV || {});
  apply(activeAvailability, f.activeAvailability || {});
  apply(activePhotos, f.activePhotos || {});
  apply(activeFavFilter, f.activeFavFilter || {});
  rangeFilters = f.rangeFilters ? JSON.parse(JSON.stringify(f.rangeFilters)) : {};
  if (f.activeSort) activeSort = f.activeSort;
  if (f.sortDir) sortDir = f.sortDir;
  if (f.textFilters) Object.assign(textFilters, f.textFilters);
  renderFilters(); renderRangeFilters(); renderGrid();
}

async function loadFilterPresets() {
  try {
    const { data: { user } } = await sbClient.auth.getUser();
    if (!user) return;
    const { data } = await sbClient.from('user_filter_presets').select('*').eq('user_id', user.id).order('created_at');
    userFilterPresets = data || [];
    renderFilterPresets();
  } catch (e) { console.error('Load presets error:', e); }
}

async function saveFilterPreset() {
  const { data: { user } } = await sbClient.auth.getUser();
  if (!user) return;
  if (userFilterPresets.length >= 5) { alert('Maximum 5 filter presets. Delete one first.'); return; }
  const name = prompt('Preset name:');
  if (!name || !name.trim()) return;
  // Deactivate all existing
  for (const p of userFilterPresets) {
    if (p.is_active) await sbClient.from('user_filter_presets').update({ is_active: false }).eq('id', p.id);
  }
  const { data, error } = await sbClient.from('user_filter_presets').insert({ user_id: user.id, name: name.trim(), filters: captureFilterState(), is_active: true }).select().single();
  if (error) { alert('Error saving preset: ' + error.message); return; }
  userFilterPresets.forEach(p => p.is_active = false);
  userFilterPresets.push(data);
  renderFilterPresets();
  renderPresetsOverlayList();
}

async function deleteFilterPreset(id) {
  const { error } = await sbClient.from('user_filter_presets').delete().eq('id', id);
  if (error) { alert('Error deleting preset: ' + error.message); return; }
  userFilterPresets = userFilterPresets.filter(p => p.id !== id);
  renderFilterPresets();
  renderPresetsOverlayList();
}

async function activateFilterPreset(id) {
  const { data: { user } } = await sbClient.auth.getUser();
  if (!user) return;
  await sbClient.from('user_filter_presets').update({ is_active: false }).eq('user_id', user.id);
  await sbClient.from('user_filter_presets').update({ is_active: true }).eq('id', id);
  userFilterPresets.forEach(p => p.is_active = (p.id === id));
  const preset = userFilterPresets.find(p => p.id === id);
  if (preset) applyFilterState(preset.filters);
  renderFilterPresets();
  renderPresetsOverlayList();
}

function renderFilterPresets() {
  const row = document.getElementById('filterPresetsRow');
  if (!row) return;
  if (!isLoggedIn()) { row.innerHTML = ''; return; }

  const activePreset = userFilterPresets.find(p => p.is_active);
  const label = activePreset ? activePreset.name : 'No Filter Selected';

  row.innerHTML = `<div class="preset-section">
    <button class="preset-open-btn" id="presetOpenBtn">
      <span class="preset-open-label">${label}</span>
      <span class="preset-open-arrow">&#9662;</span>
    </button>
  </div>`;

  document.getElementById('presetOpenBtn').onclick = () => openPresetsOverlay();
}

function openPresetsOverlay() {
  const overlay = document.getElementById('presetsOverlay');
  overlay.classList.add('open');
  renderPresetsOverlayList();
}

function closePresetsOverlay() {
  document.getElementById('presetsOverlay').classList.remove('open');
}

function renderPresetsOverlayList() {
  const container = document.getElementById('presetsOverlayList');
  const activePreset = userFilterPresets.find(p => p.is_active);

  let html = `<div class="preset-item${!activePreset ? ' active' : ''}" data-id="none">
    <span class="preset-name">No Filter</span>
  </div>`;

  html += userFilterPresets.map(p => {
    const notify = p.filters && p.filters.notifyEnabled;
    return `<div class="preset-item${p.is_active ? ' active' : ''}" data-id="${p.id}">
    <span class="preset-name">${p.name}</span>
    <button class="preset-notify${notify ? ' active' : ''}" data-id="${p.id}" title="${notify ? 'Notifications on' : 'Notifications off'}" style="background:none;border:1px solid ${notify ? '#00c864' : '#555'};border-radius:4px;padding:2px 6px;font-size:10px;cursor:pointer;color:${notify ? '#00c864' : '#555'};margin-right:4px">${notify ? '\ud83d\udd14' : '\ud83d\udd15'}</button>
    <button class="preset-delete" title="Delete preset">&times;</button>
  </div>`;
  }).join('');

  html += `<button class="preset-save-btn" id="presetSaveBtnOverlay"${userFilterPresets.length >= 5 ? ' disabled' : ''}>
    + Save Current Filters${userFilterPresets.length > 0 ? ' (' + userFilterPresets.length + '/5)' : ''}
  </button>`;

  container.innerHTML = html;

  document.getElementById('presetSaveBtnOverlay').onclick = () => saveFilterPreset();
  container.querySelectorAll('.preset-item .preset-name').forEach(el => {
    el.onclick = () => {
      const id = el.parentElement.dataset.id;
      if (id === 'none') deactivateAllPresets();
      else activateFilterPreset(id);
    };
  });
  container.querySelectorAll('.preset-notify').forEach(el => {
    el.onclick = async (e) => {
      e.stopPropagation();
      const id = el.dataset.id;
      const preset = userFilterPresets.find(p => p.id === id);
      if (!preset) return;
      const newVal = !(preset.filters && preset.filters.notifyEnabled);
      preset.filters = { ...preset.filters, notifyEnabled: newVal };
      await sbClient.from('user_filter_presets').update({ filters: preset.filters }).eq('id', id);
      renderPresetsOverlayList();
    };
  });
  container.querySelectorAll('.preset-delete').forEach(el => {
    el.onclick = (e) => { e.stopPropagation(); deleteFilterPreset(el.parentElement.dataset.id); };
  });
}

async function deactivateAllPresets() {
  const { data: { user } } = await sbClient.auth.getUser();
  if (!user) return;
  await sbClient.from('user_filter_presets').update({ is_active: false }).eq('user_id', user.id);
  userFilterPresets.forEach(p => p.is_active = false);
  // Clear all filters
  activeRegion.include.length = 0; activeRegion.exclude.length = 0;
  activeVenue.include.length = 0; activeVenue.exclude.length = 0;
  activeCountry.include.length = 0; activeCountry.exclude.length = 0;
  activeLabels.include.length = 0; activeLabels.exclude.length = 0;
  activeAV.include.length = 0; activeAV.exclude.length = 0;
  activeAvailability.include.length = 0; activeAvailability.exclude.length = 0;
  activePhotos.include.length = 0; activePhotos.exclude.length = 0;
  activeFavFilter.include.length = 0; activeFavFilter.exclude.length = 0;
  activeDateTime = ''; dtEnabled = false; dtPendingMonth = ''; dtPendingDay = '';
  rangeFilters = {}; Object.keys(textFilters).forEach(k => textFilters[k] = '');
  renderFilters(); renderRangeFilters(); renderGrid();
  renderPresetsOverlayList();
}

function restoreActivePresetOrClear() {
  const active = userFilterPresets.find(p => p.is_active);
  if (active) {
    applyFilterState(active.filters);
  } else {
    activeAvailability.include.length = 0; activeAvailability.exclude.length = 0;
    activeFavFilter.include.length = 0; activeFavFilter.exclude.length = 0;
  }
}

// ── End Filter Presets ──

const PROFILES_BASE = 'https://raw.githubusercontent.com/travanixlabs/brothel-search/main/profiles';
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

let allGirls = [];
let userPreferences = null;
let matchScores = new Map(); // girl key -> score 0-100
let matchThreshold = 0; // top 20% cutoff

let activeRegion = { include: [], exclude: [] };
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
function isReturnProfile(g) {
  if (!g.lastRostered || isNewProfile(g)) return false;
  const cal = calendarData[(g.venue || '') + ':' + g.name];
  if (!cal) return false;
  const today = new Date(); today.setHours(0,0,0,0);
  const todayStr = today.getFullYear() + '-' + String(today.getMonth()+1).padStart(2,'0') + '-' + String(today.getDate()).padStart(2,'0');
  if (!cal[todayStr]) return false;
  const prevDates = Object.keys(cal).filter(d => !d.startsWith('_') && d < todayStr).sort().reverse();
  if (!prevDates.length) return false;
  const gap = Math.round((today - new Date(prevDates[0] + 'T00:00:00')) / 86400000);
  return gap >= 7;
}
function imgProxy(url, w = 300) {
  if (!url) return '';
  return url;
}

function fmt24to12(t) {
  if (!t || !t.includes(':')) return t || '';
  const [h, m] = t.split(':').map(Number);
  if (isNaN(h)) return t;
  const suffix = h >= 12 ? 'pm' : 'am';
  const h12 = h === 0 ? 12 : h > 12 ? h - 12 : h;
  return m === 0 ? h12 + suffix : h12 + ':' + String(m).padStart(2, '0') + suffix;
}

function slotMins(t) { if (!t || !t.includes(':')) return 0; const [h, m] = t.split(':').map(Number); return (isNaN(h) ? 0 : h) * 60 + (isNaN(m) ? 0 : m); }
function validSlot(s) { return s && s.start && s.end && s.start.includes(':') && s.end.includes(':'); }

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
    if (ySlot && ySlot.start && ySlot.end && ySlot.start.includes(':') && ySlot.end.includes(':')) {
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
  if (slot && slot.start && slot.end && slot.start.includes(':') && slot.end.includes(':')) {
    const [sh, sm] = slot.start.split(':').map(Number);
    const [eh, em] = slot.end.split(':').map(Number);
    let startMins = sh * 60 + sm;
    const endMins = eh * 60 + em;
    // Shifts starting before 6am are overnight (tonight's shift, not this morning's)
    // Only treat as current-morning if we're actually before the end time AND before 6am
    if (startMins < 360 && endMins > 0 && endMins <= 600 && nowMins >= 360) {
      // This is a late-night shift (e.g. 00:00-09:00) and it's past 6am — treat as tonight
      startMins += 24 * 60;
    }
    const effectiveEnd = endMins <= startMins ? 24 * 60 + endMins : endMins;
    const timeStr = fmt24to12(slot.start) + ' - ' + fmt24to12(slot.end);
    if (nowMins >= startMins && nowMins < effectiveEnd) return 'Available Now (' + timeStr + ')';
    if (nowMins < startMins) return 'Available Later Today (' + timeStr + ')';
  } else if (slot) {
    return 'Available Now';
  }
  // Check future dates
  const futureDates = Object.keys(cal).filter(d => d > today && !d.startsWith('_'));
  if (futureDates.length) {
    const next = futureDates.sort()[0];
    const fSlot = cal[next];
    if (fSlot && fSlot.start && fSlot.end && fSlot.start.includes(':') && fSlot.end.includes(':')) {
      const fTimeStr = fmt24to12(fSlot.start) + ' - ' + fmt24to12(fSlot.end);
      const dObj = new Date(next + 'T00:00:00');
      const dayNames = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
      return 'Available Future: ' + dayNames[dObj.getDay()] + ' ' + next.slice(5) + ' (' + fTimeStr + ')';
    }
    const dObj = new Date(next + 'T00:00:00');
    const dayNames = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
    return 'Available Future: ' + dayNames[dObj.getDay()] + ' ' + next.slice(5);
  }
  return 'ended';
}

function isAvailableAt(g, dateStr, timeMins) {
  const cal = calendarData[(g.venue || '') + ':' + g.name];
  if (!cal) return false;
  const slot = cal[dateStr];
  if (!slot || !slot.start || !slot.end || !slot.start.includes(':') || !slot.end.includes(':')) return !!slot;
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
  if (slot && slot.start && slot.end && slot.start.includes(':') && slot.end.includes(':')) {
    const [sh, sm] = slot.start.split(':').map(Number);
    const [eh, em] = slot.end.split(':').map(Number);
    const nowMins = now.getHours() * 60 + now.getMinutes();
    let startMins = sh * 60 + sm;
    const endMins = eh * 60 + em;
    if (startMins < 360 && endMins > 0 && endMins <= 600 && nowMins >= 360) {
      startMins += 24 * 60;
    }
    const effectiveEnd = endMins <= startMins ? 24 * 60 + endMins : endMins;
    if (nowMins >= startMins && nowMins < effectiveEnd) return 'Available Now';
    if (nowMins < startMins) return 'Available Later Today';
  } else if (slot) {
    return 'Available Now'; // Rostered but no time info
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
  return activeRegion.include.length || activeRegion.exclude.length || activeVenue.include.length || activeVenue.exclude.length || activeCountry.include.length || activeCountry.exclude.length || activeLabels.include.length || activeLabels.exclude.length || activeAV.include.length || activeAV.exclude.length || activeAvailability.include.length || activeAvailability.exclude.length || activePhotos.include.length || activePhotos.exclude.length || activeFavFilter.include.length || activeFavFilter.exclude.length || activeDateTime || hasRangeActive || hasTextFilter;
}

function updateMoreFiltersCount() {
  const count = Object.values(textFilters).filter(v => v).length +
    Object.keys(rangeFilters).filter(k => { const d = rangeDefaults[k]; return d && (rangeFilters[k].min > d.min || rangeFilters[k].max < d.max); }).length +
    (activeDateTime ? 1 : 0);
  const badge = document.getElementById('moreFiltersBadge');
  if (badge) { badge.textContent = count || ''; badge.style.display = count ? 'inline-flex' : 'none'; }
}

function updateClearBtn() {
  const clearBtn = document.getElementById('clearAllBtn');
  if (clearBtn) clearBtn.disabled = !hasAnyFilter();
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

  // Region options
  const regionCounts = {};
  base.forEach(g => { const r = VENUE_REGIONS[g.venue] || 'other'; regionCounts[r] = (regionCounts[r] || 0) + 1; });
  const regionOpts = Object.entries(REGION_NAMES).filter(([k]) => regionCounts[k]).sort((a, b) => {
    const aActive = activeRegion.include.includes(a[0]) || activeRegion.exclude.includes(a[0]);
    const bActive = activeRegion.include.includes(b[0]) || activeRegion.exclude.includes(b[0]);
    if (aActive && !bActive) return -1;
    if (!aActive && bActive) return 1;
    return a[1].localeCompare(b[1]);
  }).map(([k, v]) => ({ value: k, label: v, count: regionCounts[k] || 0 }));

  fr.innerHTML = buildLabelDropdown('ddRegion', 'Region', regionOpts, activeRegion.include, activeRegion.exclude)
    + buildLabelDropdown('ddVenue', 'Venue', venueOpts, activeVenue.include, activeVenue.exclude)
    + buildLabelDropdown('ddCountry', 'Country', countryOpts, activeCountry.include, activeCountry.exclude)
    + buildLabelDropdown('ddLabels', 'Services', labelOpts, activeLabels.include, activeLabels.exclude)
    + buildLabelDropdown('ddAV', 'AV', [{value:'Yes',label:'Yes',count:allGirls.filter(g=>g.pornstar).length},{value:'No',label:'No',count:allGirls.filter(g=>!g.pornstar).length}], activeAV.include, activeAV.exclude)
    + buildLabelDropdown('ddPhotos', 'Photos', photosOpts, activePhotos.include, activePhotos.exclude)
    + buildLabelDropdown('ddFav', 'Favourites', [{value:'Favourite',label:'Favourite',count:allGirls.filter(g=>isFavorite(g)).length},{value:'Hidden',label:'Hidden',count:allGirls.filter(g=>isHidden(g)).length},{value:'Others',label:'Others',count:allGirls.filter(g=>!isFavorite(g)&&!isHidden(g)).length}], activeFavFilter.include, activeFavFilter.exclude)
    + buildLabelDropdown('ddAvailability', 'Availability', availOpts, activeAvailability.include, activeAvailability.exclude)
;

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
    { sel: '#ddRegion', state: activeRegion },
    { sel: '#ddVenue', state: activeVenue },
    { sel: '#ddCountry', state: activeCountry },
    { sel: '#ddLabels', state: activeLabels },
    { sel: '#ddAV', state: activeAV },
    { sel: '#ddPhotos', state: activePhotos },
    { sel: '#ddFav', state: activeFavFilter },
    { sel: '#ddAvailability', state: activeAvailability },
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

  // Clear all filters — always present, enable/disable based on state
  const clearBtn = document.getElementById('clearAllBtn');
  if (clearBtn) {
    clearBtn.disabled = !hasAnyFilter();
    clearBtn.onclick = () => { activeRegion.include.length = 0; activeRegion.exclude.length = 0; activeVenue.include.length = 0; activeVenue.exclude.length = 0; activeCountry.include.length = 0; activeCountry.exclude.length = 0; activeLabels.include.length = 0; activeLabels.exclude.length = 0; activeAV.include.length = 0; activeAV.exclude.length = 0; activeAvailability.include.length = 0; activeAvailability.exclude.length = 0; activePhotos.include.length = 0; activePhotos.exclude.length = 0; activeFavFilter.include.length = 0; activeFavFilter.exclude.length = 0; activeDateTime = ''; dtEnabled = false; dtPendingMonth = ''; dtPendingDay = ''; rangeFilters = {}; Object.keys(textFilters).forEach(k => textFilters[k] = ''); renderFilters(); renderRangeFilters(); renderGrid(); };
  }

  // Sort row (dropdown + direction toggle)
  const sr = document.getElementById('sortRow');
  const sorts = [
    { id: 'venue', label: 'Venue' },
    { id: 'name', label: 'Name' },
    { id: 'country', label: 'Country' },
    { id: 'age', label: 'Age' },
    { id: 'body', label: 'Body' },
    { id: 'height', label: 'Height' },
    { id: 'cup', label: 'Cup' },
    { id: 'val1', label: '30 Min' },
    { id: 'val2', label: '45 Min' },
    { id: 'val3', label: '60 Min' },
    { id: 'startDate', label: 'Start Date' },
    { id: 'lastModified', label: 'Last Modified' },
    { id: 'lastRostered', label: 'Latest Availability' },
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

  renderFilterPresets();
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
    const defaultMin = dataMin;
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

let _filteredCache = null;
let _filteredCachePath = null;
function invalidateFilterCache() { _filteredCache = null; }
function getFiltered() {
  const currentPath = window.location.pathname;
  if (_filteredCache && _filteredCachePath === currentPath) return _filteredCache;
  let list = [...allGirls];
  // Exclude deleted profiles (unless on Data page for admins)
  if (currentPath !== '/data') list = list.filter(g => g.deleted !== 'Yes');
  // Region filter
  if (activeRegion.include.length) list = list.filter(g => activeRegion.include.includes(VENUE_REGIONS[g.venue] || 'other'));
  if (activeRegion.exclude.length) list = list.filter(g => !activeRegion.exclude.includes(VENUE_REGIONS[g.venue] || 'other'));
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
  // Favourites filter (3-state: Favourite, Hidden, Others)
  if (activeFavFilter.include.length) list = list.filter(g => {
    const state = isFavorite(g) ? 'Favourite' : isHidden(g) ? 'Hidden' : 'Others';
    return activeFavFilter.include.includes(state);
  });
  if (activeFavFilter.exclude.length) list = list.filter(g => {
    const state = isFavorite(g) ? 'Favourite' : isHidden(g) ? 'Hidden' : 'Others';
    return !activeFavFilter.exclude.includes(state);
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
  else if (activeSort === 'country') list.sort((a, b) => emptyLast(a, b, () => ((Array.isArray(a.country) ? a.country[0] : a.country) || '').localeCompare((Array.isArray(b.country) ? b.country[0] : b.country) || '')));
  else if (activeSort === 'lastModified') list.sort((a, b) => emptyLast(a, b, () => (a.lastModified || '').localeCompare(b.lastModified || '')));
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
  _filteredCache = list;
  _filteredCachePath = currentPath;
  return list;
}

const PAGE_SIZES = { bento: 12, grid: 12, compact: 100, list: 12 };
function getPageSize() { return PAGE_SIZES[currentLayout] || 12; }
const PAGE_SIZE = 12;
let currentFiltered = [];
let currentPage = 0;
let loadingMore = false;

function renderCard(g, grid) {
    const el = document.createElement('div');
    const availText = getAvailabilityText(g);
    const glowClass = availText && availText.startsWith('Available Now') ? ' glow-now' : availText && (availText.startsWith('Available Later') || availText.startsWith('Available Future')) ? ' glow-later' : '';
    el.className = 'girl-card' + (isFavorite(g) ? ' favorited' : '') + glowClass;
    const img = g.photos && g.photos.length
      ? `<img class="card-thumb" src="${imgProxy(g.photos[0])}" alt="${(g.name || '').replace(/"/g, '&quot;')} – ${(g.venueName || '').replace(/"/g, '&quot;')} ${(VENUE_SUBURB_NAMES[g.venue] || '').replace(/"/g, '&quot;')}, Sydney" loading="lazy">`
      : '<div class="silhouette"></div>';
    const countries = countriesWithFlags(g.country);

    const lastRostered = (() => {
      const avail = getAvailabilityText(g);
      if (avail && avail !== 'ended') return avail;
      if (!g.lastRostered) return '';
      const today = new Date(); today.setHours(0,0,0,0);
      const rd = new Date(g.lastRostered + 'T00:00:00');
      const diff = Math.round((today - rd) / 86400000);
      if (diff === 0) return 'Last available: Today';
      if (diff === 1) return 'Last available: Yesterday';
      if (diff < 0) {
        // Future date
        const dayNames = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
        const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        return 'Next: ' + dayNames[rd.getDay()] + ' ' + rd.getDate() + ' ' + monthNames[rd.getMonth()];
      }
      return 'Last available: ' + diff + ' days ago';
    })();

    const girlKey = g.venue + ':' + g.name;
    const girlScore = matchScores.get(girlKey) || 0;
    const showBadge = userPreferences && girlScore > 0;

    const heartSvg = '<svg viewBox="0 0 24 24"><path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"/></svg>';
    const hideSvg = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24"/><line x1="1" y1="1" x2="23" y2="23"/></svg>';

    el.innerHTML = `
      <div class="fav-heart${isFavorite(g) ? ' active' : ''}" data-url="${(g.oldUrl||'').replace(/"/g,'&quot;')}">${heartSvg}</div>
      <div class="hide-btn${isHidden(g) ? ' active' : ''}" data-url="${(g.oldUrl||'').replace(/"/g,'&quot;')}">${hideSvg}</div>
      <button class="compare-btn${compareList.some(c => c.venue === g.venue && c.name === g.name) ? ' in-compare' : ''}" data-venue="${g.venue}" data-name="${(g.name||'').replace(/"/g,'&quot;')}" onclick="event.stopPropagation();toggleCompare('${g.venue}','${(g.name||'').replace(/'/g,"\\'")}')">${compareList.some(c => c.venue === g.venue && c.name === g.name) ? '<svg viewBox="0 0 24 24" width="12" height="12" fill="currentColor"><path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/></svg>' : '<svg viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M16 3h5v5M4 20L21 3M21 16v5h-5M15 15l6 6M4 4l6 6"/></svg>'}</button>
      <div class="card-badges">${'<span class="country-badge">' + g.venueName + '</span>'}${showBadge ? '<div class="match-badge' + (girlScore >= 90 ? ' match-gold' : '') + '">' + girlScore + '%</div>' : ''}${isNewProfile(g) ? '<span class="new-badge">New</span>' : isReturnProfile(g) ? '<span class="return-badge">Return</span>' : ''}${g.pornstar ? '<span class="av-badge">AV</span>' : ''}</div>
      <div class="card-img">${img}</div>
      <div class="card-name-overlay"><span>${g.name || ''}</span></div>
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
      </div>
      <div class="card-list-extra">
        ${lastRostered ? '<div class="cle-row"><span class="cle-label">Last Avail</span><span class="' + (lastRostered.startsWith('Available Now') ? 'available-now' : lastRostered.startsWith('Available Later') ? 'available-later' : lastRostered.startsWith('Available Future') ? 'available-future' : '') + '">' + lastRostered + '</span></div>' : ''}
        ${g.startDate ? '<div class="cle-row"><span class="cle-label">Start</span><span>' + g.startDate + '</span></div>' : ''}
        ${g.exp ? '<div class="cle-row"><span class="cle-label">Exp</span><span>' + g.exp + '</span></div>' : ''}
        ${g.lang ? '<div class="cle-row"><span class="cle-label">Lang</span><span>' + g.lang + '</span></div>' : ''}
        ${g.type ? '<div class="cle-row"><span class="cle-label">Type</span><span>' + g.type + '</span></div>' : ''}
        ${g.oldUrl ? '<div class="cle-row"><span class="cle-label">Ref</span><a href="' + g.oldUrl + '" target="_blank" rel="noopener" onclick="event.stopPropagation()" style="color:var(--accent);text-decoration:none;word-break:break-all">Link</a></div>' : ''}
      </div>
      <div class="card-list-extra card-list-extra-wide">
        ${g.desc ? '<div class="cle-desc">' + g.desc.replace(/</g, '&lt;') + '</div>' : ''}
        ${g.labels && g.labels.length ? '<div class="cle-labels">' + g.labels.map(l => '<span class="cle-label-pill">' + l + '</span>').join('') + '</div>' : ''}
      </div>`;
    el.querySelector('.fav-heart').addEventListener('click', (e) => toggleFavorite(g.oldUrl, e));
    el.querySelector('.hide-btn').addEventListener('click', (e) => toggleHidden(g.oldUrl, e));
    el.onclick = (e) => { closeFavorites(); spawnParticles(e); showProfile(g); };
    // In list view, move heart/hide icons next to the name
    if (currentLayout === 'list') {
      const nameEl = el.querySelector('.card-name');
      const heart = el.querySelector('.fav-heart');
      const hide = el.querySelector('.hide-btn');
      if (nameEl && heart && hide) { nameEl.appendChild(heart); nameEl.appendChild(hide); const cmp = el.querySelector('.compare-btn'); if (cmp) nameEl.appendChild(cmp); }
    }
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
  const ps = getPageSize();
  const start = currentPage * ps;
  const end = Math.min(start + ps, currentFiltered.length);
  if (start >= currentFiltered.length) return;
  loadingMore = true;
    const loader = document.createElement('div');
    loader.className = 'scroll-loader';
    loader.id = 'scrollLoader';
    loader.innerHTML = '<span></span><span></span><span></span>';
    document.getElementById('girlsGrid').appendChild(loader);
  const firstNewIdx = grid.children.length;
  for (let i = start; i < end; i++) renderCard(currentFiltered[i], grid);
  currentPage++;
    const sl = document.getElementById('scrollLoader');
    if (sl) sl.remove();
  // Staggered entrance for new cards
  const newCards = Array.from(grid.children).slice(firstNewIdx);
  newCards.forEach((card, i) => {
    card.style.opacity = '0';
    card.style.transform = 'translateY(20px)';
    card.style.transition = 'opacity 0.4s cubic-bezier(0.16,1,0.3,1), transform 0.4s cubic-bezier(0.16,1,0.3,1)';
    card.style.transitionDelay = (i * 0.04) + 's';
    requestAnimationFrame(() => requestAnimationFrame(() => { card.style.opacity = '1'; card.style.transform = 'translateY(0)'; }));
  });
  loadingMore = false;
}


function renderGrid() {
  invalidateFilterCache();
  // If Working Now or Data page is active, re-render with updated filters
  const activePath = window.location.pathname;
  if (activePath === '/working-now' || activePath === '/data' || activePath === '/compare' || activePath === '/analytics' || activePath.startsWith('/sydney/') || activePath === '/' || activePath === '/index.html') {
    const landing = document.getElementById('landingPage');
    if (landing && landing.style.display !== 'none') {
      if (activePath === '/working-now') landing.innerHTML = renderWorkingNow();
      else if (activePath === '/data') landing.innerHTML = renderDataPage();
      else if (activePath === '/compare') landing.innerHTML = renderComparePage();
      else if (activePath === '/analytics') landing.innerHTML = renderAnalyticsPage();
      else if (activePath === '/' || activePath === '/index.html') { landing.innerHTML = renderHomePage(); initHomePageListeners(); document.querySelectorAll('.home-stat-num').forEach(el => { el.textContent = el.dataset.target; }); }
      else handleLandingRoute(activePath);
    }
  }

  const grid = document.getElementById('girlsGrid');
  currentFiltered = getFiltered();
  currentPage = 0;

  document.getElementById('resultCount').textContent = currentFiltered.length + ' girl' + (currentFiltered.length !== 1 ? 's' : '') + ' found';
  const wnFiltered = currentFiltered.filter(g => { const a = getAvailabilityText(g); return a && a.startsWith('Available Now'); }).length;
  const wnLink = document.getElementById('navWorkingNow');
  if (wnLink) wnLink.textContent = 'Working Now' + (wnFiltered > 0 ? ' (' + wnFiltered + ')' : '');
  updateMoreFiltersCount();

  // Filter chips
  const chipsEl = document.getElementById('filterChips');
  if (chipsEl) {
    const chips = [];
    activeRegion.include.forEach(v => chips.push({ label: REGION_NAMES[v] || v, type: 'region', action: 'include' }));
    activeRegion.exclude.forEach(v => chips.push({ label: REGION_NAMES[v] || v, type: 'region', action: 'exclude' }));
    activeVenue.include.forEach(v => chips.push({ label: v, type: 'venue', action: 'include' }));
    activeVenue.exclude.forEach(v => chips.push({ label: v, type: 'venue', action: 'exclude' }));
    activeCountry.include.forEach(v => chips.push({ label: v, type: 'country', action: 'include' }));
    activeCountry.exclude.forEach(v => chips.push({ label: v, type: 'country', action: 'exclude' }));
    activeLabels.include.forEach(v => chips.push({ label: v, type: 'labels', action: 'include' }));
    activeLabels.exclude.forEach(v => chips.push({ label: v, type: 'labels', action: 'exclude' }));
    activeAvailability.include.forEach(v => chips.push({ label: v, type: 'availability', action: 'include' }));
    Object.entries(textFilters).forEach(([k, v]) => { if (v) chips.push({ label: k + ': ' + v, type: 'text', key: k }); });

    if (chips.length) {
      chipsEl.style.display = 'flex';
      chipsEl.innerHTML = chips.map(c => '<span class="filter-chip' + (c.action === 'exclude' ? ' filter-chip-exclude' : '') + '" data-type="' + c.type + '" data-label="' + (c.label || '').replace(/"/g, '&quot;') + '"' + (c.key ? ' data-key="' + c.key + '"' : '') + '>' + (c.action === 'exclude' ? '\u2013 ' : '') + c.label + ' <button>&times;</button></span>').join('');
      chipsEl.querySelectorAll('.filter-chip button').forEach(btn => {
        btn.onclick = function() {
          const chip = this.parentElement;
          const type = chip.dataset.type;
          const label = chip.dataset.label;
          if (type === 'text') { textFilters[chip.dataset.key] = ''; }
          else {
            const states = { region: activeRegion, venue: activeVenue, country: activeCountry, labels: activeLabels, availability: activeAvailability };
            const state = states[type];
            if (state) { state.include = state.include.filter(v => v !== label); state.exclude = state.exclude.filter(v => v !== label); }
          }
          renderFilters(); renderGrid();
        };
      });
    } else {
      chipsEl.style.display = 'none';
    }
  }

  if (!currentFiltered.length) {
    grid.innerHTML = '<div class="empty-msg"><svg width="80" height="80" viewBox="0 0 80 80" fill="none" style="margin-bottom:20px"><circle cx="40" cy="40" r="38" stroke="rgba(201,149,44,0.25)" stroke-width="1.5"/><circle cx="40" cy="40" r="28" stroke="rgba(201,149,44,0.15)" stroke-width="1"/><path d="M30 45c0-5.5 4.5-10 10-10s10 4.5 10 10" stroke="rgba(201,149,44,0.3)" stroke-width="1.5" stroke-linecap="round" fill="none" transform="rotate(180 40 40)"/><circle cx="33" cy="35" r="2" fill="rgba(201,149,44,0.3)"/><circle cx="47" cy="35" r="2" fill="rgba(201,149,44,0.3)"/></svg><div>No girls match your filters</div></div>';
    return;
  }

  grid.innerHTML = '';
  compactCurrentPage = 1;
  ['grid','bento','compact','list'].forEach(m => grid.classList.toggle(m, currentLayout === m));
  loadMore();
  if (currentLayout === 'compact') renderCompactPagination();
  else fillViewport();
}

let compactCurrentPage = 1;
function renderCompactPagination() {
  let pag = document.getElementById('compactPagination');
  if (!pag) { pag = document.createElement('div'); pag.id = 'compactPagination'; pag.className = 'compact-pagination'; document.getElementById('girlsGrid').after(pag); }
  const ps = getPageSize();
  const totalPages = Math.ceil(currentFiltered.length / ps);
  if (totalPages <= 1) { pag.innerHTML = ''; return; }
  let html = '<button class="compact-pag-btn" ' + (compactCurrentPage <= 1 ? 'disabled' : '') + ' onclick="compactGoPage(' + (compactCurrentPage - 1) + ')">&lsaquo; Prev</button>';
  html += '<span class="compact-pag-info">Page ' + compactCurrentPage + ' of ' + totalPages + '</span>';
  html += '<button class="compact-pag-btn" ' + (compactCurrentPage >= totalPages ? 'disabled' : '') + ' onclick="compactGoPage(' + (compactCurrentPage + 1) + ')">Next &rsaquo;</button>';
  pag.innerHTML = html;
}
window.compactGoPage = function(page) {
  const ps = getPageSize();
  const totalPages = Math.ceil(currentFiltered.length / ps);
  if (page < 1 || page > totalPages) return;
  compactCurrentPage = page;
  const grid = document.getElementById('girlsGrid');
  grid.innerHTML = '';
  const start = (page - 1) * ps;
  const end = Math.min(start + ps, currentFiltered.length);
  for (let i = start; i < end; i++) renderCard(currentFiltered[i], grid);
  renderCompactPagination();
  window.scrollTo({ top: 0, behavior: 'smooth' });
};

function fillViewport() {
  requestAnimationFrame(() => {
    while (currentPage * getPageSize() < currentFiltered.length && document.body.offsetHeight <= window.innerHeight + 400) {
      loadMore();
    }
  });
}

window.addEventListener('scroll', () => {
  if (currentLayout === 'compact') return;
  if (currentPage * getPageSize() >= currentFiltered.length) return;
  if (window.innerHeight + window.scrollY >= document.body.offsetHeight - 400) loadMore();
});

/* Roster helpers (used by Working Now page) */
let rosterSelectedDay = 0;

// renderRoster removed — roster calendar now only on Working Now page


// ── Referrals ──

async function getOrCreateReferralCode() {
  const { data: { user } } = await sbClient.auth.getUser();
  if (!user) return null;
  // Check existing
  const { data } = await sbClient.from('user_referral_codes').select('code').eq('user_id', user.id).single();
  if (data) return data.code;
  // Generate new
  const code = 'REF-' + Math.random().toString(36).substring(2, 8).toUpperCase();
  await sbClient.from('user_referral_codes').insert({ user_id: user.id, code });
  return code;
}

async function getReferralStats() {
  const { data: { user } } = await sbClient.auth.getUser();
  if (!user) return { total: 0, completed: 0, daysEarned: 0 };
  const { data } = await sbClient.from('referrals').select('status').eq('referrer_id', user.id);
  if (!data) return { total: 0, completed: 0, daysEarned: 0 };
  const completed = data.filter(r => r.status === 'completed').length;
  return { total: data.length, completed, daysEarned: completed * 7 };
}

async function validateReferralCode(code) {
  if (!code) return false;
  const { data } = await sbClient.from('user_referral_codes').select('user_id').eq('code', code.trim().toUpperCase()).single();
  return !!data;
}

// ── Notifications ──

let notifCache = [];

async function loadNotifications() {
  const { data: { user } } = await sbClient.auth.getUser();
  if (!user) return;
  const { data, error } = await sbClient.from('notifications').select('*').eq('user_id', user.id).order('created_at', { ascending: false }).limit(50);
  if (error) { console.error('Load notifications error:', error); return; }
  notifCache = data || [];
  renderNotifications();
}

function renderNotifications() {
  const bell = document.getElementById('notifBell');
  const badge = document.getElementById('notifBadge');
  const list = document.getElementById('notifList');
  if (!bell || !list) return;

  const unread = notifCache.filter(n => !n.read).length;
  badge.textContent = unread;
  badge.style.display = unread > 0 ? 'flex' : 'none';

  if (!notifCache.length) {
    list.innerHTML = '<div class="notif-empty">No notifications</div>';
    return;
  }

  list.innerHTML = notifCache.map(n => {
    const time = timeAgo(new Date(n.created_at));
    return '<div class="notif-item' + (!n.read ? ' unread' : '') + '" data-notif-id="' + n.id + '" data-venue="' + (n.venue || '') + '" data-girl="' + (n.girl_name || '') + '">' +
      (!n.read ? '<span class="notif-item-dot"></span>' : '') +
      '<div class="notif-item-title">' + n.title + '</div>' +
      '<div class="notif-item-body">' + n.body + '</div>' +
      '<div class="notif-item-time">' + time + '</div>' +
      '</div>';
  }).join('');

  // Click handlers
  list.querySelectorAll('.notif-item').forEach(el => {
    el.addEventListener('click', async function() {
      const id = this.dataset.notifId;
      const venue = this.dataset.venue;
      const girl = this.dataset.girl;
      // Mark as read
      await sbClient.from('notifications').update({ read: true }).eq('id', id);
      const n = notifCache.find(x => x.id === id);
      if (n) n.read = true;
      renderNotifications();
      // Navigate based on notification type
      document.getElementById('notifBell').classList.remove('open');
      if (venue && girl) {
        const g = allGirls.find(gg => gg.venue === venue && gg.name === girl);
        if (g) showProfile(g);
      } else {
        navigateToLanding('/working-now');
      }
    });
  });
}

function timeAgo(date) {
  const seconds = Math.floor((new Date() - date) / 1000);
  if (seconds < 60) return 'Just now';
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return minutes + 'm ago';
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return hours + 'h ago';
  const days = Math.floor(hours / 24);
  if (days < 7) return days + 'd ago';
  return date.toLocaleDateString('en-AU', { day: 'numeric', month: 'short' });
}

async function markAllNotificationsRead() {
  const unread = notifCache.filter(n => !n.read);
  if (!unread.length) return;
  await sbClient.from('notifications').update({ read: true }).in('id', unread.map(n => n.id));
  notifCache.forEach(n => n.read = true);
  renderNotifications();
}

// ── Reviews ──

async function loadReviews(venue, girlName) {
  const { data, error } = await sbClient.from('reviews').select('*').eq('venue', venue).eq('girl_name', girlName).order('created_at', { ascending: false });
  if (error) { console.error('Load reviews error:', error); return []; }
  return data || [];
}

let reviewerStatsCache = null;
let reviewerStatsPromise = null;
async function loadReviewerStats() {
  if (reviewerStatsCache) return reviewerStatsCache;
  if (reviewerStatsPromise) return reviewerStatsPromise;
  reviewerStatsPromise = (async () => {
    const { data } = await sbClient.from('reviews').select('user_id,created_at').limit(5000);
    const stats = {};
    for (const r of (data || [])) {
      if (!stats[r.user_id]) stats[r.user_id] = { count: 0, oldest: r.created_at };
      stats[r.user_id].count++;
      if (r.created_at < stats[r.user_id].oldest) stats[r.user_id].oldest = r.created_at;
    }
    reviewerStatsCache = stats;
    return stats;
  })();
  return reviewerStatsPromise;
}

function credibilityBadge(userId) {
  if (!reviewerStatsCache || !reviewerStatsCache[userId]) return '';
  const s = reviewerStatsCache[userId];
  let score = Math.min(50, s.count * 10); // 5 reviews = max 50pts
  const days = Math.floor((Date.now() - new Date(s.oldest).getTime()) / 86400000);
  score += Math.min(50, Math.round(days * 0.5)); // 100 days = max 50pts
  if (score >= 80) return '<span title="Trusted Reviewer (' + s.count + ' reviews, ' + days + ' days)" style="display:inline-block;margin-left:6px;padding:2px 6px;font-family:Orbitron,sans-serif;font-size:8px;letter-spacing:1px;color:#c9952c;border:1px solid rgba(201,149,44,0.5);border-radius:4px;text-transform:uppercase">\u2605 Trusted</span>';
  if (score >= 50) return '<span title="Verified Reviewer (' + s.count + ' reviews, ' + days + ' days)" style="display:inline-block;margin-left:6px;padding:2px 6px;font-family:Orbitron,sans-serif;font-size:8px;letter-spacing:1px;color:#00c864;border:1px solid rgba(0,200,100,0.4);border-radius:4px;text-transform:uppercase">\u2713 Verified</span>';
  if (score >= 25) return '<span title="Established Reviewer (' + s.count + ' reviews)" style="display:inline-block;margin-left:6px;padding:2px 6px;font-family:Orbitron,sans-serif;font-size:8px;letter-spacing:1px;color:#888;border:1px solid rgba(136,136,136,0.4);border-radius:4px;text-transform:uppercase">Established</span>';
  return '';
}

async function submitReview(venue, girlName, ratings, comment) {
  const { data: { user } } = await sbClient.auth.getUser();
  if (!user) return { error: 'Not logged in' };
  const userName = user.user_metadata?.display_name || user.user_metadata?.name || user.email.split('@')[0];
  const { data, error } = await sbClient.from('reviews').upsert({
    user_id: user.id, user_name: userName, venue, girl_name: girlName,
    overall: ratings.overall, professionalism: ratings.professionalism, experience: ratings.experience,
    presentation: ratings.presentation, safety: ratings.safety, transparency: ratings.transparency,
    room_quality: ratings.room_quality, visit_date: ratings.visit_date || null, duration: ratings.duration || null,
    comment: comment.substring(0, 1000),
  }, { onConflict: 'user_id,venue,girl_name' }).select();
  if (error) { console.error('Submit review error:', error); return { error: error.message }; }
  return { data };
}

async function deleteReview(reviewId) {
  const { error } = await sbClient.from('reviews').delete().eq('id', reviewId);
  if (error) { console.error('Delete review error:', error); return { error: error.message }; }
  return { success: true };
}

async function loadReplies(reviewIds) {
  if (!reviewIds.length) return [];
  const { data, error } = await sbClient.from('review_replies').select('*').in('review_id', reviewIds).order('created_at', { ascending: true });
  if (error) { console.error('Load replies error:', error); return []; }
  return data || [];
}

async function submitReply(reviewId, comment) {
  const { data: { user } } = await sbClient.auth.getUser();
  if (!user) return { error: 'Not logged in' };
  const userName = user.user_metadata?.display_name || user.user_metadata?.name || user.email.split('@')[0];
  const { data, error } = await sbClient.from('review_replies').upsert({
    review_id: reviewId, user_id: user.id, user_name: userName,
    comment: comment.substring(0, 200),
  }, { onConflict: 'review_id,user_id' }).select();
  if (error) { console.error('Submit reply error:', error); return { error: error.message }; }
  return { data };
}

async function deleteReply(replyId) {
  const { error } = await sbClient.from('review_replies').delete().eq('id', replyId);
  if (error) return { error: error.message };
  return { success: true };
}

async function notifyReviewReply(review, replyUserName, replyComment, girl) {
  // Insert bell notification for the review author
  await sbClient.from('notifications').insert({
    user_id: review.user_id,
    type: 'review_reply',
    title: 'New reply to your review',
    body: replyUserName + ' replied to your review of ' + review.girl_name + ': "' + replyComment.substring(0, 100) + (replyComment.length > 100 ? '...' : '') + '"',
    venue: review.venue,
    girl_name: review.girl_name,
  });
  // Send email via worker
  try {
    await fetch('https://brothel-search-sync.travanixlabs.workers.dev/api/notify-reply', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        review_user_id: review.user_id,
        review_user_name: review.user_name,
        girl_name: review.girl_name,
        venue: review.venue,
        reply_user_name: replyUserName,
        reply_comment: replyComment,
      }),
    });
  } catch (e) { console.error('Reply notification email error:', e); }
}

const REVIEW_LABELS = { overall: 'Overall', professionalism: 'Professionalism & Communication', experience: 'Experience Quality', presentation: 'Appearance & Presentation', safety: 'Safety & Respect', transparency: 'Value & Transparency', room_quality: 'Room Quality' };
const VENUE_REVIEW_LABELS = { overall: 'Overall', professionalism: 'Staff & Service', experience: 'Wait Time & Efficiency', presentation: 'Ambiance & Atmosphere', safety: 'Discretion & Privacy', transparency: 'Value for Money', room_quality: 'Cleanliness & Rooms' };

function renderStars(rating, interactive, category) {
  let html = '<div class="review-stars' + (interactive ? ' review-stars-interactive' : '') + '"' + (category ? ' data-category="' + category + '"' : '') + '>';
  for (let i = 1; i <= 5; i++) {
    html += '<span class="review-star' + (i <= rating ? ' active' : '') + '" data-value="' + i + '">\u2605</span>';
  }
  html += '</div>';
  return html;
}

function averageRatings(reviews) {
  if (!reviews.length) return null;
  const fields = ['overall', 'professionalism', 'experience', 'presentation', 'safety', 'transparency', 'room_quality'];
  const avg = {};
  for (const f of fields) {
    avg[f] = (reviews.reduce((sum, r) => sum + (r[f] || 0), 0) / reviews.length).toFixed(1);
  }
  avg.count = reviews.length;
  return avg;
}

function buildSimilarGirls(g) {
  const scoreGirl = gg => {
    let score = 0;
    const gc = Array.isArray(g.country) ? g.country : [g.country || ''];
    const ggc = Array.isArray(gg.country) ? gg.country : [gg.country || ''];
    if (gc.some(c => ggc.includes(c))) score += 30;
    if (g.age && gg.age && Math.abs(parseInt(g.age) - parseInt(gg.age)) <= 3) score += 20;
    if (g.body && gg.body && Math.abs(parseInt(g.body) - parseInt(gg.body)) <= 1) score += 15;
    if (g.height && gg.height && Math.abs(parseInt(g.height) - parseInt(gg.height)) <= 5) score += 10;
    if (g.cup && gg.cup && g.cup.toUpperCase() === gg.cup.toUpperCase()) score += 10;
    if (g.val1 && gg.val1 && Math.abs(parseInt(g.val1) - parseInt(gg.val1)) <= 30) score += 15;
    return score;
  };

  const candidates = allGirls.filter(gg => gg !== g && gg.venue + gg.name !== g.venue + g.name).map(gg => ({ girl: gg, score: scoreGirl(gg) }));
  const sameVenue = candidates.filter(s => s.girl.venue === g.venue && s.score >= 30).sort((a, b) => b.score - a.score).slice(0, 6);
  const otherVenues = candidates.filter(s => s.girl.venue !== g.venue && s.score >= 30).sort((a, b) => b.score - a.score).slice(0, 6);

  if (!sameVenue.length && !otherVenues.length) return '';

  const renderRow = (title, items) => {
    if (!items.length) return '';
    let h = '<div class="venue-divider"><span>\u2014 ' + title + ' \u2014</span></div>';
    h += '<div style="display:flex;gap:14px;overflow-x:auto;padding-bottom:12px;justify-content:center;flex-wrap:wrap">';
    for (const s of items) {
      const gg = s.girl;
      const img = gg.photos && gg.photos[0] ? '<img src="' + imgProxy(gg.photos[0]) + '" alt="' + (gg.name||'') + '" style="width:100px;height:133px;object-fit:cover;border-radius:10px;display:block;border:1px solid rgba(201,149,44,0.15)">' : '';
      h += '<div style="flex-shrink:0;cursor:pointer;text-align:center" onclick="showProfile(allGirls.find(g=>g.venue===\'' + gg.venue + '\'&&g.name===\'' + (gg.name||'').replace(/'/g, "\\'") + '\'))">';
      h += img;
      h += '<div style="font-family:Playfair Display,serif;font-size:12px;color:var(--gold);margin-top:6px">' + (gg.name||'') + '</div>';
      h += '<div style="font-size:9px;color:var(--text-dim)">' + (gg.venueName||'') + '</div>';
      h += '</div>';
    }
    h += '</div>';
    return h;
  };

  let html = '<div style="margin-top:20px;border-top:1px solid rgba(201,149,44,0.15);padding-top:16px">';
  html += renderRow('SIMILAR AT ' + (g.venueName || 'THIS VENUE').toUpperCase(), sameVenue);
  html += renderRow('SIMILAR AT OTHER VENUES', otherVenues);
  html += '</div>';
  return html;
}

function buildReviewSection(g, reviews) {
  const avg = averageRatings(reviews);
  const categories = ['overall', 'professionalism', 'experience', 'presentation', 'safety', 'transparency', 'room_quality'];

  let html = '<div class="review-section" style="margin-top:20px;border-top:1px solid rgba(201,149,44,0.15);padding-top:16px">';
  html += '<div style="font-family:Playfair Display,serif;font-size:18px;font-weight:700;color:var(--gold);margin-bottom:16px">Reviews</div>';

  // Average ratings summary
  if (avg) {
    html += '<div class="review-summary">';
    html += '<div class="review-avg-score">' + avg.overall + '</div>';
    html += '<div class="review-avg-detail">';
    html += '<div style="font-size:14px;color:var(--text);margin-bottom:4px">' + avg.count + ' review' + (avg.count !== 1 ? 's' : '') + '</div>';
    for (const cat of categories) {
      html += '<div class="review-avg-row"><span>' + (REVIEW_LABELS[cat] || cat) + '</span><div class="review-bar"><div class="review-bar-fill" style="width:' + (avg[cat] / 5 * 100) + '%"></div></div><span>' + avg[cat] + '</span></div>';
    }
    html += '</div></div>';
    const topComment = reviews.find(r => r.comment && r.comment.length > 20);
    if (topComment) {
      html += '<div class="review-highlight"><span class="review-highlight-star">\u2605</span> "' + topComment.comment.replace(/</g, '&lt;').substring(0, 150) + (topComment.comment.length > 150 ? '...' : '') + '" <span class="review-highlight-author">\u2014 ' + topComment.user_name + '</span></div>';
    }
  } else {
    html += '<div class="empty-msg" style="padding:32px 20px;margin-bottom:16px"><svg width="60" height="60" viewBox="0 0 60 60" fill="none" style="margin-bottom:12px"><circle cx="30" cy="30" r="28" stroke="rgba(201,149,44,0.25)" stroke-width="1.5"/><text x="30" y="36" text-anchor="middle" font-size="24" fill="rgba(201,149,44,0.3)">\u2605</text></svg><div>No reviews yet. Be the first to review!</div></div>';
  }

  // Review form (only for logged-in users)
  html += '<div id="reviewFormContainer"></div>';

  // Review list
  html += '<div id="reviewList">';
  for (const r of reviews) {
    html += renderReviewCard(r);
  }
  html += '</div>';

  html += '</div>';
  return html;
}

function buildVenueReviewSection(venueId, reviews) {
  const avg = averageRatings(reviews);
  const categories = ['overall', 'professionalism', 'experience', 'presentation', 'safety', 'transparency', 'room_quality'];
  let html = '<div class="venue-review-section" data-venue-id="' + venueId + '" style="margin:32px 0;border-top:1px solid rgba(201,149,44,0.15);padding-top:24px">';
  html += '<div class="venue-divider"><span>\u2014 VENUE REVIEWS \u2014</span></div>';
  if (avg) {
    html += '<div class="review-summary">';
    html += '<div class="review-avg-score">' + avg.overall + '</div>';
    html += '<div class="review-avg-detail">';
    html += '<div style="font-size:14px;color:var(--text);margin-bottom:4px">' + avg.count + ' review' + (avg.count !== 1 ? 's' : '') + '</div>';
    for (const cat of categories) {
      html += '<div class="review-avg-row"><span>' + (VENUE_REVIEW_LABELS[cat] || cat) + '</span><div class="review-bar"><div class="review-bar-fill" style="width:' + (avg[cat] / 5 * 100) + '%"></div></div><span>' + avg[cat] + '</span></div>';
    }
    html += '</div></div>';
    const topComment = reviews.find(r => r.comment && r.comment.length > 20);
    if (topComment) {
      html += '<div class="review-highlight"><span class="review-highlight-star">\u2605</span> "' + topComment.comment.replace(/</g, '&lt;').substring(0, 150) + (topComment.comment.length > 150 ? '...' : '') + '" <span class="review-highlight-author">\u2014 ' + topComment.user_name + '</span></div>';
    }
  } else {
    html += '<div class="empty-msg" style="padding:32px 20px;margin-bottom:16px"><svg width="60" height="60" viewBox="0 0 60 60" fill="none" style="margin-bottom:12px"><circle cx="30" cy="30" r="28" stroke="rgba(201,149,44,0.25)" stroke-width="1.5"/><text x="30" y="36" text-anchor="middle" font-size="24" fill="rgba(201,149,44,0.3)">\u2605</text></svg><div>No venue reviews yet. Be the first to review!</div></div>';
  }
  html += '<div id="venueReviewFormContainer"></div>';
  html += '<div id="venueReviewList">';
  for (const r of reviews) html += renderVenueReviewCard(r);
  html += '</div></div>';
  return html;
}

function renderVenueReviewCard(r, replies, currentUserId) {
  const categories = ['overall', 'professionalism', 'experience', 'presentation', 'safety', 'transparency', 'room_quality'];
  let html = '<div class="review-card" data-review-id="' + r.id + '">';
  html += '<div class="review-card-header"><div class="review-card-user">' + (r.user_name || 'Anonymous') + credibilityBadge(r.user_id) + '</div>';
  html += '<div class="review-card-date">' + new Date(r.created_at).toLocaleDateString('en-AU', { day: 'numeric', month: 'short', year: 'numeric' }) + '</div></div>';
  html += '<div class="review-card-ratings">';
  for (const cat of categories) {
    html += '<div class="review-card-rating"><span>' + (VENUE_REVIEW_LABELS[cat] || cat) + '</span>' + renderStars(r[cat], false) + '</div>';
  }
  html += '</div>';
  if (r.comment) html += '<div class="review-card-comment">' + r.comment.replace(/</g, '&lt;').replace(/>/g, '&gt;') + '</div>';
  const reviewReplies = (replies || []).filter(rp => rp.review_id === r.id);
  if (reviewReplies.length) {
    html += '<div class="review-replies">';
    for (const rp of reviewReplies) {
      const rpDate = new Date(rp.created_at).toLocaleDateString('en-AU', { day: 'numeric', month: 'short' });
      const isOwn = currentUserId && rp.user_id === currentUserId;
      html += '<div class="review-reply"><div class="review-reply-header"><span class="review-reply-user">' + (rp.user_name || 'Anonymous') + '</span><span class="review-reply-date">' + rpDate + '</span></div>';
      html += '<div class="review-reply-text">' + rp.comment.replace(/</g, '&lt;').replace(/>/g, '&gt;') + '</div>';
      if (isOwn) html += '<button class="review-reply-delete" data-reply-id="' + rp.id + '">Delete</button>';
      html += '</div>';
    }
    html += '</div>';
  }
  const alreadyReplied = currentUserId && reviewReplies.some(rp => rp.user_id === currentUserId);
  if (currentUserId && !alreadyReplied) {
    html += '<div class="review-reply-actions"><button class="review-reply-btn" data-review-id="' + r.id + '">Reply</button></div>';
    html += '<div class="review-reply-form" data-review-id="' + r.id + '" style="display:none">';
    html += '<textarea class="review-reply-textarea" placeholder="Write a reply (max 200 chars)" maxlength="200"></textarea>';
    html += '<div style="display:flex;gap:8px;align-items:center"><button class="review-reply-submit" data-review-id="' + r.id + '">Submit</button><button class="review-reply-cancel" data-review-id="' + r.id + '">Cancel</button><span class="review-reply-msg"></span></div>';
    html += '</div>';
  }
  html += '</div>';
  return html;
}

async function initVenueReviewSection(venueId) {
  await loadReviewerStats();
  const reviews = await loadReviews(venueId, '__venue__');
  let container = document.querySelector('.venue-review-section');
  if (!container) return;
  container.outerHTML = buildVenueReviewSection(venueId, reviews);
  container = document.querySelector('.venue-review-section');
  if (!container) return;

  const reviewIds = reviews.map(r => r.id);
  const allReplies = await loadReplies(reviewIds);
  const { data: { user } } = await sbClient.auth.getUser();
  const currentUserId = user ? user.id : null;

  const listEl = document.getElementById('venueReviewList');
  if (listEl) {
    listEl.innerHTML = reviews.map(r => renderVenueReviewCard(r, allReplies, currentUserId)).join('');
    listEl.querySelectorAll('.review-reply-btn').forEach(btn => {
      btn.onclick = () => { const rid = btn.dataset.reviewId; const form = listEl.querySelector('.review-reply-form[data-review-id="' + rid + '"]'); if (form) { form.style.display = ''; btn.style.display = 'none'; } };
    });
    listEl.querySelectorAll('.review-reply-cancel').forEach(btn => {
      btn.onclick = () => { const rid = btn.dataset.reviewId; const form = listEl.querySelector('.review-reply-form[data-review-id="' + rid + '"]'); const rb = listEl.querySelector('.review-reply-btn[data-review-id="' + rid + '"]'); if (form) form.style.display = 'none'; if (rb) rb.style.display = ''; };
    });
    listEl.querySelectorAll('.review-reply-submit').forEach(btn => {
      btn.onclick = async () => {
        const rid = btn.dataset.reviewId;
        const form = listEl.querySelector('.review-reply-form[data-review-id="' + rid + '"]');
        const comment = form.querySelector('.review-reply-textarea').value.trim();
        if (!comment) { form.querySelector('.review-reply-msg').textContent = 'Reply cannot be empty'; return; }
        btn.disabled = true; btn.textContent = 'Saving...';
        await submitReply(rid, comment);
        initVenueReviewSection(venueId);
      };
    });
    listEl.querySelectorAll('.review-reply-delete').forEach(btn => {
      btn.onclick = async () => { if (!confirm('Delete your reply?')) return; await deleteReply(btn.dataset.replyId); initVenueReviewSection(venueId); };
    });
  }

  const formContainer = document.getElementById('venueReviewFormContainer');
  if (!formContainer) return;
  if (!user) { formContainer.innerHTML = '<div style="color:var(--text-dim);font-size:13px;margin:16px 0">Log in to leave a venue review.</div>'; return; }

  const existingReview = reviews.find(r => r.user_id === user.id);
  const categories = ['overall', 'professionalism', 'experience', 'presentation', 'safety', 'transparency', 'room_quality'];
  let formHtml = '<div class="review-form">';
  formHtml += '<div style="font-family:Orbitron,sans-serif;font-size:11px;letter-spacing:2px;text-transform:uppercase;color:var(--gold);margin-bottom:12px">' + (existingReview ? 'Update Your Venue Review' : 'Review This Venue') + '</div>';
  for (const cat of categories) {
    const val = existingReview ? existingReview[cat] : 0;
    formHtml += '<div class="review-form-row"><label>' + (VENUE_REVIEW_LABELS[cat] || cat) + '</label>' + renderStars(val, true, cat) + '</div>';
  }
  formHtml += '<textarea id="venueReviewComment" class="review-textarea" placeholder="Share your experience at this venue (optional, max 1000 chars)" maxlength="1000">' + (existingReview ? (existingReview.comment || '') : '') + '</textarea>';
  formHtml += '<div style="display:flex;gap:8px;align-items:center">';
  formHtml += '<button class="review-submit" id="venueReviewSubmitBtn">' + (existingReview ? 'Update Review' : 'Submit Review') + '</button>';
  if (existingReview) formHtml += '<button class="review-delete" id="venueReviewDeleteBtn">Delete</button>';
  formHtml += '<span id="venueReviewMsg" style="font-size:12px;color:var(--gold)"></span>';
  formHtml += '</div></div>';
  formContainer.innerHTML = formHtml;

  formContainer.querySelectorAll('.review-stars-interactive').forEach(starsEl => {
    starsEl.querySelectorAll('.review-star').forEach(star => {
      star.addEventListener('click', function() {
        const val = parseInt(this.dataset.value);
        starsEl.querySelectorAll('.review-star').forEach((s, i) => s.classList.toggle('active', i < val));
      });
    });
  });

  document.getElementById('venueReviewSubmitBtn').addEventListener('click', async function() {
    const ratings = {};
    let allRated = true;
    formContainer.querySelectorAll('.review-stars-interactive').forEach(starsEl => {
      const cat = starsEl.dataset.category;
      ratings[cat] = starsEl.querySelectorAll('.review-star.active').length;
      if (ratings[cat] === 0) allRated = false;
    });
    if (!allRated) { document.getElementById('venueReviewMsg').textContent = 'Please rate all categories'; return; }
    const comment = document.getElementById('venueReviewComment').value.trim();
    this.disabled = true; this.textContent = 'Saving...';
    const result = await submitReview(venueId, '__venue__', ratings, comment);
    if (result.error) { document.getElementById('venueReviewMsg').textContent = result.error; this.disabled = false; this.textContent = existingReview ? 'Update Review' : 'Submit Review'; }
    else { initVenueReviewSection(venueId); }
  });

  const deleteBtn = document.getElementById('venueReviewDeleteBtn');
  if (deleteBtn && existingReview) {
    deleteBtn.addEventListener('click', async function() {
      if (!confirm('Delete your venue review?')) return;
      this.disabled = true;
      await deleteReview(existingReview.id);
      initVenueReviewSection(venueId);
    });
  }
}

function renderReviewCard(r, replies, currentUserId) {
  const categories = ['overall', 'professionalism', 'experience', 'presentation', 'safety', 'transparency', 'room_quality'];
  let html = '<div class="review-card" data-review-id="' + r.id + '">';
  html += '<div class="review-card-header">';
  html += '<div class="review-card-user">' + (r.user_name || 'Anonymous') + credibilityBadge(r.user_id) + '</div>';
  html += '<div class="review-card-date">' + new Date(r.created_at).toLocaleDateString('en-AU', { day: 'numeric', month: 'short', year: 'numeric' }) + '</div>';
  html += '</div>';
  html += '<div class="review-card-ratings">';
  for (const cat of categories) {
    html += '<div class="review-card-rating"><span>' + (REVIEW_LABELS[cat] || cat) + '</span>' + renderStars(r[cat], false) + '</div>';
  }
  html += '</div>';
  if (r.visit_date || r.duration) {
    let visitInfo = '';
    if (r.visit_date) {
      const vd = new Date(r.visit_date);
      visitInfo = 'Visited: ' + vd.toLocaleDateString('en-AU', { day: 'numeric', month: 'short', year: 'numeric' }) + (r.visit_date.includes('T') && !r.visit_date.endsWith('T00:00') ? ' at ' + vd.toLocaleTimeString('en-AU', { hour: 'numeric', minute: '2-digit' }) : '');
    }
    if (r.duration) visitInfo += (visitInfo ? ' \u00b7 ' : '') + r.duration;
    html += '<div style="font-family:Orbitron,sans-serif;font-size:9px;letter-spacing:1px;color:var(--text-dim);margin-top:6px">' + visitInfo + '</div>';
  }
  if (r.comment) html += '<div class="review-card-comment">' + r.comment.replace(/</g, '&lt;').replace(/>/g, '&gt;') + '</div>';

  // Replies
  const reviewReplies = (replies || []).filter(rp => rp.review_id === r.id);
  if (reviewReplies.length) {
    html += '<div class="review-replies">';
    for (const rp of reviewReplies) {
      const rpDate = new Date(rp.created_at).toLocaleDateString('en-AU', { day: 'numeric', month: 'short' });
      const isOwn = currentUserId && rp.user_id === currentUserId;
      html += '<div class="review-reply">';
      html += '<div class="review-reply-header"><span class="review-reply-user">' + (rp.user_name || 'Anonymous') + '</span><span class="review-reply-date">' + rpDate + '</span></div>';
      html += '<div class="review-reply-text">' + rp.comment.replace(/</g, '&lt;').replace(/>/g, '&gt;') + '</div>';
      if (isOwn) html += '<button class="review-reply-delete" data-reply-id="' + rp.id + '">Delete</button>';
      html += '</div>';
    }
    html += '</div>';
  }

  // Reply button (only if logged in, not own review, and haven't already replied)
  const alreadyReplied = currentUserId && reviewReplies.some(rp => rp.user_id === currentUserId);
  const isOwnReview = currentUserId && r.user_id === currentUserId;
  if (currentUserId && !alreadyReplied) {
    html += '<div class="review-reply-actions">';
    html += '<button class="review-reply-btn" data-review-id="' + r.id + '">Reply</button>';
    html += '</div>';
    html += '<div class="review-reply-form" data-review-id="' + r.id + '" style="display:none">';
    html += '<textarea class="review-reply-textarea" placeholder="Write a reply (max 200 chars)" maxlength="200"></textarea>';
    html += '<div style="display:flex;gap:8px;align-items:center"><button class="review-reply-submit" data-review-id="' + r.id + '">Submit</button><button class="review-reply-cancel" data-review-id="' + r.id + '">Cancel</button><span class="review-reply-msg"></span></div>';
    html += '</div>';
  }

  html += '</div>';
  return html;
}

async function initReviewSection(g) {
  await loadReviewerStats();
  const reviews = await loadReviews(g.venue, g.name);
  const rcEl = document.getElementById('profileReviewCount');
  if (rcEl) rcEl.textContent = reviews.length + ' review' + (reviews.length !== 1 ? 's' : '');
  let container = document.querySelector('.review-section');
  if (!container) return;

  // Rebuild the summary section with actual data
  container.outerHTML = buildReviewSection(g, reviews);
  container = document.querySelector('.review-section');
  if (!container) return;

  // Load replies for all reviews
  const reviewIds = reviews.map(r => r.id);
  const allReplies = await loadReplies(reviewIds);

  const { data: { user } } = await sbClient.auth.getUser();
  const currentUserId = user ? user.id : null;

  // Populate review list
  const listEl = document.getElementById('reviewList');
  if (listEl) {
    listEl.innerHTML = reviews.map(r => renderReviewCard(r, allReplies, currentUserId)).join('');

    // Reply button handlers
    listEl.querySelectorAll('.review-reply-btn').forEach(btn => {
      btn.onclick = () => {
        const rid = btn.dataset.reviewId;
        const form = listEl.querySelector('.review-reply-form[data-review-id="' + rid + '"]');
        if (form) { form.style.display = ''; btn.style.display = 'none'; }
      };
    });
    listEl.querySelectorAll('.review-reply-cancel').forEach(btn => {
      btn.onclick = () => {
        const rid = btn.dataset.reviewId;
        const form = listEl.querySelector('.review-reply-form[data-review-id="' + rid + '"]');
        const replyBtn = listEl.querySelector('.review-reply-btn[data-review-id="' + rid + '"]');
        if (form) form.style.display = 'none';
        if (replyBtn) replyBtn.style.display = '';
      };
    });
    listEl.querySelectorAll('.review-reply-submit').forEach(btn => {
      btn.onclick = async () => {
        const rid = btn.dataset.reviewId;
        const form = listEl.querySelector('.review-reply-form[data-review-id="' + rid + '"]');
        const textarea = form.querySelector('.review-reply-textarea');
        const msg = form.querySelector('.review-reply-msg');
        const comment = textarea.value.trim();
        if (!comment) { msg.textContent = 'Reply cannot be empty'; return; }
        btn.disabled = true; btn.textContent = 'Saving...';
        const result = await submitReply(rid, comment);
        if (result.error) { msg.textContent = result.error; btn.disabled = false; btn.textContent = 'Submit'; return; }
        // Notify the review author
        const review = reviews.find(r => r.id === rid);
        if (review && review.user_id !== currentUserId) {
          const userName = user.user_metadata?.display_name || user.user_metadata?.name || user.email.split('@')[0];
          await notifyReviewReply(review, userName, comment, g);
        }
        initReviewSection(g);
      };
    });
    listEl.querySelectorAll('.review-reply-delete').forEach(btn => {
      btn.onclick = async () => {
        if (!confirm('Delete your reply?')) return;
        await deleteReply(btn.dataset.replyId);
        initReviewSection(g);
      };
    });
  }

  // Show review form for logged-in users
  const formContainer = document.getElementById('reviewFormContainer');
  if (!formContainer) return;

  if (!user) {
    formContainer.innerHTML = '<div style="color:var(--text-dim);font-size:13px;margin:16px 0">Log in to leave a review.</div>';
    return;
  }

  const existingReview = reviews.find(r => r.user_id === user.id);
  const categories = ['overall', 'professionalism', 'experience', 'presentation', 'safety', 'transparency', 'room_quality'];

  let formHtml = '<div class="review-form">';
  formHtml += '<div style="font-family:Orbitron,sans-serif;font-size:11px;letter-spacing:2px;text-transform:uppercase;color:var(--gold);margin-bottom:12px">' + (existingReview ? 'Update Your Review' : 'Leave a Review') + '</div>';

  for (const cat of categories) {
    const val = existingReview ? existingReview[cat] : 0;
    formHtml += '<div class="review-form-row"><label>' + (REVIEW_LABELS[cat] || cat) + '</label>' + renderStars(val, true, cat) + '</div>';
  }

  const todayStr = new Date().toISOString().split('T')[0];
  const existingDate = existingReview && existingReview.visit_date ? existingReview.visit_date.substring(0, 10) : todayStr;
  const fmtDateLabel = d => { const dt = new Date(d + 'T00:00:00'); return dt.toLocaleDateString('en-AU', { day: 'numeric', month: 'short', year: 'numeric' }); };
  formHtml += '<input type="hidden" id="reviewVisitDate" value="' + existingDate + '">';
  const existingDuration = existingReview ? (existingReview.duration || '') : '';
  formHtml += '<input type="hidden" id="reviewDuration" value="' + existingDuration + '">';
  const durations = ['30 Mins','45 Mins','1 Hour','1 Hour 15 Mins','1 Hour 30 Mins','1 Hour 45 Mins','2 Hours or More'];
  const durLabel = existingDuration || 'Duration';
  formHtml += '<div class="review-form-row" style="margin-top:8px;margin-bottom:12px"><label>Date & Duration</label><div style="display:flex;gap:8px;align-items:center;position:relative"><button type="button" class="review-date-input" id="reviewDateBtn">' + fmtDateLabel(existingDate) + '</button><button type="button" class="review-date-input" id="reviewDurBtn">' + durLabel + '</button></div></div>';

  formHtml += '<textarea id="reviewComment" class="review-textarea" placeholder="Share your experience (optional, max 1000 chars)" maxlength="1000">' + (existingReview ? (existingReview.comment || '') : '') + '</textarea>';
  formHtml += '<div style="display:flex;gap:8px;align-items:center">';
  formHtml += '<button class="review-submit" id="reviewSubmitBtn">' + (existingReview ? 'Update Review' : 'Submit Review') + '</button>';
  if (existingReview) formHtml += '<button class="review-delete" id="reviewDeleteBtn">Delete</button>';
  formHtml += '<span id="reviewMsg" style="font-size:12px;color:var(--gold)"></span>';
  formHtml += '</div></div>';

  formContainer.innerHTML = formHtml;

  // Star click handlers
  formContainer.querySelectorAll('.review-stars-interactive').forEach(starsEl => {
    starsEl.querySelectorAll('.review-star').forEach(star => {
      star.addEventListener('click', function() {
        const val = parseInt(this.dataset.value);
        starsEl.querySelectorAll('.review-star').forEach((s, i) => s.classList.toggle('active', i < val));
      });
    });
  });

  // Date picker popup
  document.getElementById('reviewDateBtn').addEventListener('click', function() {
    const btn = this;
    if (document.getElementById('reviewDatePopup')) { document.getElementById('reviewDatePopup').remove(); return; }
    if (document.getElementById('reviewTimePopup')) document.getElementById('reviewTimePopup').remove();
    const current = document.getElementById('reviewVisitDate').value || todayStr;
    let viewYear = parseInt(current.substring(0, 4));
    let viewMonth = parseInt(current.substring(5, 7)) - 1;

    const popup = document.createElement('div');
    popup.id = 'reviewDatePopup';
    popup.className = 'review-picker-popup';
    btn.parentElement.appendChild(popup);

    function renderCal() {
      const selDate = document.getElementById('reviewVisitDate').value;
      const first = new Date(viewYear, viewMonth, 1);
      const last = new Date(viewYear, viewMonth + 1, 0);
      const startDay = first.getDay();
      const monthNames = ['January','February','March','April','May','June','July','August','September','October','November','December'];

      let h = '<div class="review-picker-header">';
      h += '<button type="button" class="review-picker-nav" id="rpPrev">&lt;</button>';
      h += '<span class="review-picker-title">' + monthNames[viewMonth] + ' ' + viewYear + '</span>';
      h += '<button type="button" class="review-picker-nav" id="rpNext">&gt;</button>';
      h += '</div>';
      h += '<div class="review-picker-days">';
      ['S','M','T','W','T','F','S'].forEach(d => { h += '<span class="review-picker-dayhead">' + d + '</span>'; });
      for (let i = 0; i < startDay; i++) h += '<span></span>';
      for (let d = 1; d <= last.getDate(); d++) {
        const ds = viewYear + '-' + String(viewMonth + 1).padStart(2, '0') + '-' + String(d).padStart(2, '0');
        const isToday = ds === todayStr;
        const isSel = ds === selDate;
        h += '<button type="button" class="review-picker-day' + (isSel ? ' selected' : '') + (isToday ? ' today' : '') + '" data-date="' + ds + '">' + d + '</button>';
      }
      h += '</div>';
      popup.innerHTML = h;

      popup.querySelector('#rpPrev').onclick = e => { e.stopPropagation(); viewMonth--; if (viewMonth < 0) { viewMonth = 11; viewYear--; } renderCal(); };
      popup.querySelector('#rpNext').onclick = e => { e.stopPropagation(); viewMonth++; if (viewMonth > 11) { viewMonth = 0; viewYear++; } renderCal(); };
      popup.querySelectorAll('.review-picker-day').forEach(el => {
        el.onclick = e => {
          e.stopPropagation();
          document.getElementById('reviewVisitDate').value = el.dataset.date;
          const dt = new Date(el.dataset.date + 'T00:00:00');
          btn.textContent = dt.toLocaleDateString('en-AU', { day: 'numeric', month: 'short', year: 'numeric' });
          popup.remove();
        };
      });
    }
    renderCal();
    const closeOnClick = e => { if (!popup.contains(e.target) && e.target !== btn) { popup.remove(); document.removeEventListener('click', closeOnClick); } };
    setTimeout(() => document.addEventListener('click', closeOnClick), 0);
  });

  // Duration picker popup
  document.getElementById('reviewDurBtn').addEventListener('click', function() {
    const btn = this;
    if (document.getElementById('reviewDurPopup')) { document.getElementById('reviewDurPopup').remove(); return; }
    if (document.getElementById('reviewDatePopup')) document.getElementById('reviewDatePopup').remove();
    if (document.getElementById('reviewTimePopup')) document.getElementById('reviewTimePopup').remove();
    const popup = document.createElement('div');
    popup.id = 'reviewDurPopup';
    popup.className = 'review-picker-popup review-dur-popup';
    const durs = ['30 Mins','45 Mins','1 Hour','1 Hour 15 Mins','1 Hour 30 Mins','1 Hour 45 Mins','2 Hours or More'];
    const sel = document.getElementById('reviewDuration').value;
    let h = '<div class="review-picker-header"><span class="review-picker-title">Duration</span></div>';
    h += '<div class="review-dur-list">';
    durs.forEach(d => { h += '<button type="button" class="review-dur-opt' + (d === sel ? ' selected' : '') + '" data-dur="' + d + '">' + d + '</button>'; });
    h += '</div>';
    popup.innerHTML = h;
    btn.parentElement.appendChild(popup);

    popup.querySelectorAll('.review-dur-opt').forEach(el => {
      el.onclick = e => {
        e.stopPropagation();
        document.getElementById('reviewDuration').value = el.dataset.dur;
        btn.textContent = el.dataset.dur;
        popup.remove();
      };
    });
    const closeOnClick = e => { if (!popup.contains(e.target) && e.target !== btn) { popup.remove(); document.removeEventListener('click', closeOnClick); } };
    setTimeout(() => document.addEventListener('click', closeOnClick), 0);
  });

  // Submit handler
  document.getElementById('reviewSubmitBtn').addEventListener('click', async function() {
    const ratings = {};
    let allRated = true;
    formContainer.querySelectorAll('.review-stars-interactive').forEach(starsEl => {
      const cat = starsEl.dataset.category;
      const val = starsEl.querySelectorAll('.review-star.active').length;
      ratings[cat] = val;
      if (val === 0) allRated = false;
    });

    if (!allRated) { document.getElementById('reviewMsg').textContent = 'Please rate all categories'; return; }

    const visitDate = document.getElementById('reviewVisitDate').value;
    if (visitDate) ratings.visit_date = visitDate + 'T00:00';
    ratings.duration = document.getElementById('reviewDuration').value || null;

    const comment = document.getElementById('reviewComment').value.trim();
    this.disabled = true;
    this.textContent = 'Saving...';

    const result = await submitReview(g.venue, g.name, ratings, comment);
    if (result.error) {
      document.getElementById('reviewMsg').textContent = result.error;
      this.disabled = false;
      this.textContent = existingReview ? 'Update Review' : 'Submit Review';
    } else {
      // Refresh the whole review section
      const freshReviews = await loadReviews(g.venue, g.name);
      const section = document.querySelector('.review-section');
      if (section) section.outerHTML = buildReviewSection(g, freshReviews);
      initReviewSection(g);
    }
  });

  // Delete handler
  const deleteBtn = document.getElementById('reviewDeleteBtn');
  if (deleteBtn && existingReview) {
    deleteBtn.addEventListener('click', async function() {
      if (!confirm('Delete your review?')) return;
      this.disabled = true;
      await deleteReview(existingReview.id);
      const freshReviews = await loadReviews(g.venue, g.name);
      const section = document.querySelector('.review-section');
      if (section) section.outerHTML = buildReviewSection(g, freshReviews);
      initReviewSection(g);
    });
  }
}

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

const VENUE_REGIONS = {
  ginzaempire: 'cbdandcentral', ginzaclub: 'cbdandcentral', kyoto206: 'cbdandcentral',
  sakura57: 'cbdandcentral', top127: 'cbdandcentral', fantasyclub35: 'innerwest', '429city': 'cbdandcentral',
  pennys77: 'innerwest', thegoldenapple: 'cbdandcentral', blackcatparlour: 'cbdandcentral', bellevue12: 'cbdandcentral',
  thegatewayclub: 'innerwest', marrickvillebrothel: 'innerwest', springhouse: 'innerwest',
  stiletto: 'innerwest', wivesonly: 'innerwest', jinia: 'westernsuburbs',
};
const VENUE_SUBURB_NAMES = {
  ginzaempire: 'Surry Hills', ginzaclub: 'Surry Hills', kyoto206: 'Surry Hills',
  sakura57: 'Surry Hills', top127: 'Chippendale', fantasyclub35: 'Annandale', '429city': 'Haymarket',
  pennys77: 'Newtown', thegoldenapple: 'Surry Hills', blackcatparlour: 'Surry Hills', bellevue12: 'Surry Hills',
  thegatewayclub: 'Petersham', marrickvillebrothel: 'Marrickville', springhouse: 'Marrickville',
  stiletto: 'Camperdown', wivesonly: 'St Peters', jinia: 'Strathfield South',
};
const REGION_NAMES = { cbdandcentral: 'CBD & Central', innerwest: 'Inner West', easternsuburbs: 'Eastern Suburbs', northshore: 'North Shore', northernbeaches: 'Northern Beaches', northwest: 'North West', westernsuburbs: 'Western Suburbs', southwesternsuburbs: 'South Western Suburbs', southernsuburbs: 'Southern Suburbs' };
const REGION_ORDER = ['cbdandcentral', 'innerwest', 'easternsuburbs', 'northshore', 'northernbeaches', 'northwest', 'westernsuburbs', 'southwesternsuburbs', 'southernsuburbs'];
// Backwards compat aliases
const VENUE_SUBURBS = VENUE_REGIONS;
const SUBURB_REGIONS = {}; for (const [k, v] of Object.entries(REGION_NAMES)) SUBURB_REGIONS[k] = v;

function slugify(s) { return (s || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/-+$/, ''); }

function profilePath(g) {
  const name = slugify(g.name);
  const region = VENUE_REGIONS[g.venue] || 'other';
  const suburb = VENUE_DATA[g.venue] ? VENUE_DATA[g.venue].suburbSlug : 'other';
  const country = slugify(Array.isArray(g.country) ? g.country[0] : g.country) || 'other';
  return '/sydney/' + region + '/' + suburb + '/' + g.venue + '/' + country + '/' + name;
}

function findGirlByPath(path) {
  const parts = path.replace(/^\//, '').split('/');
  // New format: /sydney/{region}/{suburb}/{venue}/{country}/{name}
  if (parts.length === 6 && parts[0] === 'sydney') {
    const venue = parts[3];
    const slug = parts[5];
    return allGirls.find(g => g.venue === venue && slugify(g.name) === slug);
  }
  // Previous format: /sydney/{suburb}/{venue}/{country}/{name}
  if (parts.length === 5 && parts[0] === 'sydney') {
    const venue = parts[2];
    const slug = parts[4];
    return allGirls.find(g => g.venue === venue && slugify(g.name) === slug);
  }
  // Older format: /sydney/{suburb}/{venue}/{name}
  if (parts.length === 4 && parts[0] === 'sydney') {
    const venue = parts[2];
    const slug = parts[3];
    return allGirls.find(g => g.venue === venue && slugify(g.name) === slug);
  }
  // Legacy: /{venue}/{name}
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
  if (!isLoggedIn()) {
    document.getElementById('authOverlay').style.display = 'flex';
    document.body.style.overflow = 'hidden';
    return;
  }
  if (isSubscribed !== true && userRole !== 'admin') { showPaywall(); return; }
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
  const landingEl = document.getElementById('landingPage');
  const mainSection = document.querySelector('section.section');
  const countries = countriesWithFlags(g.country);
  const photos = g.photos || [];
  const mainImg = photos.length ? photos[0] : '';

  // Track current profile index in filtered list
  const profileIdx = currentFiltered.indexOf(g);
  window._currentProfileIdx = profileIdx;

  // Render as dedicated page
  landingEl.innerHTML = `<div class="profile-panel${isFavorite(g) ? ' favorited' : ''}">
    <button class="profile-share" onclick="generateBurnLink(this)" title="Copy 4-hour burn link">Share</button>
    <div class="profile-body">
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:12px;flex-wrap:wrap">
      <div class="fav-heart${isFavorite(g) ? ' active' : ''}" id="profileFavHeart" data-url="${(g.oldUrl||'').replace(/"/g,'&quot;')}" onclick="toggleFavorite('${(g.oldUrl||'').replace(/'/g,"\\'")}',event)" style="position:relative;top:auto;left:auto"><svg viewBox="0 0 24 24"><path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"/></svg></div>
      <div class="hide-btn${isHidden(g) ? ' active' : ''}" id="profileHideBtn" data-url="${(g.oldUrl||'').replace(/"/g,'&quot;')}" onclick="toggleHidden('${(g.oldUrl||'').replace(/'/g,"\\'")}',event)" style="position:relative;top:auto;left:auto"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24"/><line x1="1" y1="1" x2="23" y2="23"/></svg></div>
      <div class="country-badge">${g.venueName}</div>
      ${(() => { const k = g.venue + ':' + g.name; const s = matchScores.get(k) || 0; return userPreferences && s > 0 ? '<div class="match-badge' + (s >= 90 ? ' match-gold' : '') + '" style="position:static;pointer-events:auto">' + s + '%</div>' : ''; })()}
      ${isNewProfile(g) ? '<span class="new-badge">New</span>' : isReturnProfile(g) ? '<span class="return-badge">Return</span>' : ''}
      ${g.pornstar ? '<span class="av-badge">AV</span>' : ''}
    </div>
    <div class="profile-name">${g.name || ''}</div>
    <div class="profile-quick-stats">
      <span>${(g.photos || []).length} photos</span>
      <span class="pqs-dot">\u00b7</span>
      <span id="profileReviewCount">0 reviews</span>
      <span class="pqs-dot">\u00b7</span>
      <span>${g.startDate ? Math.round((Date.now() - new Date(g.startDate + 'T00:00:00').getTime()) / 86400000) + ' days' : 'N/A'}</span>
    </div>
    <div class="profile-layout">
      <div class="profile-gallery">
        <div class="profile-main-wrap">
          <img id="profileMainImg" src="${mainImg}" alt="${(g.name || '').replace(/"/g, '&quot;')}" style="${!mainImg ? 'display:none' : 'cursor:pointer'}" onclick="openLightbox(window._profilePhotoIdx || 0)">
          ${photos.length > 1 ? '<div class="photo-counter" id="photoCounter">1 / ' + photos.length + '</div>' : ''}
        </div>
        <div class="profile-thumbs">
          ${photos.map((p, i) => `<img src="${imgProxy(p, 120)}" alt="${(g.name || '')} photo ${i + 1} of ${photos.length}" class="${i === 0 ? 'active' : ''}" onclick="selectProfilePhoto(${i})">`).join('')}
        </div>
      </div>
      <div class="profile-details-wrap">
        <div class="profile-details-grid">
          <div class="profile-detail">
            ${detailRow('Country', countries)}
            ${detailRow('Age', g.age)}
            ${detailRow('Body', g.body)}
            ${detailRow('Height', g.height ? g.height + ' cm' : '')}
            ${detailRow('Cup', g.cup)}
            ${detailRow('30 min', g.val1 ? '$' + g.val1 : '')}
            ${detailRow('45 min', g.val2 ? '$' + g.val2 : '')}
            ${detailRow('60 min', g.val3 ? '$' + g.val3 : '')}
          </div>
          <div class="profile-detail">
            ${detailRow('Start Date', g.startDate)}
            ${g.lastModified ? detailRow('Last Modified', g.lastModified.split('T')[0]) : ''}
            ${(() => {
              if (!g.lastRostered) return '';
              const rd = new Date(g.lastRostered + 'T00:00:00');
              const today = new Date(); today.setHours(0,0,0,0);
              const diff = Math.round((today - rd) / 86400000);
              if (diff < 0) {
                const dayNames = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
                const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
                const label = 'Next: ' + dayNames[rd.getDay()] + ' ' + rd.getDate() + ' ' + monthNames[rd.getMonth()];
                return '<div class="profile-detail-row"><span>Latest Availability</span><span><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:#4a9eff;margin-right:8px;box-shadow:0 0 6px #4a9eff40"></span>' + label + '</span></div>';
              }
              const color = diff === 0 ? '#00c864' : diff <= 3 ? '#f5e6a3' : diff <= 7 ? '#c9952c' : '#555';
              const label = diff === 0 ? 'Today' : diff === 1 ? 'Yesterday' : diff + ' days ago';
              return '<div class="profile-detail-row"><span>Latest Availability</span><span><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:' + color + ';margin-right:8px;box-shadow:0 0 6px ' + color + '40"></span>' + label + '</span></div>';
            })()}
            ${(() => { const avail = getAvailabilityText(g); return avail && avail !== 'ended' ? '<div class="profile-detail-row"><span>Availability</span><span class="' + (avail.startsWith('Available Now') ? 'available-now' : avail.startsWith('Available Later') ? 'available-later' : avail.startsWith('Available Future') ? 'available-future' : '') + '">' + avail + '</span></div>' : ''; })()}
            ${detailRow('Experience', g.exp)}
            ${detailRow('Special', g.special)}
            ${detailRow('Language', g.lang)}
            ${detailRow('Type', g.type)}
            ${g.oldUrl ? '<div class="profile-detail-row"><span>Reference</span><span><a href="' + g.oldUrl + '" target="_blank" rel="noopener" style="color:var(--accent);text-decoration:none;word-break:break-all">' + g.oldUrl + '</a></span></div>' : ''}
          </div>
        </div>
        ${g.desc ? '<div class="profile-desc">' + g.desc + '</div>' : ''}
        ${g.labels && g.labels.length ? '<div class="card-labels">' + g.labels.map(l => '<span class="card-label">' + l + '</span>').join('') + '</div>' : ''}
      </div>
    </div>
    ${buildProfileCalendar(g)}
    ${buildRosterPredictions(g)}
    ${buildRosterHistory(g)}
    ${buildReviewSection(g, [])}
    ${buildSimilarGirls(g)}
    </div>
  </div>`;
  landingEl.style.display = '';
  mainSection.style.display = 'none';
  window.scrollTo({ top: 0 });
  // Show nav strip
  setTimeout(renderProfileNavStrip, 100);
  // Load and init reviews
  initReviewSection(g);
  document.body.style.overflow = '';

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
  // Clean up any stale images from previous crossfades
  wrap.querySelectorAll('img:not(#profileMainImg)').forEach(el => el.remove());
  const next = document.createElement('img');
  next.src = photos[idx];
  next.alt = current.alt;
  next.id = '';
  next.style.opacity = '0';
  next.style.transition = 'opacity .5s ease';
  next.style.position = 'absolute';
  next.style.inset = '0';
  next.style.width = '100%';
  next.style.aspectRatio = '3/4';
  next.style.objectFit = 'cover';
  next.style.zIndex = '2';
  wrap.appendChild(next);
  requestAnimationFrame(() => {
    next.style.opacity = '1';
    current.style.opacity = '0';
  });
  setTimeout(() => {
    current.remove();
    next.id = 'profileMainImg';
    next.style.position = '';
    next.style.inset = '';
    next.style.width = '';
    next.style.aspectRatio = '';
    next.style.objectFit = '';
    next.style.zIndex = '';
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

// ── Photo Lightbox ──
function openLightbox(idx) {
  const photos = window._profilePhotos || [];
  if (!photos.length) return;
  window._lightboxIdx = idx || 0;
  let overlay = document.getElementById('lightboxOverlay');
  if (!overlay) {
    overlay = document.createElement('div');
    overlay.id = 'lightboxOverlay';
    overlay.className = 'lightbox-overlay';
    overlay.innerHTML = '<button class="lightbox-close" onclick="closeLightbox()">&times;</button><button class="lightbox-prev" onclick="lightboxNav(-1)">&lsaquo;</button><img class="lightbox-img" id="lightboxImg"><button class="lightbox-next" onclick="lightboxNav(1)">&rsaquo;</button><div class="lightbox-counter" id="lightboxCounter"></div>';
    document.body.appendChild(overlay);
    overlay.addEventListener('click', function(e) { if (e.target === overlay) closeLightbox(); });
  }
  document.getElementById('lightboxImg').src = photos[window._lightboxIdx];
  document.getElementById('lightboxCounter').textContent = (window._lightboxIdx + 1) + ' / ' + photos.length;
  overlay.style.display = 'flex';
  document.body.style.overflow = 'hidden';
}

function closeLightbox() {
  const overlay = document.getElementById('lightboxOverlay');
  if (overlay) overlay.style.display = 'none';
  document.body.style.overflow = '';
}

function lightboxNav(dir) {
  const photos = window._profilePhotos || [];
  if (!photos.length) return;
  window._lightboxIdx = (window._lightboxIdx + dir + photos.length) % photos.length;
  document.getElementById('lightboxImg').src = photos[window._lightboxIdx];
  document.getElementById('lightboxCounter').textContent = (window._lightboxIdx + 1) + ' / ' + photos.length;
}

function getProfileNavVisible() {
  const thumbSize = window.innerWidth <= 600 ? 40 : 52;
  const gap = 6;
  const arrowSpace = 2 * (40 + 12); // two arrows + gaps
  const available = window.innerWidth - arrowSpace - 48; // padding
  return Math.max(3, Math.floor(available / (thumbSize + gap)));
}

function renderProfileNavStrip() {
  const strip = document.getElementById('profileNavStrip');
  const thumbs = document.getElementById('profileNavThumbs');
  const upBtn = document.getElementById('profileNavUp');
  const downBtn = document.getElementById('profileNavDown');
  if (!strip || !thumbs) return;

  const idx = window._currentProfileIdx;
  if (idx < 0 || currentFiltered.length <= 1) { strip.style.display = 'none'; return; }

  strip.style.display = '';

  // Show a window of thumbnails around the current index
  const visCount = getProfileNavVisible();
  const half = Math.floor(visCount / 2);
  let start = Math.max(0, idx - half);
  let end = Math.min(currentFiltered.length, start + visCount);
  if (end - start < visCount) start = Math.max(0, end - visCount);
  window._profileNavStart = start;
  window._profileNavEnd = end;

  let html = '';
  for (let i = start; i < end; i++) {
    const g = currentFiltered[i];
    const isActive = i === idx;
    const photo = g.photos && g.photos[0] ? '<img src="' + imgProxy(g.photos[0], 52) + '" alt="' + (g.name || '').replace(/"/g, '&quot;') + '">' : '<div class="thumb-placeholder">' + (g.name || '?').charAt(0) + '</div>';
    html += '<div class="profile-nav-thumb' + (isActive ? ' active' : '') + '" data-nav-idx="' + i + '" title="' + (g.name || '').replace(/"/g, '&quot;') + '">' + photo + '</div>';
  }
  thumbs.innerHTML = html;

  upBtn.disabled = start === 0 && idx === 0;
  downBtn.disabled = end === currentFiltered.length && idx === currentFiltered.length - 1;

  // Click handlers
  thumbs.querySelectorAll('.profile-nav-thumb').forEach(el => {
    el.onclick = () => {
      const i = parseInt(el.dataset.navIdx);
      if (i !== idx) showProfile(currentFiltered[i]);
    };
  });
  upBtn.onclick = () => { if (idx > 0) showProfile(currentFiltered[idx - 1]); };
  downBtn.onclick = () => { if (idx < currentFiltered.length - 1) showProfile(currentFiltered[idx + 1]); };
}

function navigateProfile(dir) {
  const idx = window._currentProfileIdx;
  if (idx < 0) return;
  const newIdx = idx + dir;
  if (newIdx < 0 || newIdx >= currentFiltered.length) return;
  showProfile(currentFiltered[newIdx]);
}

function closeProfile() {
  clearInterval(window._profileRotate);
  document.getElementById('profileNavStrip').style.display = 'none';
  window._currentProfileIdx = -1;
  document.body.style.overflow = '';
  // Navigate back to profiles
  history.pushState(null, '', '/profiles/' + currentLayout);
  handleLandingRoute('/profiles/' + currentLayout);
}

// Close profile on Escape
document.addEventListener('keydown', e => {
  // Escape — close profile or go back
  if (e.key === 'Escape') {
    if (window._currentProfileIdx >= 0 && document.querySelector('.profile-panel')) { closeProfile(); return; }
    const landing = document.getElementById('landingPage');
    if (landing && landing.style.display !== 'none') { history.pushState(null, '', '/'); showMainSection(); updateMeta('Brothel Search \u2013 Girls, Rosters & Venues', 'Find who\u2019s working today at local Australian brothels. Browse live rosters, girl profiles, photos and availability.', 'https://brothelsearch.com/og-preview.png', 'https://brothelsearch.com/', null); return; }
  }

  // Don't handle shortcuts when typing in inputs
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') return;

  const profileOpen = window._currentProfileIdx >= 0 && !!document.querySelector('.profile-panel');

  // Lightbox navigation
  const lbOverlay = document.getElementById('lightboxOverlay');
  if (lbOverlay && lbOverlay.style.display === 'flex') {
    if (e.key === 'ArrowLeft') { lightboxNav(-1); return; }
    if (e.key === 'ArrowRight') { lightboxNav(1); return; }
    if (e.key === 'Escape') { closeLightbox(); return; }
  }

  // Profile photo navigation
  if (profileOpen) {
    if (e.key === 'ArrowLeft') { const idx = ((window._profilePhotoIdx || 0) - 1 + (window._profilePhotos || []).length) % (window._profilePhotos || []).length; selectProfilePhoto(idx); return; }
    if (e.key === 'ArrowRight') { const idx = ((window._profilePhotoIdx || 0) + 1) % (window._profilePhotos || []).length; selectProfilePhoto(idx); return; }
    return;
  }

  // Grid navigation
  const cards = document.querySelectorAll('#girlsGrid .girl-card');
  if (!cards.length) return;

  if (e.key === 'ArrowRight' || e.key === 'ArrowDown' || e.key === 'j') {
    e.preventDefault();
    const focused = document.querySelector('.girl-card.kb-focus');
    const idx = focused ? Array.from(cards).indexOf(focused) : -1;
    const next = cards[Math.min(idx + 1, cards.length - 1)];
    if (focused) focused.classList.remove('kb-focus');
    next.classList.add('kb-focus');
    next.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
  } else if (e.key === 'ArrowLeft' || e.key === 'ArrowUp' || e.key === 'k') {
    e.preventDefault();
    const focused = document.querySelector('.girl-card.kb-focus');
    const idx = focused ? Array.from(cards).indexOf(focused) : 1;
    const prev = cards[Math.max(idx - 1, 0)];
    if (focused) focused.classList.remove('kb-focus');
    prev.classList.add('kb-focus');
    prev.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
  } else if (e.key === 'Enter') {
    const focused = document.querySelector('.girl-card.kb-focus');
    if (focused) focused.click();
  }
});

// Browser back/forward navigation
window.addEventListener('popstate', () => {
  const path = window.location.pathname;
  if (path === '/profiles' || path === '/profiles/bento' || path === '/profiles/grid' || path === '/profiles/compact' || path === '/profiles/list') {
    clearInterval(window._profileRotate);
    document.getElementById('profileNavStrip').style.display = 'none';
    window._currentProfileIdx = -1;
    document.body.style.overflow = '';
    const parts = path.split('/');
    const layoutPart = parts[2] || 'bento';
    const layout = ['grid','bento','compact','list'].includes(layoutPart) ? layoutPart : 'bento';
    setLayout(layout, false);
    showMainSection();
    updateMeta('Browse All Profiles \u2013 Rosters Included | Brothel Search', 'Browse all girl profiles across Australian brothels. Filter by venue, country, availability, pricing and preferences. Photos, rosters and reviews.', 'https://brothelsearch.com/og-preview.png', 'https://brothelsearch.com/profiles', null);
  } else if (path === '/' || path === '/index.html') {
    handleLandingRoute(path);
  } else if ((path.startsWith('/sydney/') || path === '/working-now' || path === '/compare' || path === '/analytics' || path === '/roadmap') && !findGirlByPath(path)) {
    handleLandingRoute(path);
  } else {
    const g = findGirlByPath(path);
    if (g) showProfile(g);
  }
});
// Profile overlay removed — now renders as page


// Back to top
const btt = document.getElementById('backToTop');
const scrollDownBtn = document.getElementById('scrollDown');
if (window.scrollY <= 400) scrollDownBtn.classList.add('visible');
window.addEventListener('scroll', () => {
  const scrolledDown = window.scrollY > 400;
  btt.classList.toggle('visible', scrolledDown);
  scrollDownBtn.classList.toggle('visible', !scrolledDown);
});
btt.onclick = () => window.scrollTo({ top: 0, behavior: 'smooth' });
scrollDownBtn.onclick = () => window.scrollTo({ top: window.innerHeight, behavior: 'smooth' });

// ── Side-by-Side Compare ──
const compareList = [];
function toggleCompare(venue, name) {
  const idx = compareList.findIndex(c => c.venue === venue && c.name === name);
  if (idx >= 0) { compareList.splice(idx, 1); }
  else if (compareList.length < 10) { compareList.push({ venue, name }); }
  updateCompareTray();
}
function updateCompareTray() {
  const tray = document.getElementById('compareTray');
  const thumbs = document.getElementById('compareTrayThumbs');
  const count = document.getElementById('compareTrayCount');
  tray.style.display = compareList.length > 0 ? '' : 'none';
  count.textContent = compareList.length;
  thumbs.innerHTML = compareList.map(c => {
    const g = allGirls.find(gg => gg.venue === c.venue && gg.name === c.name);
    return g && g.photos && g.photos[0] ? '<img src="' + imgProxy(g.photos[0], 36) + '" title="' + (g.name || '') + '">' : '';
  }).join('');
  // Update all compare buttons on cards
  document.querySelectorAll('.compare-btn').forEach(btn => {
    const inList = compareList.some(c => c.venue === btn.dataset.venue && c.name === btn.dataset.name);
    btn.classList.toggle('in-compare', inList);
    btn.innerHTML = inList ? '<svg viewBox="0 0 24 24" width="12" height="12" fill="currentColor"><path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/></svg>' : '<svg viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M16 3h5v5M4 20L21 3M21 16v5h-5M15 15l6 6M4 4l6 6"/></svg>';
  });
}
document.getElementById('compareTrayBtn').onclick = () => {
  if (compareList.length < 2) return;
  showSideBySideCompare();
};
document.getElementById('compareTrayClear').onclick = () => {
  compareList.length = 0;
  updateCompareTray();
};

function showSideBySideCompare() {
  const girls = compareList.map(c => allGirls.find(g => g.venue === c.venue && g.name === c.name)).filter(Boolean);
  if (girls.length < 2) return;

  const fields = [
    { key: 'venueName', label: 'Venue' },
    { key: 'country', label: 'Country', fmt: v => Array.isArray(v) ? v.join(', ') : v || '' },
    { key: 'age', label: 'Age' },
    { key: 'body', label: 'Body' },
    { key: 'height', label: 'Height' },
    { key: 'cup', label: 'Cup' },
    { key: 'val1', label: '30 Min', fmt: v => v ? '$' + v : '' },
    { key: 'val2', label: '45 Min', fmt: v => v ? '$' + v : '' },
    { key: 'val3', label: '60 Min', fmt: v => v ? '$' + v : '' },
    { key: 'exp', label: 'Experience' },
    { key: 'lang', label: 'Language' },
    { key: 'startDate', label: 'Start Date' },
    { key: 'lastRostered', label: 'Last Rostered' },
  ];

  let html = '<div class="auth-overlay" id="sideBySideOverlay" style="display:flex;z-index:1001;overflow-y:auto">';
  html += '<div class="auth-box" style="max-width:' + Math.min(900, girls.length * 180 + 120) + 'px;width:95%;padding:24px;overflow-x:auto">';
  html += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px"><h1 style="font-size:16px;margin:0">Side-by-Side Compare</h1><button onclick="document.getElementById(\'sideBySideOverlay\').remove();document.body.style.overflow=\'\'" style="background:none;border:none;color:var(--gold);font-size:24px;cursor:pointer">&times;</button></div>';
  html += '<table style="width:100%;border-collapse:collapse;font-size:12px">';

  // Photo row
  html += '<tr><td style="padding:8px 4px;color:var(--text-dim);font-weight:600;vertical-align:top;width:80px"></td>';
  for (const g of girls) {
    const img = g.photos && g.photos[0] ? '<img src="' + imgProxy(g.photos[0]) + '" style="width:100%;max-width:140px;height:180px;object-fit:cover;border-radius:8px;display:block;cursor:pointer" onclick="document.getElementById(\'sideBySideOverlay\').remove();document.body.style.overflow=\'\';showProfile(allGirls.find(gg=>gg.venue===\'' + g.venue + '\'&&gg.name===\'' + (g.name||'').replace(/'/g,"\\'") + '\'))">' : '';
    html += '<td style="padding:8px 4px;text-align:center;vertical-align:top">' + img + '<div style="font-family:Playfair Display,serif;font-size:14px;font-weight:700;color:var(--gold);margin-top:6px">' + (g.name || '') + '</div></td>';
  }
  html += '</tr>';

  // Match score row
  if (userPreferences) {
    html += '<tr style="border-top:1px solid rgba(201,149,44,0.1)"><td style="padding:6px 4px;color:var(--text-dim)">Match</td>';
    for (const g of girls) {
      const score = matchScores.get(g.venue + ':' + g.name) || 0;
      const color = score >= 90 ? '#00c864' : score >= 70 ? '#c9952c' : '#888';
      html += '<td style="padding:6px 4px;text-align:center;color:' + color + ';font-weight:700">' + (score > 0 ? score + '%' : '-') + '</td>';
    }
    html += '</tr>';
  }

  // Data rows
  const highlightFields = new Set(['age', 'body', 'height', 'cup', 'val1', 'val2', 'val3']);
  const cupToNum = c => { const s = String(c || '').toUpperCase().charAt(0); return 'ABCDEFGH'.indexOf(s); };
  for (const f of fields) {
    html += '<tr style="border-top:1px solid rgba(201,149,44,0.06)"><td style="padding:6px 4px;color:var(--text-dim)">' + f.label + '</td>';
    const vals = girls.map(g => f.fmt ? f.fmt(g[f.key]) : (g[f.key] || ''));
    // Compute min/max for highlighted fields
    let minIdx = -1, maxIdx = -1;
    if (highlightFields.has(f.key)) {
      const numVals = f.key === 'cup'
        ? girls.map(g => g.cup ? cupToNum(g.cup) : -1)
        : girls.map(g => { const v = parseFloat(String(g[f.key] || '').replace('$', '')); return isNaN(v) ? -1 : v; });
      const valid = numVals.filter(v => v >= 0);
      if (valid.length >= 2) {
        const minVal = Math.min(...valid);
        const maxVal = Math.max(...valid);
        if (minVal !== maxVal) {
          minIdx = numVals.indexOf(minVal);
          maxIdx = numVals.indexOf(maxVal);
        }
      }
    }
    for (let i = 0; i < girls.length; i++) {
      const color = i === minIdx ? 'color:#00c864;font-weight:700' : i === maxIdx ? 'color:#e74c3c;font-weight:700' : '';
      html += '<td style="padding:6px 4px;text-align:center;' + color + '">' + (vals[i] || '<span style="color:#444">-</span>') + '</td>';
    }
    html += '</tr>';
  }

  html += '</table></div></div>';

  document.body.insertAdjacentHTML('beforeend', html);
  document.body.style.overflow = 'hidden';
}

// More filters toggle handled in renderFilters()

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
  // Update Working Now nav count — girls available right now
  const wnCount = allGirls.filter(g => { const a = getAvailabilityText(g); return a && a.startsWith('Available Now'); }).length;
  const wnLink = document.getElementById('navWorkingNow');
  if (wnLink && wnCount > 0) wnLink.textContent = 'Working Now (' + wnCount + ')';
  // Handle URL path on load
  const path = window.location.pathname;
  if (path === '/' || path === '/index.html') {
    handleLandingRoute('/');
  } else if (path !== '/profiles') {
    const g = findGirlByPath(path);
    if (g) {
      // Defer profile load until auth + subscription is resolved
      window._pendingProfileGirl = g;
      // Hide main section while waiting
      const mainSec = document.querySelector('section.section');
      if (mainSec) mainSec.style.display = 'none';
      const landingEl = document.getElementById('landingPage');
      if (landingEl) { landingEl.style.display = ''; landingEl.innerHTML = '<div style="text-align:center;padding:120px 20px;color:var(--text-dim)">Loading...</div>'; }
    }
    else if (path.startsWith('/sydney') || path === '/working-now' || path === '/compare' || path === '/analytics' || path === '/roadmap') { handleLandingRoute(path); }
    else if (path === '/profiles' || path.startsWith('/profiles/')) { handleLandingRoute(path); }
  }
});
checkAuth().then(async (loggedIn) => {
  loadPreferences().then(() => {
    if (hasPreferencesSet(userPreferences)) { computeMatchScores(); renderGrid(); }
    else if (loggedIn) { showPrefsPrompt(); }
  });
  // Check subscription before resolving pending profile
  if (loggedIn) {
    if (userRole === 'admin') { isSubscribed = true; }
    else {
      const sub = await checkSubscription();
      isSubscribed = sub && sub.status === 'active';
    }
  }
  // Now show pending profile if one was deferred
  if (window._pendingProfileGirl) {
    const g = window._pendingProfileGirl;
    window._pendingProfileGirl = null;
    showProfile(g);
  }
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
// Track subscription status for navigation gating
let isSubscribed = null;

setTimeout(async () => {
  try {
    const { data: { session } } = await sbClient.auth.getSession();
    if (!session) { isSubscribed = false; return; }
    if (userRole === 'admin') { isSubscribed = true; return; }
    const sub = await checkSubscription();
    isSubscribed = sub && sub.status === 'active';
    // Only show paywall on non-home pages
    if (isSubscribed && window.location.hash === '#subscribe') hidePaywall();
  } catch(e) { console.error('Paywall check:', e); isSubscribed = false; }
}, 2000);

// Gate navigation for unsubscribed users
function requireSubscription() {
  return true; // Access open to all users
}

// hashchange handled above in unified listener

function sectionHeader(title) {
  return '<div class="section-header"><div class="section-line"></div><div><div class="section-tag">Brothel Search</div><h1 class="section-title">' + title + '</h1></div></div>';
}

// ── Seasonal/Event Highlights ──

function buildSeasonalHighlights() {
  const now = new Date();
  const year = now.getFullYear();
  const mmdd = String(now.getMonth() + 1).padStart(2, '0') + '-' + String(now.getDate()).padStart(2, '0');

  // Australian public holidays & seasonal events (show 7 days before through the day)
  const events = [
    { start: '01-01', end: '01-01', title: "New Year's Day", desc: 'Start the year right. See who\u2019s working today.', icon: '\ud83c\udf89' },
    { start: '01-19', end: '01-26', title: 'Australia Day', desc: 'Long weekend plans? Browse the full roster.', icon: '\ud83c\udde6\ud83c\uddfa' },
    { start: '02-07', end: '02-14', title: "Valentine's Day", desc: 'Treat yourself this Valentine\u2019s. New girls added this week.', icon: '\u2764\ufe0f' },
    { start: '03-28', end: '04-02', title: 'Easter Long Weekend', desc: 'Four days off. Full rosters across all venues.', icon: '\ud83d\udc30' },
    { start: '04-18', end: '04-25', title: 'Anzac Day', desc: 'Public holiday rosters now available.', icon: '\ud83c\udf3f' },
    { start: '06-02', end: '06-09', title: "Queen's Birthday", desc: 'Long weekend ahead. Plan your visit.', icon: '\ud83d\udc51' },
    { start: '09-25', end: '10-02', title: 'AFL Grand Final / Long Weekend', desc: 'Grand final weekend rosters are live.', icon: '\ud83c\udfc8' },
    { start: '12-18', end: '12-25', title: 'Christmas', desc: 'Holiday season rosters available. Book your visit.', icon: '\ud83c\udf84' },
    { start: '12-26', end: '12-31', title: "New Year's Eve", desc: 'End the year with a celebration. See tonight\u2019s roster.', icon: '\ud83c\udf86' },
  ];

  // Season-based fallback if no event is near
  const month = now.getMonth();
  const seasons = [
    { months: [11, 0, 1], title: 'Summer Vibes', desc: 'The hottest season. Explore new arrivals across Sydney.', icon: '\u2600\ufe0f' },
    { months: [2, 3, 4], title: 'Autumn Escapes', desc: 'Cooler evenings, warmer encounters. Browse tonight\u2019s roster.', icon: '\ud83c\udf42' },
    { months: [5, 6, 7], title: 'Winter Warmth', desc: 'Cold outside? Find warmth inside. See who\u2019s working now.', icon: '\u2744\ufe0f' },
    { months: [8, 9, 10], title: 'Spring Refresh', desc: 'New season, new faces. Check out the latest profiles.', icon: '\ud83c\udf38' },
  ];

  let active = null;
  for (const e of events) {
    if (mmdd >= e.start && mmdd <= e.end) { active = e; break; }
  }
  if (!active) {
    active = seasons.find(s => s.months.includes(month));
  }
  if (!active) return '';

  let html = '<div class="venue-divider"><span>\u2014 ' + active.icon + ' ' + active.title.toUpperCase() + ' ' + active.icon + ' \u2014</span></div>';
  html += '<div style="text-align:center;margin-bottom:32px;padding:24px 20px;background:linear-gradient(135deg,rgba(201,149,44,0.06),rgba(8,8,14,0.8));border:1px solid rgba(201,149,44,0.12);border-radius:12px">';
  html += '<div style="font-family:Playfair Display,serif;font-size:20px;font-weight:700;color:var(--gold);margin-bottom:8px">' + active.title + '</div>';
  html += '<div style="font-size:14px;color:var(--text-dim);margin-bottom:16px">' + active.desc + '</div>';
  html += '<a href="/working-now" onclick="event.preventDefault();navigateToLanding(\'/working-now\')" style="display:inline-block;padding:10px 28px;background:transparent;border:1px solid rgba(201,149,44,0.3);color:var(--gold);border-radius:8px;font-family:Rajdhani,sans-serif;font-size:13px;font-weight:600;letter-spacing:2px;text-transform:uppercase;text-decoration:none;cursor:pointer;transition:all .3s">See Who\u2019s Working</a>';
  html += '</div>';
  return html;
}

function buildMyTypeDiscovery() {
  if (!isLoggedIn() || !userFavorites.length) return '';
  // Analyse favourite girls to build a type profile
  const favGirls = allGirls.filter(g => g.oldUrl && userFavorites.includes(g.oldUrl));
  if (favGirls.length < 3) return '';

  const countryCounts = {}, ageBuckets = [], bodyCounts = {};
  for (const g of favGirls) {
    const cs = Array.isArray(g.country) ? g.country : [g.country || ''];
    cs.forEach(c => { if (c) countryCounts[c] = (countryCounts[c] || 0) + 1; });
    if (g.age) ageBuckets.push(parseInt(g.age));
    if (g.body) bodyCounts[g.body] = (bodyCounts[g.body] || 0) + 1;
  }

  const topCountries = Object.entries(countryCounts).sort((a, b) => b[1] - a[1]).slice(0, 3).map(e => e[0]);
  const avgAge = ageBuckets.length ? Math.round(ageBuckets.reduce((a, b) => a + b, 0) / ageBuckets.length) : 0;

  // Find girls matching the type who aren't favourited
  const favSet = new Set(userFavorites);
  const discoveries = allGirls.filter(g => {
    if (!g.oldUrl || favSet.has(g.oldUrl)) return false;
    if (isHidden(g)) return false;
    const cs = Array.isArray(g.country) ? g.country : [g.country || ''];
    const countryMatch = cs.some(c => topCountries.includes(c));
    const ageMatch = g.age && avgAge ? Math.abs(parseInt(g.age) - avgAge) <= 5 : false;
    return countryMatch && ageMatch && g.photos && g.photos.length;
  }).sort((a, b) => (matchScores.get(b.venue + ':' + b.name) || 0) - (matchScores.get(a.venue + ':' + a.name) || 0)).slice(0, 8);

  if (!discoveries.length) return '';

  let html = '<div class="venue-divider"><span>\u2014 DISCOVER YOUR TYPE \u2014</span></div>';
  html += '<div style="text-align:center;font-size:12px;color:var(--text-dim);margin-bottom:12px">Based on your favourites: ' + topCountries.join(', ') + (avgAge ? ', ~' + avgAge + ' years' : '') + '</div>';
  html += '<div class="venue-carousel wrap">';
  for (const g of discoveries) {
    const score = matchScores.get(g.venue + ':' + g.name) || 0;
    const img = g.photos[0] ? '<img src="' + imgProxy(g.photos[0]) + '" alt="' + (g.name||'') + '" style="width:120px;height:160px;object-fit:cover;display:block;border-radius:10px 10px 0 0">' : '';
    html += '<div class="venue-carousel-item" style="width:120px;cursor:pointer;text-align:center" data-venue="' + g.venue + '" data-name="' + (g.name || '').replace(/"/g, '&quot;') + '">';
    html += img + '<div class="venue-carousel-info"><div class="venue-carousel-name">' + (g.name||'') + '</div><div class="venue-carousel-meta">' + (g.venueName||'') + '</div>';
    if (score > 0) html += '<div class="venue-carousel-meta" style="color:var(--gold)">' + score + '% match</div>';
    html += '</div></div>';
  }
  html += '</div>';
  return html;
}

function buildCrowdIndicator(venueId) {
  const todayStr = (() => { const n = new Date(); return n.getFullYear() + '-' + String(n.getMonth()+1).padStart(2,'0') + '-' + String(n.getDate()).padStart(2,'0'); })();
  const venueGirls = allGirls.filter(g => g.venue === venueId);
  const rosteredToday = venueGirls.filter(g => {
    const cal = calendarData[(g.venue || '') + ':' + g.name];
    return cal && cal[todayStr];
  }).length;

  // Compute historical average
  const dayOfWeek = new Date().getDay();
  let totalForDay = 0, daysCount = 0;
  for (const g of venueGirls) {
    const cal = calendarData[(g.venue || '') + ':' + g.name];
    if (!cal) continue;
    for (const d of Object.keys(cal)) {
      if (d.startsWith('_')) continue;
      if (new Date(d + 'T00:00:00').getDay() === dayOfWeek && d < todayStr) { totalForDay++; }
    }
  }
  // Count unique historical dates for this day of week
  const uniqueDates = new Set();
  for (const g of venueGirls) {
    const cal = calendarData[(g.venue || '') + ':' + g.name];
    if (!cal) continue;
    for (const d of Object.keys(cal)) {
      if (d.startsWith('_')) continue;
      if (new Date(d + 'T00:00:00').getDay() === dayOfWeek && d < todayStr) uniqueDates.add(d);
    }
  }
  const avgForDay = uniqueDates.size > 0 ? totalForDay / uniqueDates.size : 0;
  if (avgForDay === 0 && rosteredToday === 0) return '';

  const ratio = avgForDay > 0 ? rosteredToday / avgForDay : 1;
  const level = ratio >= 1.2 ? 'Busy' : ratio >= 0.8 ? 'Moderate' : 'Quiet';
  const color = level === 'Busy' ? '#00c864' : level === 'Moderate' ? '#c9952c' : '#888';
  return '<span style="display:inline-block;padding:2px 8px;border-radius:4px;font-family:Orbitron,sans-serif;font-size:9px;letter-spacing:1px;color:' + color + ';border:1px solid ' + color + '40;text-transform:uppercase">' + level + '</span>';
}

function buildRosterHistory(g) {
  const cal = calendarData[(g.venue || '') + ':' + g.name];
  if (!cal) return '';
  const today = (() => { const n = new Date(); return n.getFullYear() + '-' + String(n.getMonth()+1).padStart(2,'0') + '-' + String(n.getDate()).padStart(2,'0'); })();
  const sixMonthsAgo = new Date(); sixMonthsAgo.setDate(sixMonthsAgo.getDate() - 180);
  const sixMonthsAgoStr = sixMonthsAgo.toISOString().split('T')[0];

  const past = Object.keys(cal).filter(d => !d.startsWith('_') && d < today && d >= sixMonthsAgoStr).sort();
  // Include today's roster too for display completeness
  const todayWorked = !!cal[today];
  if (past.length === 0 && !todayWorked) return '';

  // Build 26-week grid (7 × 26 = 182 days)
  const days = [];
  for (let i = 180; i >= 0; i--) {
    const d = new Date(); d.setDate(d.getDate() - i);
    const ds = d.getFullYear() + '-' + String(d.getMonth()+1).padStart(2,'0') + '-' + String(d.getDate()).padStart(2,'0');
    days.push({ date: ds, worked: !!cal[ds] });
  }

  // Stats
  const workedDays = past.length;
  const totalWeeks = 26;
  const avgPerWeek = (workedDays / totalWeeks).toFixed(1);
  const firstSeen = past[0];
  const firstSeenLabel = new Date(firstSeen + 'T00:00:00').toLocaleDateString('en-AU', { day: 'numeric', month: 'short', year: 'numeric' });

  // Build heatmap (columns = weeks, rows = days of week)
  let html = '<div style="margin-top:20px;border-top:1px solid rgba(201,149,44,0.15);padding-top:16px">';
  html += '<div class="venue-divider"><span>\u2014 ROSTER HISTORY \u2014</span></div>';
  html += '<div style="display:flex;gap:16px;justify-content:center;flex-wrap:wrap;margin-bottom:16px">';
  html += '<div style="text-align:center"><div style="font-size:18px;font-weight:700;color:var(--gold)">' + workedDays + '</div><div style="font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px">Days Rostered (6mo)</div></div>';
  html += '<div style="text-align:center"><div style="font-size:18px;font-weight:700;color:var(--gold)">' + avgPerWeek + '</div><div style="font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px">Avg/Week</div></div>';
  html += '<div style="text-align:center"><div style="font-size:18px;font-weight:700;color:var(--gold)">' + firstSeenLabel + '</div><div style="font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px">First Seen</div></div>';
  html += '</div>';

  // GitHub-style contribution heatmap
  html += '<div style="overflow-x:auto"><div style="display:inline-grid;grid-template-columns:repeat(' + Math.ceil(days.length / 7) + ',10px);grid-auto-flow:column;grid-template-rows:repeat(7,10px);gap:2px;margin:0 auto;direction:ltr">';
  // Align first day to correct day of week
  const startDay = new Date(days[0].date + 'T00:00:00').getDay();
  for (let i = 0; i < startDay; i++) html += '<div></div>';
  for (const d of days) {
    const color = d.worked ? '#c9952c' : 'rgba(255,255,255,0.05)';
    const title = d.date + (d.worked ? ' \u2022 rostered' : '');
    html += '<div title="' + title + '" style="width:10px;height:10px;background:' + color + ';border-radius:2px"></div>';
  }
  html += '</div></div>';
  html += '<div style="display:flex;justify-content:center;gap:16px;margin-top:8px;font-size:10px;color:var(--text-dim)"><span><span style="display:inline-block;width:8px;height:8px;background:rgba(255,255,255,0.05);border-radius:2px;margin-right:4px;vertical-align:middle"></span>Not rostered</span><span><span style="display:inline-block;width:8px;height:8px;background:#c9952c;border-radius:2px;margin-right:4px;vertical-align:middle"></span>Rostered</span></div>';
  html += '</div>';
  return html;
}

function buildRosterPredictions(g) {
  const cal = calendarData[(g.venue || '') + ':' + g.name];
  if (!cal) return '';
  const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const dayCounts = [0, 0, 0, 0, 0, 0, 0];
  const dayTotals = [new Set(), new Set(), new Set(), new Set(), new Set(), new Set(), new Set()];
  const todayStr = (() => { const n = new Date(); return n.getFullYear() + '-' + String(n.getMonth()+1).padStart(2,'0') + '-' + String(n.getDate()).padStart(2,'0'); })();

  for (const d of Object.keys(cal)) {
    if (d.startsWith('_') || d >= todayStr) continue;
    const date = new Date(d + 'T00:00:00');
    const dow = date.getDay();
    dayCounts[dow]++;
    dayTotals[dow].add(d);
  }

  // Need at least 2 weeks of data
  const totalDates = Object.keys(cal).filter(d => !d.startsWith('_') && d < todayStr).length;
  if (totalDates < 5) return '';

  // Calculate probability for each day (appearances / total weeks with that day in data)
  const fourWeeksAgo = new Date(); fourWeeksAgo.setDate(fourWeeksAgo.getDate() - 28);
  const fourWeeksStr = fourWeeksAgo.toISOString().split('T')[0];
  const recentWeeks = new Set();
  for (const d of Object.keys(cal)) {
    if (d.startsWith('_') || d < fourWeeksStr || d >= todayStr) continue;
    const weekNum = Math.floor((new Date(todayStr + 'T00:00:00') - new Date(d + 'T00:00:00')) / (7 * 86400000));
    recentWeeks.add(weekNum);
  }
  const weeksOfData = Math.max(recentWeeks.size, 1);

  // Count recent appearances per day
  const recentDayCounts = [0, 0, 0, 0, 0, 0, 0];
  for (const d of Object.keys(cal)) {
    if (d.startsWith('_') || d < fourWeeksStr || d >= todayStr) continue;
    recentDayCounts[new Date(d + 'T00:00:00').getDay()]++;
  }

  const probabilities = recentDayCounts.map(c => Math.min(100, Math.round((c / weeksOfData) * 100)));
  const maxProb = Math.max(...probabilities);
  if (maxProb === 0) return '';

  let html = '<div style="margin-top:20px;border-top:1px solid rgba(201,149,44,0.15);padding-top:16px">';
  html += '<div class="venue-divider"><span>\u2014 ROSTER PREDICTION \u2014</span></div>';
  html += '<div style="display:flex;gap:6px;justify-content:center;align-items:flex-end;height:80px;padding:0 12px">';
  for (let i = 0; i < 7; i++) {
    const pct = probabilities[i];
    const height = Math.max(6, Math.round((pct / 100) * 60));
    const color = pct >= 70 ? '#00c864' : pct >= 40 ? '#c9952c' : '#555';
    html += '<div style="flex:1;text-align:center;max-width:50px">';
    html += '<div style="font-size:11px;font-weight:600;color:' + color + ';margin-bottom:3px">' + pct + '%</div>';
    html += '<div style="height:' + height + 'px;background:' + color + ';border-radius:3px 3px 0 0"></div>';
    html += '<div style="font-size:10px;color:var(--text-dim);margin-top:3px">' + dayNames[i] + '</div>';
    html += '</div>';
  }
  html += '</div>';
  html += '<div style="text-align:center;font-size:11px;color:var(--text-dim);margin-top:6px">Likelihood of being rostered (based on last 4 weeks)</div>';
  html += '</div>';
  return html;
}

// ── Home Page ──

function renderHomePage() {
  updateMeta(
    'Brothel Search \u2013 Girls, Rosters & Venues',
    'Find who\u2019s working today at local Australian brothels. Browse live rosters, girl profiles, photos and availability. Compare venues, pricing and reviews.',
    'https://brothelsearch.com/og-preview.png',
    'https://brothelsearch.com/',
    null
  );

  const hour = new Date().getHours();
  const greeting = hour < 12 ? 'Good morning' : hour < 17 ? 'Good afternoon' : 'Good evening';

  const totalVenues = Object.keys(VENUE_DATA).length;
  const sevenDaysAgoStats = new Date(); sevenDaysAgoStats.setDate(sevenDaysAgoStats.getDate() - 7);
  const sevenDayStrStats = sevenDaysAgoStats.toISOString().split('T')[0];
  const thirtyDaysAgoStats = new Date(); thirtyDaysAgoStats.setDate(thirtyDaysAgoStats.getDate() - 30);
  const thirtyDayStrStats = thirtyDaysAgoStats.toISOString().split('T')[0];
  const newCount = allGirls.filter(g => g.startDate && g.startDate >= thirtyDayStrStats).length;
  const activeGirls = allGirls.filter(g => g.lastRostered && g.lastRostered >= sevenDayStrStats).length;
  const homeTodayStr = (() => { const n = new Date(); return n.getFullYear() + '-' + String(n.getMonth()+1).padStart(2,'0') + '-' + String(n.getDate()).padStart(2,'0'); })();
  const workingToday = Object.entries(calendarData).filter(([k, cal]) => cal && cal[homeTodayStr]).length;
  const workingNow = allGirls.filter(g => { const a = getAvailabilityText(g); return a && a.startsWith('Available Now'); }).length;

  // Title letters for staggered reveal
  const titleText = 'Sydney';
  const titleLetters = titleText.split('').map((c, i) => '<span class="hero-letter" style="animation-delay:' + (0.5 + i * 0.08) + 's">' + c + '</span>').join('');

  let html = '';
  // ── Full-Screen Cinematic Hero ──
  html += '<section class="hero-section">';
  html += '<div class="hero-bg"><div class="hero-vignette"></div></div>';
  html += '<div class="hero-content">';
  html += '<div class="hero-tag hero-enter hero-enter-d1">Brothel Search</div>';
  html += '<div class="hero-line-el hero-line-anim"></div>';
  html += '<h1 class="hero-title-main shimmer-text">' + titleLetters + '</h1>';
  html += '<p class="hero-tagline hero-enter" style="text-align:center;animation-delay:1.2s">' + greeting + '. A curated selection across Sydney\u2019s finest venues.</p>';
  html += '<div class="home-stats hero-enter" style="animation-delay:1.5s">';
  html += '<div class="home-stat glass-stat"><span class="home-stat-num" data-target="' + totalVenues + '">0</span><span class="home-stat-label">Venues</span></div>';
  html += '<div class="home-stat glass-stat"><span class="home-stat-num" data-target="' + newCount + '">0</span><span class="home-stat-label">New</span></div>';
  html += '<div class="home-stat glass-stat"><span class="home-stat-num" data-target="' + activeGirls + '">0</span><span class="home-stat-label">Active Girls</span></div>';
  html += '<div class="home-stat glass-stat"><span class="home-stat-num" data-target="' + workingToday + '">0</span><span class="home-stat-label">Working Today</span></div>';
  html += '<div class="home-stat glass-stat"><span class="home-stat-num" data-target="' + workingNow + '">0</span><span class="home-stat-label">Working Now</span></div>';
  html += '</div>';
  html += '<div class="home-search-wrap hero-enter" style="animation-delay:1.8s"><input type="text" class="home-search" id="homeSearch" placeholder="Search by name, country or venue..." autocomplete="off"></div>';
  html += '</div>';
  html += '';
  html += '</section>';

  // ── Below-fold content ──
  html += '<div class="landing-page" style="padding-top:20px">';

  // Live Roster Feed
  {
    const workingNowGirls = allGirls.filter(g => {
      const a = getAvailabilityText(g);
      return a && (a.startsWith('Available Now') || a.startsWith('Available Later'));
    }).sort(() => Math.random() - 0.5).slice(0, 30);
    if (workingNowGirls.length > 4) {
      html += '<div class="venue-divider"><span>\u2014 LIVE ROSTER FEED \u2014</span></div>';
      html += '<div class="live-ticker"><div class="live-ticker-track">';
      const items = [...workingNowGirls, ...workingNowGirls];
      for (const g of items) {
        const avail = getAvailabilityText(g);
        const isNow = avail && avail.startsWith('Available Now');
        const dotColor = isNow ? '#00c864' : '#c9952c';
        const photo = g.photos && g.photos[0] ? '<img src="' + imgProxy(g.photos[0], 28) + '" style="width:28px;height:28px;border-radius:6px;object-fit:cover">' : '';
        html += '<div class="live-ticker-item" style="cursor:pointer" data-venue="' + g.venue + '" data-name="' + (g.name || '').replace(/"/g, '&quot;') + '">';
        html += '<span class="live-ticker-dot" style="background:' + dotColor + ';box-shadow:0 0 6px ' + dotColor + '60"></span>';
        html += photo;
        html += '<span class="live-ticker-name">' + (g.name || '') + '</span>';
        html += '<span class="live-ticker-venue">' + (g.venueName || '') + '</span>';
        html += '</div>';
      }
      html += '</div></div>';
    }
  }

  // Seasonal/Event Highlights
  html += buildSeasonalHighlights();

  const thirtyDaysAgoDigest = new Date(); thirtyDaysAgoDigest.setDate(thirtyDaysAgoDigest.getDate() - 30);
  const thirtyDayStrDigest = thirtyDaysAgoDigest.toISOString().split('T')[0];

  // Recent reviews placeholder
  html += '<div class="venue-divider"><span>\u2014 RECENT REVIEWS \u2014</span></div>';
  html += '<div id="homeRecentReviews" style="margin-bottom:40px"><div style="text-align:center;color:var(--text-dim);font-size:12px;padding:16px 0">Loading reviews...</div></div>';

  // My Type Auto-Discovery (moved above Venues)
  html += buildMyTypeDiscovery();

  // Venue showcase — sorted by preference match (or active count if no preferences)
  const thirtyDaysAgoHome = new Date(); thirtyDaysAgoHome.setDate(thirtyDaysAgoHome.getDate() - 30);
  const thirtyDayStrHome = thirtyDaysAgoHome.toISOString().split('T')[0];
  const venuesSorted = Object.entries(VENUE_DATA).map(([id, v]) => {
    const active = allGirls.filter(g => g.venue === id && g.lastRostered && g.lastRostered >= thirtyDayStrHome);
    const activeCount = active.length;
    let avgMatch = 0;
    if (userPreferences && active.length) {
      const scores = active.map(g => scoreGirl(g, userPreferences)).filter(s => s > 0);
      avgMatch = scores.length ? Math.round(scores.reduce((a, b) => a + b, 0) / scores.length) : 0;
    }
    return { id, ...v, activeCount, avgMatch };
  }).sort((a, b) => userPreferences ? b.avgMatch - a.avgMatch : b.activeCount - a.activeCount);
  const topVenues = venuesSorted.slice(0, 5);
  const moreCount = venuesSorted.length - 5;

  html += '<div class="venue-divider"><span>\u2014 VENUES \u2014</span></div>';
  html += '<div class="venue-carousel wrap">';
  for (const v of topVenues) {
    const topGirl = allGirls.filter(g => g.venue === v.id && g.photos && g.photos.length).sort((a, b) => (matchScores.get(b.venue + ':' + b.name) || 0) - (matchScores.get(a.venue + ':' + a.name) || 0))[0];
    const thumb = topGirl ? imgProxy(topGirl.photos[0]) : '';
    html += '<div class="venue-carousel-item" onclick="navigateToLanding(\'/sydney/' + VENUE_REGIONS[v.id] + '/' + v.suburbSlug + '/' + v.id + '/\')">';
    if (thumb) html += '<img src="' + thumb + '" alt="' + v.name + '">';
    html += '<div class="venue-carousel-info"><div class="venue-carousel-name">' + v.name + '</div><div class="venue-carousel-meta">' + v.suburb + ' \u00b7 ' + v.activeCount + ' active</div></div>';
    html += '</div>';
  }
  if (moreCount > 0) {
    html += '<div class="venue-carousel-item venue-carousel-more" onclick="navigateToLanding(\'/compare\')">';
    html += '<div style="display:flex;align-items:center;justify-content:center;height:120px;font-family:Playfair Display,serif;font-size:28px;font-weight:700;color:var(--gold)">+' + moreCount + '</div>';
    html += '<div class="venue-carousel-info"><div class="venue-carousel-name">More Venues</div><div class="venue-carousel-meta">Compare all \u2192</div></div>';
    html += '</div>';
  }
  html += '</div>';

  // Quick links
  html += '<div class="venue-divider"><span>\u2014 EXPLORE \u2014</span></div>';
  html += '<div class="landing-grid" style="margin-top:20px;justify-content:center">';
  html += '<a href="/profiles" class="landing-card" onclick="event.preventDefault();navigateToLanding(\'/profiles\')"><h2 class="landing-card-title">Browse All Profiles</h2><div class="landing-card-stat">' + allGirls.length + ' girls across ' + Object.keys(VENUE_DATA).length + ' venues</div><div class="landing-card-link">View profiles \u2192</div></a>';
  html += '<a href="/working-now" class="landing-card" onclick="event.preventDefault();navigateToLanding(\'/working-now\')"><h2 class="landing-card-title">Who\u2019s Working Now</h2><div class="landing-card-stat">Live roster across all venues</div><div class="landing-card-link">See who\u2019s available \u2192</div></a>';
  html += '<a href="/compare" class="landing-card" onclick="event.preventDefault();navigateToLanding(\'/compare\')"><h2 class="landing-card-title">Compare Venues</h2><div class="landing-card-stat">Side-by-side comparison</div><div class="landing-card-link">Compare now \u2192</div></a>';
  html += '<a href="/sydney/" class="landing-card" onclick="event.preventDefault();navigateToLanding(\'/sydney/\')"><h2 class="landing-card-title">Browse by Location</h2><div class="landing-card-stat">Interactive map of Sydney</div><div class="landing-card-link">View map \u2192</div></a>';
  html += '</div>';


  // Referral promo
  html += '<div class="referral-promo">';
  html += '<div class="referral-promo-inner">';
  html += '<div class="referral-promo-icon">\ud83c\udf81</div>';
  html += '<h3 class="referral-promo-title">Invite Friends, Earn Free Days</h3>';
  html += '<p class="referral-promo-text">Share your referral code with friends. When they purchase a monthly pass, you get <strong>7 free days</strong> added to your membership. They get <strong>5 bonus days</strong> on top of their plan.</p>';
  html += '<div id="homeReferralCode" class="referral-code-wrap" style="display:none"></div>';
  html += '</div></div>';

  html += '</div>';
  return html;
}

// ── Analytics (Members Only) ──

function renderAnalyticsPage() {
  if (!userRole) return '<div class="landing-page" style="padding-top:20px">' + sectionHeader('Analytics') + '<p class="landing-desc">Log in to view analytics.</p></div>';

  updateMeta('Analytics \u2013 Data Insights | Brothel Search', 'Data insights across Sydney brothels. Busiest days, country breakdown and roster trends. Members-only analytics.', 'https://brothelsearch.com/og-preview.png', 'https://brothelsearch.com/analytics', null);

  const venueIds = Object.keys(VENUE_DATA);

  const thirtyDaysAgo = new Date(); thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
  const thirtyDayStr = thirtyDaysAgo.toISOString().split('T')[0];
  const filtered = getFiltered();

  // ── Busiest Days (avg unique girls per day of week, last 30 days) ──
  const filteredKeys = new Set(filtered.map(g => g.venue + ':' + g.name));
  const daySets = [new Set(), new Set(), new Set(), new Set(), new Set(), new Set(), new Set()]; // Sun-Sat
  const dayNames = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
  for (const [key, cal] of Object.entries(calendarData)) {
    if (key.startsWith('_')) continue;
    if (!filteredKeys.has(key)) continue;
    for (const dateStr of Object.keys(cal)) {
      if (dateStr < thirtyDayStr) continue;
      const d = new Date(dateStr + 'T00:00:00');
      if (!isNaN(d)) daySets[d.getDay()].add(key);
    }
  }
  const dayCounts = daySets.map(s => s.size);
  const maxDay = Math.max(...dayCounts, 1);


  // ── Country Breakdown (rostered within 30 days) ──
  const countryTotals = {};
  filtered.filter(g => g.lastRostered && g.lastRostered >= thirtyDayStr).forEach(g => {
    const cs = Array.isArray(g.country) ? g.country : [g.country || ''];
    cs.forEach(c => { if (c) countryTotals[c] = (countryTotals[c] || 0) + 1; });
  });
  const topCountries = Object.entries(countryTotals).sort((a,b) => b[1] - a[1]).slice(0, 10);
  const maxCountry = topCountries.length ? topCountries[0][1] : 1;

  // ── Build HTML ──
  let html = '<div class="landing-page" style="padding-top:20px">';
  html += sectionHeader('Analytics');
  const filteredVenueCount = new Set(filtered.map(g => g.venue)).size;
  html += '<p class="landing-desc">Data insights across ' + filtered.length + ' girls and ' + filteredVenueCount + ' venues.</p>';

  // Busiest Days
  html += '<div class="analytics-section"><h2 class="analytics-heading">Busiest Days (Roster Frequency)</h2>';
  html += '<div class="analytics-bars">';
  for (let i = 0; i < 7; i++) {
    const pct = (dayCounts[i] / maxDay * 100).toFixed(0);
    html += '<div class="analytics-bar-row"><span class="analytics-bar-label">' + dayNames[i] + '</span><div class="analytics-bar-track"><div class="analytics-bar-fill" style="width:' + pct + '%"></div></div><span class="analytics-bar-val">' + dayCounts[i] + '</span></div>';
  }
  html += '</div></div>';

  // Country Breakdown
  html += '<div class="analytics-section"><h2 class="analytics-heading">Country Breakdown (rostered within 30 days)</h2>';
  html += '<div class="analytics-bars">';
  for (const [country, count] of topCountries) {
    const pct = (count / maxCountry * 100).toFixed(0);
    html += '<div class="analytics-bar-row"><span class="analytics-bar-label">' + country + '</span><div class="analytics-bar-track"><div class="analytics-bar-fill" style="width:' + pct + '%"></div></div><span class="analytics-bar-val">' + count + '</span></div>';
  }
  html += '</div></div>';


  html += '</div>';
  return html;
}

// ── Venue Comparison ──

let compareSort = { col: 'rank', dir: -1 };
function sortCompareTable(col) {
  if (compareSort.col === col) compareSort.dir *= -1;
  else { compareSort.col = col; compareSort.dir = 1; }
  const landing = document.getElementById('landingPage');
  landing.innerHTML = renderComparePage();
}

function runVenueCalculator() {
  const idA = document.getElementById('venueCalcA').value;
  const idB = document.getElementById('venueCalcB').value;
  const resultEl = document.getElementById('venueCalcResult');
  if (!idA || !idB || idA === idB) { resultEl.innerHTML = '<div style="text-align:center;color:#e74c3c;font-size:12px">Please select two different venues</div>'; return; }

  const today = (() => { const n = new Date(); return n.getFullYear() + '-' + String(n.getMonth()+1).padStart(2,'0') + '-' + String(n.getDate()).padStart(2,'0'); })();
  const thirtyDaysAgo = new Date(); thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
  const thirtyAgo = thirtyDaysAgo.toISOString().split('T')[0];
  const sevenDaysAgo = new Date(); sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
  const sevenAgo = sevenDaysAgo.toISOString().split('T')[0];

  const stats = (id) => {
    const v = VENUE_DATA[id];
    const girls = allGirls.filter(g => g.venue === id);
    const active = girls.filter(g => g.lastRostered && g.lastRostered >= thirtyAgo);
    const rosteredToday = girls.filter(g => { const c = calendarData[(g.venue || '') + ':' + g.name]; return c && c[today]; }).length;
    const newWeek = girls.filter(g => g.startDate && g.startDate >= sevenAgo).length;
    const avg = field => { const vals = active.map(g => parseInt(g[field])).filter(p => p > 0); return vals.length ? Math.round(vals.reduce((a, b) => a + b, 0) / vals.length) : 0; };
    const avgMatch = userPreferences && active.length ? (() => { const scores = active.map(g => scoreGirl(g, userPreferences)).filter(s => s > 0); return scores.length ? Math.round(scores.reduce((a, b) => a + b, 0) / scores.length) : 0; })() : 0;
    // Retention: girls from 3+ months ago still active
    const threeMonthsAgo = new Date(); threeMonthsAgo.setDate(threeMonthsAgo.getDate() - 90);
    const threeMonthStr = threeMonthsAgo.toISOString().split('T')[0];
    const oldGirls = girls.filter(g => g.startDate && g.startDate <= threeMonthStr);
    const stillActive = oldGirls.filter(g => g.lastRostered && g.lastRostered >= sevenAgo);
    const retention = oldGirls.length > 0 ? Math.round((stillActive.length / oldGirls.length) * 100) : 0;
    return { name: v.name, suburb: v.suburb, total: girls.length, active: active.length, rosteredToday, newWeek, avg30: avg('val1'), avg45: avg('val2'), avg60: avg('val3'), avgMatch, retention };
  };

  const a = stats(idA), b = stats(idB);
  const cell = (va, vb, higherWins = true) => {
    if (va === vb || va === 0 || vb === 0) return [va, vb, '', ''];
    const aWins = higherWins ? va > vb : va < vb;
    return [va, vb, aWins ? 'color:#00c864;font-weight:700' : '', aWins ? '' : 'color:#00c864;font-weight:700'];
  };

  const rows = [
    { label: 'Suburb', vals: [a.suburb, b.suburb, '', ''] },
    { label: 'Total Profiles', vals: cell(a.total, b.total, true) },
    { label: 'Active (30d)', vals: cell(a.active, b.active, true) },
    { label: 'Rostered Today', vals: cell(a.rosteredToday, b.rosteredToday, true) },
    { label: 'New This Week', vals: cell(a.newWeek, b.newWeek, true) },
    { label: 'Avg 30 min', vals: cell(a.avg30, b.avg30, false).map((v, i) => i < 2 ? (v ? '$' + v : '—') : v) },
    { label: 'Avg 45 min', vals: cell(a.avg45, b.avg45, false).map((v, i) => i < 2 ? (v ? '$' + v : '—') : v) },
    { label: 'Avg 60 min', vals: cell(a.avg60, b.avg60, false).map((v, i) => i < 2 ? (v ? '$' + v : '—') : v) },
    { label: '3-Month Retention', vals: cell(a.retention, b.retention, true).map((v, i) => i < 2 ? (v ? v + '%' : '—') : v) },
  ];
  if (userPreferences) rows.push({ label: 'Avg Match Score', vals: cell(a.avgMatch, b.avgMatch, true).map((v, i) => i < 2 ? (v ? v + '%' : '—') : v) });

  // Count wins
  let aWins = 0, bWins = 0;
  rows.forEach(r => { if (r.vals[2].includes('00c864')) aWins++; if (r.vals[3].includes('00c864')) bWins++; });

  let html = '<div style="max-width:600px;margin:0 auto;border:1px solid rgba(201,149,44,0.2);border-radius:12px;padding:20px;background:rgba(12,12,20,0.3)">';
  html += '<table style="width:100%;border-collapse:collapse;font-size:13px">';
  html += '<tr><td></td><td style="text-align:center;padding:8px;color:var(--gold);font-weight:700;font-family:Playfair Display,serif;font-size:16px">' + a.name + '</td><td style="text-align:center;padding:8px;color:var(--gold);font-weight:700;font-family:Playfair Display,serif;font-size:16px">' + b.name + '</td></tr>';
  for (const r of rows) {
    html += '<tr style="border-top:1px solid rgba(201,149,44,0.1)"><td style="padding:8px;color:var(--text-dim)">' + r.label + '</td><td style="padding:8px;text-align:center;' + r.vals[2] + '">' + r.vals[0] + '</td><td style="padding:8px;text-align:center;' + r.vals[3] + '">' + r.vals[1] + '</td></tr>';
  }
  html += '</table>';
  const winner = aWins > bWins ? a.name + ' wins (' + aWins + '-' + bWins + ')' : bWins > aWins ? b.name + ' wins (' + bWins + '-' + aWins + ')' : 'Tied (' + aWins + '-' + bWins + ')';
  const winnerColor = aWins === bWins ? '#c9952c' : '#00c864';
  html += '<div style="text-align:center;margin-top:16px;padding-top:16px;border-top:1px solid rgba(201,149,44,0.1);font-family:Orbitron,sans-serif;font-size:13px;letter-spacing:2px;color:' + winnerColor + ';font-weight:700;text-transform:uppercase">' + winner + '</div>';
  html += '</div>';
  resultEl.innerHTML = html;
}

function renderComparePage() {
  const filtered = getFiltered();
  let venueIds = Object.keys(VENUE_DATA);

  updateMeta(
    'Compare Brothels in Sydney | Brothel Search',
    'Compare ' + venueIds.length + ' Sydney brothels side-by-side. Rankings by preference match, pricing, girl count, countries and availability. ' + venueIds.map(id => VENUE_DATA[id].name).join(', ') + '.',
    'https://brothelsearch.com/og-preview.png',
    'https://brothelsearch.com/compare',
    null
  );

  const thirtyDaysAgoCmp = new Date(); thirtyDaysAgoCmp.setDate(thirtyDaysAgoCmp.getDate() - 30);
  const thirtyDayStrCmp = thirtyDaysAgoCmp.toISOString().split('T')[0];

  const cmpTodayStr = (() => { const n = new Date(); return n.getFullYear() + '-' + String(n.getMonth()+1).padStart(2,'0') + '-' + String(n.getDate()).padStart(2,'0'); })();
  const cmpTomorrowStr = (() => { const n = new Date(); n.setDate(n.getDate()+1); return n.getFullYear() + '-' + String(n.getMonth()+1).padStart(2,'0') + '-' + String(n.getDate()).padStart(2,'0'); })();

  const rankings = venueIds.map(id => {
    const v = VENUE_DATA[id];
    const active = filtered.filter(g => g.venue === id && g.lastRostered && g.lastRostered >= thirtyDayStrCmp);
    const filteredNames = new Set(filtered.filter(g => g.venue === id).map(g => g.venue + ':' + g.name));
    const rostered = Object.entries(calendarData).filter(([k, cal]) => k.startsWith(id + ':') && filteredNames.has(k) && cal && cal[cmpTodayStr]).length;
    const rosteredTomorrow = Object.entries(calendarData).filter(([k, cal]) => k.startsWith(id + ':') && filteredNames.has(k) && cal && cal[cmpTomorrowStr]).length;
    const avgOf = field => { const vals = active.map(g => parseInt(g[field])).filter(p => p > 0); return vals.length ? Math.round(vals.reduce((a,b) => a+b, 0) / vals.length) : 0; };
    const countryCounts = {};
    active.forEach(g => { const cs = Array.isArray(g.country) ? g.country : [g.country || '']; cs.forEach(c => { if (c && c !== 'N/A') countryCounts[c] = (countryCounts[c] || 0) + 1; }); });
    const topCountries = Object.entries(countryCounts).sort((a, b) => b[1] - a[1]).slice(0, 3).map(([c]) => c).join(', ');
    const newCount = filtered.filter(g => g.venue === id && g.startDate && g.startDate >= thirtyDayStrCmp).length;
    let avgMatch = 0;
    if (userPreferences && active.length) {
      const scores = active.map(g => scoreGirl(g, userPreferences)).filter(s => s > 0);
      avgMatch = scores.length ? Math.round(scores.reduce((a,b) => a+b, 0) / scores.length) : 0;
    }
    return { id, name: v.name, suburb: v.suburb, rostered, rosteredTomorrow, avg30: avgOf('val1'), avg45: avgOf('val2'), avg60: avgOf('val3'), topCountries, newCount, avgMatch, activeCount: active.length };
  });

  // Assign rank by preference match or active count
  rankings.sort((a, b) => userPreferences ? b.avgMatch - a.avgMatch : b.activeCount - a.activeCount);
  rankings.forEach((r, i) => { r.rank = i + 1; });

  // Sort by selected column
  rankings.sort((a, b) => {
    let va = a[compareSort.col], vb = b[compareSort.col];
    if (typeof va === 'string') return va.localeCompare(vb) * compareSort.dir;
    return ((vb || 0) - (va || 0)) * compareSort.dir;
  });

  let html = '<div class="landing-page" style="padding-top:20px">';
  html += sectionHeader('Compare Venues');
  html += '<p class="landing-desc">' + (userPreferences ? 'Ranked by your preferences' : 'Ranked by active girl count') + ' (rostered within 30 days).</p>';
  if (!userPreferences) html += '<div style="font-size:12px;color:var(--text-dim);margin-bottom:12px">Set your <a href="/#profile/preferences" style="color:var(--gold)">preferences</a> to see personalised rankings.</div>';

  // Venue Comparison Calculator — pick 2 venues head-to-head
  html += '<div class="venue-divider"><span>\u2014 VENUE CALCULATOR \u2014</span></div>';
  html += '<div style="text-align:center;margin-bottom:16px;font-size:12px;color:var(--text-dim)">Pick two venues to compare head-to-head</div>';
  html += '<div style="display:flex;gap:12px;justify-content:center;margin-bottom:20px;flex-wrap:wrap">';
  const venueOptsHtml = venueIds.map(id => '<option value="' + id + '">' + VENUE_DATA[id].name + '</option>').join('');
  html += '<select id="venueCalcA" style="background:rgba(12,12,20,0.7);border:1px solid rgba(201,149,44,0.3);color:var(--gold);padding:8px 14px;border-radius:8px;font-family:Rajdhani,sans-serif;font-size:13px;cursor:pointer">' + venueOptsHtml + '</select>';
  html += '<span style="color:var(--gold);font-weight:700;display:flex;align-items:center">VS</span>';
  html += '<select id="venueCalcB" style="background:rgba(12,12,20,0.7);border:1px solid rgba(201,149,44,0.3);color:var(--gold);padding:8px 14px;border-radius:8px;font-family:Rajdhani,sans-serif;font-size:13px;cursor:pointer">' + venueOptsHtml + '</select>';
  html += '<button class="auth-btn" id="venueCalcBtn" onclick="runVenueCalculator()" style="margin:0;width:auto;padding:8px 20px;font-size:11px">Compare</button>';
  html += '</div>';
  html += '<div id="venueCalcResult" style="margin-bottom:32px"></div>';

  const cmpCols = [
    { key: 'name', label: 'Venue' },
    { key: 'rank', label: 'Rank' },
    { key: '_address', label: 'Address' },
    { key: '_website', label: 'Website' },
    { key: 'topCountries', label: 'Top Countries' },
    { key: 'newCount', label: 'New' },
    { key: 'activeCount', label: 'Active Girls' },
    { key: 'rostered', label: 'Working Today' },
    { key: 'rosteredTomorrow', label: 'Working Tomorrow' },
  ];
  if (userPreferences) cmpCols.push({ key: 'avgMatch', label: 'Avg Match' });
  cmpCols.push({ key: 'avg30', label: 'Avg 30min' }, { key: 'avg45', label: 'Avg 45min' }, { key: 'avg60', label: 'Avg 60min' });

  html += '<div class="compare-table-wrap"><table class="compare-table"><thead><tr>';
  for (const c of cmpCols) {
    const sortable = !c.key.startsWith('_');
    const arrow = compareSort.col === c.key ? (compareSort.dir === 1 ? ' \u25BC' : ' \u25B2') : '';
    html += '<th class="compare-label"' + (sortable ? ' style="cursor:pointer" onclick="sortCompareTable(\'' + c.key + '\')"' : '') + '>' + c.label + arrow + '</th>';
  }
  html += '</tr></thead><tbody>';
  // Calculate median for each price column
  const median = arr => { const s = arr.filter(v => v > 0).sort((a,b) => a-b); return s.length ? s[Math.floor(s.length/2)] : 0; };
  const med30 = median(rankings.map(r => r.avg30));
  const med45 = median(rankings.map(r => r.avg45));
  const med60 = median(rankings.map(r => r.avg60));

  rankings.forEach((r, i) => {
    const v = VENUE_DATA[r.id];
    html += '<tr>';
    html += '<td class="compare-venue-header" onclick="navigateToLanding(\'/sydney/' + v.suburbSlug + '/' + r.id + '/\')">' + r.name + '</td>';
    html += '<td style="color:var(--gold);font-weight:700">#' + r.rank + '</td>';
    html += '<td style="font-size:11px"><a href="https://www.google.com/maps/dir/?api=1&destination=' + encodeURIComponent(v.address) + '" target="_blank" rel="noopener" style="color:var(--gold);text-decoration:none">' + v.address + '</a></td>';
    html += '<td><a href="' + v.url + '" target="_blank" rel="noopener" style="color:var(--gold);text-decoration:none;font-size:11px">' + v.url.replace(/^https?:\/\//, '').replace(/\/$/, '') + '</a></td>';
    html += '<td style="font-size:12px">' + (r.topCountries || '\u2014') + '</td>';
    html += '<td' + (r.newCount === 0 ? ' style="color:#ff4444"' : ' style="color:#00c864"') + '>' + r.newCount + '</td>';
    html += '<td' + (r.activeCount === 0 ? ' style="color:#ff4444"' : ' style="color:#00c864"') + '>' + r.activeCount + '</td>';
    html += '<td' + (r.rostered === 0 ? ' style="color:#ff4444"' : ' style="color:#00c864"') + '>' + r.rostered + '</td>';
    html += '<td' + (r.rosteredTomorrow === 0 ? ' style="color:#ff4444"' : ' style="color:#00c864"') + '>' + r.rosteredTomorrow + '</td>';
    if (userPreferences) html += '<td style="color:' + (r.avgMatch >= 90 ? '#00c864' : '#ff4444') + '">' + r.avgMatch + '%</td>';
    html += '<td' + (r.avg30 ? ' style="color:' + (r.avg30 <= med30 ? '#00c864' : '#ff4444') + '"' : '') + '>' + (r.avg30 ? '$' + r.avg30 : '\u2014') + '</td>';
    html += '<td' + (r.avg45 ? ' style="color:' + (r.avg45 <= med45 ? '#00c864' : '#ff4444') + '"' : '') + '>' + (r.avg45 ? '$' + r.avg45 : '\u2014') + '</td>';
    html += '<td' + (r.avg60 ? ' style="color:' + (r.avg60 <= med60 ? '#00c864' : '#ff4444') + '"' : '') + '>' + (r.avg60 ? '$' + r.avg60 : '\u2014') + '</td>';
    html += '</tr>';
  });
  html += '</tbody></table></div>';
  html += '</div>';
  return html;
}

// ── Roadmap ──

async function loadRoadmapItems() {
  const { data, error } = await sbClient.from('roadmap').select('*').order('created_at', { ascending: false });
  if (error) { console.error('Load roadmap error:', error); return []; }
  return data || [];
}

async function createRoadmapItem(item) {
  const { data: { user } } = await sbClient.auth.getUser();
  if (!user) return { error: 'Not logged in' };

  // Check member limit (3 active items)
  if (userRole !== 'admin') {
    const { data: existing } = await sbClient.from('roadmap').select('id').eq('creator_id', user.id).neq('status', 'completed');
    if (existing && existing.length >= 3) return { error: 'Members can only have 3 active items' };
  }

  const { data, error } = await sbClient.from('roadmap').insert({
    creator_id: user.id,
    initiator: userRole === 'admin' ? 'Admin' : 'Member',
    status: 'review',
    category: 'review',
    title: item.title.substring(0, 25),
    description: item.description.substring(0, 250),
  }).select();
  if (error) return { error: error.message };
  return { data };
}

async function updateRoadmapItem(id, fields) {
  const { error } = await sbClient.from('roadmap').update(fields).eq('id', id);
  if (error) return { error: error.message };
  return { success: true };
}

async function deleteRoadmapItem(id) {
  const { error } = await sbClient.from('roadmap').delete().eq('id', id);
  if (error) return { error: error.message };
  return { success: true };
}

async function loadRoadmapVotes() {
  const { data } = await sbClient.from('roadmap_votes').select('roadmap_id,user_id,vote');
  return data || [];
}

window.roadmapVote = async function(roadmapId, vote) {
  const { data: { user } } = await sbClient.auth.getUser();
  if (!user || (!isSubscribed && userRole !== 'admin')) return;
  const existing = window._roadmapVotes.find(v => v.roadmap_id === roadmapId && v.user_id === user.id);
  if (existing && existing.vote === vote) {
    await sbClient.from('roadmap_votes').delete().eq('roadmap_id', roadmapId).eq('user_id', user.id);
  } else if (existing) {
    await sbClient.from('roadmap_votes').update({ vote }).eq('roadmap_id', roadmapId).eq('user_id', user.id);
  } else {
    await sbClient.from('roadmap_votes').insert({ roadmap_id: roadmapId, user_id: user.id, vote });
  }
  window._roadmapVotes = await loadRoadmapVotes();
  initRoadmapPage();
};

let dataSort = { col: 'venue', dir: 1 };
function renderDataPage() {
  const cols = [
    { key: 'venueName', label: 'Venue', fmtG: g => '<a href="/sydney/' + (VENUE_REGIONS[g.venue] || 'other') + '/' + (VENUE_DATA[g.venue] ? VENUE_DATA[g.venue].suburbSlug : '') + '/' + g.venue + '/" onclick="event.preventDefault();navigateToLanding(\'/sydney/' + (VENUE_REGIONS[g.venue] || 'other') + '/' + (VENUE_DATA[g.venue] ? VENUE_DATA[g.venue].suburbSlug : '') + '/' + g.venue + '/\')" style="color:var(--gold);text-decoration:none">' + g.venueName + '</a>' },
    { key: 'name', label: 'Name', fmtG: g => '<a href="' + profilePath(g) + '" onclick="event.preventDefault();showProfile(allGirls.find(gg=>gg.venue===\'' + g.venue + '\'&&gg.name===\'' + (g.name||'').replace(/'/g, "\\'") + '\'))" style="color:var(--gold);text-decoration:none">' + g.name + '</a>' },
    { key: 'country', label: 'Country', fmt: v => Array.isArray(v) ? v.join(', ') : (v || '') },
    { key: 'age', label: 'Age' },
    { key: 'height', label: 'Height' },
    { key: 'cup', label: 'Cup' },
    { key: 'body', label: 'Body' },
    { key: 'val1', label: '30 min', fmt: v => v ? '$' + v : '' },
    { key: 'val2', label: '45 min', fmt: v => v ? '$' + v : '' },
    { key: 'val3', label: '60 min', fmt: v => v ? '$' + v : '' },
    { key: 'startDate', label: 'Start Date' },
    { key: 'lastRostered', label: 'Last Rostered' },
    { key: 'deleted', label: 'Deleted', fmt: v => v === 'Yes' ? '<span style="color:#e74c3c">Yes</span>' : '' },
  ];

  const filtered = getFiltered();
  const sorted = [...filtered].sort((a, b) => {
    let va = a[dataSort.col], vb = b[dataSort.col];
    if (dataSort.col === 'country') { va = Array.isArray(va) ? va.join(', ') : (va || ''); vb = Array.isArray(vb) ? vb.join(', ') : (vb || ''); }
    if (dataSort.col === 'age' || dataSort.col === 'height' || dataSort.col === 'body' || dataSort.col === 'val1' || dataSort.col === 'val2' || dataSort.col === 'val3') {
      va = parseFloat(va) || 0; vb = parseFloat(vb) || 0;
    }
    va = va || ''; vb = vb || '';
    if (va < vb) return -1 * dataSort.dir;
    if (va > vb) return 1 * dataSort.dir;
    return 0;
  });

  let html = '<div class="landing-page" style="padding-top:20px">';
  html += sectionHeader('Data');
  html += '<p class="landing-desc">' + filtered.length + ' of ' + allGirls.length + ' profiles</p>';
  html += '<div style="overflow-x:auto;margin-top:16px"><table style="width:100%;border-collapse:collapse;font-size:12px;white-space:nowrap">';
  html += '<thead><tr>';
  for (const col of cols) {
    const arrow = dataSort.col === col.key ? (dataSort.dir === 1 ? ' \u25B2' : ' \u25BC') : '';
    html += '<th style="padding:8px 10px;text-align:left;border-bottom:2px solid var(--gold);cursor:pointer;color:var(--gold);font-family:Orbitron,sans-serif;font-size:10px;letter-spacing:1px" onclick="sortDataTable(\'' + col.key + '\')">' + col.label + arrow + '</th>';
  }
  html += '</tr></thead><tbody>';
  for (const g of sorted) {
    const isDeleted = g.deleted === 'Yes';
    html += '<tr style="border-bottom:1px solid rgba(201,149,44,0.1)' + (isDeleted ? ';opacity:0.4' : '') + '">';
    for (const col of cols) {
      const val = col.fmtG ? col.fmtG(g) : col.fmt ? col.fmt(g[col.key]) : (g[col.key] || '');
      html += '<td style="padding:6px 10px;color:var(--text)">' + val + '</td>';
    }
    html += '</tr>';
  }
  html += '</tbody></table></div></div>';
  return html;
}

function sortDataTable(col) {
  if (dataSort.col === col) dataSort.dir *= -1;
  else { dataSort.col = col; dataSort.dir = 1; }
  const landing = document.getElementById('landingPage');
  landing.innerHTML = renderDataPage();
  window.scrollTo({ top: 0 });
}

let roadmapSort = { col: 'status', dir: 1 };
const ROADMAP_STATUS_ORDER = { review: 0, planned: 1, 'in progress': 2, completed: 3 };

window.sortRoadmapTable = function(col) {
  if (roadmapSort.col === col) roadmapSort.dir *= -1;
  else { roadmapSort.col = col; roadmapSort.dir = 1; }
  const landing = document.getElementById('landingPage');
  if (landing) {
    landing.innerHTML = renderRoadmapPage();
    setTimeout(initRoadmapPage, 50);
  }
};

function renderRoadmapPage() {
  updateMeta(
    'Roadmap \u2013 Development Timeline | Brothel Search',
    'Upcoming features, fixes and improvements. Track the development progress of Brothel Search.',
    'https://brothelsearch.com/og-preview.png',
    'https://brothelsearch.com/roadmap',
    null
  );

  let html = '<div class="landing-page" style="padding-top:20px">';
  html += sectionHeader('Roadmap');
  html += '<p class="landing-desc">Development timeline of upcoming changes, new features, and fixes.</p>';
  html += '<div id="roadmapHint" class="landing-desc" style="display:none"></div>';

  // New item form
  html += '<div id="roadmapFormWrap" style="margin-bottom:24px">';
  html += '<button class="roadmap-add-btn" id="roadmapAddBtn">+ New Item</button>';
  html += '<div id="roadmapForm" style="display:none" class="roadmap-form">';
  html += '<input type="text" id="roadmapTitle" class="roadmap-input" placeholder="Title (max 25 chars)" maxlength="25">';
  html += '<textarea id="roadmapDesc" class="roadmap-textarea" placeholder="Description (max 250 chars)" maxlength="250"></textarea>';
  html += '<div style="display:flex;gap:8px;align-items:center"><button class="roadmap-submit" id="roadmapSubmitBtn">Submit</button><button class="roadmap-cancel" id="roadmapCancelBtn">Cancel</button><span id="roadmapMsg" style="font-size:12px;color:var(--gold)"></span></div>';
  html += '</div></div>';

  // Table
  html += '<div class="roadmap-table-wrap"><table class="roadmap-table" id="roadmapTable">';
  const sortArrow = col => roadmapSort.col === col ? (roadmapSort.dir === 1 ? ' \u25B2' : ' \u25BC') : '';
  const sortTh = (col, label, style) => '<th style="cursor:pointer;' + (style || '') + '" onclick="sortRoadmapTable(\'' + col + '\')">' + label + sortArrow(col) + '</th>';
  html += '<thead><tr>';
  html += sortTh('category', 'Type', 'width:50px');
  html += sortTh('title', 'Summary', '');
  html += '<th>Description</th>';
  html += sortTh('status', 'Status', 'width:100px');
  html += sortTh('votes', 'Votes', 'width:70px');
  html += sortTh('initiator', 'Initiator', 'width:80px');
  html += sortTh('created_at', 'Created', 'width:90px');
  html += '<th style="width:80px"></th>';
  html += '</tr></thead>';
  html += '<tbody id="roadmapBody"><tr><td colspan="8" class="roadmap-empty">Loading...</td></tr></tbody>';
  html += '</table></div>';

  html += '</div>';
  return html;
}

function renderRoadmapRow(item) {
  const isAdmin = userRole === 'admin';
  const isOwner = window._currentUserId && item.creator_id === window._currentUserId;
  const canEdit = isAdmin || isOwner;
  const date = new Date(item.created_at).toLocaleDateString('en-AU', { day: 'numeric', month: 'short' });

  const statusClass = { review: 'review', planned: 'planned', 'in progress': 'inprogress', completed: 'completed' };
  const catClass = { review: 'review', feature: 'feature', fix: 'fix', improvement: 'improvement', content: 'content' };
  const catIcons = { review: '\ud83d\udcdd', feature: '\u2728', fix: '\ud83d\udd27', improvement: '\u26a1', content: '\ud83d\udcc4' };
  const id = item.id;

  let html = '<tr class="roadmap-row" data-id="' + id + '">';

  // Type (admin inline select)
  if (isAdmin) {
    html += '<td><select class="roadmap-inline-select roadmap-lozenge roadmap-lozenge-' + (catClass[item.category] || 'review') + '" onchange="roadmapInlineSave(\'' + id + '\',{category:this.value});this.className=\'roadmap-inline-select roadmap-lozenge roadmap-lozenge-\'+({review:\'review\',feature:\'feature\',fix:\'fix\',improvement:\'improvement\',content:\'content\'}[this.value]||\'review\')">';
    ['review','feature','fix','improvement','content'].forEach(function(v) { html += '<option value="' + v + '"' + (item.category === v ? ' selected' : '') + '>' + (catIcons[v]||'') + ' ' + v + '</option>'; });
    html += '</select></td>';
  } else {
    html += '<td><span class="roadmap-lozenge roadmap-lozenge-' + (catClass[item.category] || 'review') + '">' + (catIcons[item.category] || '') + ' ' + item.category + '</span></td>';
  }

  // Summary (editable for owner/admin)
  if (canEdit) {
    html += '<td><div class="roadmap-title-text roadmap-editable" onclick="roadmapInlineEdit(this,\'' + id + '\',\'title\',25)">' + item.title.replace(/</g, '&lt;') + '</div></td>';
  } else {
    html += '<td><div class="roadmap-title-text">' + item.title.replace(/</g, '&lt;') + '</div></td>';
  }

  // Description (editable for owner/admin)
  if (canEdit) {
    html += '<td class="roadmap-desc roadmap-editable" onclick="roadmapInlineEdit(this,\'' + id + '\',\'description\',250)">' + item.description.replace(/</g, '&lt;') + '</td>';
  } else {
    html += '<td class="roadmap-desc">' + item.description.replace(/</g, '&lt;') + '</td>';
  }

  // Status (admin inline select)
  if (isAdmin) {
    html += '<td><select class="roadmap-inline-select roadmap-lozenge roadmap-lozenge-' + (statusClass[item.status] || 'review') + '" onchange="roadmapInlineSave(\'' + id + '\',{status:this.value});this.className=\'roadmap-inline-select roadmap-lozenge roadmap-lozenge-\'+({review:\'review\',planned:\'planned\',\'in progress\':\'inprogress\',completed:\'completed\'}[this.value]||\'review\')">';
    ['review','planned','in progress','completed'].forEach(function(v) { html += '<option value="' + v + '"' + (item.status === v ? ' selected' : '') + '>' + v + '</option>'; });
    html += '</select></td>';
  } else {
    html += '<td><span class="roadmap-lozenge roadmap-lozenge-' + (statusClass[item.status] || 'review') + '">' + item.status + '</span></td>';
  }

  // Votes
  const votes = (window._roadmapVotes || []).filter(v => v.roadmap_id === id);
  const ups = votes.filter(v => v.vote === 1).length;
  const downs = votes.filter(v => v.vote === -1).length;
  const myVote = window._currentUserId ? (votes.find(v => v.user_id === window._currentUserId) || {}).vote : 0;
  html += '<td><div class="roadmap-votes">';
  html += '<button class="roadmap-vote-btn' + (myVote === 1 ? ' active-up' : '') + '" onclick="roadmapVote(\'' + id + '\',1)" title="Upvote">\u25B2</button>';
  html += '<span class="roadmap-vote-count vote-positive">' + ups + '</span>';
  html += '<button class="roadmap-vote-btn' + (myVote === -1 ? ' active-down' : '') + '" onclick="roadmapVote(\'' + id + '\',-1)" title="Downvote">\u25BC</button>';
  html += '<span class="roadmap-vote-count vote-negative">' + downs + '</span>';
  html += '</div></td>';

  html += '<td><span class="roadmap-initiator-' + item.initiator.toLowerCase() + '">' + item.initiator + '</span></td>';
  html += '<td class="roadmap-key">' + date + '</td>';
  html += '<td><div class="roadmap-actions">';
  if (isAdmin) html += '<button class="roadmap-action-btn roadmap-delete-btn" onclick="deleteRoadmapItemUI(\'' + id + '\')" title="Delete">\ud83d\uddd1</button>';
  html += '</div></td>';
  html += '</tr>';
  return html;
}

window.roadmapInlineEdit = function(el, id, field, maxLen) {
  if (el.querySelector('input,textarea')) return; // already editing
  const current = el.textContent;
  const isLong = field === 'description';
  if (isLong) {
    el.innerHTML = '<textarea class="roadmap-inline-input" maxlength="' + maxLen + '" style="min-height:50px">' + current + '</textarea>';
  } else {
    el.innerHTML = '<input class="roadmap-inline-input" type="text" maxlength="' + maxLen + '" value="' + current.replace(/"/g, '&quot;') + '">';
  }
  const input = el.querySelector('input,textarea');
  input.focus();
  input.addEventListener('blur', function() {
    const val = this.value.trim();
    if (val && val !== current) {
      const fields = {}; fields[field] = val;
      roadmapInlineSave(id, fields);
    }
    el.textContent = val || current;
  });
  input.addEventListener('keydown', function(e) {
    if (e.key === 'Enter' && !isLong) { this.blur(); }
    if (e.key === 'Escape') { el.textContent = current; }
  });
};

window.roadmapInlineSave = async function(id, fields) {
  await updateRoadmapItem(id, fields);
  window._roadmapItems = null;
  initRoadmapPage();
};

async function initRoadmapPage() {
  // Get current user ID
  const { data: { user } } = await sbClient.auth.getUser();
  window._currentUserId = user ? user.id : null;
  const isLoggedIn = !!user;
  const canInteract = isLoggedIn && (isSubscribed || userRole === 'admin');

  // Show hint for non-interactive users
  const hintEl = document.getElementById('roadmapHint');
  if (hintEl) {
    hintEl.style.display = 'none';
  }

  if (!window._roadmapItems) {
    window._roadmapVotes = await loadRoadmapVotes();
    window._roadmapItems = await loadRoadmapItems();
  }
  const items = [...window._roadmapItems];
  const body = document.getElementById('roadmapBody');
  if (!body) return;

  // Sort items
  const votes = window._roadmapVotes || [];
  items.sort((a, b) => {
    const col = roadmapSort.col;
    const dir = roadmapSort.dir;
    let va, vb;
    if (col === 'status') {
      va = ROADMAP_STATUS_ORDER[a.status] ?? 99;
      vb = ROADMAP_STATUS_ORDER[b.status] ?? 99;
    } else if (col === 'votes') {
      va = votes.filter(v => v.roadmap_id === a.id && v.vote === 1).length;
      vb = votes.filter(v => v.roadmap_id === b.id && v.vote === 1).length;
    } else if (col === 'created_at') {
      va = a.created_at || '';
      vb = b.created_at || '';
    } else {
      va = (a[col] || '').toLowerCase();
      vb = (b[col] || '').toLowerCase();
    }
    if (va < vb) return -1 * dir;
    if (va > vb) return 1 * dir;
    return 0;
  });

  if (!items.length) {
    body.innerHTML = '<tr><td colspan="8" class="roadmap-empty">No roadmap items yet.</td></tr>';
  } else {
    body.innerHTML = items.map(i => renderRoadmapRow(i)).join('');
  }

  // Add button — only for logged-in users
  const addBtn = document.getElementById('roadmapAddBtn');
  const form = document.getElementById('roadmapForm');
  if (addBtn && form) {
    if (!isLoggedIn || (isSubscribed !== true && userRole !== 'admin')) {
      addBtn.style.display = 'none';
    } else {
      addBtn.onclick = () => { form.style.display = ''; addBtn.style.display = 'none'; };
      document.getElementById('roadmapCancelBtn').onclick = () => { form.style.display = 'none'; addBtn.style.display = ''; };
      document.getElementById('roadmapSubmitBtn').onclick = async () => {
        const title = document.getElementById('roadmapTitle').value.trim();
        const desc = document.getElementById('roadmapDesc').value.trim();
        const msg = document.getElementById('roadmapMsg');
        if (!title) { msg.textContent = 'Title required'; return; }
        if (!desc) { msg.textContent = 'Description required'; return; }
        msg.textContent = 'Submitting...';
        const result = await createRoadmapItem({ title, description: desc });
        if (result.error) { msg.textContent = result.error; return; }
        msg.textContent = '';
        document.getElementById('roadmapTitle').value = '';
        document.getElementById('roadmapDesc').value = '';
        form.style.display = 'none';
        addBtn.style.display = '';
        window._roadmapItems = null;
        initRoadmapPage();
      };
    }
  }
}


window.deleteRoadmapItemUI = async function(id) {
  if (!confirm('Delete this roadmap item?')) return;
  await deleteRoadmapItem(id);
  window._roadmapItems = null;
  initRoadmapPage();
};

// ── Working Now ──

let wnSelectedDay = 0;
function setWnDay(i) { wnSelectedDay = i; const landing = document.getElementById('landingPage'); if (landing) landing.innerHTML = renderWorkingNow(); }

function renderWorkingNow() {
  const now = new Date();
  const rosterNow = new Date(now);
  if (rosterNow.getHours() < 6) rosterNow.setDate(rosterNow.getDate() - 1);
  const todayStr = rosterNow.getFullYear() + '-' + String(rosterNow.getMonth() + 1).padStart(2, '0') + '-' + String(rosterNow.getDate()).padStart(2, '0');
  const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
  const dayNamesShort = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

  const filtered = getFiltered();
  const filteredOrder = new Map();
  filtered.forEach((g, i) => { filteredOrder.set((g.venue || '') + ':' + g.name, i); });

  // Collect 7 days of roster data
  const dates = [];
  for (let i = 0; i < 7; i++) {
    const d = new Date(rosterNow); d.setDate(d.getDate() + i);
    dates.push({ str: d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0'), date: d });
  }

  const rosterDays = [];
  for (const { str: dateStr, date } of dates) {
    const entries = [];
    for (const [key, cal] of Object.entries(calendarData)) {
      if (!cal[dateStr]) continue;
      if (!filteredOrder.has(key)) continue;
      const g = filtered.find(g => (g.venue || '') + ':' + g.name === key);
      if (!g) continue;
      if (dateStr === todayStr) {
        const slot = cal[dateStr];
        if (validSlot(slot)) {
          let nowMins = now.getHours() * 60 + now.getMinutes();
          if (now.getHours() < 6 && rosterNow.getDate() !== now.getDate()) nowMins += 24 * 60;
          const startMins = slotMins(slot.start), endMins = slotMins(slot.end);
          const effectiveEnd = endMins <= startMins ? 24 * 60 + endMins : endMins;
          if (nowMins >= effectiveEnd) continue;
        }
      }
      entries.push({ girl: g, slot: cal[dateStr], order: filteredOrder.get(key) });
    }
    if (entries.length > 0) {
      entries.sort((a, b) => a.order - b.order);
      rosterDays.push({ dateStr, date, entries });
    }
  }

  // Count today's roster only
  const todayRoster = rosterDays.find(d => d.dateStr === todayStr);
  const todayTotal = todayRoster ? todayRoster.entries.length : 0;
  const nowCount = filtered.filter(g => { const a = getAvailabilityText(g); return a && a.startsWith('Available Now'); }).length;
  const laterTodayCount = todayTotal - nowCount;

  updateMeta(
    'Who\'s Working Now \u2013 Live Roster | Brothel Search',
    nowCount + ' girls available now across Sydney brothels. ' + (laterTodayCount > 0 ? laterTodayCount + ' more starting later.' : ''),
    'https://brothelsearch.com/og-preview.png',
    'https://brothelsearch.com/working-now',
    null
  );

  let html = '<div class="landing-page" style="padding-top:20px">';
  html += sectionHeader('Who\u2019s Working Now');
  // Subtitle based on selected day
  const selectedDay = rosterDays[wnSelectedDay] || todayRoster;
  const selectedIsToday = selectedDay && selectedDay.dateStr === todayStr;
  const selectedCount = selectedDay ? selectedDay.entries.length : 0;
  if (selectedIsToday) {
    html += '<p class="landing-desc">' + selectedCount + ' rostered today</p>';
  } else {
    html += '<p class="landing-desc">' + selectedCount + ' rostered</p>';
  }

  if (!rosterDays.length) {
    html += '<div class="empty-msg"><svg width="80" height="80" viewBox="0 0 80 80" fill="none" style="margin-bottom:20px"><circle cx="40" cy="40" r="38" stroke="rgba(201,149,44,0.25)" stroke-width="1.5"/><circle cx="40" cy="40" r="28" stroke="rgba(201,149,44,0.15)" stroke-width="1"/><path d="M30 45c0-5.5 4.5-10 10-10s10 4.5 10 10" stroke="rgba(201,149,44,0.3)" stroke-width="1.5" stroke-linecap="round" fill="none" transform="rotate(180 40 40)"/><circle cx="33" cy="35" r="2" fill="rgba(201,149,44,0.3)"/><circle cx="47" cy="35" r="2" fill="rgba(201,149,44,0.3)"/></svg><div>No roster data available. Check back later!</div></div>';
    html += '</div>';
    return html;
  }

  if (wnSelectedDay >= rosterDays.length) wnSelectedDay = 0;

  // Day tabs
  html += '<div class="roster-day-tabs">';
  rosterDays.forEach((day, i) => {
    const isToday = day.dateStr === todayStr;
    const label = isToday ? 'Today' : dayNamesShort[day.date.getDay()];
    const dateLabel = day.date.getDate() + ' ' + monthNames[day.date.getMonth()];
    html += '<button class="roster-day-tab' + (i === wnSelectedDay ? ' active' : '') + '" onclick="setWnDay(' + i + ')">' + label + ' <span style="opacity:0.7">' + dateLabel + '</span><span class="tab-count">' + day.entries.length + '</span></button>';
  });
  html += '</div>';

  // Timeline
  const TIMELINE_START = 6, TIMELINE_HOURS = 24;
  const hours = [];
  for (let i = 0; i <= TIMELINE_HOURS; i += 2) {
    const h = (TIMELINE_START + i) % 24;
    hours.push(fmt24to12(String(h).padStart(2, '0') + ':00'));
  }

  const day = rosterDays[wnSelectedDay];
  const isToday = day.dateStr === todayStr;

  html += '<div class="roster-day"><div class="roster-timeline"><div class="roster-timeline-header"><div></div><div class="roster-timeline-hours">' + hours.map(h => '<span>' + h + '</span>').join('') + '</div></div>';

  for (const { girl: g, slot } of day.entries) {
    const thumb = g.photos && g.photos.length ? imgProxy(g.photos[0], 72) : '';
    const sM = validSlot(slot) ? slotMins(slot.start) : 0;
    const eM = validSlot(slot) ? slotMins(slot.end) : 0;
    let startOffset = sM - TIMELINE_START * 60; if (startOffset < 0) startOffset += 24 * 60;
    let endOffset = eM - TIMELINE_START * 60; if (endOffset < 0) endOffset += 24 * 60;
    if (endOffset <= startOffset) endOffset += 24 * 60;
    const totalMins = TIMELINE_HOURS * 60;
    const leftPct = Math.max(0, (startOffset / totalMins) * 100);
    const widthPct = Math.min(100 - leftPct, ((endOffset - startOffset) / totalMins) * 100);
    let barClass = 'future';
    if (isToday) {
      let nowOffset = (now.getHours() - TIMELINE_START) * 60 + now.getMinutes();
      if (nowOffset < 0) nowOffset += 24 * 60;
      if (nowOffset >= startOffset && nowOffset < endOffset) barClass = 'now';
      else if (nowOffset < startOffset) barClass = 'later';
    }
    const timeStr = validSlot(slot) ? fmt24to12(slot.start) + ' - ' + fmt24to12(slot.end) : 'Rostered';
    const priceStr = (g.val1 || g.val2 || g.val3) ? [g.val1 ? '$' + g.val1 : '', g.val2 ? '$' + g.val2 : '', g.val3 ? '$' + g.val3 : ''].filter(Boolean).join(' / ') : g.venueName;

    html += '<div class="roster-entry" onclick="showProfile(allGirls.find(g=>g.venue===\'' + g.venue + '\'&&g.name===\'' + g.name.replace(/'/g, "\\'") + '\'))">';
    html += '<div class="roster-entry-info">';
    html += thumb ? '<img class="roster-entry-thumb" src="' + thumb + '" alt="">' : '<div class="roster-entry-thumb" style="background:rgba(255,255,255,0.06)"></div>';
    html += '<div><div class="roster-entry-name">' + g.name + '</div><div class="roster-entry-venue">' + priceStr + '</div></div></div>';
    html += '<div class="roster-entry-bar-container"><div class="roster-entry-bar ' + barClass + '" style="left:' + leftPct + '%;width:' + widthPct + '%" title="' + timeStr + '"><span>' + timeStr + '</span></div></div>';
    html += '</div>';
  }

  // Now line for today
  if (isToday) {
    let nowOffset = (now.getHours() - TIMELINE_START) * 60 + now.getMinutes();
    if (nowOffset < 0) nowOffset += 24 * 60;
    const nowPct = (nowOffset / (TIMELINE_HOURS * 60)) * 100;
    if (nowPct >= 0 && nowPct <= 100) {
      html += '<div class="roster-timeline-line now" style="position:absolute;top:0;bottom:0;left:calc(172px + (100% - 172px) * ' + (nowPct / 100) + ')"><div class="roster-timeline-line-label">Now</div></div>';
    }
  }

  html += '</div></div></div>';
  return html;
}

function renderWorkingNowCard(g) {
  const countries = countriesWithFlags(g.country);
  const girlKey = g.venue + ':' + g.name;
  const girlScore = matchScores.get(girlKey) || 0;
  const showBadge = userPreferences && girlScore > 0;
  const avail = getAvailabilityText(g);
  const img = g.photos && g.photos.length
    ? '<img class="card-thumb" src="' + imgProxy(g.photos[0]) + '" alt="' + (g.name || '').replace(/"/g, '&quot;') + ' \u2013 ' + (g.venueName || '') + '" loading="lazy">'
    : '<div class="silhouette"></div>';
  const heartSvg = '<svg viewBox="0 0 24 24"><path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"/></svg>';

  const hideSvg = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24"/><line x1="1" y1="1" x2="23" y2="23"/></svg>';
  const glowClassWN = avail && avail.startsWith('Available Now') ? ' glow-now' : avail && (avail.startsWith('Available Later') || avail.startsWith('Available Future')) ? ' glow-later' : '';
  let html = '<div class="girl-card card-settled' + (isFavorite(g) ? ' favorited' : '') + glowClassWN + '" data-venue="' + g.venue + '" data-name="' + (g.name || '').replace(/"/g, '&quot;') + '">';
  html += '<div class="fav-heart' + (isFavorite(g) ? ' active' : '') + '" data-url="' + (g.oldUrl||'').replace(/"/g, '&quot;') + '" onclick="event.stopPropagation();toggleFavorite(\'' + (g.oldUrl||'').replace(/'/g, "\\'") + '\',event)">' + heartSvg + '</div>';
  html += '<div class="hide-btn' + (isHidden(g) ? ' active' : '') + '" data-url="' + (g.oldUrl||'').replace(/"/g, '&quot;') + '" onclick="event.stopPropagation();toggleHidden(\'' + (g.oldUrl||'').replace(/'/g, "\\'") + '\',event)">' + hideSvg + '</div>';
  html += '<div class="card-badges"><span class="country-badge">' + (g.venueName || '') + '</span>';
  if (showBadge) html += '<div class="match-badge' + (girlScore >= 90 ? ' match-gold' : '') + '">' + girlScore + '%</div>';
  if (isNewProfile(g)) html += '<span class="new-badge">New</span>'; else if (isReturnProfile(g)) html += '<span class="return-badge">Return</span>';
  if (g.pornstar) html += '<span class="av-badge">AV</span>';
  html += '</div>';
  html += '<div class="card-img">' + img + '</div>';
  html += '<div class="card-info">';
  html += '<div class="card-name">' + (g.name || '') + '</div>';
  html += '<div class="card-country">' + countries + '</div>';
  html += '<div class="card-stats">';
  if (g.age) html += '<span>Age ' + g.age + '</span>';
  if (g.body) html += '<span>Body ' + g.body + '</span>';
  if (g.height) html += '<span>' + g.height + 'cm</span>';
  if (g.cup) html += '<span>' + g.cup + ' cup</span>';
  html += '</div>';
  if (g.val1 || g.val2 || g.val3) html += '<div class="card-rates">' + [g.val1 ? '$'+g.val1 : '', g.val2 ? '$'+g.val2 : '', g.val3 ? '$'+g.val3 : ''].filter(Boolean).join(' / ') + '</div>';
  if (avail) html += '<div class="card-last-rostered' + (avail.startsWith('Available Now') ? ' available-now' : ' available-later') + '">' + avail + '</div>';
  html += '</div></div>';
  return html;
}

// ── Landing Pages (City / Suburb / Venue) ──

const VENUE_DATA = {
  ginzaempire: { name: 'Ginza Empire', suburb: 'Surry Hills', suburbSlug: 'surryhills', url: 'https://479ginza.com.au/', address: '479 Elizabeth St, Surry Hills NSW 2010', lat: -33.88698124490204, lng: 151.20805761312394, desc: 'Luxuriously appointed themed rooms designed with your comfort and pleasure in mind. From the Japanese Emperor\u2019s Palace to the Regal French suite, each room offers a unique experience with Sydney\u2019s most desirable Asian beauties.' },
  ginzaclub: { name: 'Ginza Club', suburb: 'Surry Hills', suburbSlug: 'surryhills', url: 'https://www.ginzaclub.com.au/', address: '10 Cleveland St, Surry Hills NSW 2010', lat: -33.88993022667204, lng: 151.20912609962915, desc: 'A renovated venue featuring beautifully themed rooms and a curated selection of gorgeous ladies from across Asia. Located in the heart of Surry Hills with discreet rear entrance from Goodlet Lane.' },
  kyoto206: { name: 'Kyoto 206', suburb: 'Surry Hills', suburbSlug: 'surryhills', url: 'https://citybrothel.com.au/', address: '206 Commonwealth St, Surry Hills NSW 2010', lat: -33.88317474375967, lng: 151.2108589818634, desc: 'Just two minutes from Central Station, Kyoto 206 is one of Sydney\u2019s most accessible venues. Known for young, beautiful Asian girls and a welcoming atmosphere in the heart of the CBD.' },
  sakura57: { name: 'Sakura 57', suburb: 'Surry Hills', suburbSlug: 'surryhills', url: 'https://www.surryhillsbrothel.com.au/', address: '2/57 Reservoir St, Surry Hills NSW 2010', lat: -33.8812693750108, lng: 151.21102289535847, desc: 'A well-established Surry Hills venue offering day and night shifts with a diverse selection of ladies. Conveniently located in the heart of the suburb with a welcoming and professional environment.' },
  top127: { name: 'Top 127', suburb: 'Chippendale', suburbSlug: 'chippendale', url: 'https://127city.com/', address: '127 Regent St, Chippendale NSW 2008', lat: -33.887895846811354, lng: 151.20126815545416, desc: 'Located on Regent Street in Chippendale, Top 127 offers a selection of beautiful girls with competitive rates and a friendly, no-pressure atmosphere close to the city centre.' },
  fantasyclub35: { name: 'Fantasy Club 35', suburb: 'Annandale', suburbSlug: 'innerwest', url: 'https://fantasyclub35.com.au/', address: '33/35 Parramatta Rd, Annandale NSW 2038', lat: -33.88719005098213, lng: 151.1706113116501, desc: 'An upmarket Sydney brothel boasting a wide range of beauties. Highly reputable for many years, offering sophisticated full-service Asian girls in well-appointed rooms along Parramatta Road.' },
  '429city': { name: '429 City', suburb: 'Haymarket', suburbSlug: 'haymarket', url: 'https://www.429city.com/', address: '429A Pitt St, Haymarket NSW 2000', lat: -33.87874734224782, lng: 151.20694241127885, desc: 'Sydney\u2019s Haymarket venue on Pitt Street, featuring a diverse roster of beauties from across Asia. Known for friendly service and a central CBD location just steps from Chinatown.' },
  pennys77: { name: "Penny's 77", suburb: 'Newtown', suburbSlug: 'newtown', url: 'https://pennys77.com.au/', address: '77A Enmore Rd, Newtown NSW 2042', lat: -33.898490435845176, lng: 151.17499527337114, desc: "A popular Newtown venue on Enmore Road offering a friendly and relaxed atmosphere with a diverse selection of ladies." },
  thegoldenapple: { name: 'The Golden Apple', suburb: 'Surry Hills', suburbSlug: 'surryhills', url: 'https://www.thegoldenapple.com.au/', address: '377 Riley St, Surry Hills NSW 2010', lat: -33.884634181442, lng: 151.2126912705119, desc: "Located on Riley Street in Surry Hills, The Golden Apple is known for its genuine photo guarantee and wide selection of escorts." },
  blackcatparlour: { name: 'Black Cat Parlour', suburb: 'Surry Hills', suburbSlug: 'surryhills', url: 'https://blackcatparlour.com.au/', address: '371 Riley St, Surry Hills NSW 2010', lat: -33.88444709795769, lng: 151.21255926239868, desc: "A well-established Surry Hills parlour on Riley Street featuring a curated roster of ladies with detailed profiles and reviews." },
  bellevue12: { name: 'Bellevue 12', suburb: 'Surry Hills', suburbSlug: 'surryhills', url: 'https://bellevue12.com.au/', address: '12 Bellevue St, Surry Hills NSW 2010', lat: -33.88358115264125, lng: 151.21205877114085, desc: "A discreet Surry Hills venue on Bellevue Street, close to the CBD, offering Asian beauties in a comfortable environment." },
  thegatewayclub: { name: 'The Gateway Club', suburb: 'Petersham', suburbSlug: 'petersham', url: 'https://www.gatewayclub.com.au/', address: '74 Parramatta Rd, Petersham NSW 2049', lat: -33.888249251526354, lng: 151.15849643516688, desc: 'A well-known Inner West venue on Parramatta Road in Petersham, offering a relaxed and welcoming atmosphere.' },
  marrickvillebrothel: { name: 'Marrickville Brothel', suburb: 'Marrickville', suburbSlug: 'marrickville', url: 'https://www.marrickvillebrothel.com/', address: '143 Marrickville Rd, Marrickville NSW 2204', lat: -33.911427703493736, lng: 151.16157536214604, desc: 'Located on Marrickville Road, a popular local venue with a friendly environment and diverse selection of ladies.' },
  springhouse: { name: 'Spring House', suburb: 'Marrickville', suburbSlug: 'marrickville', url: 'https://46springhouse.com.au/', address: '46 Sydenham Rd, Marrickville NSW 2204', lat: -33.91183862895485, lng: 151.16550426446886, desc: 'A Marrickville venue on Sydenham Road offering a comfortable and discreet experience.' },
  stiletto: { name: 'Stiletto', suburb: 'Camperdown', suburbSlug: 'camperdown', url: 'https://www.stilettosydney.com/', address: '82 Parramatta Road, Larkin St, Camperdown NSW 2000', lat: -33.88535604131434, lng: 151.1813658994193, desc: 'Stiletto Sydney on Parramatta Road in Camperdown, known for premium adult entertainment services.' },
  wivesonly: { name: 'Wives Only', suburb: 'St Peters', suburbSlug: 'stpeters', url: 'https://wivesonly.com.au/', address: '673 King St, St Peters NSW 2044', lat: -33.90812288485341, lng: 151.18155845118417, desc: 'Located on King Street in St Peters, Wives Only offers a unique and welcoming experience.' },
  jinia: { name: 'Jinia', suburb: 'Strathfield South', suburbSlug: 'strathfieldsouth', url: 'https://jinia.com.au/', address: '2/81-89 Cosgrove Rd, Strathfield South NSW 2136', lat: -33.89686475038878, lng: 151.07533399699363, desc: 'A Western Sydney venue in Strathfield South offering Asian beauties in a comfortable setting.' },
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
  return getFiltered().filter(g => g.venue === venueId).length;
}

function venuePriceRange(venueId, field) {
  field = field || 'val1';
  const girls = getFiltered().filter(g => g.venue === venueId && g[field]);
  if (!girls.length) return '';
  const prices = girls.map(g => parseInt(g[field])).filter(p => p > 0);
  if (!prices.length) return '';
  return '$' + Math.min(...prices) + ' \u2013 $' + Math.max(...prices);
}

function venueAvgPrice(venueId, field) {
  field = field || 'val1';
  const prices = getFiltered().filter(g => g.venue === venueId && g[field]).map(g => parseInt(g[field])).filter(p => p > 0);
  if (!prices.length) return '';
  return '$' + Math.round(prices.reduce((a, b) => a + b, 0) / prices.length);
}

function venueRosteredCount(venueId) {
  return getFiltered().filter(g => g.venue === venueId && getAvailabilityText(g) && getAvailabilityText(g) !== 'ended').length;
}

function renderCityPage() {
  const suburbs = getSuburbs();
  const filtered = getFiltered();
  const totalVenues = new Set(filtered.map(g => g.venue)).size;
  const totalGirls = filtered.length;

  updateMeta(
    'Brothels in Sydney \u2013 Browse All Venues | Brothel Search',
    'Browse ' + totalVenues + ' brothels across Sydney with ' + totalGirls + '+ girls. Compare venues in Surry Hills, Chippendale, Haymarket and Annandale.',
    'https://brothelsearch.com/og-preview.png',
    'https://brothelsearch.com/sydney/',
    { '@context': 'https://schema.org', '@type': 'ItemList', name: 'Brothels in Sydney', numberOfItems: totalVenues, itemListElement: suburbs.flatMap(s => s.venues.map((v, i) => ({ '@type': 'ListItem', position: i + 1, item: { '@type': 'LocalBusiness', name: v.name, address: v.address } }))) }
  );

  let html = '<div class="landing-map-container"><div id="venueMap"></div></div>';
  html += '<div class="landing-page">';
  html += '<h1 class="landing-title">Brothels in Sydney</h1>';
  html += '<p class="landing-desc">' + totalVenues + ' venues across ' + suburbs.length + ' suburbs with ' + totalGirls + '+ girls available.</p>';
  // Group venues by region
  const regionGroups = {};
  for (const [id, v] of Object.entries(VENUE_DATA)) {
    const region = VENUE_REGIONS[id] || 'other';
    if (!regionGroups[region]) regionGroups[region] = { venues: [], girlCount: 0 };
    regionGroups[region].venues.push({ id, ...v });
    regionGroups[region].girlCount += venueGirlCount(id);
  }

  html += '<div class="landing-grid">';
  for (const regionSlug of REGION_ORDER) {
    const group = regionGroups[regionSlug];
    if (!group || !group.venues.length) continue;
    const regionName = REGION_NAMES[regionSlug];
    html += '<a href="/sydney/' + regionSlug + '/" class="landing-card" onclick="event.preventDefault();navigateToLanding(\'/sydney/' + regionSlug + '/\')">';
    html += '<h2 class="landing-card-title">' + regionName + '</h2>';
    html += '<div class="landing-card-stat">' + group.venues.length + ' venue' + (group.venues.length !== 1 ? 's' : '') + '</div>';
    html += '<div class="landing-card-stat">' + group.girlCount + ' girls</div>';
    html += '<div style="font-size:11px;color:var(--text-dim);margin-top:4px">' + group.venues.map(v => v.suburb).filter((v,i,a) => a.indexOf(v) === i).join(', ') + '</div>';
    html += '<div class="landing-card-link">Browse region \u2192</div>';
    html += '</a>';
  }
  html += '</div></div>';
  return html;
}

function haversine(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat/2) * Math.sin(dLat/2) + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon/2) * Math.sin(dLon/2);
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

async function initVenueMap() {
  const mapEl = document.getElementById('venueMap');
  if (!mapEl || typeof L === 'undefined') return;
  if (window._venueMap) { window._venueMap.remove(); window._venueMap = null; }

  const filterSuburb = mapEl.dataset.suburb || null;
  const filterRegion = mapEl.dataset.region || null;

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

  const venues = Object.entries(VENUE_DATA).filter(([id, v]) => {
    if (filterRegion) return VENUE_REGIONS[id] === filterRegion;
    if (filterSuburb) return v.suburbSlug === filterSuburb;
    return true;
  });

  // Get user location for distance labels
  let userLat = null, userLng = null;
  if (navigator.geolocation) {
    try {
      const pos = await new Promise((resolve, reject) => navigator.geolocation.getCurrentPosition(resolve, reject, { timeout: 5000 }));
      userLat = pos.coords.latitude;
      userLng = pos.coords.longitude;
    } catch {}
  }

  for (const [id, v] of venues) {
    const count = venueGirlCount(id);
    const venueGirls = allGirls.filter(g => g.venue === id);
    const nowCount = venueGirls.filter(g => { const a = getAvailabilityText(g); return a && a.startsWith('Available Now'); }).length;
    const laterCount = venueGirls.filter(g => { const a = getAvailabilityText(g); return a && (a.startsWith('Available Later') || a.startsWith('Available Future')); }).length;
    let label = v.name;
    if (userLat !== null) {
      const dist = haversine(userLat, userLng, v.lat, v.lng);
      label += ' <span style="opacity:0.6;font-size:9px">' + dist.toFixed(1) + 'km</span>';
    }
    label += '<br><span style="font-size:9px"><span style="color:#00c864">' + nowCount + ' Working Now</span> <span style="opacity:0.6">|</span> <span style="color:#3c78ff">' + laterCount + ' Working Later</span></span>';
    const marker = L.marker([v.lat, v.lng], {
      icon: L.divIcon({ html: '<div class="venue-marker">' + label + '</div>', className: 'venue-marker-icon', iconSize: null, iconAnchor: [60, 50] }),
    });
    marker.on('click', function() { navigateToLanding('/sydney/' + (VENUE_REGIONS[id] || 'other') + '/' + v.suburbSlug + '/' + id + '/'); });
    marker.bindTooltip('<strong>' + v.name + '</strong><br>' + v.address + '<br>' + count + ' girls<br><span style="color:#00c864">' + nowCount + ' now</span> | <span style="color:#3c78ff">' + laterCount + ' later</span>', { className: 'venue-tooltip', direction: 'top', offset: [0, -20] });
    clusters.addLayer(marker);
  }

  map.addLayer(clusters);
  const bounds = L.latLngBounds(venues.map(([id, v]) => [v.lat, v.lng]));
  map.fitBounds(bounds.pad(filterSuburb ? 0.5 : 0.3));
}

function renderRegionPage(regionSlug) {
  const regionName = REGION_NAMES[regionSlug];
  if (!regionName) return null;

  const venueIds = Object.keys(VENUE_DATA).filter(id => VENUE_REGIONS[id] === regionSlug);
  if (!venueIds.length) return null;
  const venues = venueIds.map(id => ({ id, ...VENUE_DATA[id] }));

  const sevenDaysAgoSub = new Date(); sevenDaysAgoSub.setDate(sevenDaysAgoSub.getDate() - 7);
  const sevenDayStrSub = sevenDaysAgoSub.toISOString().split('T')[0];
  const filtered = getFiltered();
  const activeCount = venueIds.reduce((sum, id) => sum + filtered.filter(g => g.venue === id && g.lastRostered && g.lastRostered >= sevenDayStrSub).length, 0);

  updateMeta(
    'Brothels in ' + regionName + ', Sydney | Brothel Search',
    'Browse ' + venues.length + ' brothels in ' + regionName + ', Sydney: ' + venues.map(v => v.name).join(', ') + '. ' + activeCount + ' girls active.',
    'https://brothelsearch.com/og-preview.png',
    'https://brothelsearch.com/sydney/' + regionSlug + '/',
    { '@context': 'https://schema.org', '@type': 'ItemList', name: 'Brothels in ' + regionName + ', Sydney', numberOfItems: venues.length }
  );

  let html = '<div class="landing-map-container"><div id="venueMap" data-region="' + regionSlug + '"></div></div>';
  html += '<div class="landing-page">';
  const suburbRegion = regionName;
  html += sectionHeader('Brothels in ' + regionName);
  html += '<p class="landing-desc">' + suburbRegion + ' \u00b7 ' + venues.length + ' venues with ' + activeCount + ' girls active in ' + regionName + ', Sydney.</p>';
  html += '<div class="landing-grid">';

  for (const v of venues) {
    const count = venueGirlCount(v.id);
    const priceRange = venuePriceRange(v.id);
    html += '<a href="/sydney/' + regionSlug + '/' + v.suburbSlug + '/' + v.id + '/" class="landing-card" onclick="event.preventDefault();navigateToLanding(\'/sydney/' + regionSlug + '/' + v.suburbSlug + '/' + v.id + '/\')">';
    html += '<h2 class="landing-card-title">' + v.name + '</h2>';
    html += '<div class="landing-card-address">' + v.address + '</div>';
    html += '<div class="landing-card-stat">' + count + ' girls ' + buildCrowdIndicator(v.id) + '</div>';
    if (priceRange) html += '<div class="landing-card-stat">From ' + priceRange + ' (30min)</div>';
    html += '<div class="landing-card-link">View profiles \u2192</div>';
    html += '</a>';
  }

  html += '</div></div>';
  return html;
}

function buildBestTimeToVisit(venueId) {
  // Analyse historical calendar data for this venue's girls to find busiest days
  const venueGirls = allGirls.filter(g => g.venue === venueId);
  const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const dayCounts = [0, 0, 0, 0, 0, 0, 0];
  const dayDates = [new Set(), new Set(), new Set(), new Set(), new Set(), new Set(), new Set()];

  for (const g of venueGirls) {
    const cal = calendarData[venueId + ':' + g.name] || calendarData[(g.venue || '') + ':' + g.name];
    if (!cal) continue;
    for (const d of Object.keys(cal)) {
      if (d.startsWith('_')) continue;
      const date = new Date(d + 'T00:00:00');
      const dow = date.getDay();
      dayCounts[dow]++;
      dayDates[dow].add(d);
    }
  }

  const maxCount = Math.max(...dayCounts);
  if (maxCount === 0) return '';

  // Average girls per day (count / unique dates)
  const avgPerDay = dayCounts.map((count, i) => dayDates[i].size > 0 ? Math.round(count / dayDates[i].size) : 0);
  const maxAvg = Math.max(...avgPerDay);
  if (maxAvg === 0) return '';

  let html = '<div style="margin:24px 0">';
  html += '<div class="venue-divider"><span>\u2014 BEST TIME TO VISIT \u2014</span></div>';
  html += '<div style="display:flex;gap:6px;justify-content:center;align-items:flex-end;height:100px;padding:0 12px">';
  for (let i = 0; i < 7; i++) {
    const pct = maxAvg > 0 ? (avgPerDay[i] / maxAvg) : 0;
    const height = Math.max(8, Math.round(pct * 80));
    const hue = pct >= 0.8 ? '#00c864' : pct >= 0.5 ? '#c9952c' : '#555';
    html += '<div style="flex:1;text-align:center;max-width:60px">';
    html += '<div style="font-size:12px;font-weight:600;color:' + hue + ';margin-bottom:4px">' + avgPerDay[i] + '</div>';
    html += '<div style="height:' + height + 'px;background:' + hue + ';border-radius:4px 4px 0 0;transition:height .3s"></div>';
    html += '<div style="font-size:10px;color:var(--text-dim);margin-top:4px">' + dayNames[i] + '</div>';
    html += '</div>';
  }
  html += '</div>';
  html += '<div style="text-align:center;font-size:11px;color:var(--text-dim);margin-top:8px">Average girls rostered per day of the week</div>';
  html += '</div>';
  return html;
}

function buildVenueTrends(venueId) {
  const venueGirls = allGirls.filter(g => g.venue === venueId);
  const now = new Date();
  const thirtyDaysAgo = new Date(); thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
  const thirtyDayStr = thirtyDaysAgo.toISOString().split('T')[0];
  const sevenDaysAgo = new Date(); sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
  const sevenDayStr = sevenDaysAgo.toISOString().split('T')[0];

  const newThisMonth = venueGirls.filter(g => g.startDate && g.startDate >= thirtyDayStr).length;
  const newThisWeek = venueGirls.filter(g => g.startDate && g.startDate >= sevenDayStr).length;
  const activeRecent = venueGirls.filter(g => g.lastRostered && g.lastRostered >= sevenDayStr).length;
  const totalProfiles = venueGirls.length;

  // Count average roster size per week over last 4 weeks
  const weeklyCounts = [];
  for (let w = 0; w < 4; w++) {
    const weekStart = new Date(); weekStart.setDate(weekStart.getDate() - (w + 1) * 7);
    const weekEnd = new Date(); weekEnd.setDate(weekEnd.getDate() - w * 7);
    const wsStr = weekStart.toISOString().split('T')[0];
    const weStr = weekEnd.toISOString().split('T')[0];
    let count = 0;
    for (const g of venueGirls) {
      const cal = calendarData[(g.venue || '') + ':' + g.name];
      if (!cal) continue;
      for (const d of Object.keys(cal)) {
        if (d.startsWith('_')) continue;
        if (d >= wsStr && d < weStr) { count++; break; }
      }
    }
    weeklyCounts.push(count);
  }
  weeklyCounts.reverse(); // oldest first
  const trend = weeklyCounts.length >= 2 ? weeklyCounts[weeklyCounts.length - 1] - weeklyCounts[0] : 0;
  const trendLabel = trend > 0 ? '\u2191 ' + trend + ' more' : trend < 0 ? '\u2193 ' + Math.abs(trend) + ' fewer' : '\u2194 Stable';
  const trendColor = trend > 0 ? '#00c864' : trend < 0 ? '#e74c3c' : '#c9952c';

  let html = '<div style="margin:24px 0">';
  html += '<div class="venue-divider"><span>\u2014 VENUE TRENDS \u2014</span></div>';
  html += '<div style="display:flex;gap:12px;justify-content:center;flex-wrap:wrap;margin-bottom:16px">';
  html += '<div style="text-align:center;padding:12px 20px;background:rgba(201,149,44,0.05);border:1px solid rgba(201,149,44,0.12);border-radius:8px;min-width:80px"><div style="font-size:22px;font-weight:700;color:var(--gold)">' + totalProfiles + '</div><div style="font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px">Total</div></div>';
  html += '<div style="text-align:center;padding:12px 20px;background:rgba(0,200,100,0.05);border:1px solid rgba(0,200,100,0.12);border-radius:8px;min-width:80px"><div style="font-size:22px;font-weight:700;color:#00c864">' + newThisMonth + '</div><div style="font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px">New (30d)</div></div>';
  html += '<div style="text-align:center;padding:12px 20px;background:rgba(74,158,255,0.05);border:1px solid rgba(74,158,255,0.12);border-radius:8px;min-width:80px"><div style="font-size:22px;font-weight:700;color:#4a9eff">' + activeRecent + '</div><div style="font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px">Active (7d)</div></div>';
  html += '<div style="text-align:center;padding:12px 20px;background:rgba(201,149,44,0.05);border:1px solid rgba(201,149,44,0.12);border-radius:8px;min-width:80px"><div style="font-size:22px;font-weight:700;color:' + trendColor + '">' + trendLabel + '</div><div style="font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px">4-Week Trend</div></div>';
  html += '</div>';

  // Mini sparkline of weekly roster counts
  if (weeklyCounts.some(c => c > 0)) {
    const sparkMax = Math.max(...weeklyCounts);
    html += '<div style="display:flex;gap:4px;justify-content:center;align-items:flex-end;height:40px;margin-bottom:8px">';
    for (let i = 0; i < weeklyCounts.length; i++) {
      const h = sparkMax > 0 ? Math.max(4, Math.round((weeklyCounts[i] / sparkMax) * 36)) : 4;
      const label = i === weeklyCounts.length - 1 ? 'This week' : (weeklyCounts.length - 1 - i) + 'w ago';
      html += '<div style="flex:1;max-width:50px;text-align:center" title="' + label + ': ' + weeklyCounts[i] + ' girls">';
      html += '<div style="height:' + h + 'px;background:var(--gold);border-radius:2px;margin:0 auto;width:70%"></div>';
      html += '</div>';
    }
    html += '</div>';
    html += '<div style="text-align:center;font-size:10px;color:var(--text-dim)">Weekly roster participation (last 4 weeks)</div>';
  }

  html += '</div>';
  return html;
}

function buildProfileRetention(venueId) {
  const all = allGirls.filter(g => g.venue === venueId);
  if (all.length < 3) return '';
  const now = new Date();
  const sevenAgo = new Date(); sevenAgo.setDate(sevenAgo.getDate() - 7);
  const sevenStr = sevenAgo.toISOString().split('T')[0];
  const msPerDay = 86400000;

  const active = all.filter(g => g.deleted !== 'Yes' && g.lastRostered && g.lastRostered >= sevenStr);
  const departed = all.filter(g => g.deleted === 'Yes' && g.deletedAt && g.startDate);

  // Career / tenure calculations
  const activeTenures = active.filter(g => g.startDate).map(g => Math.floor((now - new Date(g.startDate + 'T00:00:00')) / msPerDay));
  const departedCareers = departed.map(g => Math.floor((new Date(g.deletedAt) - new Date(g.startDate + 'T00:00:00')) / msPerDay)).filter(d => d >= 0);

  const avg = arr => arr.length ? Math.round(arr.reduce((a, b) => a + b, 0) / arr.length) : 0;
  const avgActiveDays = avg(activeTenures);
  const avgCareerDays = avg(departedCareers);
  const totalEverSeen = all.length;
  const churnRate = totalEverSeen > 0 ? Math.round((departed.length / totalEverSeen) * 100) : 0;

  const daysLabel = d => {
    if (d < 30) return d + ' days';
    if (d < 365) return Math.round(d / 30) + ' mo';
    const yr = Math.floor(d / 365); const rem = Math.round((d % 365) / 30);
    return yr + ' yr' + (rem > 0 ? ' ' + rem + ' mo' : '');
  };

  // Tenure distribution — 5 buckets
  const buckets = [
    { label: '< 1mo', min: 0, max: 30, active: 0, departed: 0, color: '#e74c3c' },
    { label: '1-3mo', min: 30, max: 90, active: 0, departed: 0, color: '#f39c12' },
    { label: '3-6mo', min: 90, max: 180, active: 0, departed: 0, color: '#c9952c' },
    { label: '6-12mo', min: 180, max: 365, active: 0, departed: 0, color: '#4a9eff' },
    { label: '1yr+', min: 365, max: Infinity, active: 0, departed: 0, color: '#00c864' },
  ];
  for (const d of activeTenures) {
    const b = buckets.find(bb => d >= bb.min && d < bb.max);
    if (b) b.active++;
  }
  for (const d of departedCareers) {
    const b = buckets.find(bb => d >= bb.min && d < bb.max);
    if (b) b.departed++;
  }
  const maxBucket = Math.max(...buckets.map(b => b.active + b.departed), 1);

  // Monthly churn timeline (last 6 months)
  const churnByMonth = [];
  for (let i = 5; i >= 0; i--) {
    const mDate = new Date(now); mDate.setMonth(mDate.getMonth() - i); mDate.setDate(1);
    const mStart = mDate.toISOString().substring(0, 7); // YYYY-MM
    const mLabel = mDate.toLocaleDateString('en-AU', { month: 'short' });
    const count = departed.filter(g => g.deletedAt && g.deletedAt.substring(0, 7) === mStart).length;
    churnByMonth.push({ label: mLabel, count });
  }
  const maxChurn = Math.max(...churnByMonth.map(c => c.count), 1);

  // Hall of Fame — current veterans (active, top 3 tenure)
  const veterans = active.filter(g => g.startDate).map(g => ({ g, days: Math.floor((now - new Date(g.startDate + 'T00:00:00')) / msPerDay) })).sort((a, b) => b.days - a.days).slice(0, 3);

  // Legends who left — top 3 longest careers of departed
  const legends = departed.map(g => ({ g, days: Math.floor((new Date(g.deletedAt) - new Date(g.startDate + 'T00:00:00')) / msPerDay) })).filter(x => x.days >= 0).sort((a, b) => b.days - a.days).slice(0, 3);

  let html = '<div style="margin:24px 0">';
  html += '<div class="venue-divider"><span>\u2014 PROFILE RETENTION \u2014</span></div>';

  // Stats row
  html += '<div style="display:flex;gap:12px;justify-content:center;flex-wrap:wrap;margin-bottom:20px">';
  html += '<div style="text-align:center;padding:12px 20px;background:rgba(0,200,100,0.05);border:1px solid rgba(0,200,100,0.12);border-radius:8px;min-width:100px"><div style="font-size:18px;font-weight:700;color:#00c864">' + (avgActiveDays ? daysLabel(avgActiveDays) : '\u2014') + '</div><div style="font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px">Avg Active Tenure</div></div>';
  html += '<div style="text-align:center;padding:12px 20px;background:rgba(201,149,44,0.05);border:1px solid rgba(201,149,44,0.12);border-radius:8px;min-width:100px"><div style="font-size:18px;font-weight:700;color:var(--gold)">' + (avgCareerDays ? daysLabel(avgCareerDays) : '\u2014') + '</div><div style="font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px">Avg Career (Departed)</div></div>';
  html += '<div style="text-align:center;padding:12px 20px;background:rgba(231,76,60,0.05);border:1px solid rgba(231,76,60,0.12);border-radius:8px;min-width:100px"><div style="font-size:18px;font-weight:700;color:#e74c3c">' + churnRate + '%</div><div style="font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px">Churn Rate</div></div>';
  html += '<div style="text-align:center;padding:12px 20px;background:rgba(74,158,255,0.05);border:1px solid rgba(74,158,255,0.12);border-radius:8px;min-width:100px"><div style="font-size:18px;font-weight:700;color:#4a9eff">' + departed.length + '</div><div style="font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px">Departed (All-Time)</div></div>';
  html += '</div>';

  // Tenure distribution chart
  html += '<div style="text-align:center;font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:2px;margin-bottom:12px">Tenure Distribution</div>';
  html += '<div style="display:flex;gap:8px;justify-content:center;align-items:flex-end;height:120px;padding:0 12px;margin-bottom:8px">';
  for (const b of buckets) {
    const total = b.active + b.departed;
    const pct = total / maxBucket;
    const height = Math.max(6, Math.round(pct * 100));
    const activeHeight = total > 0 ? Math.round((b.active / total) * height) : 0;
    const departedHeight = height - activeHeight;
    html += '<div style="flex:1;max-width:70px;text-align:center">';
    html += '<div style="font-size:11px;color:var(--text-dim);margin-bottom:4px">' + total + '</div>';
    html += '<div style="display:flex;flex-direction:column-reverse;height:' + height + 'px;background:rgba(255,255,255,0.05);border-radius:4px 4px 0 0;overflow:hidden">';
    if (activeHeight > 0) html += '<div style="height:' + activeHeight + 'px;background:' + b.color + '" title="Active: ' + b.active + '"></div>';
    if (departedHeight > 0) html += '<div style="height:' + departedHeight + 'px;background:rgba(136,136,136,0.5)" title="Departed: ' + b.departed + '"></div>';
    html += '</div>';
    html += '<div style="font-size:10px;color:var(--text-dim);margin-top:4px">' + b.label + '</div>';
    html += '</div>';
  }
  html += '</div>';
  html += '<div style="display:flex;gap:16px;justify-content:center;font-size:10px;color:var(--text-dim);margin-bottom:24px"><span><span style="display:inline-block;width:8px;height:8px;background:#c9952c;border-radius:2px;margin-right:4px;vertical-align:middle"></span>Active</span><span><span style="display:inline-block;width:8px;height:8px;background:rgba(136,136,136,0.5);border-radius:2px;margin-right:4px;vertical-align:middle"></span>Departed</span></div>';

  // Monthly churn timeline
  if (departed.length > 0) {
    html += '<div style="text-align:center;font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:2px;margin-bottom:12px">Departures (Last 6 Months)</div>';
    html += '<div style="display:flex;gap:4px;justify-content:center;align-items:flex-end;height:40px;margin-bottom:8px">';
    for (const m of churnByMonth) {
      const h = Math.max(4, Math.round((m.count / maxChurn) * 36));
      html += '<div style="flex:1;max-width:60px;text-align:center" title="' + m.label + ': ' + m.count + ' departures">';
      html += '<div style="height:' + h + 'px;background:#e74c3c;border-radius:2px;margin:0 auto;width:60%"></div>';
      html += '<div style="font-size:9px;color:var(--text-dim);margin-top:2px">' + m.label + '</div>';
      html += '</div>';
    }
    html += '</div>';
    html += '<div style="margin-bottom:24px"></div>';
  }

  // Current Veterans
  if (veterans.length) {
    html += '<div style="text-align:center;font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:2px;margin-bottom:12px">\u2605 Current Veterans</div>';
    html += '<div style="display:flex;gap:14px;justify-content:center;flex-wrap:wrap;margin-bottom:16px">';
    for (const v of veterans) {
      const g = v.g;
      const img = g.photos && g.photos[0] ? '<img src="' + imgProxy(g.photos[0]) + '" alt="' + (g.name||'') + '" style="width:80px;height:100px;object-fit:cover;border-radius:8px;border:1px solid rgba(201,149,44,0.3)">' : '';
      html += '<div style="text-align:center;cursor:pointer" onclick="showProfile(allGirls.find(gg=>gg.venue===\'' + g.venue + '\'&&gg.name===\'' + (g.name||'').replace(/'/g,"\\'") + '\'))">';
      html += img;
      html += '<div style="font-family:Playfair Display,serif;font-size:12px;color:var(--gold);margin-top:6px">' + (g.name || '') + '</div>';
      html += '<div style="font-size:10px;color:#00c864;font-weight:600">' + daysLabel(v.days) + '</div>';
      html += '</div>';
    }
    html += '</div>';
  }

  // Legends who left
  if (legends.length) {
    html += '<div style="text-align:center;font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:2px;margin-bottom:12px;margin-top:16px">Legends Who Left</div>';
    html += '<div style="display:flex;gap:14px;justify-content:center;flex-wrap:wrap;margin-bottom:8px;opacity:0.75">';
    for (const v of legends) {
      const g = v.g;
      const img = g.photos && g.photos[0] ? '<img src="' + imgProxy(g.photos[0]) + '" alt="' + (g.name||'') + '" style="width:80px;height:100px;object-fit:cover;border-radius:8px;border:1px solid rgba(136,136,136,0.3);filter:grayscale(30%)">' : '';
      html += '<div style="text-align:center">';
      html += img;
      html += '<div style="font-family:Playfair Display,serif;font-size:12px;color:var(--text-dim);margin-top:6px">' + (g.name || '') + '</div>';
      html += '<div style="font-size:10px;color:#888">' + daysLabel(v.days) + '</div>';
      html += '</div>';
    }
    html += '</div>';
  }

  html += '</div>';
  return html;
}

function buildTurnoverReport(venueId) {
  const venueGirls = allGirls.filter(g => g.venue === venueId);
  if (venueGirls.length < 3) return '';
  const threeMonthsAgo = new Date(); threeMonthsAgo.setDate(threeMonthsAgo.getDate() - 90);
  const threeMonthStr = threeMonthsAgo.toISOString().split('T')[0];
  const oneMonthAgo = new Date(); oneMonthAgo.setDate(oneMonthAgo.getDate() - 30);
  const oneMonthStr = oneMonthAgo.toISOString().split('T')[0];

  const existedThreeMonths = venueGirls.filter(g => g.startDate && g.startDate <= threeMonthStr);
  const stillActive = existedThreeMonths.filter(g => g.lastRostered && g.lastRostered >= oneMonthStr);
  const retention = existedThreeMonths.length > 0 ? Math.round((stillActive.length / existedThreeMonths.length) * 100) : 0;

  const newLastMonth = venueGirls.filter(g => g.startDate && g.startDate >= oneMonthStr).length;
  const inactive = venueGirls.filter(g => !g.lastRostered || g.lastRostered < oneMonthStr).length;

  const retColor = retention >= 70 ? '#00c864' : retention >= 40 ? '#c9952c' : '#e74c3c';
  const retLabel = retention >= 70 ? 'High Stability' : retention >= 40 ? 'Moderate' : 'High Turnover';

  let html = '<div style="margin:24px 0">';
  html += '<div class="venue-divider"><span>\u2014 TURNOVER REPORT \u2014</span></div>';
  html += '<div style="display:flex;gap:12px;justify-content:center;flex-wrap:wrap;margin-bottom:8px">';
  html += '<div style="text-align:center;padding:12px 20px;background:rgba(201,149,44,0.05);border:1px solid rgba(201,149,44,0.12);border-radius:8px;min-width:90px"><div style="font-size:22px;font-weight:700;color:' + retColor + '">' + retention + '%</div><div style="font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px">3-Month Retention</div></div>';
  html += '<div style="text-align:center;padding:12px 20px;background:rgba(0,200,100,0.05);border:1px solid rgba(0,200,100,0.12);border-radius:8px;min-width:90px"><div style="font-size:22px;font-weight:700;color:#00c864">' + newLastMonth + '</div><div style="font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px">New (30d)</div></div>';
  html += '<div style="text-align:center;padding:12px 20px;background:rgba(231,76,60,0.05);border:1px solid rgba(231,76,60,0.12);border-radius:8px;min-width:90px"><div style="font-size:22px;font-weight:700;color:#e74c3c">' + inactive + '</div><div style="font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px">Inactive (30d+)</div></div>';
  html += '</div>';
  html += '<div style="text-align:center;font-size:11px;color:' + retColor + ';font-weight:600">' + retLabel + ' \u2014 ' + stillActive.length + '/' + existedThreeMonths.length + ' girls from 3 months ago still active</div>';
  html += '</div>';
  return html;
}

function buildPriceIndex(venueId) {
  const venueGirls = allGirls.filter(g => g.venue === venueId && g.val1);
  if (venueGirls.length < 3) return '';

  const prices30 = venueGirls.map(g => parseInt(g.val1)).filter(v => v > 0);
  const prices45 = venueGirls.filter(g => g.val2).map(g => parseInt(g.val2)).filter(v => v > 0);
  const prices60 = venueGirls.filter(g => g.val3).map(g => parseInt(g.val3)).filter(v => v > 0);

  const avg = arr => arr.length ? Math.round(arr.reduce((a, b) => a + b, 0) / arr.length) : 0;
  const min = arr => arr.length ? Math.min(...arr) : 0;
  const max = arr => arr.length ? Math.max(...arr) : 0;

  // Compare to all-venue average
  const allPrices30 = allGirls.filter(g => g.val1).map(g => parseInt(g.val1)).filter(v => v > 0);
  const globalAvg30 = avg(allPrices30);
  const venueAvg30 = avg(prices30);
  const diff = globalAvg30 > 0 ? Math.round(((venueAvg30 - globalAvg30) / globalAvg30) * 100) : 0;
  const diffLabel = diff > 5 ? diff + '% above avg' : diff < -5 ? Math.abs(diff) + '% below avg' : 'Near average';
  const diffColor = diff > 5 ? '#e74c3c' : diff < -5 ? '#00c864' : '#c9952c';

  let html = '<div style="margin:24px 0">';
  html += '<div class="venue-divider"><span>\u2014 PRICE INDEX \u2014</span></div>';
  html += '<table style="width:100%;max-width:400px;margin:0 auto;border-collapse:collapse;font-size:13px">';
  html += '<tr style="border-bottom:1px solid rgba(201,149,44,0.12)"><td style="padding:6px 0;color:var(--text-dim)">Duration</td><td style="padding:6px 8px;text-align:center;color:var(--text-dim)">Min</td><td style="padding:6px 8px;text-align:center;color:var(--text-dim)">Avg</td><td style="padding:6px 8px;text-align:center;color:var(--text-dim)">Max</td></tr>';
  if (prices30.length) html += '<tr><td style="padding:6px 0;color:var(--gold)">30 min</td><td style="text-align:center">$' + min(prices30) + '</td><td style="text-align:center;font-weight:600;color:var(--gold)">$' + avg(prices30) + '</td><td style="text-align:center">$' + max(prices30) + '</td></tr>';
  if (prices45.length) html += '<tr><td style="padding:6px 0;color:var(--gold)">45 min</td><td style="text-align:center">$' + min(prices45) + '</td><td style="text-align:center;font-weight:600;color:var(--gold)">$' + avg(prices45) + '</td><td style="text-align:center">$' + max(prices45) + '</td></tr>';
  if (prices60.length) html += '<tr><td style="padding:6px 0;color:var(--gold)">60 min</td><td style="text-align:center">$' + min(prices60) + '</td><td style="text-align:center;font-weight:600;color:var(--gold)">$' + avg(prices60) + '</td><td style="text-align:center">$' + max(prices60) + '</td></tr>';
  html += '</table>';
  html += '<div style="text-align:center;font-size:11px;margin-top:8px;color:' + diffColor + ';font-weight:600">vs Sydney average (30 min): ' + diffLabel + '</div>';
  html += '</div>';
  return html;
}

function renderVenuePage(regionSlug, suburbSlug, venueId) {
  const v = VENUE_DATA[venueId];
  if (!v) return null;

  const girls = getFiltered().filter(g => g.venue === venueId);
  const priceRange = venuePriceRange(venueId);
  const sevenDaysAgoVen = new Date(); sevenDaysAgoVen.setDate(sevenDaysAgoVen.getDate() - 7);
  const sevenDayStrVen = sevenDaysAgoVen.toISOString().split('T')[0];
  const venueActiveCount = girls.filter(g => g.lastRostered && g.lastRostered >= sevenDayStrVen).length;

  updateMeta(
    v.name + ' \u2013 ' + v.suburb + ', Sydney | Brothel Search',
    v.name + ' at ' + v.address + '. ' + venueActiveCount + ' girls active.' + (priceRange ? ' Prices from ' + priceRange + '.' : '') + ' Browse profiles, photos and rosters.',
    'https://brothelsearch.com/og-preview.png',
    'https://brothelsearch.com/sydney/' + (regionSlug || VENUE_REGIONS[venueId] || 'other') + '/' + v.suburbSlug + '/' + venueId + '/',
    { '@context': 'https://schema.org', '@type': 'LocalBusiness', name: v.name, url: v.url, address: { '@type': 'PostalAddress', streetAddress: v.address.split(',')[0], addressLocality: v.suburb, addressRegion: 'NSW', addressCountry: 'AU' } }
  );

  let html = '';
  html += '<div class="landing-page">';
  html += '<h1 class="landing-title">' + v.name + '</h1>';
  html += '<div class="landing-venue-meta">';
  html += '<div class="landing-card-address">' + v.address + '</div>';
  html += '<a href="' + v.url + '" target="_blank" rel="noopener" class="landing-venue-link">' + v.url.replace(/^https?:\/\//, '').replace(/\/$/, '') + '</a>';
  html += '<a href="https://www.google.com/maps/dir/?api=1&destination=' + encodeURIComponent(v.address) + '" target="_blank" rel="noopener" class="landing-venue-link" style="margin-left:16px">Open in Google Maps \u2192</a>';
  html += '</div>';
  if (v.desc) html += '<blockquote class="venue-pullquote">' + v.desc + '</blockquote>';
  const thirtyDaysAgoV = new Date(); thirtyDaysAgoV.setDate(thirtyDaysAgoV.getDate() - 30);
  const thirtyDayStrV = thirtyDaysAgoV.toISOString().split('T')[0];
  const activeCount = girls.filter(g => g.lastRostered && g.lastRostered >= thirtyDayStrV).length;
  const a30 = venueAvgPrice(venueId, 'val1');
  const a45 = venueAvgPrice(venueId, 'val2');
  const a60 = venueAvgPrice(venueId, 'val3');
  html += '<p class="landing-desc">' + activeCount + '/' + girls.length + ' girls active in past month. ' + buildCrowdIndicator(venueId);
  if (a30) html += ' Average ' + a30 + ' for 30 min.';
  if (a45) html += ' Average ' + a45 + ' for 45 min.';
  if (a60) html += ' Average ' + a60 + ' for 60 min.';
  html += '</p>';
  html += '<hr class="gold-divider">';
  html += buildVenueReviewSection(venueId, []);
  html += buildBestTimeToVisit(venueId);
  html += buildVenueTrends(venueId);
  html += buildProfileRetention(venueId);
  html += buildPriceIndex(venueId);
  html += '<hr class="gold-divider">';
  const venueBasePath = '/sydney/' + (regionSlug || VENUE_REGIONS[venueId] || 'other') + '/' + v.suburbSlug + '/' + venueId;
  const layoutSvgs = {
    grid: '<svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor"><rect x="1" y="1" width="6" height="6" rx="1"/><rect x="9" y="1" width="6" height="6" rx="1"/><rect x="1" y="9" width="6" height="6" rx="1"/><rect x="9" y="9" width="6" height="6" rx="1"/></svg>',
    bento: '<svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor"><rect x="1" y="1" width="6" height="9" rx="1"/><rect x="9" y="1" width="6" height="4" rx="1"/><rect x="1" y="12" width="6" height="3" rx="1"/><rect x="9" y="7" width="6" height="8" rx="1"/></svg>',
    compact: '<svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor"><rect x="1" y="1" width="3" height="3"/><rect x="6.5" y="1" width="3" height="3"/><rect x="12" y="1" width="3" height="3"/><rect x="1" y="6.5" width="3" height="3"/><rect x="6.5" y="6.5" width="3" height="3"/><rect x="12" y="6.5" width="3" height="3"/><rect x="1" y="12" width="3" height="3"/><rect x="6.5" y="12" width="3" height="3"/><rect x="12" y="12" width="3" height="3"/></svg>',
    list: '<svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor"><rect x="1" y="2" width="14" height="3" rx="1"/><rect x="1" y="6.5" width="14" height="3" rx="1"/><rect x="1" y="11" width="14" height="3" rx="1"/></svg>',
  };
  let toggleBtns = '';
  for (const m of ['bento','grid','compact','list']) {
    toggleBtns += '<button class="' + (currentLayout === m ? 'active' : '') + '" onclick="history.pushState(null,\'\',\'' + venueBasePath + '/' + m + '\');handleLandingRoute(\'' + venueBasePath + '/' + m + '\')" title="' + m.charAt(0).toUpperCase() + m.slice(1) + '">' + layoutSvgs[m] + '</button>';
  }
  html += '<div style="display:flex;align-items:center;justify-content:flex-end;margin-top:12px;margin-bottom:8px"><div class="layout-toggle">' + toggleBtns + '</div></div>';
  html += '<div class="girls-grid ' + currentLayout + '" id="venuePageGrid" style="margin-top:0">';

  girls.sort((a, b) => (matchScores.get(b.venue + ':' + b.name) || 0) - (matchScores.get(a.venue + ':' + a.name) || 0));

  // Paginate: 50 for compact, 12 for other layouts
  const venuePageSize = currentLayout === 'compact' ? 100 : 12;
  window._venueAllGirls = girls;
  window._venueVenue = v;
  window._venuePageShown = Math.min(venuePageSize, girls.length);
  window._venuePageSize = venuePageSize;
  const girlsToShow = girls.slice(0, window._venuePageShown);

  for (const g of girlsToShow) {
    const countries = countriesWithFlags(g.country);
    const girlKey = g.venue + ':' + g.name;
    const girlScore = matchScores.get(girlKey) || 0;
    const showBadge = userPreferences && girlScore > 0;
    const lastRostered = (() => {
      const avail = getAvailabilityText(g);
      if (avail && avail !== 'ended') return avail;
      if (!g.lastRostered) return '';
      const today = new Date(); today.setHours(0,0,0,0);
      const rd = new Date(g.lastRostered + 'T00:00:00');
      const diff = Math.round((today - rd) / 86400000);
      if (diff === 0) return 'Last rostered: Today';
      if (diff === 1) return 'Last rostered: Yesterday';
      if (diff < 0) {
        const dayNames = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
        const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        return 'Next: ' + dayNames[rd.getDay()] + ' ' + rd.getDate() + ' ' + monthNames[rd.getMonth()];
      }
      return 'Last rostered: ' + diff + ' days ago';
    })();
    const img = g.photos && g.photos.length
      ? '<img class="card-thumb" src="' + imgProxy(g.photos[0]) + '" alt="' + (g.name || '').replace(/"/g, '&quot;') + ' \u2013 ' + v.name + ' ' + v.suburb + ', Sydney" loading="lazy">'
      : '<div class="silhouette"></div>';
    const heartSvg = '<svg viewBox="0 0 24 24"><path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"/></svg>';
    const hideSvg2 = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24"/><line x1="1" y1="1" x2="23" y2="23"/></svg>';
    const availTextV = getAvailabilityText(g);
    const glowClassV = availTextV && availTextV.startsWith('Available Now') ? ' glow-now' : availTextV && (availTextV.startsWith('Available Later') || availTextV.startsWith('Available Future')) ? ' glow-later' : '';
    html += '<div class="girl-card card-settled' + (isFavorite(g) ? ' favorited' : '') + glowClassV + '">';
    html += '<div class="fav-heart' + (isFavorite(g) ? ' active' : '') + '" data-url="' + (g.oldUrl||'').replace(/"/g,'&quot;') + '" onclick="event.stopPropagation();toggleFavorite(\'' + (g.oldUrl||'').replace(/'/g, "\\'") + '\',event)">' + heartSvg + '</div>';
    html += '<div class="hide-btn' + (isHidden(g) ? ' active' : '') + '" data-url="' + (g.oldUrl||'').replace(/"/g,'&quot;') + '" onclick="event.stopPropagation();toggleHidden(\'' + (g.oldUrl||'').replace(/'/g, "\\'") + '\',event)">' + hideSvg2 + '</div>';
    html += '<div class="card-badges">' + '<span class="country-badge">' + v.name + '</span>';
    if (showBadge) html += '<div class="match-badge' + (girlScore >= 90 ? ' match-gold' : '') + '">' + girlScore + '%</div>';
    if (isNewProfile(g)) html += '<span class="new-badge">New</span>'; else if (isReturnProfile(g)) html += '<span class="return-badge">Return</span>';
    if (g.pornstar) html += '<span class="av-badge">AV</span>';
    html += '</div>';
    html += '<div class="card-img">' + img + '</div>';
    html += '<div class="card-name-overlay"><span>' + (g.name || '') + '</span></div>';
    html += '<div class="card-info">';
    html += '<div class="card-name">' + (g.name || '') + '</div>';
    html += '<div class="card-country">' + countries + '</div>';
    html += '<div class="card-stats">';
    if (g.age) html += '<span>Age ' + g.age + '</span>';
    if (g.body) html += '<span>Body ' + g.body + '</span>';
    if (g.height) html += '<span>' + g.height + 'cm</span>';
    if (g.cup) html += '<span>' + g.cup + ' cup</span>';
    html += '</div>';
    if (g.val1 || g.val2 || g.val3) html += '<div class="card-rates">' + [g.val1 ? '$'+g.val1 : '', g.val2 ? '$'+g.val2 : '', g.val3 ? '$'+g.val3 : ''].filter(Boolean).join(' / ') + '</div>';
    if (lastRostered) html += '<div class="card-last-rostered' + (lastRostered.startsWith('Available Now') ? ' available-now' : lastRostered.startsWith('Available Later') ? ' available-later' : lastRostered.startsWith('Available Future') ? ' available-future' : '') + '">' + lastRostered + '</div>';
    html += '</div>';
    // Extra columns for list view
    html += '<div class="card-list-extra">';
    if (lastRostered) html += '<div class="cle-row"><span class="cle-label">Last Avail</span><span class="' + (lastRostered.startsWith('Available Now') ? 'available-now' : lastRostered.startsWith('Available Later') ? 'available-later' : lastRostered.startsWith('Available Future') ? 'available-future' : '') + '">' + lastRostered + '</span></div>';
    if (g.startDate) html += '<div class="cle-row"><span class="cle-label">Start</span><span>' + g.startDate + '</span></div>';
    if (g.exp) html += '<div class="cle-row"><span class="cle-label">Exp</span><span>' + g.exp + '</span></div>';
    if (g.lang) html += '<div class="cle-row"><span class="cle-label">Lang</span><span>' + g.lang + '</span></div>';
    if (g.type) html += '<div class="cle-row"><span class="cle-label">Type</span><span>' + g.type + '</span></div>';
    if (g.oldUrl) html += '<div class="cle-row"><span class="cle-label">Ref</span><a href="' + g.oldUrl + '" target="_blank" rel="noopener" onclick="event.stopPropagation()" style="color:var(--accent);text-decoration:none;word-break:break-all">Link</a></div>';
    html += '</div>';
    html += '<div class="card-list-extra card-list-extra-wide">';
    if (g.desc) html += '<div class="cle-desc">' + g.desc.replace(/</g, '&lt;') + '</div>';
    if (g.labels && g.labels.length) html += '<div class="cle-labels">' + g.labels.map(l => '<span class="cle-label-pill">' + l + '</span>').join('') + '</div>';
    html += '</div>';
    html += '</div>';
  }

  html += '</div></div>';

  // Attach click handlers after render
  setTimeout(() => {
    const grid = document.querySelector('#landingPage .girls-grid');
    if (!grid) return;
    grid.querySelectorAll('.girl-card').forEach((card, i) => {
      const g = girls[i];
      if (!g) return;
      card.style.cursor = 'pointer';
      card.onclick = (e) => { if (!e.target.closest('.fav-heart')) showProfile(g); };
      const heart = card.querySelector('.fav-heart');
      if (heart) heart.addEventListener('click', (e) => { e.stopPropagation(); toggleFavorite(g.oldUrl, e); });
    });
    if (currentLayout === 'compact') renderVenueCompactPagination();
  }, 50);

  return html;
}

// Build a single venue card HTML (for pagination load-more)
function buildVenueCardHtml(g, v) {
  const countries = countriesWithFlags(g.country);
  const girlKey = g.venue + ':' + g.name;
  const girlScore = matchScores.get(girlKey) || 0;
  const showBadge = userPreferences && girlScore > 0;
  const lastRostered = (() => {
    const avail = getAvailabilityText(g);
    if (avail && avail !== 'ended') return avail;
    if (!g.lastRostered) return '';
    const today = new Date(); today.setHours(0,0,0,0);
    const rd = new Date(g.lastRostered + 'T00:00:00');
    const diff = Math.round((today - rd) / 86400000);
    if (diff === 0) return 'Last rostered: Today';
    if (diff === 1) return 'Last rostered: Yesterday';
    if (diff < 0) {
      const dayNames = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
      const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
      return 'Next: ' + dayNames[rd.getDay()] + ' ' + rd.getDate() + ' ' + monthNames[rd.getMonth()];
    }
    return 'Last rostered: ' + diff + ' days ago';
  })();
  const img = g.photos && g.photos.length
    ? '<img class="card-thumb" src="' + imgProxy(g.photos[0]) + '" alt="' + (g.name || '').replace(/"/g, '&quot;') + '" loading="lazy">'
    : '<div class="silhouette"></div>';
  const heartSvg = '<svg viewBox="0 0 24 24"><path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"/></svg>';
  const hideSvg2 = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24"/><line x1="1" y1="1" x2="23" y2="23"/></svg>';
  const availTextV = getAvailabilityText(g);
  const glowClassV = availTextV && availTextV.startsWith('Available Now') ? ' glow-now' : availTextV && (availTextV.startsWith('Available Later') || availTextV.startsWith('Available Future')) ? ' glow-later' : '';
  let h = '<div class="girl-card card-settled' + (isFavorite(g) ? ' favorited' : '') + glowClassV + '">';
  h += '<div class="fav-heart' + (isFavorite(g) ? ' active' : '') + '" data-url="' + (g.oldUrl||'').replace(/"/g,'&quot;') + '" onclick="event.stopPropagation();toggleFavorite(\'' + (g.oldUrl||'').replace(/'/g, "\\'") + '\',event)">' + heartSvg + '</div>';
  h += '<div class="hide-btn' + (isHidden(g) ? ' active' : '') + '" data-url="' + (g.oldUrl||'').replace(/"/g,'&quot;') + '" onclick="event.stopPropagation();toggleHidden(\'' + (g.oldUrl||'').replace(/'/g, "\\'") + '\',event)">' + hideSvg2 + '</div>';
  h += '<div class="card-badges"><span class="country-badge">' + v.name + '</span>';
  if (showBadge) h += '<div class="match-badge' + (girlScore >= 90 ? ' match-gold' : '') + '">' + girlScore + '%</div>';
  if (isNewProfile(g)) h += '<span class="new-badge">New</span>'; else if (isReturnProfile(g)) h += '<span class="return-badge">Return</span>';
  if (g.pornstar) h += '<span class="av-badge">AV</span>';
  h += '</div>';
  h += '<div class="card-img">' + img + '</div>';
  h += '<div class="card-name-overlay"><span>' + (g.name || '') + '</span></div>';
  h += '<div class="card-info">';
  h += '<div class="card-name">' + (g.name || '') + '</div>';
  h += '<div class="card-country">' + countries + '</div>';
  h += '<div class="card-stats">';
  if (g.age) h += '<span>Age ' + g.age + '</span>';
  if (g.body) h += '<span>Body ' + g.body + '</span>';
  if (g.height) h += '<span>' + g.height + 'cm</span>';
  if (g.cup) h += '<span>' + g.cup + ' cup</span>';
  h += '</div>';
  if (g.val1 || g.val2 || g.val3) h += '<div class="card-rates">' + [g.val1 ? '$'+g.val1 : '', g.val2 ? '$'+g.val2 : '', g.val3 ? '$'+g.val3 : ''].filter(Boolean).join(' / ') + '</div>';
  if (lastRostered) h += '<div class="card-last-rostered' + (lastRostered.startsWith('Available Now') ? ' available-now' : lastRostered.startsWith('Available Later') ? ' available-later' : lastRostered.startsWith('Available Future') ? ' available-future' : '') + '">' + lastRostered + '</div>';
  h += '</div>';
  h += '<div class="card-list-extra">';
  if (lastRostered) h += '<div class="cle-row"><span class="cle-label">Last Avail</span><span class="' + (lastRostered.startsWith('Available Now') ? 'available-now' : lastRostered.startsWith('Available Later') ? 'available-later' : lastRostered.startsWith('Available Future') ? 'available-future' : '') + '">' + lastRostered + '</span></div>';
  if (g.startDate) h += '<div class="cle-row"><span class="cle-label">Start</span><span>' + g.startDate + '</span></div>';
  if (g.exp) h += '<div class="cle-row"><span class="cle-label">Exp</span><span>' + g.exp + '</span></div>';
  if (g.lang) h += '<div class="cle-row"><span class="cle-label">Lang</span><span>' + g.lang + '</span></div>';
  if (g.type) h += '<div class="cle-row"><span class="cle-label">Type</span><span>' + g.type + '</span></div>';
  if (g.oldUrl) h += '<div class="cle-row"><span class="cle-label">Ref</span><a href="' + g.oldUrl + '" target="_blank" rel="noopener" onclick="event.stopPropagation()" style="color:var(--accent);text-decoration:none;word-break:break-all">Link</a></div>';
  h += '</div>';
  h += '<div class="card-list-extra card-list-extra-wide">';
  if (g.desc) h += '<div class="cle-desc">' + g.desc.replace(/</g, '&lt;') + '</div>';
  if (g.labels && g.labels.length) h += '<div class="cle-labels">' + g.labels.map(l => '<span class="cle-label-pill">' + l + '</span>').join('') + '</div>';
  h += '</div>';
  h += '</div>';
  return h;
}

// Infinite scroll load-more for venue pages
function loadMoreVenuePage() {
  if (window._venueLoadingMore) return;
  const grid = document.getElementById('venuePageGrid');
  const girls = window._venueAllGirls;
  const v = window._venueVenue;
  if (!grid || !girls || !v) return;
  if (window._venuePageShown >= girls.length) return;
  window._venueLoadingMore = true;
  const nextEnd = Math.min(window._venuePageShown + window._venuePageSize, girls.length);
  const startIdx = window._venuePageShown;
  for (let i = startIdx; i < nextEnd; i++) {
    const wrap = document.createElement('div');
    wrap.innerHTML = buildVenueCardHtml(girls[i], v);
    const card = wrap.firstChild;
    grid.appendChild(card);
    card.style.cursor = 'pointer';
    const g = girls[i];
    card.onclick = (e) => { if (!e.target.closest('.fav-heart') && !e.target.closest('.hide-btn')) showProfile(g); };
    if (currentLayout === 'list') {
      const nameEl = card.querySelector('.card-name');
      const heart = card.querySelector('.fav-heart');
      const hide = card.querySelector('.hide-btn');
      if (nameEl && heart && hide) { nameEl.appendChild(heart); nameEl.appendChild(hide); const cmp = el.querySelector('.compare-btn'); if (cmp) nameEl.appendChild(cmp); }
    }
  }
  window._venuePageShown = nextEnd;
  window._venueLoadingMore = false;
}

window.addEventListener('scroll', () => {
  if (!document.getElementById('venuePageGrid')) return;
  if (currentLayout === 'compact') return;
  if (window.innerHeight + window.scrollY >= document.body.offsetHeight - 600) {
    loadMoreVenuePage();
  }
});

function renderVenueCompactPagination() {
  const grid = document.getElementById('venuePageGrid');
  if (!grid || !window._venueAllGirls) return;
  let pag = document.getElementById('venueCompactPagination');
  if (!pag) { pag = document.createElement('div'); pag.id = 'venueCompactPagination'; pag.className = 'compact-pagination'; grid.after(pag); }
  const ps = window._venuePageSize;
  const total = window._venueAllGirls.length;
  const totalPages = Math.ceil(total / ps);
  const curPage = Math.ceil(window._venuePageShown / ps);
  if (totalPages <= 1) { pag.innerHTML = ''; return; }
  let html = '<button class="compact-pag-btn" ' + (curPage <= 1 ? 'disabled' : '') + ' onclick="venueCompactGoPage(' + (curPage - 1) + ')">&lsaquo; Prev</button>';
  html += '<span class="compact-pag-info">Page ' + curPage + ' of ' + totalPages + '</span>';
  html += '<button class="compact-pag-btn" ' + (curPage >= totalPages ? 'disabled' : '') + ' onclick="venueCompactGoPage(' + (curPage + 1) + ')">Next &rsaquo;</button>';
  pag.innerHTML = html;
}
window.venueCompactGoPage = function(page) {
  const girls = window._venueAllGirls;
  const v = window._venueVenue;
  const ps = window._venuePageSize;
  if (!girls || !v) return;
  const totalPages = Math.ceil(girls.length / ps);
  if (page < 1 || page > totalPages) return;
  const grid = document.getElementById('venuePageGrid');
  grid.innerHTML = '';
  const start = (page - 1) * ps;
  const end = Math.min(start + ps, girls.length);
  for (let i = start; i < end; i++) {
    const wrap = document.createElement('div');
    wrap.innerHTML = buildVenueCardHtml(girls[i], v);
    const card = wrap.firstChild;
    grid.appendChild(card);
    card.style.cursor = 'pointer';
    const g = girls[i];
    card.onclick = (e) => { if (!e.target.closest('.fav-heart') && !e.target.closest('.hide-btn')) showProfile(g); };
  }
  window._venuePageShown = end;
  renderVenueCompactPagination();
  window.scrollTo({ top: 0, behavior: 'smooth' });
};

function isLoggedIn() {
  return document.getElementById('userMenu').style.display !== 'none';
}

function navigateToLanding(path) {
  // Non-logged-in users can only access home
  if (path !== '/' && path !== '/index.html' && !isLoggedIn()) {
    document.getElementById('authOverlay').style.display = 'flex';
    document.body.style.overflow = 'hidden';
    return;
  }
  // Logged-in users must have active subscription (block while loading too)
  if (path !== '/' && path !== '/index.html' && isLoggedIn() && isSubscribed !== true && userRole !== 'admin') {
    showPaywall();
    return;
  }
  const dd = document.getElementById('navBrothelsDropdown');
  if (dd) dd.classList.remove('open');
  const landing = document.getElementById('landingPage');
  if (landing && landing.style.display !== 'none') {
    landing.classList.add('fading');
    setTimeout(() => { history.pushState({ landing: true }, '', path); handleLandingRoute(path); landing.classList.remove('fading'); }, 150);
  } else {
    history.pushState({ landing: true }, '', path);
    handleLandingRoute(path);
  }
}

function initHomePageListeners() {
  const landingEl = document.getElementById('landingPage');
  if (!landingEl) return;
  const homeSearch = document.getElementById('homeSearch');
  if (homeSearch) {
    homeSearch.addEventListener('input', function() {
      const q = this.value.trim().toLowerCase();
      if (q.length >= 2) {
        const results = allGirls.filter(g => (g.name || '').toLowerCase().includes(q) || (Array.isArray(g.country) ? g.country.join(' ') : g.country || '').toLowerCase().includes(q) || (g.venueName || '').toLowerCase().includes(q)).slice(0, 5);
        let dropdown = document.getElementById('homeSearchResults');
        if (!dropdown) { dropdown = document.createElement('div'); dropdown.id = 'homeSearchResults'; dropdown.className = 'home-search-results'; homeSearch.parentElement.appendChild(dropdown); }
        if (results.length) {
          dropdown.innerHTML = results.map(g => '<div class="home-search-item" data-venue="' + g.venue + '" data-name="' + (g.name||'').replace(/"/g,'&quot;') + '">' + (g.photos && g.photos[0] ? '<img src="' + imgProxy(g.photos[0], 40) + '">' : '') + '<div><strong>' + (g.name||'') + '</strong><br><span>' + (g.venueName||'') + ' \u00b7 ' + countriesWithFlags(g.country) + '</span></div></div>').join('');
          dropdown.style.display = 'block';
          dropdown.querySelectorAll('.home-search-item').forEach(el => { el.onclick = () => { const g = allGirls.find(gg => gg.venue === el.dataset.venue && gg.name === el.dataset.name); if (g) showProfile(g); }; });
        } else { dropdown.innerHTML = '<div class="home-search-item"><span>No results</span></div>'; dropdown.style.display = 'block'; }
      } else {
        const dd = document.getElementById('homeSearchResults');
        if (dd) dd.style.display = 'none';
      }
    });
  }
  landingEl.querySelectorAll('.girls-grid .girl-card').forEach(card => {
    card.style.cursor = 'pointer';
    const venue = card.dataset.venue;
    const name = card.dataset.name;
    card.onclick = (e) => {
      if (e.target.closest('.fav-heart')) return;
      const g = allGirls.find(gg => gg.venue === venue && gg.name === name);
      if (g) showProfile(g);
    };
    const heart = card.querySelector('.fav-heart');
    if (heart) heart.addEventListener('click', (e) => {
      e.stopPropagation();
      const url = heart.dataset.url;
      if (url) toggleFavorite(url, e);
    });
  });
  landingEl.querySelectorAll('[data-venue][data-name]:not(.girl-card)').forEach(el => {
    el.style.cursor = 'pointer';
    el.onclick = () => {
      const g = allGirls.find(gg => gg.venue === el.dataset.venue && gg.name === el.dataset.name);
      if (g) showProfile(g);
    };
  });

  // Load recent reviews
  const reviewsContainer = document.getElementById('homeRecentReviews');
  if (reviewsContainer) {
    sbClient.from('reviews').select('*').neq('girl_name', '__venue__').order('created_at', { ascending: false }).limit(4).then(({ data: recentReviews }) => {
      if (!recentReviews || !recentReviews.length) {
        reviewsContainer.innerHTML = '<div style="text-align:center;color:var(--text-dim);font-size:12px;padding:16px 0">No reviews yet.</div>';
        return;
      }
      let rhtml = '<div class="venue-carousel review-carousel wrap">';
      for (const r of recentReviews) {
        const stars = '\u2605'.repeat(r.overall) + '\u2606'.repeat(5 - r.overall);
        const date = new Date(r.created_at).toLocaleDateString('en-AU', { day: 'numeric', month: 'short' });
        const girl = allGirls.find(g => g.venue === r.venue && g.name === r.girl_name);
        const photo = girl && girl.photos && girl.photos[0] ? '<img src="' + imgProxy(girl.photos[0], 40) + '" style="width:40px;height:40px;border-radius:8px;object-fit:cover;flex-shrink:0">' : '';
        const venueName = girl ? girl.venueName : r.venue;
        rhtml += '<div class="home-review-card" style="cursor:pointer" data-rv-venue="' + r.venue + '" data-rv-girl="' + (r.girl_name || '').replace(/"/g, '&quot;') + '">';
        rhtml += '<div style="display:flex;gap:10px;align-items:flex-start">';
        rhtml += photo;
        rhtml += '<div style="flex:1;min-width:0">';
        rhtml += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:2px"><span style="font-family:Orbitron,sans-serif;font-size:10px;color:var(--gold);letter-spacing:1px">' + (r.girl_name || '') + '</span><span style="font-size:10px;color:var(--text-dim)">' + date + '</span></div>';
        rhtml += '<div style="font-size:9px;color:var(--text-dim);margin-bottom:4px">' + venueName + ' \u00b7 by ' + (r.user_name || 'Anonymous') + '</div>';
        rhtml += '<div style="color:#d4af37;font-size:12px;letter-spacing:2px;margin-bottom:4px">' + stars + '</div>';
        if (r.comment) rhtml += '<div style="font-size:12px;color:var(--text);line-height:1.4;overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical">' + r.comment.replace(/</g, '&lt;').substring(0, 150) + '</div>';
        rhtml += '</div></div></div>';
      }
      rhtml += '</div>';
      reviewsContainer.innerHTML = rhtml;

      reviewsContainer.querySelectorAll('.home-review-card').forEach(card => {
        card.onclick = () => {
          const g = allGirls.find(gg => gg.venue === card.dataset.rvVenue && gg.name === card.dataset.rvGirl);
          if (g) showProfile(g);
        };
      });
    });
  }
}

function handleLandingRoute(path) {
  invalidateFilterCache();
  // Particles only on home page
  const isHome = path === '/' || path === '/index.html';
  if (isHome && window._particlesStart) window._particlesStart();
  else if (window._particlesStop) window._particlesStop();
  // Hide profile nav strip on any non-profile navigation
  document.getElementById('profileNavStrip').style.display = 'none';
  window._currentProfileIdx = -1;
  clearInterval(window._profileRotate);

  const parts = path.replace(/^\//, '').replace(/\/$/, '').split('/');
  const landingEl = document.getElementById('landingPage');
  const mainSection = document.querySelector('section.section');

  let html = null;
  const cleanPath = path.replace(/^\//, '').replace(/\/$/, '');

  if (cleanPath === '' || cleanPath === 'index.html') {
    html = renderHomePage();
  } else if (cleanPath === 'profiles' || cleanPath === 'profiles/bento' || cleanPath === 'profiles/grid' || cleanPath === 'profiles/compact' || cleanPath === 'profiles/list') {
    const layoutPart = cleanPath.split('/')[1];
    const layout = ['grid','bento','compact','list'].includes(layoutPart) ? layoutPart : 'bento';
    setLayout(layout, false);
    if (cleanPath === 'profiles') history.replaceState(null, '', '/profiles/' + layout);
    restoreActivePresetOrClear();
    renderFilters(); renderGrid();
    // Show the main profiles section instead
    landingEl.style.display = 'none';
    mainSection.style.display = '';
    document.querySelectorAll('.nav-link').forEach(l => l.classList.toggle('active', l.id === 'navProfiles'));
    window.scrollTo({ top: 0 });
    return true;
  } else if (cleanPath === 'working-now') {
    restoreActivePresetOrClear();
    renderFilters(); renderGrid();
    html = renderWorkingNow();
  } else if (cleanPath === 'compare') {
    restoreActivePresetOrClear();
    renderFilters(); renderGrid();
    html = renderComparePage();
  } else if (cleanPath === 'analytics') {
    html = renderAnalyticsPage();
  } else if (cleanPath === 'roadmap') {
    html = renderRoadmapPage();
  } else if (cleanPath === 'data') {
    if (userRole !== 'admin') { navigateToLanding('/'); return true; }
    restoreActivePresetOrClear();
    renderFilters(); renderGrid();
    html = renderDataPage();
  } else if (parts.length === 1 && parts[0] === 'sydney') {
    html = renderCityPage();
  } else if (parts.length === 2 && parts[0] === 'sydney') {
    // /sydney/{region} — region page
    html = renderRegionPage(parts[1]);
  } else if (parts.length === 3 && parts[0] === 'sydney') {
    // /sydney/{region}/{venue} — venue page (suburb-less legacy or venue directly under region)
    html = renderVenuePage(parts[1], null, parts[2]);
  } else if (parts.length === 4 && parts[0] === 'sydney' && VENUE_DATA[parts[3]]) {
    // /sydney/{region}/{suburb}/{venue} — always default to bento
    setLayout('bento', false);
    history.replaceState(null, '', '/sydney/' + parts[1] + '/' + parts[2] + '/' + parts[3] + '/bento');
    html = renderVenuePage(parts[1], parts[2], parts[3]);
  } else if (parts.length === 5 && parts[0] === 'sydney' && ['bento','grid','compact','list'].includes(parts[4])) {
    // /sydney/{region}/{suburb}/{venue}/{layout}
    setLayout(parts[4], false);
    html = renderVenuePage(parts[1], parts[2], parts[3]);
  }

  if (html) {
    landingEl.innerHTML = html;
    landingEl.style.display = '';
    mainSection.style.display = 'none';
    const activeLinkId = cleanPath === '' || cleanPath === 'index.html' ? 'navHome' : path.includes('working-now') ? 'navWorkingNow' : path.includes('compare') ? 'navCompare' : path.includes('analytics') ? 'navAnalytics' : path.includes('roadmap') ? 'navRoadmap' : 'navBrothels';
    document.querySelectorAll('.nav-link').forEach(l => l.classList.toggle('active', l.id === activeLinkId));
    window.scrollTo({ top: 0 });
    // Init map if on city page
    if (document.getElementById('venueMap')) setTimeout(initVenueMap, 50);
    // Init venue reviews
    const vrs = document.querySelector('.venue-review-section');
    if (vrs) initVenueReviewSection(vrs.dataset.venueId);
    // For list view, move heart/hide icons next to name in venue page cards
    if (currentLayout === 'list') {
      landingEl.querySelectorAll('.girl-card').forEach(el => {
        const nameEl = el.querySelector('.card-name');
        const heart = el.querySelector('.fav-heart');
        const hide = el.querySelector('.hide-btn');
        if (nameEl && heart && hide && !nameEl.querySelector('.fav-heart')) { nameEl.appendChild(heart); nameEl.appendChild(hide); const cmp = el.querySelector('.compare-btn'); if (cmp) nameEl.appendChild(cmp); }
      });
    }
    // Animate count-up numbers — delayed to sync with hero animation
    setTimeout(() => {
      document.querySelectorAll('.home-stat-num').forEach(el => {
        const target = parseInt(el.dataset.target);
        const duration = 1500;
        const start = performance.now();
        function tick(now) {
          const progress = Math.min((now - start) / duration, 1);
          const eased = 1 - Math.pow(1 - progress, 3);
          el.textContent = Math.round(eased * target);
          if (progress < 1) requestAnimationFrame(tick);
        }
        requestAnimationFrame(tick);
      });
    }, 1500);
    // Load referral code for home page
    const refCodeEl = document.getElementById('homeReferralCode');
    if (refCodeEl) {
      getOrCreateReferralCode().then(code => {
        if (code) {
          refCodeEl.style.display = '';
          const refLink = 'https://brothelsearch.com/?ref=' + code;
          refCodeEl.innerHTML = '<div class="referral-code-label">Your referral code</div><div class="referral-code-box"><span>' + code + '</span><button onclick="navigator.clipboard.writeText(\'' + code + '\').then(()=>{this.textContent=\'Copied!\';setTimeout(()=>this.textContent=\'Copy\',1500)})">Copy</button></div>' +
            '<div class="referral-code-label" style="margin-top:12px">Or share this link</div><div class="referral-code-box"><span style="font-size:11px;letter-spacing:1px">' + refLink + '</span><button onclick="navigator.clipboard.writeText(\'' + refLink + '\').then(()=>{this.textContent=\'Copied!\';setTimeout(()=>this.textContent=\'Copy\',1500)})">Copy</button></div>';
        }
      });
    }
    // Init roadmap
    if (document.getElementById('roadmapTable')) setTimeout(initRoadmapPage, 50);
    // Show filter bar on Working Now page
    const fsb = document.getElementById('filterSortBar');
    if (fsb) {
      fsb.style.display = '';
      fsb.classList.remove('open'); // collapse on page change
    }
    initHomePageListeners();
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
  const fsb = document.getElementById('filterSortBar');
  fsb.style.display = '';
  fsb.classList.remove('open');
  document.querySelectorAll('.nav-link').forEach(l => l.classList.toggle('active', l.id === 'navProfiles'));
}

// Nav link click handlers
document.getElementById('navHome').addEventListener('click', function(e) {
  e.preventDefault();
  navigateToLanding('/');
});

document.getElementById('navProfiles').addEventListener('click', function(e) {
  e.preventDefault();
  if (!isLoggedIn()) {
    document.getElementById('authOverlay').style.display = 'flex';
    document.body.style.overflow = 'hidden';
    return;
  }
  if (isSubscribed !== true && userRole !== 'admin') { showPaywall(); return; }
  setLayout('bento', false);
  restoreActivePresetOrClear();
  renderFilters(); renderGrid();
  history.pushState(null, '', '/profiles/bento');
  showMainSection();
  updateMeta('Browse All Profiles \u2013 Rosters Included | Brothel Search', 'Browse all girl profiles across Australian brothels. Filter by venue, country, availability, pricing and preferences. Photos, rosters and reviews.', 'https://brothelsearch.com/og-preview.png', 'https://brothelsearch.com/profiles', null);
});

document.getElementById('navWorkingNow').addEventListener('click', function(e) {
  e.preventDefault();
  navigateToLanding('/working-now');
});

document.getElementById('navCompare').addEventListener('click', function(e) {
  e.preventDefault();
  navigateToLanding('/compare');
});

document.getElementById('navAnalytics').addEventListener('click', function(e) {
  e.preventDefault();
  navigateToLanding('/analytics');
});

document.getElementById('navRoadmap').addEventListener('click', function(e) {
  e.preventDefault();
  navigateToLanding('/roadmap');
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

document.getElementById('navData').addEventListener('click', function(e) {
  e.preventDefault();
  navigateToLanding('/data');
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

// ══════════════════════════════════════════════
// ── Animations & Visual Effects ──
// ══════════════════════════════════════════════

// ── 1. Scroll Reveal Observer ──
const revealObserver = new IntersectionObserver((entries) => {
  entries.forEach(e => { if (e.isIntersecting) { e.target.classList.add('visible'); revealObserver.unobserve(e.target); } });
}, { threshold: 0.1, rootMargin: '0px 0px -40px 0px' });

function initScrollReveals() {
  document.querySelectorAll('.venue-divider, .home-stats, .home-search-wrap, .venue-carousel, #homeRecentReviews, .review-section, .analytics-section, .compare-table-wrap, .roadmap-table-wrap, .live-ticker').forEach(el => {
    if (!el.classList.contains('reveal') && !el.classList.contains('reveal-scale')) { el.classList.add('reveal'); revealObserver.observe(el); }
  });
  // Stagger children for card grids
  document.querySelectorAll('.girls-grid').forEach(el => {
    if (!el.classList.contains('reveal-stagger')) { el.classList.add('reveal-stagger'); revealObserver.observe(el); }
  });
  // Landing grid cards alternate left/right
  document.querySelectorAll('.landing-grid').forEach(grid => {
    Array.from(grid.children).forEach((child, i) => {
      if (child.classList.contains('reveal-left') || child.classList.contains('reveal-right')) return;
      child.classList.add(i % 2 === 0 ? 'reveal-left' : 'reveal-right');
      child.style.transitionDelay = (i * 0.08) + 's';
      revealObserver.observe(child);
    });
  });
  // Scale reveal for analytics bars
  document.querySelectorAll('.analytics-bars').forEach(el => {
    if (!el.classList.contains('reveal-scale')) { el.classList.add('reveal-scale'); revealObserver.observe(el); }
  });
}

// Re-init reveals after page renders
const _origRenderGrid = renderGrid;
renderGrid = function() { _origRenderGrid(); setTimeout(initScrollReveals, 50); };

// ── 2. Gold Shimmer on Section Titles ──
function initShimmerText() {
  document.querySelectorAll('.section-title, .landing-title').forEach(el => {
    if (!el.classList.contains('shimmer-text')) el.classList.add('shimmer-text');
  });
}

// ── 3. Gold Particle Parallax Background ──
(function initGoldParticles() {
  const canvas = document.getElementById('goldParticleCanvas');
  if (!canvas || window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;
  const ctx = canvas.getContext('2d');
  const MAX_PARTICLES = 35;
  let particles = [];
  let w, h, mouseX = 0, mouseY = 0, animId;
  let running = false;

  function resize() { w = canvas.width = window.innerWidth; h = canvas.height = window.innerHeight; }
  resize();
  window.addEventListener('resize', resize);

  document.addEventListener('mousemove', e => { mouseX = e.clientX; mouseY = e.clientY; });

  for (let i = 0; i < MAX_PARTICLES; i++) {
    particles.push({
      x: Math.random() * w, y: Math.random() * h,
      vx: (Math.random() - 0.5) * 0.3, vy: -Math.random() * 0.4 - 0.1,
      r: Math.random() * 2 + 0.5,
      a: Math.random() * 0.3 + 0.05,
      parallax: Math.random() * 0.5 + 0.5,
    });
  }

  function draw() {
    if (!running) return;
    ctx.clearRect(0, 0, w, h);
    for (const p of particles) {
      const dx = (mouseX - w / 2) * 0.01 * p.parallax;
      const dy = (mouseY - h / 2) * 0.01 * p.parallax;
      p.x += p.vx; p.y += p.vy;
      if (p.y < -10) { p.y = h + 10; p.x = Math.random() * w; }
      if (p.x < -10) p.x = w + 10; if (p.x > w + 10) p.x = -10;
      ctx.beginPath();
      ctx.arc(p.x + dx, p.y + dy, p.r, 0, Math.PI * 2);
      ctx.fillStyle = `rgba(201, 149, 44, ${p.a})`;
      ctx.fill();
    }
    animId = requestAnimationFrame(draw);
  }

  window._particlesStart = function() { if (!running) { running = true; draw(); canvas.style.display = ''; } };
  window._particlesStop = function() { running = false; cancelAnimationFrame(animId); ctx.clearRect(0, 0, w, h); canvas.style.display = 'none'; };

  // Start on home page only
  const p = window.location.pathname;
  if (p === '/' || p === '/index.html') window._particlesStart();

  document.addEventListener('visibilitychange', () => {
    if (document.hidden) { running = false; cancelAnimationFrame(animId); }
    else if (canvas.style.display !== 'none') { running = true; draw(); }
  });
})();

// ── 4. Hero Section Entry Classes ──
function initHeroAnimations() {
  const tag = document.querySelector('.section-tag');
  const title = document.querySelector('.section-title');
  const line = document.querySelector('.section-line');
  const tagline = document.querySelector('.hero-tagline');
  const stats = document.querySelector('.home-stats');
  const search = document.querySelector('.home-search-wrap');

  if (tag && !tag.classList.contains('hero-enter')) { tag.classList.add('hero-enter', 'hero-enter-d1'); }
  if (line && !line.classList.contains('hero-line-anim')) { line.classList.add('hero-line-anim'); }
  if (title && !title.classList.contains('hero-enter')) { title.classList.add('hero-enter', 'hero-enter-d2'); }
  if (tagline && !tagline.classList.contains('hero-enter')) { tagline.classList.add('hero-enter', 'hero-enter-d3'); }
  if (search && !search.classList.contains('hero-enter')) { search.classList.add('hero-enter', 'hero-enter-d4'); }
  if (stats && !stats.classList.contains('hero-enter')) { stats.classList.add('hero-enter', 'hero-enter-d5'); }
}

// ── 5. Page Transition ──
const pageTransEl = document.getElementById('pageTransition');
const _origNavigateToLanding = navigateToLanding;
navigateToLanding = function(path) {
  if (!pageTransEl || window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    return _origNavigateToLanding(path);
  }
  pageTransEl.classList.add('active');
  setTimeout(() => {
    _origNavigateToLanding(path);
    setTimeout(() => pageTransEl.classList.remove('active'), 50);
  }, 250);
};

// ── 6. Venue Carousel Drag ──
function initCarouselDrag() {
  document.querySelectorAll('.venue-carousel').forEach(el => {
    if (el._dragInit) return;
    el._dragInit = true;
    let isDown = false, startX, scrollLeft;
    el.addEventListener('mousedown', e => { isDown = true; el.classList.add('grabbing'); startX = e.pageX - el.offsetLeft; scrollLeft = el.scrollLeft; });
    el.addEventListener('mouseleave', () => { isDown = false; el.classList.remove('grabbing'); });
    el.addEventListener('mouseup', () => { isDown = false; el.classList.remove('grabbing'); });
    el.addEventListener('mousemove', e => { if (!isDown) return; e.preventDefault(); const x = e.pageX - el.offsetLeft; el.scrollLeft = scrollLeft - (x - startX) * 1.5; });
  });
}

// ── 7a. Layout Toggle (Grid / Bento / Compact / List) ──
let currentLayout = 'bento';
const LAYOUTS = ['grid', 'bento', 'compact', 'list'];
LAYOUTS.forEach(m => {
  const btn = document.getElementById('layout' + m.charAt(0).toUpperCase() + m.slice(1));
  if (btn) btn.onclick = () => setLayout(m, true);
});
function setLayout(mode, pushState) {
  const prevLayout = currentLayout;
  currentLayout = mode;
  const grid = document.getElementById('girlsGrid');
  if (grid) {
    LAYOUTS.forEach(m => grid.classList.toggle(m, mode === m));
  }
  LAYOUTS.forEach(m => {
    const btn = document.getElementById('layout' + m.charAt(0).toUpperCase() + m.slice(1));
    if (btn) btn.classList.toggle('active', mode === m);
  });
  if (pushState) history.pushState(null, '', '/profiles/' + mode);
  // Re-render grid with new page size if layout changed and we're on profiles page
  if (prevLayout !== mode && grid && document.querySelector('section.section').style.display !== 'none') {
    renderGrid();
  }
}

// ── 7. Card Glow Follow ──
function initCardTilt() {
  document.querySelectorAll('.girl-card').forEach(card => {
    if (card._tiltInit) return;
    card._tiltInit = true;
    card.addEventListener('mousemove', e => {
      const rect = card.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const y = e.clientY - rect.top;
      card.style.setProperty('--mouse-x', (x / rect.width * 100) + '%');
      card.style.setProperty('--mouse-y', (y / rect.height * 100) + '%');
    });
  });
}

// ── 8. Live Activity Ticker (removed from Working Now, now on home page only) ──
function initLiveTicker() {}

// ── 9. Init all animations after page render ──
function initAllAnimations() {
  setTimeout(() => {
    initHeroAnimations();
    initShimmerText();
    initScrollReveals();
    initCarouselDrag();
    initCardTilt();
    initLiveTicker();
  }, 100);
}

// Hook into page renders
const _origInitHomeListeners = initHomePageListeners;
initHomePageListeners = function() {
  _origInitHomeListeners();
  initAllAnimations();
};

// Hook into landing route renders
const _origHandleLanding = handleLandingRoute;
handleLandingRoute = function(path) {
  const result = _origHandleLanding(path);
  initAllAnimations();
  return result;
};

// ── Mobile Menu ──
const logoBtn = document.getElementById('logoBtn');
const mobileMenu = document.getElementById('mobileMenu');
if (logoBtn && mobileMenu) {
  logoBtn.addEventListener('click', () => {
    if (window.innerWidth <= 768) {
      mobileMenu.classList.toggle('open');
    } else {
      navigateToLanding('/');
    }
  });
}
function closeMobileMenu() {
  const mm = document.getElementById('mobileMenu');
  if (mm) mm.classList.remove('open');
}

// ── Inactivity Auto-Logout ──
(function initInactivityTimer() {
  const WARN_AT = 10 * 60 * 1000;   // 10 min — show warning
  const LOGOUT_AT = 15 * 60 * 1000; // 15 min — auto logout
  const STORAGE_KEY = 'lastActivityTs';
  const PERSIST_THROTTLE = 5000;    // write to localStorage at most every 5s

  // Seed from stored timestamp so inactivity persists across PC restarts / tab closes.
  // If the stored timestamp is already past LOGOUT_AT, sign out any lingering session.
  const stored = parseInt(localStorage.getItem(STORAGE_KEY) || '0', 10);
  let lastActivity = (stored && Date.now() - stored < LOGOUT_AT) ? stored : Date.now();
  let lastPersist = 0;
  let warningShown = false;
  let countdownInterval = null;

  if (stored && Date.now() - stored >= LOGOUT_AT) {
    try { localStorage.removeItem(STORAGE_KEY); } catch (_) {}
    // Force sign-out. onAuthStateChange SIGNED_OUT handler will update the UI.
    // We also call our custom signOut() if available for a complete reset.
    if (typeof signOut === 'function') {
      signOut().catch(() => {});
    } else {
      sbClient.auth.signOut().catch(() => {});
    }
  }

  function persistActivity() {
    try { localStorage.setItem(STORAGE_KEY, String(lastActivity)); } catch (_) {}
    lastPersist = lastActivity;
  }

  function resetActivity() {
    if (warningShown) return; // force user to click Stay/Logout
    lastActivity = Date.now();
    if (lastActivity - lastPersist > PERSIST_THROTTLE) persistActivity();
  }

  function showWarning() {
    if (!isLoggedIn()) return;
    warningShown = true;
    const overlay = document.getElementById('inactivityOverlay');
    overlay.style.display = 'flex';
    document.body.style.overflow = 'hidden';
    updateCountdown();
    countdownInterval = setInterval(updateCountdown, 1000);
  }

  function hideWarning() {
    warningShown = false;
    const overlay = document.getElementById('inactivityOverlay');
    overlay.style.display = 'none';
    document.body.style.overflow = '';
    if (countdownInterval) { clearInterval(countdownInterval); countdownInterval = null; }
  }

  function updateCountdown() {
    const remaining = Math.max(0, LOGOUT_AT - (Date.now() - lastActivity));
    const mins = Math.floor(remaining / 60000);
    const secs = Math.floor((remaining % 60000) / 1000);
    const el = document.getElementById('inactivityCountdown');
    if (el) el.textContent = mins + ':' + String(secs).padStart(2, '0');
    if (remaining <= 0) doLogout();
  }

  function doLogout() {
    hideWarning();
    try { localStorage.removeItem(STORAGE_KEY); } catch (_) {}
    signOut();
  }

  // Activity listeners
  ['mousemove', 'mousedown', 'keydown', 'touchstart', 'scroll'].forEach(evt => {
    document.addEventListener(evt, resetActivity, { passive: true });
  });

  // Check every 10 seconds
  setInterval(() => {
    if (!isLoggedIn()) return;
    // Pick up activity from other tabs
    const s = parseInt(localStorage.getItem(STORAGE_KEY) || '0', 10);
    if (s > lastActivity) lastActivity = s;
    const elapsed = Date.now() - lastActivity;
    if (elapsed >= LOGOUT_AT) doLogout();
    else if (elapsed >= WARN_AT && !warningShown) showWarning();
  }, 10000);

  // Button handlers
  document.getElementById('inactivityStayBtn').addEventListener('click', () => { hideWarning(); resetActivity(); });
  document.getElementById('inactivityLogoutBtn').addEventListener('click', () => { doLogout(); });
})();

// Init on first load
document.addEventListener('DOMContentLoaded', () => setTimeout(initAllAnimations, 200));

