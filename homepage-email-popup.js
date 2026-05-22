(() => {
  const isHome = window.location.pathname === '/' || window.location.pathname === '/index.html';
  if (!isHome) return;

  const SHEET_ENDPOINT = ['https://script.google.com/macros/s','AKfycbw_7DAtiZJQWFwhphywmmnKuAssYQlumY32lPlJSwaSEQ9lWYx5PXOEP8BDW8_QeSsQ','exec'].join('/');
  const SIGNED_UP_KEY = 'bldHomepageEmailPopupSignedUp';
  const SESSION_COUNT_KEY = 'bldHomepageEmailPopupSessionCount';
  const SESSION_SEEN_KEY = 'bldHomepageEmailPopupSeenThisSession';
  const POPUP_DELAY_MS = 6000;
  const SHOW_EVERY_N_SESSIONS = 3;

  function getSessionCount() {
    const current = Number(localStorage.getItem(SESSION_COUNT_KEY) || '0');
    return Number.isFinite(current) ? current : 0;
  }

  function markSession() {
    if (sessionStorage.getItem(SESSION_SEEN_KEY) === 'true') return getSessionCount();
    const nextCount = getSessionCount() + 1;
    localStorage.setItem(SESSION_COUNT_KEY, String(nextCount));
    sessionStorage.setItem(SESSION_SEEN_KEY, 'true');
    return nextCount;
  }

  const sessionCount = markSession();

  function shouldSkipPopup() {
    if (localStorage.getItem(SIGNED_UP_KEY) === 'true') return true;
    return sessionCount % SHOW_EVERY_N_SESSIONS !== 0;
  }

  function injectStyles() {
    if (document.getElementById('bld-home-email-popup-styles')) return;
    const style = document.createElement('style');
    style.id = 'bld-home-email-popup-styles';
    style.textContent = `
      .bld-home-email-popup{position:fixed;inset:0;z-index:10000;display:none;align-items:center;justify-content:center;padding:18px}
      .bld-home-email-popup.is-visible{display:flex}
      .bld-home-email-popup-backdrop{position:absolute;inset:0;background:rgba(17,24,39,.58);backdrop-filter:blur(3px)}
      .bld-home-email-popup-card{position:relative;width:min(94vw,470px);background:#fff;border:2px solid #fed7aa;border-radius:24px;padding:26px;box-shadow:0 30px 80px rgba(17,24,39,.28);text-align:left;font-family:Arial,sans-serif}
      .bld-home-email-popup-close{position:absolute;top:12px;right:14px;width:34px;height:34px;border:0;border-radius:999px;background:#fff7ed;color:#9a3412;font-size:24px;line-height:1;cursor:pointer}
      .bld-home-email-popup-kicker{display:inline-flex;border-radius:999px;background:#ffedd5;color:#9a3412;padding:6px 10px;font-size:12px;font-weight:950;text-transform:uppercase;letter-spacing:.08em;margin-bottom:12px}
      .bld-home-email-popup-card h2{font-family:Georgia,serif;font-size:30px;line-height:1.1;color:#111827;margin:0 0 10px}
      .bld-home-email-popup-card p{color:#4b5563;font-size:15px;line-height:1.6;margin:0 0 16px}
      .bld-home-email-popup-form{display:grid;grid-template-columns:1fr;gap:10px}
      .bld-home-email-popup-form input{min-height:46px;border:1px solid #fdba74;border-radius:12px;padding:0 13px;font-size:15px;font-weight:700;outline:none}
      .bld-home-email-popup-form input:focus{border-color:#f97316;box-shadow:0 0 0 3px rgba(249,115,22,.14)}
      .bld-home-email-popup-form button{min-height:46px;border:0;border-radius:12px;background:linear-gradient(135deg,#12385b,#0f4c81);color:#fff;font-size:15px;font-weight:950;cursor:pointer}
      .bld-home-email-popup-note{margin-top:11px;color:#6b7280;font-size:12px;line-height:1.5}
      .bld-home-email-popup-success{margin-top:12px;border-radius:12px;background:#ecfdf5;color:#166534;padding:10px 12px;font-size:13px;font-weight:800}
      .bld-home-popup-open{overflow:hidden}
      @media(max-width:520px){.bld-home-email-popup-card{padding:22px}.bld-home-email-popup-card h2{font-size:26px}}
    `;
    document.head.appendChild(style);
  }

  function closePopup() {
    const popup = document.getElementById('bld-home-email-popup');
    if (popup) popup.classList.remove('is-visible');
    document.body.classList.remove('bld-home-popup-open');
  }

  function showPopup() {
    if (shouldSkipPopup()) return;
    const popup = document.getElementById('bld-home-email-popup');
    if (!popup) return;
    popup.classList.add('is-visible');
    popup.setAttribute('aria-hidden', 'false');
    document.body.classList.add('bld-home-popup-open');
    const emailInput = document.getElementById('bld-home-email-popup-input');
    if (emailInput) setTimeout(() => emailInput.focus(), 120);
  }

  function setFormState(isSaving) {
    const button = document.querySelector('#bld-home-email-popup-form button[type="submit"]');
    if (!button) return;
    button.disabled = isSaving;
    button.textContent = isSaving ? 'Saving...' : 'Send Me the Deals';
  }

  async function submitSignup(email) {
    const params = new URLSearchParams();
    params.append('email', email);
    params.append('phone', '');
    params.append('source', 'homepage-popup');
    params.append('page', window.location.href);
    params.append('referrer', document.referrer || '');
    params.append('session_count', String(sessionCount));
    await fetch(SHEET_ENDPOINT + '?' + params.toString(), { method: 'GET', mode: 'no-cors' });
  }

  function initPopup() {
    if (shouldSkipPopup()) return;
    injectStyles();
    document.body.insertAdjacentHTML('beforeend', `
      <div class="bld-home-email-popup" id="bld-home-email-popup" aria-hidden="true">
        <div class="bld-home-email-popup-backdrop" data-bld-home-email-close></div>
        <div class="bld-home-email-popup-card" role="dialog" aria-modal="true" aria-labelledby="bld-home-email-popup-title">
          <button class="bld-home-email-popup-close" type="button" aria-label="Close email signup" data-bld-home-email-close>&times;</button>
          <div class="bld-home-email-popup-kicker">Black Lab Deals</div>
          <h2 id="bld-home-email-popup-title">Get the best deals before they sell out</h2>
          <p>Join the Black Lab Deals list for the best Amazon price drops, coupons, tools, electronics, home, kitchen, and everyday deals we find.</p>
          <form class="bld-home-email-popup-form" id="bld-home-email-popup-form">
            <label class="sr-only" for="bld-home-email-popup-input">Email address</label>
            <input id="bld-home-email-popup-input" name="email" type="email" placeholder="Enter your email" autocomplete="email" required>
            <button type="submit">Send Me the Deals</button>
          </form>
          <div class="bld-home-email-popup-note">No spam. Just the best deals we find. You can close this and keep browsing.</div>
          <div class="bld-home-email-popup-success" id="bld-home-email-popup-success" hidden>Thanks! You're on the list.</div>
        </div>
      </div>
    `);

    document.querySelectorAll('[data-bld-home-email-close]').forEach(button => button.addEventListener('click', closePopup));
    document.addEventListener('keydown', event => {
      if (event.key === 'Escape' && document.getElementById('bld-home-email-popup')?.classList.contains('is-visible')) closePopup();
    });

    const form = document.getElementById('bld-home-email-popup-form');
    form?.addEventListener('submit', async event => {
      event.preventDefault();
      const email = document.getElementById('bld-home-email-popup-input')?.value.trim();
      if (!email) return;
      setFormState(true);
      try {
        await submitSignup(email);
        localStorage.setItem(SIGNED_UP_KEY, 'true');
        localStorage.setItem('bldHomepageEmail', email);
        const success = document.getElementById('bld-home-email-popup-success');
        if (success) success.hidden = false;
        if (typeof fbq === 'function') fbq('trackCustom', 'HomepageEmailPopupSubmit');
        setTimeout(closePopup, 1200);
      } catch (error) {
        console.error('Homepage email signup error:', error);
        const success = document.getElementById('bld-home-email-popup-success');
        if (success) {
          success.hidden = false;
          success.textContent = 'Something went wrong. Please try again.';
        }
      } finally {
        setFormState(false);
      }
    });

    setTimeout(showPopup, POPUP_DELAY_MS);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', initPopup);
  else initPopup();
})();
