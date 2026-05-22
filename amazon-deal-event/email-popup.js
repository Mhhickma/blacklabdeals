(() => {
  const PAGE_SLUG = (window.location.pathname || '').split('/').filter(Boolean)[0];
  if (PAGE_SLUG !== 'amazon-deal-event') return;

  const SHEET_ENDPOINT = 'https://script.google.com/macros/s/AKfycbw_7DAtiZJQWFwhphywmmnKuAssYQlumY32lPlJSwaSEQ9lWYx5PXOEP8BDW8_QeSsQ/exec';
  const SIGNED_UP_KEY = 'bldSalesEventEmailPopupSignedUp';
  const IS_PRELAUNCH_GATE = document.body.dataset.bldPrelaunchGate === 'true';
  const POPUP_DELAY_MS = IS_PRELAUNCH_GATE ? 100 : 10000;

  function shouldSkipPopup() {
    return !IS_PRELAUNCH_GATE && localStorage.getItem(SIGNED_UP_KEY) === 'true';
  }

  function closePopup() {
    if (IS_PRELAUNCH_GATE) return;
    const popup = document.getElementById('bld-email-popup');
    if (popup) popup.classList.remove('is-visible');
    document.body.classList.remove('bld-popup-open');
  }

  function showPopup() {
    if (shouldSkipPopup()) return;
    const popup = document.getElementById('bld-email-popup');
    if (!popup) return;
    popup.classList.add('is-visible');
    popup.setAttribute('aria-hidden', 'false');
    document.body.classList.add('bld-popup-open');
    const emailInput = document.getElementById('bld-email-popup-input');
    if (emailInput) setTimeout(() => emailInput.focus(), 120);
  }

  function setFormState(isSaving) {
    const button = document.querySelector('#bld-email-popup-form button[type="submit"]');
    if (!button) return;
    button.disabled = isSaving;
    button.textContent = isSaving ? 'Saving...' : 'Send Me the Deals';
  }

  async function submitSignup(email) {
    const params = new URLSearchParams();
    params.append('email', email);
    params.append('phone', '');
    params.append('source', IS_PRELAUNCH_GATE ? 'amazon-deal-event-prelaunch-gate' : 'amazon-deal-event-popup');
    params.append('page', window.location.href);
    params.append('referrer', document.referrer || '');
    await fetch(`${SHEET_ENDPOINT}?${params.toString()}`, { method: 'GET', mode: 'no-cors' });
  }

  function initPopup() {
    if (shouldSkipPopup()) return;

    document.body.insertAdjacentHTML('beforeend', `
      <div class="bld-email-popup${IS_PRELAUNCH_GATE ? ' bld-email-gate' : ''}" id="bld-email-popup" aria-hidden="true">
        <div class="bld-email-popup-backdrop"></div>
        <div class="bld-email-popup-card" role="dialog" aria-modal="true" aria-labelledby="bld-email-popup-title">
          ${IS_PRELAUNCH_GATE ? '' : '<button class="bld-email-popup-close" type="button" aria-label="Close email signup" data-bld-email-close>&times;</button>'}
          <div class="bld-email-popup-kicker">Black Lab Deals Early Access</div>
          <h2 id="bld-email-popup-title">Get early access to the best Amazon deals</h2>
          <p>The deal page is opening soon. Join the Black Lab Deals list now and we’ll send the best Amazon Sales Event finds when they go live.</p>
          <form class="bld-email-popup-form" id="bld-email-popup-form">
            <label class="sr-only" for="bld-email-popup-input">Email address</label>
            <input id="bld-email-popup-input" name="email" type="email" placeholder="Enter your email" autocomplete="email" required>
            <button type="submit">Send Me the Deals</button>
          </form>
          <div class="bld-email-popup-note">No spam. Just the best tool, electronics, home, kitchen, and everyday deals we find.</div>
          <div class="bld-email-popup-success" id="bld-email-popup-success" hidden>Thanks! You're on the early access list.</div>
        </div>
      </div>
    `);

    document.querySelectorAll('[data-bld-email-close]').forEach(button => button.addEventListener('click', closePopup));
    document.addEventListener('keydown', event => {
      if (event.key === 'Escape' && !IS_PRELAUNCH_GATE && document.getElementById('bld-email-popup')?.classList.contains('is-visible')) closePopup();
    });

    const form = document.getElementById('bld-email-popup-form');
    form?.addEventListener('submit', async event => {
      event.preventDefault();
      const email = document.getElementById('bld-email-popup-input')?.value.trim();
      if (!email) return;
      setFormState(true);
      try {
        await submitSignup(email);
        localStorage.setItem(SIGNED_UP_KEY, 'true');
        localStorage.setItem('bldSalesEventEmail', email);
        const success = document.getElementById('bld-email-popup-success');
        if (success) success.hidden = false;
        if (typeof fbq === 'function') fbq('trackCustom', 'SalesEventEmailPopupSubmit');
        if (!IS_PRELAUNCH_GATE) setTimeout(closePopup, 1200);
      } catch (error) {
        console.error('Sales Event email signup error:', error);
        const success = document.getElementById('bld-email-popup-success');
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
