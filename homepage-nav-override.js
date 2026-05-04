/* Homepage shared-header bridge.
   The homepage uses the shared /site-header.js header, just like the other pages. */
(function () {
  function loadSharedHeaderScript() {
    if (document.querySelector('script[src="/site-header.js"]')) return;
    const script = document.createElement('script');
    script.src = '/site-header.js';
    script.defer = true;
    document.body.appendChild(script);
  }

  function removeOldHomepageHeaderParts() {
    document.querySelectorAll('nav, .nav-inner, .desktop-nav, .mobile-drawer, .mobile-drawer-overlay').forEach(function (el) {
      if (!el.closest('#site-header')) el.remove();
    });
  }

  function useSharedHeaderOnHomepage() {
    let mount = document.getElementById('site-header');

    if (!mount) {
      mount = document.createElement('div');
      mount.id = 'site-header';
      document.body.insertBefore(mount, document.body.firstChild);
    }

    removeOldHomepageHeaderParts();
    loadSharedHeaderScript();
  }

  function removeCompletedDealPagesPanel() {
    const panel = document.querySelector('footer .browse-more-links');
    if (panel) panel.remove();
  }

  function protectSharedDropdownFromOldHomepageScript() {
    const header = document.getElementById('site-header');
    const wrap = document.querySelector('#site-header .bld-category-wrap');
    const trigger = document.querySelector('#site-header .bld-category-trigger');
    const menu = document.querySelector('#site-header .bld-mega-menu');
    if (!header || !wrap || !trigger || !menu || trigger.dataset.homepageSharedNavReady === 'true') return;

    trigger.dataset.homepageSharedNavReady = 'true';
    wrap.classList.add('nav-dropdown');
    trigger.classList.add('nav-dropdown-trigger');

    function openMenu() {
      menu.classList.add('show');
      trigger.setAttribute('aria-expanded', 'true');
    }

    function closeMenu() {
      menu.classList.remove('show');
      trigger.setAttribute('aria-expanded', 'false');
    }

    trigger.addEventListener('click', function (event) {
      event.preventDefault();
      event.stopPropagation();
      event.stopImmediatePropagation();
      if (menu.classList.contains('show')) closeMenu();
      else openMenu();
    }, true);

    menu.addEventListener('click', function (event) {
      event.stopPropagation();
      event.stopImmediatePropagation();
    }, true);

    document.addEventListener('click', function (event) {
      if (!header.contains(event.target)) closeMenu();
    }, true);
  }

  function runHomepageBridge() {
    useSharedHeaderOnHomepage();
    removeCompletedDealPagesPanel();
    protectSharedDropdownFromOldHomepageScript();
    setTimeout(protectSharedDropdownFromOldHomepageScript, 100);
    setTimeout(protectSharedDropdownFromOldHomepageScript, 500);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', runHomepageBridge);
  } else {
    runHomepageBridge();
  }
})();
