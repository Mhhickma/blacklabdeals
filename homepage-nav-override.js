/* Homepage shared-header bridge.
   The homepage previously had its own hard-coded navigation. This script now
   replaces that old homepage header with the shared /site-header.js header so
   the main page uses the same navigation as the rest of the site. */
(function () {
  function loadSharedHeaderScript() {
    if (document.querySelector('script[src="/site-header.js"]')) return;
    const script = document.createElement('script');
    script.src = '/site-header.js';
    script.defer = true;
    document.body.appendChild(script);
  }

  function useSharedHeaderOnHomepage() {
    let mount = document.getElementById('site-header');

    if (!mount) {
      mount = document.createElement('div');
      mount.id = 'site-header';

      const oldHeader = document.querySelector('header.bld-header-shell');
      if (oldHeader) {
        oldHeader.replaceWith(mount);
      } else {
        const backToTop = document.getElementById('back-to-top');
        if (backToTop && backToTop.parentNode) {
          backToTop.insertAdjacentElement('afterend', mount);
        } else {
          document.body.insertBefore(mount, document.body.firstChild);
        }
      }
    }

    loadSharedHeaderScript();
  }

  function removeCompletedDealPagesPanel() {
    const panel = document.querySelector('footer .browse-more-links');
    if (panel) panel.remove();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', useSharedHeaderOnHomepage);
    document.addEventListener('DOMContentLoaded', removeCompletedDealPagesPanel);
  } else {
    useSharedHeaderOnHomepage();
    removeCompletedDealPagesPanel();
  }
})();
