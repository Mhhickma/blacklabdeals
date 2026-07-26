(function () {
  function track(eventName, params = {}) {
    try {
      if (typeof window.gtag !== 'function') return;
      window.gtag('event', eventName, {
        page_location: window.location.href,
        page_path: window.location.pathname,
        page_title: document.title,
        ...params
      });
    } catch (error) {}
  }

  document.addEventListener('click', event => {
    const link = event.target.closest('a[href]');
    if (!link) return;
    track('site_link_click', {
      link_text: link.textContent.trim().slice(0, 100),
      link_url: link.href
    });
  }, true);

  const marks = [25, 50, 75, 90];
  const tracked = new Set();
  function checkScroll() {
    const doc = document.documentElement;
    const height = Math.max(doc.scrollHeight, document.body.scrollHeight) - window.innerHeight;
    if (height <= 0) return;
    const percent = Math.round((window.scrollY / height) * 100);
    marks.forEach(mark => {
      if (percent >= mark && !tracked.has(mark)) {
        tracked.add(mark);
        track('scroll_depth', { percent_scrolled: mark });
      }
    });
  }

  window.addEventListener('scroll', checkScroll, { passive: true });
  window.addEventListener('load', checkScroll);
})();
