(function () {
  const links = [
    { title: 'Home', href: '/' },
    { title: 'About', href: '/about/' },
    { title: 'Cleanup Status', href: '/categories/' }
  ];

  function navLinks(className) {
    return links.map(link => `<a class="${className}" href="${link.href}">${link.title}</a>`).join('');
  }

  function buildHeader() {
    return `
      <header class="bld-header-shell">
        <div class="bld-header-main">
          <a href="/" class="bld-brand" aria-label="Black Lab Deals home">
            <img class="bld-brand-logo" src="/logo-128.jpg" alt="Black Lab Deals logo" width="84" height="84" decoding="async">
            <div>
              <div class="bld-brand-title">Black Lab <span>Deals</span></div>
              <div class="bld-brand-tagline">Site shell active. Public item feeds removed.</div>
            </div>
          </a>
          <nav class="bld-desktop-nav" aria-label="Main navigation">${navLinks('bld-nav-link')}</nav>
          <button class="bld-mobile-menu-btn" type="button" aria-label="Open menu" aria-expanded="false">Menu</button>
        </div>
        <nav class="bld-mobile-nav" aria-label="Mobile navigation" hidden>${navLinks('bld-mobile-link')}</nav>
      </header>
    `;
  }

  function mountHeader() {
    const mount = document.getElementById('site-header');
    if (!mount) return;
    mount.innerHTML = buildHeader();
    const button = mount.querySelector('.bld-mobile-menu-btn');
    const nav = mount.querySelector('.bld-mobile-nav');
    if (!button || !nav) return;
    button.addEventListener('click', () => {
      const isOpen = button.getAttribute('aria-expanded') === 'true';
      button.setAttribute('aria-expanded', String(!isOpen));
      nav.hidden = isOpen;
    });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', mountHeader);
  else mountHeader();
})();
