/* Black Lab Deals uniform header navigation.
   Shared header used by pages that include /site-header.js. */

(function () {
  const NAV_LINKS = [
    { title: 'Hot Deals', href: '/#hot-section', icon: '&#9889;', group: 'Featured' },
    { title: 'Categories', href: '/categories/', icon: '&#8594;', group: 'Featured' },
    { title: 'Best Seller Deals', href: '/best-seller-deals.html', icon: '&#9733;', group: 'Featured' },
    { title: 'Top 100 Deals', href: '/top-100-amazon-deals-today/', icon: '#', group: 'Featured' },
    { title: 'Deals Under $50', href: '/best-amazon-deals-under-50/', icon: '$', group: 'Featured' },
    { title: 'Tool Deals', href: '/best-amazon-tool-deals/', icon: '&#128295;', group: 'Popular Categories' },
    { title: 'Home & Kitchen', href: '/best-amazon-home-kitchen-deals/', icon: '&#8962;', group: 'Popular Categories' },
    { title: 'Electronics', href: '/best-amazon-electronics-deals/', icon: '&#9632;', group: 'Popular Categories' },
    { title: 'Automotive', href: '/best-amazon-automotive-deals/', icon: '&#9679;', group: 'Popular Categories' },
    { title: 'Patio & Garden', href: '/best-amazon-patio-lawn-garden-deals/', icon: '&#9827;', group: 'Popular Categories' },
    { title: 'Sports & Outdoors', href: '/best-amazon-sports-outdoors-deals/', icon: '&#9670;', group: 'More' },
    { title: 'Pet Supplies', href: '/best-amazon-pet-supplies-deals/', icon: '&#9675;', group: 'More' },
    { title: 'Toys & Games', href: '/best-amazon-toys-games-deals/', icon: '&#9734;', group: 'More' },
    { title: 'Office Products', href: '/best-amazon-office-products-deals/', icon: '&#9633;', group: 'More' },
    { title: 'Health & Household', href: '/best-amazon-health-household-deals/', icon: '+', group: 'More' }
  ];

  function getHomeAwareHref(anchor) {
    return window.location.pathname === '/' || window.location.pathname === '/index.html' ? anchor : '/' + anchor;
  }

  function groupedLinks(group) {
    return NAV_LINKS
      .filter(link => link.group === group)
      .map(link => `<a class="bld-mega-link" href="${link.href}"><span class="bld-mega-icon">${link.icon}</span>${link.title}</a>`)
      .join('');
  }

  function drawerLinks(group) {
    return NAV_LINKS
      .filter(link => link.group === group)
      .map(link => `<a class="bld-mobile-drawer-link" href="${link.href}"><span>${link.icon}</span>${link.title}</a>`)
      .join('');
  }

  const MEGA_MENU_HTML = `
    <div class="bld-mega-header">
      <div>
        <div class="bld-mega-title">Shop by Category</div>
        <div class="bld-mega-subtitle">Find the Amazon deal page that matches what you want.</div>
      </div>
      <div class="bld-mega-pill">Updated Daily</div>
    </div>
    <div class="bld-mega-grid">
      <div class="bld-mega-column"><h3>Featured</h3>${groupedLinks('Featured')}</div>
      <div class="bld-mega-column"><h3>Popular Categories</h3>${groupedLinks('Popular Categories')}</div>
      <div class="bld-mega-column"><h3>More</h3>${groupedLinks('More')}</div>
    </div>
    <a class="bld-mega-footer" href="/categories/"><span>View All Categories</span><span>&rarr;</span></a>
  `;

  const MOBILE_DRAWER_HTML = `
    <div class="bld-mobile-drawer-overlay" data-bld-mobile-close hidden></div>
    <aside class="bld-mobile-drawer" id="bld-mobile-drawer" aria-label="Mobile navigation" aria-hidden="true">
      <div class="bld-mobile-drawer-header">
        <a href="/" class="bld-mobile-drawer-brand" aria-label="Black Lab Deals home">
          <img src="/logo-128.jpg" alt="Black Lab Deals logo" width="58" height="58" decoding="async">
          <div>
            <div class="bld-mobile-drawer-title">Black Lab <span>Deals</span></div>
            <div class="bld-mobile-drawer-subtitle">Fresh Amazon deals</div>
          </div>
        </a>
        <button class="bld-mobile-drawer-close" type="button" aria-label="Close menu" data-bld-mobile-close>&times;</button>
      </div>
      <div class="bld-mobile-drawer-content">
        <form class="bld-mobile-drawer-search" action="/search.html" method="get" role="search">
          <input type="search" name="q" placeholder="Search Here" aria-label="Search all deals">
          <button type="submit">Search</button>
        </form>
        <a class="bld-mobile-drawer-alert" href="/#alerts-box"><span>&bull;</span> Get Deal Alerts</a>
        <div class="bld-mobile-drawer-section"><h3>Featured Deal Pages</h3>${drawerLinks('Featured')}</div>
        <div class="bld-mobile-drawer-section"><h3>Popular Categories</h3>${drawerLinks('Popular Categories')}</div>
        <div class="bld-mobile-drawer-section"><h3>More Categories</h3>${drawerLinks('More')}<a class="bld-mobile-drawer-link bld-mobile-drawer-all" href="/categories/"><span>&rarr;</span>View All Categories</a></div>
      </div>
    </aside>
  `;

  const MOBILE_DRAWER_CSS = `
    .bld-header-search-row{background:var(--surface,#fff);border-top:1px solid var(--border,#e8e6e1);border-bottom:1px solid var(--border,#e8e6e1);padding:12px 24px 14px;}
    .bld-header-search{max-width:760px;margin:0 auto;display:flex;align-items:center;gap:10px;}
    .bld-header-search input{width:100%;height:44px;border:1px solid var(--border,#e8e6e1);border-radius:999px;background:var(--bg,#f9f8f5);color:var(--text-primary,#1a1a18);font-family:Arial, sans-serif;font-size:15px;font-weight:700;padding:0 18px;outline:none;box-shadow:0 1px 3px rgba(0,0,0,.04);}
    .bld-header-search input:focus{border-color:var(--accent,#1a3a5c);box-shadow:0 0 0 3px rgba(26,58,92,.12);}
    .bld-header-search input::placeholder{color:var(--text-muted,#9e9e97);}
    .bld-header-search button{height:44px;border:0;border-radius:999px;background:var(--accent,#1a3a5c);color:#fff;font-family:Arial, sans-serif;font-size:14px;font-weight:900;padding:0 18px;cursor:pointer;white-space:nowrap;}
    .bld-header-search button:hover{background:var(--accent-mid,#2a5a8c);}
    .bld-mobile-drawer-search{display:flex;gap:8px;margin-bottom:2px;}
    .bld-mobile-drawer-search input{min-width:0;flex:1;height:42px;border:1px solid var(--border,#e8e6e1);border-radius:999px;background:var(--bg,#f9f8f5);padding:0 14px;font-size:15px;font-weight:800;color:var(--text-primary,#1a1a18);outline:none;}
    .bld-mobile-drawer-search button{height:42px;border:0;border-radius:999px;background:var(--accent,#1a3a5c);color:#fff;font-size:13px;font-weight:900;padding:0 14px;}
    .bld-mobile-menu-btn{width:44px;height:44px;border:1px solid var(--border,#e8e6e1);border-radius:14px;background:var(--surface,#fff);color:var(--text-primary,#1a1a18);display:inline-flex;align-items:center;justify-content:center;cursor:pointer;box-shadow:var(--shadow,0 1px 3px rgba(0,0,0,.06));}
    .bld-mobile-menu-btn svg{width:22px;height:22px;}
    .bld-mobile-drawer-overlay{position:fixed;inset:0;background:rgba(0,0,0,.42);z-index:998;opacity:0;pointer-events:none;transition:opacity .22s ease;}
    .bld-mobile-drawer-overlay.show{opacity:1;pointer-events:auto;}
    .bld-mobile-drawer{position:fixed;top:0;right:0;width:min(88vw,390px);height:100vh;background:var(--surface,#fff);z-index:999;box-shadow:-18px 0 42px rgba(0,0,0,.2);transform:translateX(100%);transition:transform .24s ease;display:flex;flex-direction:column;}
    .bld-mobile-drawer.show{transform:translateX(0);}
    .bld-mobile-drawer-header{display:flex;align-items:center;justify-content:space-between;gap:14px;padding:16px;border-bottom:1px solid var(--border,#e8e6e1);}
    .bld-mobile-drawer-brand{display:flex;align-items:center;gap:12px;color:var(--text-primary,#1a1a18);text-decoration:none;min-width:0;}
    .bld-mobile-drawer-brand img{width:58px;height:58px;border-radius:50%;object-fit:cover;border:2px solid var(--gold,#c9a84c);background:#fff;}
    .bld-mobile-drawer-title{font-family:Georgia, serif;font-size:23px;line-height:1;color:var(--text-primary,#1a1a18);white-space:nowrap;}
    .bld-mobile-drawer-title span{color:var(--gold,#c9a84c);}
    .bld-mobile-drawer-subtitle{font-size:12px;color:var(--text-muted,#9e9e97);margin-top:4px;font-weight:700;}
    .bld-mobile-drawer-close{width:42px;height:42px;border:1px solid var(--border,#e8e6e1);border-radius:13px;background:var(--bg,#f9f8f5);font-size:30px;line-height:1;color:var(--text-primary,#1a1a18);cursor:pointer;}
    .bld-mobile-drawer-content{padding:16px;overflow-y:auto;display:flex;flex-direction:column;gap:16px;}
    .bld-mobile-drawer-alert{display:flex;align-items:center;justify-content:center;gap:10px;background:var(--accent,#1a3a5c);color:#fff!important;border-radius:999px;padding:13px 16px;font-size:15px;font-weight:900;text-decoration:none;box-shadow:0 8px 18px rgba(26,58,92,.22);}
    .bld-mobile-drawer-alert span{color:var(--gold,#c9a84c);}
    .bld-mobile-drawer-section{border:1px solid var(--border,#e8e6e1);border-radius:18px;background:var(--bg,#f9f8f5);padding:12px;}
    .bld-mobile-drawer-section h3{font-size:12px;text-transform:uppercase;letter-spacing:.08em;color:var(--text-muted,#9e9e97);font-family:Arial, sans-serif;font-weight:900;margin:0 0 8px;}
    .bld-mobile-drawer-link{display:flex;align-items:center;gap:10px;padding:12px 10px;border-radius:13px;background:#fff;color:var(--text-primary,#1a1a18)!important;text-decoration:none;font-size:15px;font-weight:900;margin-top:8px;border:1px solid var(--border,#e8e6e1);}
    .bld-mobile-drawer-link span{width:26px;height:26px;border-radius:9px;background:var(--accent-light,#e8eef5);display:inline-flex;align-items:center;justify-content:center;flex-shrink:0;}
    .bld-mobile-drawer-all{background:var(--accent-light,#e8eef5);color:var(--accent,#1a3a5c)!important;}
    body.bld-mobile-menu-open{overflow:hidden;}
    @media (min-width:901px){.bld-mobile-drawer,.bld-mobile-drawer-overlay{display:none!important;}.bld-mega-menu.show{position:fixed!important;top:var(--bld-menu-top,150px)!important;left:50%!important;right:auto!important;transform:translateX(-50%) translateY(0)!important;z-index:10050!important;}}
    @media (max-width:700px){.bld-header-search-row{padding:10px 14px 12px;}.bld-header-search{gap:8px;}.bld-header-search input{height:42px;font-size:15px;}.bld-header-search button{height:42px;padding:0 14px;font-size:13px;}}
    @media (max-width:520px){.bld-mobile-actions .bld-alert-btn{display:none;}.bld-mobile-drawer{width:92vw;}.bld-mobile-drawer-title{font-size:21px;}}
  `;

  function injectMobileDrawerStyles() {
    if (document.getElementById('bld-mobile-drawer-styles')) return;
    const style = document.createElement('style');
    style.id = 'bld-mobile-drawer-styles';
    style.textContent = MOBILE_DRAWER_CSS;
    document.head.appendChild(style);
  }

  function injectMobileDealFixes() {
    if (document.body.dataset.mobileFixes !== 'true') return;
    if (document.querySelector('script[src^="/mobile-deal-fixes.js"]')) return;
    const script = document.createElement('script');
    script.src = '/mobile-deal-fixes.js?v=1';
    script.defer = true;
    document.body.appendChild(script);
  }

  function buildHeader() {
    return `
      <header class="bld-header-shell">
        <div class="bld-header-main">
          <a href="/" class="bld-brand" aria-label="Black Lab Deals home">
            <img class="bld-brand-logo" src="/logo-128.jpg" alt="Black Lab Deals logo" width="118" height="118" decoding="async" fetchpriority="high">
            <div>
              <div class="bld-brand-title">Black Lab <span>Deals</span></div>
              <div class="bld-brand-rule"></div>
              <div class="bld-brand-tagline">Fresh Amazon deals updated daily</div>
            </div>
          </a>

          <div class="bld-header-actions">
            <nav class="bld-desktop-nav" aria-label="Main navigation">
              <a class="bld-hot-link" href="${getHomeAwareHref('#hot-section')}">Hot Deals</a>
              <span class="bld-nav-divider" aria-hidden="true"></span>
              <div class="bld-category-wrap">
                <button class="bld-category-trigger" type="button" aria-expanded="false">Categories <span aria-hidden="true">&#9662;</span></button>
                <div class="dropdown-menu bld-mega-menu" id="menu-categories">${MEGA_MENU_HTML}</div>
              </div>
              <span class="bld-nav-divider" aria-hidden="true"></span>
              <a class="bld-all-link" href="${getHomeAwareHref('#deals-section')}">All Deals</a>
            </nav>
            <a class="bld-alert-btn" href="${getHomeAwareHref('#alerts-box')}"><span class="bld-alert-icon">&bull;</span> Get Alerts</a>
          </div>

          <div class="bld-mobile-actions">
            <a class="bld-alert-btn" href="${getHomeAwareHref('#alerts-box')}"><span class="bld-alert-icon">&bull;</span> Get Alerts</a>
            <button class="bld-mobile-menu-btn" type="button" aria-label="Open menu" aria-expanded="false" aria-controls="bld-mobile-drawer">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" aria-hidden="true"><path d="M4 6h16"></path><path d="M4 12h16"></path><path d="M4 18h16"></path></svg>
            </button>
          </div>
        </div>
        <div class="bld-header-search-row">
          <form class="bld-header-search" action="/search.html" method="get" role="search">
            <input type="search" name="q" placeholder="Search Here" aria-label="Search all Black Lab Deals">
            <button type="submit">Search</button>
          </form>
        </div>
        ${MOBILE_DRAWER_HTML}
      </header>
    `;
  }

  function wireHomepageScrollLinks(root) {
    const isHome = window.location.pathname === '/' || window.location.pathname === '/index.html';
    if (!isHome) return;
    const hotLink = root.querySelector('.bld-hot-link');
    const allLink = root.querySelector('.bld-all-link');
    if (hotLink && typeof window.scrollToHotDeals === 'function') {
      hotLink.addEventListener('click', function (event) { event.preventDefault(); window.scrollToHotDeals(); });
    }
    if (allLink && typeof window.scrollToAllDeals === 'function') {
      allLink.addEventListener('click', function (event) { event.preventDefault(); window.scrollToAllDeals(); });
    }
  }

  function wireMobileMenu(root) {
    const btn = root.querySelector('.bld-mobile-menu-btn');
    const drawer = root.querySelector('.bld-mobile-drawer');
    const overlay = root.querySelector('.bld-mobile-drawer-overlay');
    if (!btn || !drawer || !overlay) return;

    function openMenu() {
      overlay.hidden = false;
      window.requestAnimationFrame(function () {
        drawer.classList.add('show');
        overlay.classList.add('show');
      });
      drawer.setAttribute('aria-hidden', 'false');
      btn.setAttribute('aria-expanded', 'true');
      document.body.classList.add('bld-mobile-menu-open');
    }

    function closeMenu() {
      drawer.classList.remove('show');
      overlay.classList.remove('show');
      drawer.setAttribute('aria-hidden', 'true');
      btn.setAttribute('aria-expanded', 'false');
      document.body.classList.remove('bld-mobile-menu-open');
      setTimeout(function () { overlay.hidden = true; }, 240);
    }

    btn.addEventListener('click', openMenu);
    root.querySelectorAll('[data-bld-mobile-close]').forEach(el => el.addEventListener('click', closeMenu));
    root.querySelectorAll('.bld-mobile-drawer a').forEach(link => link.addEventListener('click', closeMenu));
    document.addEventListener('keydown', function (event) { if (event.key === 'Escape') closeMenu(); });
    window.openMobileMenu = openMenu;
    window.closeMobileMenu = closeMenu;
  }

  function initMegaMenu(root) {
    const trigger = root.querySelector('.bld-category-trigger');
    const menu = root.querySelector('.bld-mega-menu');
    const header = root.querySelector('.bld-header-shell');
    if (!trigger || !menu) return;

    function setMenuPosition() {
      if (!header) return;
      const rect = header.getBoundingClientRect();
      document.documentElement.style.setProperty('--bld-menu-top', Math.max(0, rect.bottom + 8) + 'px');
    }

    function closeMegaMenu() {
      menu.classList.remove('show');
      trigger.setAttribute('aria-expanded', 'false');
      document.body.classList.remove('bld-mega-menu-open');
    }

    function openMegaMenu() {
      setMenuPosition();
      menu.classList.add('show');
      trigger.setAttribute('aria-expanded', 'true');
      document.body.classList.add('bld-mega-menu-open');
    }

    trigger.addEventListener('click', function (event) {
      event.preventDefault();
      event.stopPropagation();
      if (menu.classList.contains('show')) closeMegaMenu();
      else openMegaMenu();
    });

    menu.addEventListener('click', function (event) { event.stopPropagation(); });
    window.addEventListener('scroll', function () { if (menu.classList.contains('show')) setMenuPosition(); }, { passive: true });
    window.addEventListener('resize', function () { if (menu.classList.contains('show')) setMenuPosition(); });
    document.addEventListener('click', function (event) { if (!root.contains(event.target) && !menu.contains(event.target)) closeMegaMenu(); });
    document.addEventListener('keydown', function (event) { if (event.key === 'Escape') closeMegaMenu(); });
  }

  function mountHeader() {
    const mount = document.getElementById('site-header');
    if (!mount) return;
    injectMobileDrawerStyles();
    mount.innerHTML = buildHeader();
    wireHomepageScrollLinks(mount);
    wireMobileMenu(mount);
    initMegaMenu(mount);
    injectMobileDealFixes();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', mountHeader);
  } else {
    mountHeader();
  }
})();
