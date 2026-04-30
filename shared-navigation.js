// Shared Black Lab Deals navigation
// Any page can load this file and include <div id="site-navigation"></div>.
// Update this one file to keep navigation uniform across the site.

(function () {
  const styles = `
    .bld-disclosure-bar {
      background: #fffbf0;
      border-bottom: 1px solid #f0e8d0;
      padding: 8px 24px;
      text-align: center;
      font-size: 11px;
      color: #8a6a20;
      letter-spacing: 0.02em;
      font-family: 'DM Sans', sans-serif;
    }
    .bld-disclosure-bar span { max-width: 900px; display: inline-block; }
    .bld-nav {
      background: #ffffff;
      border-bottom: 1px solid #e8e6e1;
      position: sticky;
      top: 0;
      z-index: 300;
      font-family: 'DM Sans', sans-serif;
    }
    .bld-nav-inner {
      max-width: 1100px;
      margin: 0 auto;
      padding: 0 24px;
      height: 128px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }
    .bld-logo-wrap {
      display: flex;
      align-items: center;
      gap: 10px;
      text-decoration: none;
      color: #1a1a18;
    }
    .bld-logo-img {
      width: 120px;
      height: 120px;
      border-radius: 50%;
      object-fit: cover;
      border: 2px solid #c9a84c;
    }
    .bld-logo-text {
      font-family: 'Playfair Display', serif;
      font-size: 20px;
      color: #1a1a18;
      letter-spacing: -0.5px;
      line-height: 1.1;
    }
    .bld-logo-text span { color: #c9a84c; }
    .bld-nav-tagline {
      font-size: 12px;
      color: #9e9e97;
      letter-spacing: 0.05em;
      text-transform: uppercase;
      margin-top: 3px;
    }
    .bld-desktop-nav {
      display: flex;
      align-items: center;
      gap: 4px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .bld-desktop-nav a {
      text-decoration: none;
      color: #6b6b65;
      font-size: 14px;
      font-weight: 500;
      transition: color 0.15s, background 0.15s;
      padding: 6px 10px;
      border-radius: 8px;
      display: flex;
      align-items: center;
      gap: 4px;
    }
    .bld-desktop-nav a:hover,
    .bld-desktop-nav a.active {
      color: #1a3a5c;
      background: #e8eef5;
    }
    .bld-nav-alert-btn {
      display: inline-flex !important;
      align-items: center;
      gap: 8px;
      padding: 10px 14px !important;
      border-radius: 999px !important;
      background: #1a3a5c !important;
      color: #ffffff !important;
      font-size: 13px !important;
      font-weight: 700 !important;
      text-decoration: none !important;
    }
    .bld-nav-alert-btn:hover { background: #2a5a8c !important; color: #ffffff !important; }
    @media(max-width: 900px) {
      .bld-nav-inner {
        height: auto;
        min-height: 112px;
        align-items: flex-start;
        flex-direction: column;
        padding: 12px 18px 16px;
      }
      .bld-logo-img { width: 92px; height: 92px; }
      .bld-desktop-nav { justify-content: flex-start; }
    }
  `;

  const currentPath = window.location.pathname;
  const nav = `
    <div class="bld-disclosure-bar"><span>As an Amazon Associate, Black Lab Deals may earn from qualifying purchases.</span></div>
    <nav class="bld-nav">
      <div class="bld-nav-inner">
        <a class="bld-logo-wrap" href="/">
          <img class="bld-logo-img" src="/logo.png" alt="Black Lab Deals logo">
          <div>
            <div class="bld-logo-text">Black Lab <span>Deals</span></div>
            <div class="bld-nav-tagline">Amazon price drops</div>
          </div>
        </a>
        <div class="bld-desktop-nav" aria-label="Main navigation">
          <a href="/" class="${currentPath === '/' || currentPath === '/index.html' ? 'active' : ''}">All Deals</a>
          <a href="/best-seller-deals.html" class="${currentPath.includes('best-seller-deals') ? 'active' : ''}">Best Seller Deals</a>
          <a href="/#hot-deals">Hot Deals</a>
          <a href="/#categories">Categories</a>
          <a class="bld-nav-alert-btn" href="/#alerts">Get Deal Alerts</a>
        </div>
      </div>
    </nav>
  `;

  function injectNavigation() {
    let target = document.getElementById('site-navigation');
    if (!target) {
      target = document.createElement('div');
      target.id = 'site-navigation';
      document.body.insertBefore(target, document.body.firstChild);
    }
    target.innerHTML = nav;
  }

  function injectStyles() {
    if (document.getElementById('shared-navigation-styles')) return;
    const style = document.createElement('style');
    style.id = 'shared-navigation-styles';
    style.textContent = styles;
    document.head.appendChild(style);
  }

  injectStyles();
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', injectNavigation);
  } else {
    injectNavigation();
  }
})();
