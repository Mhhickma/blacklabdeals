/* Shared conservative Product Picks page renderer. Public cards only use the approved feed fields. */
(function () {
  const PRICE_DISCLAIMER = 'Product prices and availability are accurate as of the date/time indicated and are subject to change. Any price and availability information displayed on Amazon at the time of purchase will apply to the purchase of this product.';
  const PAGE_SIZE = 50;
  const MOBILE_INITIAL_SIZE = 24;
  const HOMEPAGE_CATEGORY_SAMPLE_SIZE = 5;
  const SORT_OPTIONS = new Set(['best', 'price-asc', 'price-desc', 'newest']);
  const APPROVED_FIELDS = new Set(['asin','title','brand','cat','image','price','price_amount','currency','availability','link','desc','seen_at','updated_at']);
  const HOMEPAGE_CATEGORY_ORDER = ['Tools & Home Improvement','Home & Kitchen','Electronics','Patio, Lawn & Garden','Pet Supplies','Toys & Games','Office Products','Health & Household','Baby Products','Sports & Outdoors','Musical Instruments','Automotive'];
  const CATEGORY_SCROLLER_LINKS = [
    ['Top 100', '/top-100-amazon-deals-today/'],
    ['Best Sellers', '/best-seller-deals.html'],
    ['Under $50', '/best-amazon-deals-under-50/'],
    ['All Categories', '/categories/'],
    ['Tools', '/best-amazon-tool-deals/'],
    ['Home & Kitchen', '/best-amazon-home-kitchen-deals/'],
    ['Electronics', '/best-amazon-electronics-deals/'],
    ['Automotive', '/best-amazon-automotive-deals/'],
    ['Patio, Lawn & Garden', '/best-amazon-patio-lawn-garden-deals/'],
    ['Sports & Outdoors', '/best-amazon-sports-outdoors-deals/'],
    ['Pet Supplies', '/best-amazon-pet-supplies-deals/'],
    ['Toys & Games', '/best-amazon-toys-games-deals/'],
    ['Office Products', '/best-amazon-office-products-deals/'],
    ['Health & Household', '/best-amazon-health-household-deals/'],
    ['Baby Products', '/best-amazon-baby-products-deals/'],
    ['Musical Instruments', '/best-amazon-musical-instruments-deals/']
  ];

  const cfg = window.BLD_PAGE_CONFIG || {};
  const params = new URLSearchParams(window.location.search);
  let all = [];
  let visible = initialVisibleCount();
  let query = params.get('q')?.trim() || '';
  let sort = SORT_OPTIONS.has(params.get('sort')) ? params.get('sort') : 'best';
  let lastSearchTracked = '';

  function isMobile() {
    return window.matchMedia && window.matchMedia('(max-width: 700px)').matches;
  }

  function initialVisibleCount() {
    if (cfg.limit) return cfg.limit;
    return isMobile() ? MOBILE_INITIAL_SIZE : PAGE_SIZE;
  }

  const esc = value => String(value ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
  const attr = value => esc(value).replace(/`/g, '&#96;');
  const track = (name, params) => { try { if (typeof window.BLDTrack === 'function') window.BLDTrack(name, params || {}); else if (typeof window.gtag === 'function') window.gtag('event', name, params || {}); } catch (error) {} };
  const safeValue = (item, key) => APPROVED_FIELDS.has(key) && item && item[key] !== undefined && item[key] !== null ? String(item[key]) : '';
  const safeNumber = value => { const n = Number(String(value ?? '').replace(/[^0-9.]/g, '')); return Number.isFinite(n) ? n : 0; };
  const money = value => value ? new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(value) : '';

  function stamp(value) {
    const parsed = Date.parse(value || '');
    const date = Number.isFinite(parsed) ? new Date(parsed) : new Date();
    return new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit', timeZoneName: 'short' }).format(date);
  }

  function asinImage(asin) {
    const value = String(asin || '').trim().toUpperCase();
    return /^[A-Z0-9]{10}$/.test(value) ? 'https://images-na.ssl-images-amazon.com/images/P/' + value + '.01._SL160_.jpg' : '';
  }

  function addCategoryScrollerStyles() {
    if (document.getElementById('bld-category-scroller-style')) return;
    const style = document.createElement('style');
    style.id = 'bld-category-scroller-style';
    style.textContent = '.bld-category-scroll-card{grid-column:1/-1;background:linear-gradient(135deg,#102942,#1a3a5c);color:#fff;border-radius:22px;padding:24px;box-shadow:0 16px 38px rgba(26,58,92,.22);display:grid;gap:16px;align-items:center;margin:6px 0}.bld-category-scroll-kicker{font-size:12px;font-weight:900;text-transform:uppercase;letter-spacing:.14em;color:#f1d88b}.bld-category-scroll-title{font-family:Georgia,serif;font-size:clamp(26px,4vw,38px);line-height:1.05;margin:0}.bld-category-scroll-copy{margin:0;color:#dbe8f3;font-size:15px;max-width:760px}.bld-category-scroll-links{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:9px}.bld-category-scroll-link{display:inline-flex;align-items:center;justify-content:center;background:#fff;color:#12304d!important;border:1px solid rgba(255,255,255,.8);border-radius:999px;padding:10px 12px;font-size:13px;font-weight:900;text-decoration:none;box-shadow:0 6px 18px rgba(0,0,0,.08);text-align:center;min-height:40px}.bld-category-scroll-link:hover,.bld-category-scroll-link:focus{background:#f8f1db;color:#102942!important;outline:2px solid rgba(255,255,255,.75);outline-offset:2px}@media(max-width:920px){.bld-category-scroll-links{grid-template-columns:repeat(3,minmax(0,1fr))}}@media(max-width:620px){.bld-category-scroll-card{padding:20px 16px}.bld-category-scroll-links{grid-template-columns:1fr 1fr}.bld-category-scroll-link{width:100%;padding:11px 9px;font-size:12px}}';
    document.head.appendChild(style);
  }

  function normalize(rawItems) {
    return rawItems.filter(item => item && typeof item === 'object').map(item => {
      const priceNum = safeNumber(safeValue(item, 'price_amount'));
      const asin = safeValue(item, 'asin');
      const title = safeValue(item, 'title') || 'Amazon product';
      const category = safeValue(item, 'cat') || 'Everything Else';
      const image = safeValue(item, 'image') || asinImage(asin);
      const link = safeValue(item, 'link') || '#';
      const price = safeValue(item, 'price') || money(priceNum) || 'Check current price on Amazon';
      const updatedValue = safeValue(item, 'updated_at') || safeValue(item, 'seen_at');
      return { asin, title, brand: safeValue(item, 'brand'), category, image, link, price, priceNum, updated: Date.parse(updatedValue || '') || Date.now() };
    }).filter(product => product.title && product.link && product.link !== '#');
  }

  function matchesCategory(product) {
    if (!cfg.category || cfg.category === 'all') return true;
    return product.category.toLowerCase() === String(cfg.category).toLowerCase();
  }

  function isHomepageAllProducts() {
    return (!cfg.category || cfg.category === 'all') && (document.body?.dataset?.bldHomepage === 'true' || location.pathname === '/' || location.pathname === '/index.html');
  }

  function byTitle(a, b) { return a.title.localeCompare(b.title); }
  function categoryRank(category) { const index = HOMEPAGE_CATEGORY_ORDER.indexOf(category); return index === -1 ? HOMEPAGE_CATEGORY_ORDER.length + 1 : index; }

  function balancedHomepageOrder(list) {
    const source = [...list].sort((a, b) => categoryRank(a.category) - categoryRank(b.category) || a.category.localeCompare(b.category) || byTitle(a, b));
    const used = new Set();
    const balanced = [];
    HOMEPAGE_CATEGORY_ORDER.forEach(category => {
      let taken = 0;
      for (const product of source) {
        if (taken >= HOMEPAGE_CATEGORY_SAMPLE_SIZE) break;
        if (product.category !== category || used.has(product.asin || product.link || product.title)) continue;
        used.add(product.asin || product.link || product.title);
        balanced.push(product);
        taken += 1;
      }
    });
    source.forEach(product => { const key = product.asin || product.link || product.title; if (!used.has(key)) { used.add(key); balanced.push(product); } });
    return balanced;
  }

  function ensureSortUi() {
    const section = document.getElementById('deals-section');
    if (!section) return;
    let bar = document.querySelector('.bld-sort-toolbar');
    let select = document.getElementById('sort-select');
    if (!bar) {
      bar = document.createElement('div');
      bar.className = 'bld-sort-toolbar';
      const header = section.querySelector('.section-header');
      section.insertBefore(bar, header || section.firstChild);
    }
    if (!select) {
      select = document.createElement('select');
      select.id = 'sort-select';
      select.className = 'sort-select';
      select.setAttribute('aria-label', 'Sort product picks');
    }
    if (!bar.contains(select)) {
      const oldParent = select.parentElement;
      if (oldParent && oldParent.classList.contains('toolbar')) oldParent.remove();
      bar.appendChild(select);
    }
  }

  function normalizeSortSelect() {
    ensureSortUi();
    const select = document.getElementById('sort-select');
    if (!select) return;
    select.innerHTML = '<option value="best">Product Picks</option><option value="price-asc">Price: Low to High</option><option value="price-desc">Price: High to Low</option><option value="newest">Newest First</option>';
    select.value = SORT_OPTIONS.has(sort) ? sort : 'best';
  }

  function sortByPriceAsc(a, b) { const ap = a.priceNum || Number.MAX_SAFE_INTEGER; const bp = b.priceNum || Number.MAX_SAFE_INTEGER; return ap - bp || a.title.localeCompare(b.title); }
  function sortByPriceDesc(a, b) { return (b.priceNum || 0) - (a.priceNum || 0) || a.title.localeCompare(b.title); }

  function filtered() {
    let list = all.filter(matchesCategory);
    if (cfg.maxPrice) list = list.filter(product => product.priceNum && product.priceNum <= cfg.maxPrice);
    if (query) {
      const q = query.toLowerCase();
      list = list.filter(product => (product.title + ' ' + product.brand + ' ' + product.category + ' ' + product.asin).toLowerCase().includes(q));
    }
    if (sort === 'price-asc') list.sort(sortByPriceAsc);
    else if (sort === 'price-desc') list.sort(sortByPriceDesc);
    else if (sort === 'newest') list.sort((a, b) => b.updated - a.updated);
    else if (isHomepageAllProducts()) list = balancedHomepageOrder(list);
    else list.sort((a, b) => a.category.localeCompare(b.category) || a.title.localeCompare(b.title));
    return cfg.limit ? list.slice(0, cfg.limit) : list;
  }

  function card(product) {
    const image = product.image ? '<img src="' + esc(product.image) + '" alt="' + esc(product.title) + '" loading="lazy" decoding="async">' : '';
    return '<article class="bld-product-card" role="link" tabindex="0" aria-label="View ' + attr(product.title) + ' on Amazon" data-link="' + attr(product.link) + '" data-asin="' + attr(product.asin) + '" data-title="' + attr(product.title) + '" data-category="' + attr(product.category) + '">'
      + '<div class="bld-product-img">' + image + '</div>'
      + '<div class="bld-product-body">'
      + '<div class="bld-product-title">' + esc(product.title) + '</div>'
      + '<div class="bld-product-category">' + esc(product.category) + '</div>'
      + '<div class="bld-product-price">' + esc(product.price) + '</div>'
      + '<div class="bld-price-stamp">Product information shown as of ' + esc(stamp(product.updated)) + '. Confirm final price and availability on Amazon.</div>'
      + '<div class="bld-card-disclaimer">' + PRICE_DISCLAIMER + '</div>'
      + '<a class="bld-view-btn" href="' + esc(product.link) + '" target="_blank" rel="nofollow sponsored noopener" data-asin="' + attr(product.asin) + '" data-title="' + attr(product.title) + '" data-category="' + attr(product.category) + '">View on Amazon</a>'
      + '</div></article>';
  }

  function categoryScrollerCard() {
    const links = CATEGORY_SCROLLER_LINKS.map(([label, href]) => '<a class="bld-category-scroll-link" href="' + esc(href) + '" data-category-scroll-choice="' + attr(label) + '">' + esc(label) + '</a>').join('');
    return '<section class="bld-category-scroll-card" aria-label="Shop by category"><div><div class="bld-category-scroll-kicker">Still browsing?</div><h2 class="bld-category-scroll-title">Shop by Category</h2><p class="bld-category-scroll-copy">Jump to any product-pick category and find what you are looking for faster.</p></div><div class="bld-category-scroll-links">' + links + '</div></section>';
  }

  function shouldShowCategoryScroller(shown) {
    const minimumItems = isMobile() ? 20 : 8;
    return !(cfg.limit || query || shown.length < minimumItems);
  }

  function productGridHtml(shown) {
    if (!shouldShowCategoryScroller(shown)) return shown.map(card).join('');
    const insertAt = isMobile() ? 18 : 8;
    const html = shown.map(card);
    html.splice(Math.min(insertAt, html.length), 0, categoryScrollerCard());
    return html.join('');
  }

  function openProductCard(cardEl) {
    if (!cardEl) return;
    const link = cardEl.getAttribute('data-link');
    if (!link) return;
    track('product_card_click', { asin: cardEl.getAttribute('data-asin') || '', product_title: (cardEl.getAttribute('data-title') || '').slice(0, 120), product_category: cardEl.getAttribute('data-category') || '', page_category: cfg.category || 'all' });
    window.open(link, '_blank', 'noopener');
  }

  function render() {
    const list = filtered();
    const shown = list.slice(0, visible);
    const countEl = document.getElementById('deal-count');
    const gridEl = document.getElementById('products-grid');
    const moreEl = document.getElementById('load-more');
    if (countEl) countEl.textContent = list.length ? 'Showing ' + Math.min(visible, list.length) + ' of ' + list.length + ' Product Picks' : '0 Product Picks';
    if (gridEl) gridEl.innerHTML = shown.length ? productGridHtml(shown) : '<div class="bld-empty">No matching product picks found right now.</div>';
    if (moreEl) { moreEl.hidden = !!cfg.limit || visible >= list.length; moreEl.textContent = 'Keep Browsing Product Picks'; }
  }

  function applySearch(value, shouldUpdateUrl) {
    query = String(value || '').trim();
    visible = cfg.limit || PAGE_SIZE;
    const input = document.getElementById('search-input');
    if (input) input.value = query;
    if (shouldUpdateUrl) {
      const url = new URL(window.location.href);
      if (query) url.searchParams.set('q', query); else url.searchParams.delete('q');
      if (sort && sort !== 'best') url.searchParams.set('sort', sort); else url.searchParams.delete('sort');
      url.hash = 'deals-section';
      history.replaceState(null, '', url);
    }
    render();
    document.getElementById('deals-section')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  window.BLDApplyProductSearch = applySearch;

  async function load() {
    try {
      const feedPath = cfg.feedPath || '/deals.json';
      const response = await fetch(feedPath, { cache: 'force-cache' });
      if (!response.ok) throw new Error('Feed unavailable');
      const data = await response.json();
      all = normalize(Array.isArray(data) ? data : (data.deals || []));
      track('product_feed_loaded', { feed_path: feedPath, feed_count: all.length, page_category: cfg.category || 'all', page_limit: cfg.limit || '' });
      render();
      if (query) applySearch(query, false);
    } catch (error) {
      track('product_feed_error', { page_category: cfg.category || 'all' });
      const gridEl = document.getElementById('products-grid');
      const countEl = document.getElementById('deal-count');
      if (gridEl) gridEl.innerHTML = '<div class="bld-empty">Product picks are temporarily unavailable. Please check back soon.</div>';
      if (countEl) countEl.textContent = 'Product Picks unavailable';
    }
  }

  document.addEventListener('DOMContentLoaded', () => {
    addCategoryScrollerStyles();
    normalizeSortSelect();
    const input = document.getElementById('search-input');
    if (input && query) input.value = query;
    input?.addEventListener('input', event => {
      query = event.target.value.trim();
      visible = cfg.limit || PAGE_SIZE;
      render();
      if (query.length >= 3 && query !== lastSearchTracked) {
        lastSearchTracked = query;
        track('product_search', { search_term: query.slice(0, 120), search_length: query.length, result_count: filtered().length, page_category: cfg.category || 'all' });
      }
    });
    const grid = document.getElementById('products-grid');
    grid?.addEventListener('click', event => {
      const categoryChoice = event.target.closest('[data-category-scroll-choice]');
      if (categoryChoice) {
        track('category_scroll_choice', { choice: categoryChoice.getAttribute('data-category-scroll-choice') || '', page_category: cfg.category || 'all', source: location.pathname });
        return;
      }
      if (event.target.closest('a,button,input,select,textarea')) return;
      openProductCard(event.target.closest('.bld-product-card[data-link]'));
    });
    grid?.addEventListener('keydown', event => {
      if (event.key !== 'Enter' && event.key !== ' ') return;
      const cardEl = event.target.closest('.bld-product-card[data-link]');
      if (!cardEl) return;
      event.preventDefault();
      openProductCard(cardEl);
    });
    document.getElementById('sort-select')?.addEventListener('change', event => {
      sort = SORT_OPTIONS.has(event.target.value) ? event.target.value : 'best';
      visible = cfg.limit || PAGE_SIZE;
      const url = new URL(window.location.href);
      if (sort && sort !== 'best') url.searchParams.set('sort', sort); else url.searchParams.delete('sort');
      history.replaceState(null, '', url);
      track('product_sort_change', { sort_value: sort, page_category: cfg.category || 'all' });
      render();
    });
    document.getElementById('load-more')?.addEventListener('click', () => {
      visible += PAGE_SIZE;
      track('product_load_more', { visible_count: visible, page_category: cfg.category || 'all' });
      render();
    });
    load();
  });
})();