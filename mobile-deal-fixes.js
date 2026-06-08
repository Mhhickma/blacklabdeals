/* Mobile-only deal list fixes for Black Lab Deals. */
(function () {
  const mobileQuery = window.matchMedia('(max-width: 760px)');
  const PAGE_SIZE = 25;
  const state = new WeakMap();
  const $ = (selector, root = document) => root.querySelector(selector);
  const $$ = (selector, root = document) => Array.from(root.querySelectorAll(selector));
  const isMobile = () => mobileQuery.matches;
  const n = value => Number(value || 0) || 0;
  const price = deal => n(deal.price_amount ?? deal.current_price ?? deal.price ?? deal.sale_price);
  const cat = deal => String(deal.cat || deal.category || 'Amazon products');
  const title = deal => String(deal.title || deal.name || deal.product_title || 'Amazon product');
  const link = deal => String(deal.link || deal.amazon_url || deal.url || deal.affiliate_url || '#');
  const image = deal => String(deal.image || deal.image_url || deal.thumbnail || '').replace(/\._SL\d+_\./, '._SL240_.');
  const updated = deal => Date.parse(deal.updated_at || deal.updatedAt || deal.posted_at || deal.checked_at || deal.seen_at || 0) || 0;
  const money = value => value ? new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(value) : '';
  const esc = value => String(value || '').replace(/[&<>"']/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]));

  const categoryKeywords = {
    electronics: ['electronics', 'cell phones', 'computers', 'camera', 'audio', 'headphones', 'tablet', 'tv'],
    automotive: ['automotive', 'car', 'truck', 'vehicle', 'garage'],
    patio: ['patio', 'lawn', 'garden', 'outdoor', 'yard'],
    sports: ['sports', 'outdoors', 'camping', 'fitness', 'hunting', 'fishing'],
    pet: ['pet', 'pets', 'dog', 'cat'],
    toys: ['toys', 'games'],
    office: ['office', 'school supplies'],
    health: ['health', 'household', 'beauty', 'personal care', 'cleaning'],
    baby: ['baby'],
    music: ['musical instruments', 'music', 'instrument'],
    tools: ['tools', 'home improvement', 'tool'],
    home: ['home', 'kitchen']
  };

  function norm(value) {
    return String(value || '').toLowerCase().replace(/&/g, 'and').replace(/[^a-z0-9]+/g, ' ').trim();
  }

  function matchesCategory(deal, key) {
    const haystack = `${norm(cat(deal))} ${norm(title(deal))}`;
    return (categoryKeywords[norm(key)] || [norm(key)]).some(word => haystack.includes(norm(word)));
  }

  function cleanPath() {
    return String(location.pathname || '/').replace(/[#?].*$/, '').replace(/\/+$/, '') || '/';
  }

  function pageDeals(deals) {
    const mode = document.body.dataset.mode || 'all';
    const pageCategory = (document.body.dataset.category || '').trim();
    const path = cleanPath();
    if (pageCategory) return deals.filter(deal => matchesCategory(deal, pageCategory));
    if (mode === 'tools') return deals.filter(deal => matchesCategory(deal, 'tools'));
    if (mode === 'home') return deals.filter(deal => matchesCategory(deal, 'home'));
    if (mode === 'under50') return deals.filter(deal => price(deal) > 0 && price(deal) <= 50);
    if (path.includes('best-amazon-electronics-deals')) return deals.filter(deal => matchesCategory(deal, 'electronics'));
    if (path.includes('best-amazon-automotive-deals')) return deals.filter(deal => matchesCategory(deal, 'automotive'));
    if (path.includes('best-amazon-patio-lawn-garden-deals')) return deals.filter(deal => matchesCategory(deal, 'patio'));
    if (path.includes('best-amazon-pet-supplies-deals')) return deals.filter(deal => matchesCategory(deal, 'pet'));
    if (path.includes('best-amazon-sports-outdoors-deals')) return deals.filter(deal => matchesCategory(deal, 'sports'));
    if (path.includes('best-amazon-toys-games-deals')) return deals.filter(deal => matchesCategory(deal, 'toys'));
    if (path.includes('best-amazon-office-products-deals')) return deals.filter(deal => matchesCategory(deal, 'office'));
    if (path.includes('best-amazon-health-household-deals')) return deals.filter(deal => matchesCategory(deal, 'health'));
    if (path.includes('best-amazon-baby-products-deals')) return deals.filter(deal => matchesCategory(deal, 'baby'));
    if (path.includes('best-amazon-musical-instruments-deals')) return deals.filter(deal => matchesCategory(deal, 'music'));
    return deals;
  }

  function sortDeals(deals, sort = 'newest') {
    const list = [...deals];
    if (sort === 'price-low') return list.sort((a, b) => price(a) - price(b));
    if (sort === 'price-high') return list.sort((a, b) => price(b) - price(a));
    if (sort === 'newest') return list.sort((a, b) => updated(b) - updated(a));
    return list.sort((a, b) => updated(b) - updated(a) || price(a) - price(b) || title(a).localeCompare(title(b)));
  }

  function normalizeBestSellerLabels(root = document.body) {
    if (!root || !isMobile()) return;
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, {
      acceptNode(node) {
        const parent = node.parentElement;
        if (!parent || ['SCRIPT', 'STYLE', 'NOSCRIPT'].includes(parent.tagName)) return NodeFilter.FILTER_REJECT;
        return /Best Seller/.test(node.nodeValue) ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_SKIP;
      }
    });
    const nodes = [];
    while (walker.nextNode()) nodes.push(walker.currentNode);
    nodes.forEach(node => {
      node.nodeValue = node.nodeValue
        .replace(/Best Sellers+/g, 'Best Sellers')
        .replace(/Best Seller Deals/g, 'Best Sellers')
        .replace(/Best Seller(?!s)/g, 'Best Sellers');
    });
  }

  function hideMobileExtras() {
    $$('.popular-category-nav, .related-deal-pages').forEach(el => {
      el.hidden = true;
      el.style.display = 'none';
    });
  }

  function cleanTop100MobileSections() {
    const isTop = (document.body.dataset.mode || '') === 'top100' || location.pathname.includes('top-100-amazon-deals-today');
    if (!isTop) return;
    $$('.hero, .section-head, .filter-row, .status-line, .popular-category-nav, .related-deal-pages').forEach(el => {
      el.hidden = true;
      el.style.display = 'none';
    });
    const strip = $('.hot-strip');
    if (strip) {
      strip.style.marginTop = '0';
      strip.style.paddingTop = '10px';
    }
  }

  function dealCard(deal, index, ranked) {
    const now = deal.price || money(price(deal)) || 'See current price on Amazon';
    const img = image(deal) ? `<img src="${esc(image(deal))}" alt="${esc(title(deal))}" loading="lazy">` : '<div class="img-fallback">Product image unavailable</div>';
    return `<a class="hot-card deal-card" href="${esc(link(deal))}" target="_blank" rel="nofollow sponsored noopener">
      <div class="hot-card-img card-img">${img}${ranked ? `<div class="rank-badge">#${index + 1}</div>` : ''}</div>
      <div class="hot-card-body card-body">
        <div class="category-pill card-category">${esc(cat(deal))}</div>
        <div class="hot-card-title card-title">${esc(title(deal))}</div>
        <div class="hot-card-prices card-footer"><span class="hot-price-now price-now">${esc(now)}</span></div>
        <span class="hot-btn btn-deal">View on Amazon</span>
      </div>
    </a>`;
  }

  function bestSellerCard(deal) {
    const img = image(deal) ? `<img src="${esc(image(deal))}" alt="${esc(title(deal))}" loading="lazy">` : '<div class="img-fallback">Product image unavailable</div>';
    return `<article class="best-seller-card">
      <div class="best-seller-img">${img}</div>
      <div class="best-seller-body">
        <div class="best-seller-badges"><span class="best-seller-badge">Best Seller Product Pick</span></div>
        <div class="best-seller-title">${esc(title(deal))}</div>
        <div class="best-seller-category">${esc(cat(deal) || 'Best Sellers')}</div>
        <div class="best-seller-price-row"><span class="best-seller-price">${esc(deal.price || money(price(deal)) || 'See current price on Amazon')}</span></div>
        <a class="best-seller-btn" href="${esc(link(deal))}" target="_blank" rel="nofollow sponsored noopener">View on Amazon</a>
      </div>
    </article>`;
  }

  function renderPaged(grid, deals, cardFn, key) {
    if (!grid) return;
    const existing = state.get(grid) || { count: PAGE_SIZE, key: '' };
    const count = existing.key === key ? existing.count : PAGE_SIZE;
    state.set(grid, { count, key });
    grid.dataset.bldMobileFixed = 'true';
    grid.innerHTML = deals.slice(0, count).map(cardFn).join('') || '<div class="empty-state">No matching product picks found right now.</div>';

    let wrap = grid.nextElementSibling && grid.nextElementSibling.classList.contains('bld-mobile-load-more-wrap') ? grid.nextElementSibling : null;
    if (!wrap) {
      wrap = document.createElement('div');
      wrap.className = 'bld-mobile-load-more-wrap load-more-wrap';
      wrap.innerHTML = '<button class="load-more-btn" type="button">Keep Browsing Product Picks</button>';
      grid.insertAdjacentElement('afterend', wrap);
      wrap.querySelector('button').addEventListener('click', () => {
        const current = state.get(grid) || { count: PAGE_SIZE, key };
        state.set(grid, { count: current.count + PAGE_SIZE, key });
        renderPaged(grid, deals, cardFn, key);
      });
    }

    const remaining = Math.max(0, deals.length - count);
    const button = wrap.querySelector('button');
    wrap.hidden = remaining <= 0;
    wrap.classList.toggle('hidden', remaining <= 0);
    if (button) button.textContent = `Keep Browsing Product Picks${remaining ? ` (${remaining} remaining)` : ''}`;
    normalizeBestSellerLabels(grid.parentElement || document.body);
  }

  function updateCounts(total, shown) {
    ['deal-count', 'count-label'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.textContent = total ? `Showing ${Math.min(shown, total)} of ${total} product picks` : '0 product picks';
    });
  }

  function installCategoryControlStyles() {
    if ($('#bld-mobile-category-controls-style')) return;
    const style = document.createElement('style');
    style.id = 'bld-mobile-category-controls-style';
    style.textContent = `@media (max-width: 760px){.bld-mobile-category-controls{display:grid;gap:12px;margin:0 0 12px;padding:12px 0}.bld-mobile-category-controls input,.bld-mobile-category-controls select{width:100%;min-height:52px;border:1px solid var(--border,#e8e6e1);border-radius:999px;background:var(--surface,#fff);color:var(--text-primary,#1a1a18);font:700 16px/1.2 Arial, sans-serif;padding:0 18px;box-shadow:0 1px 3px rgba(0,0,0,.04)}.bld-mobile-category-controls input::placeholder{color:var(--text-muted,#9e9e97);font-weight:600}}`;
    document.head.appendChild(style);
  }

  function ensureCategoryControls(allDeals, draw) {
    installCategoryControlStyles();
    let controls = $('#bld-mobile-category-controls');
    if (!controls) {
      controls = document.createElement('div');
      controls.id = 'bld-mobile-category-controls';
      controls.className = 'bld-mobile-category-controls';
      controls.innerHTML = '<input id="bld-mobile-category-search" type="search" placeholder="Search product picks..." aria-label="Search product picks"><select id="bld-mobile-category-filter" aria-label="Filter by category"><option value="all">All categories</option></select><select id="bld-mobile-category-sort" aria-label="Sort product picks"><option value="newest">Newest</option><option value="price-low">Price: low to high</option><option value="price-high">Price: high to low</option></select>';
      const target = $('.hot-strip') || $('#hot-grid');
      if (target && target.parentNode) target.parentNode.insertBefore(controls, target);
    }
    const categorySelect = $('#bld-mobile-category-filter');
    if (categorySelect && !categorySelect.dataset.bldFilled) {
      [...new Set(allDeals.map(cat).filter(Boolean))].sort((a, b) => a.localeCompare(b)).forEach(category => {
        const option = document.createElement('option');
        option.value = category;
        option.textContent = category;
        categorySelect.appendChild(option);
      });
      categorySelect.dataset.bldFilled = 'true';
    }
    $$('#bld-mobile-category-search,#bld-mobile-category-filter,#bld-mobile-category-sort').forEach(control => {
      if (control.dataset.bldBound) return;
      control.dataset.bldBound = 'true';
      control.addEventListener(control.tagName === 'INPUT' ? 'input' : 'change', draw);
    });
  }

  function filteredCategoryDeals(allDeals, defaultDeals) {
    const query = $('#bld-mobile-category-search') ? $('#bld-mobile-category-search').value.trim().toLowerCase() : '';
    const selectedCategory = $('#bld-mobile-category-filter') ? $('#bld-mobile-category-filter').value : 'all';
    const selectedSort = $('#bld-mobile-category-sort') ? $('#bld-mobile-category-sort').value : 'newest';
    let deals = selectedCategory === 'all' ? defaultDeals : allDeals.filter(deal => cat(deal) === selectedCategory);
    if (query) deals = deals.filter(deal => `${title(deal)} ${deal.brand || ''} ${cat(deal)}`.toLowerCase().includes(query));
    return sortDeals(deals, selectedSort);
  }

  async function renderDealPages() {
    const response = await fetch('/deals.json', { cache: 'default' });
    if (!response.ok) return;
    const data = await response.json();
    const all = Array.isArray(data) ? data : Array.isArray(data.deals) ? data.deals : [];
    const pageList = sortDeals(pageDeals(all), 'newest');
    const isHome = cleanPath() === '/';
    const isTop = (document.body.dataset.mode || '') === 'top100' || location.pathname.includes('top-100-amazon-deals-today');

    if (isHome) {
      renderPaged($('#hot-grid'), pageList.slice(0, PAGE_SIZE), (deal, i) => dealCard(deal, i, false), `home-current-${pageList.length}`);
      renderPaged($('#deals-grid'), pageList, (deal, i) => dealCard(deal, i, false), `home-all-${pageList.length}`);
      updateCounts(pageList.length, (state.get($('#deals-grid')) || {}).count || PAGE_SIZE);
      return;
    }

    const grid = $('#hot-grid') || $('#deals-grid') || $('#dealsGrid');
    const drawCategoryPage = () => {
      if (grid) state.delete(grid);
      const filtered = isTop ? pageList.slice(0, 100) : filteredCategoryDeals(all, pageList);
      const filterKey = `${$('#bld-mobile-category-search') ? $('#bld-mobile-category-search').value : ''}-${$('#bld-mobile-category-filter') ? $('#bld-mobile-category-filter').value : ''}-${$('#bld-mobile-category-sort') ? $('#bld-mobile-category-sort').value : ''}`;
      renderPaged(grid, filtered, (deal, i) => dealCard(deal, i, isTop), `page-${location.pathname}-${filtered.length}-${filterKey}`);
      updateCounts(filtered.length, (state.get(grid) || {}).count || PAGE_SIZE);
      hideMobileExtras();
      cleanTop100MobileSections();
    };
    if (!isTop) ensureCategoryControls(all, drawCategoryPage);
    drawCategoryPage();
  }

  async function renderBestSellers() {
    const response = await fetch('/best_seller_deals.json', { cache: 'default' });
    if (!response.ok) return;
    const data = await response.json();
    const all = sortDeals(Array.isArray(data.deals) ? data.deals : [], 'newest');
    const grid = $('#dealsGrid');
    const search = $('#searchBox');
    const category = $('#categoryFilter');
    const sort = $('#sortFilter');
    if (sort) sort.value = 'newest';
    function filtered() {
      const q = search ? search.value.trim().toLowerCase() : '';
      const c = category ? category.value : 'all';
      let deals = all.filter(deal => (!q || `${title(deal)} ${deal.brand || ''} ${cat(deal)}`.toLowerCase().includes(q)) && (c === 'all' || cat(deal) === c));
      return sortDeals(deals, sort ? sort.value : 'newest');
    }
    function draw(reset) {
      if (reset && grid) state.delete(grid);
      const list = filtered();
      renderPaged(grid, list, bestSellerCard, `best-${search ? search.value : ''}-${category ? category.value : ''}-${sort ? sort.value : ''}-${list.length}`);
      updateCounts(list.length, (state.get(grid) || {}).count || PAGE_SIZE);
    }
    [search, category, sort].forEach(el => el && el.addEventListener(el.tagName === 'INPUT' ? 'input' : 'change', () => draw(true)));
    draw(true);
  }

  function installMobileNav() {
    $$('.mobile-deal-nav,.bld-mobile-deal-nav').forEach(el => el.remove());
    const nav = document.createElement('nav');
    nav.className = 'bld-mobile-deal-nav';
    nav.setAttribute('aria-label', 'Mobile deal navigation');
    nav.innerHTML = '<button type="button" data-mobile-target="hot">Product Picks</button><a href="/categories/">Categories</a><a href="/best-seller-deals.html">Best Sellers</a>';
    document.body.appendChild(nav);
    nav.addEventListener('click', event => {
      const button = event.target.closest('button[data-mobile-target="hot"]');
      if (!button) return;
      const target = $('#hot-section') || $('#hot-deals') || $('.hot-strip') || $('#hot-grid');
      if (target) target.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
  }

  function hideCompetingButtons() {
    $$('.load-more-wrap:not(.bld-mobile-load-more-wrap),.bld-load-more-wrap,.bld-home-load-more-wrap').forEach(el => {
      if (!el.closest('.bld-mobile-load-more-wrap')) el.style.display = 'none';
    });
  }

  function start() {
    if (!isMobile()) return;
    installMobileNav();
    normalizeBestSellerLabels();
    hideMobileExtras();
    cleanTop100MobileSections();
    if (location.pathname.includes('best-seller-deals')) renderBestSellers().then(hideCompetingButtons).catch(console.error);
    else renderDealPages().then(hideCompetingButtons).catch(console.error);
    setTimeout(() => {
      normalizeBestSellerLabels();
      hideMobileExtras();
      cleanTop100MobileSections();
      hideCompetingButtons();
    }, 1000);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start);
  else start();
})();
