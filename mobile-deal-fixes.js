/* Mobile-only deal list fixes for Black Lab Deals. */
(function () {
  const mobileQuery = window.matchMedia('(max-width: 760px)');
  const PAGE_SIZE = 25;
  const DEALS_URL = '/deals.json';
  const BEST_SELLER_URL = '/best_seller_deals.json';
  const state = new WeakMap();

  const $ = (selector, root = document) => root.querySelector(selector);
  const $$ = (selector, root = document) => Array.from(root.querySelectorAll(selector));
  const isMobile = () => mobileQuery.matches;
  const cleanPath = path => String(path || '').replace(/[#?].*$/, '').replace(/\/+$/, '') || '/';
  const n = value => Number(value || 0) || 0;
  const pct = deal => n(deal.pct ?? deal.drop_percent ?? deal.discount_percent ?? deal.percent_off ?? deal.percentOff);
  const price = deal => n(deal.price_amount ?? deal.current_price ?? deal.price ?? deal.sale_price);
  const cat = deal => String(deal.cat || deal.category || 'Amazon Deals');
  const title = deal => String(deal.title || deal.name || deal.product_title || 'Amazon Deal');
  const link = deal => String(deal.link || deal.amazon_url || deal.url || deal.affiliate_url || '#');
  const image = deal => String(deal.image || deal.image_url || deal.thumbnail || '');
  const updated = deal => Date.parse(deal.updated_at || deal.updatedAt || deal.posted_at || deal.checked_at || deal.seen_at || 0) || 0;
  const money = value => value ? new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(value) : '';
  const esc = value => String(value || '').replace(/[&<>"']/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]));

  const CATEGORY_KEYWORDS = {
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
    const words = CATEGORY_KEYWORDS[norm(key)] || [norm(key)];
    return words.some(word => haystack.includes(norm(word)));
  }

  function sortDeals(deals, sort = 'discount') {
    const list = [...deals];
    if (sort === 'price-low') return list.sort((a, b) => price(a) - price(b));
    if (sort === 'price-high') return list.sort((a, b) => price(b) - price(a));
    if (sort === 'newest') return list.sort((a, b) => updated(b) - updated(a));
    return list.sort((a, b) => pct(b) - pct(a) || price(a) - price(b) || title(a).localeCompare(title(b)));
  }

  function pageDeals(deals) {
    const mode = document.body.dataset.mode || 'all';
    const pageCategory = (document.body.dataset.category || '').trim();
    const path = cleanPath(location.pathname);
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

  function discountText(deal) {
    return deal.discount || (pct(deal) ? `${pct(deal)}% off` : 'Deal');
  }

  function normalizeBestSellerLabels(root = document.body) {
    if (!root || !isMobile()) return;
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, {
      acceptNode(node) {
        const parent = node.parentElement;
        if (!parent || ['SCRIPT', 'STYLE', 'NOSCRIPT'].includes(parent.tagName)) return NodeFilter.FILTER_REJECT;
        return node.nodeValue.includes('Best Seller') ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_SKIP;
      }
    });
    const nodes = [];
    while (walker.nextNode()) nodes.push(walker.currentNode);
    nodes.forEach(node => {
      node.nodeValue = node.nodeValue
        .replace(/Best Seller Deals/g, 'Best Sellers')
        .replace(/Best Seller/g, 'Best Sellers');
    });
  }

  function dealCard(deal, index, ranked) {
    const now = deal.price || money(price(deal)) || 'See deal';
    const was = deal.was || deal.old_price || deal.previous_price || '';
    const img = image(deal) ? `<img src="${esc(image(deal))}" alt="${esc(title(deal))}" loading="lazy">` : '<div class="img-fallback">Deal image unavailable</div>';
    return `<a class="hot-card deal-card" href="${esc(link(deal))}" target="_blank" rel="nofollow sponsored noopener" data-deal-discount="${pct(deal)}">
      <div class="hot-card-img card-img">${img}${ranked ? `<div class="rank-badge">#${index + 1}</div>` : ''}<div class="hot-card-badge">${pct(deal) >= 40 || deal.hot ? 'Hot Deal' : 'Deal'}</div></div>
      <div class="hot-card-body card-body">
        <div class="category-pill card-category">${esc(cat(deal))}</div>
        <div class="hot-card-title card-title">${esc(title(deal))}</div>
        <div class="hot-card-prices card-footer"><span class="hot-price-now price-now">${esc(now)}</span>${was ? `<span class="hot-price-was price-was">${esc(was)}</span>` : ''}<span class="hot-off discount-badge">${esc(discountText(deal)).replace('-', '')}</span></div>
        <span class="hot-btn btn-deal">See Deal on Amazon →</span>
      </div>
    </a>`;
  }

  function bestSellerCard(deal) {
    const img = image(deal) ? `<img src="${esc(image(deal))}" alt="${esc(title(deal))}" loading="lazy">` : '<div class="img-fallback">Deal image unavailable</div>';
    const rank = deal.bestSellerRank ? `#${deal.bestSellerRank}` : 'Best Sellers';
    return `<article class="best-seller-card" data-deal-discount="${pct(deal)}">
      <div class="best-seller-img">${img}</div>
      <div class="best-seller-body">
        <div class="best-seller-badges"><span class="best-seller-badge">${esc(discountText(deal))}</span><span class="best-seller-badge rank">${esc(rank)}</span></div>
        <div class="best-seller-title">${esc(title(deal))}</div>
        <div class="best-seller-category">${esc(cat(deal) || 'Best Sellers')}</div>
        <div class="best-seller-price-row"><span class="best-seller-price">${esc(deal.price || money(price(deal)) || 'See price')}</span>${deal.was ? `<span class="best-seller-was">${esc(deal.was)}</span>` : ''}</div>
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
    grid.innerHTML = deals.slice(0, count).map(cardFn).join('') || '<div class="empty-state">No matching deals found right now.</div>';

    let wrap = grid.nextElementSibling && grid.nextElementSibling.classList.contains('bld-mobile-load-more-wrap') ? grid.nextElementSibling : null;
    if (!wrap) {
      wrap = document.createElement('div');
      wrap.className = 'bld-mobile-load-more-wrap load-more-wrap';
      wrap.innerHTML = '<button class="load-more-btn" type="button">Load 25 More Deals</button>';
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
    if (button) button.textContent = `Load ${Math.min(PAGE_SIZE, remaining)} More Deals${remaining ? ` (${remaining} remaining)` : ''}`;
    normalizeBestSellerLabels(grid.parentElement || document.body);
  }

  function updateCounts(total, shown) {
    ['deal-count', 'count-label'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.textContent = total ? `Showing ${Math.min(shown, total)} of ${total} deals` : '0 deals';
    });
  }

  function cleanTop100MobileSections() {
    if ((document.body.dataset.mode || '') !== 'top100') return;
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

  function installCategoryControlStyles() {
    if ($('#bld-mobile-category-controls-style')) return;
    const style = document.createElement('style');
    style.id = 'bld-mobile-category-controls-style';
    style.textContent = `
      @media (max-width: 760px) {
        .bld-mobile-category-controls {
          display: grid;
          gap: 12px;
          margin: 0 0 12px;
          padding: 12px 0;
        }
        .bld-mobile-category-controls input,
        .bld-mobile-category-controls select {
          width: 100%;
          min-height: 52px;
          border: 1px solid var(--border, #e8e6e1);
          border-radius: 999px;
          background: var(--surface, #fff);
          color: var(--text-primary, #1a1a18);
          font: 700 16px/1.2 'DM Sans', sans-serif;
          padding: 0 18px;
          box-shadow: 0 1px 3px rgba(0,0,0,.04);
        }
        .bld-mobile-category-controls input::placeholder {
          color: var(--text-muted, #9e9e97);
          font-weight: 600;
        }
      }
    `;
    document.head.appendChild(style);
  }

  function ensureCategoryControls(allDeals, draw) {
    installCategoryControlStyles();
    let controls = $('#bld-mobile-category-controls');
    if (!controls) {
      controls = document.createElement('div');
      controls.id = 'bld-mobile-category-controls';
      controls.className = 'bld-mobile-category-controls';
      controls.innerHTML = `
        <input id="bld-mobile-category-search" type="search" placeholder="Search deals..." aria-label="Search deals">
        <select id="bld-mobile-category-filter" aria-label="Filter by category"><option value="all">All categories</option></select>
        <select id="bld-mobile-category-sort" aria-label="Sort deals">
          <option value="discount">Biggest discount</option>
          <option value="price-low">Price: low to high</option>
          <option value="price-high">Price: high to low</option>
          <option value="newest">Newest</option>
        </select>
      `;
      const target = $('.hot-strip') || $('#hot-grid');
      if (target && target.parentNode) target.parentNode.insertBefore(controls, target);
    }

    const categorySelect = $('#bld-mobile-category-filter');
    if (categorySelect && !categorySelect.dataset.bldFilled) {
      const categories = [...new Set(allDeals.map(cat).filter(Boolean))].sort((a, b) => a.localeCompare(b));
      categories.forEach(category => {
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

    return controls;
  }

  function filteredCategoryDeals(allDeals, defaultDeals) {
    const search = $('#bld-mobile-category-search');
    const category = $('#bld-mobile-category-filter');
    const sort = $('#bld-mobile-category-sort');
    const query = search ? search.value.trim().toLowerCase() : '';
    const selectedCategory = category ? category.value : 'all';
    const selectedSort = sort ? sort.value : 'discount';

    let deals = selectedCategory === 'all'
      ? defaultDeals
      : allDeals.filter(deal => cat(deal) === selectedCategory);

    if (query) {
      deals = deals.filter(deal => `${title(deal)} ${deal.brand || ''} ${cat(deal)}`.toLowerCase().includes(query));
    }

    return sortDeals(deals, selectedSort);
  }

  async function renderDealPages() {
    const response = await fetch(`${DEALS_URL}?mobile=${Date.now()}`, { cache: 'no-store' });
    if (!response.ok) return;
    const data = await response.json();
    const all = Array.isArray(data) ? data : Array.isArray(data.deals) ? data.deals : [];
    const pageList = sortDeals(pageDeals(all), 'discount');
    const hotList = sortDeals(pageList.filter(deal => deal.hot || pct(deal) >= 40), 'discount');
    const isHome = cleanPath(location.pathname) === '/';
    const isTop = (document.body.dataset.mode || '') === 'top100' || location.pathname.includes('top-100-amazon-deals-today');

    if (isHome) {
      renderPaged($('#hot-grid'), hotList, (deal, i) => dealCard(deal, i, false), `home-hot-${hotList.length}`);
      renderPaged($('#deals-grid'), pageList, (deal, i) => dealCard(deal, i, false), `home-all-${pageList.length}`);
      updateCounts(pageList.length, (state.get($('#deals-grid')) || {}).count || PAGE_SIZE);
    } else {
      const grid = $('#hot-grid') || $('#deals-grid') || $('#dealsGrid');
      const drawCategoryPage = () => {
        if (grid) state.delete(grid);
        const filtered = isTop
          ? pageList.slice(0, 100)
          : filteredCategoryDeals(all, pageList);
        renderPaged(grid, filtered, (deal, i) => dealCard(deal, i, isTop), `page-${location.pathname}-${filtered.length}-${$('#bld-mobile-category-search') ? $('#bld-mobile-category-search').value : ''}-${$('#bld-mobile-category-filter') ? $('#bld-mobile-category-filter').value : ''}-${$('#bld-mobile-category-sort') ? $('#bld-mobile-category-sort').value : ''}`);
        updateCounts(filtered.length, (state.get(grid) || {}).count || PAGE_SIZE);
        cleanTop100MobileSections();
      };

      if (!isTop) ensureCategoryControls(all, drawCategoryPage);
      drawCategoryPage();
    }
  }

  async function renderBestSellers() {
    const response = await fetch(`${BEST_SELLER_URL}?mobile=${Date.now()}`, { cache: 'no-store' });
    if (!response.ok) return;
    const data = await response.json();
    const all = sortDeals(Array.isArray(data.deals) ? data.deals : [], 'discount');
    const grid = $('#dealsGrid');
    const search = $('#searchBox');
    const category = $('#categoryFilter');
    const sort = $('#sortFilter');
    if (sort) sort.value = 'discount';

    function filtered() {
      const q = search ? search.value.trim().toLowerCase() : '';
      const c = category ? category.value : 'all';
      let deals = all.filter(deal => {
        const matchesSearch = !q || `${title(deal)} ${deal.brand || ''} ${cat(deal)}`.toLowerCase().includes(q);
        const matchesCat = c === 'all' || cat(deal) === c;
        return matchesSearch && matchesCat;
      });
      return sortDeals(deals, sort ? sort.value : 'discount');
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
    nav.innerHTML = '<button type="button" data-mobile-target="hot">Hot</button><a href="/categories/">Categories</a><a href="/best-seller-deals.html">Best Sellers</a>';
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
    cleanTop100MobileSections();
    if (location.pathname.includes('best-seller-deals')) renderBestSellers().then(hideCompetingButtons).catch(console.error);
    else renderDealPages().then(hideCompetingButtons).catch(console.error);
    setTimeout(() => {
      normalizeBestSellerLabels();
      cleanTop100MobileSections();
      hideCompetingButtons();
    }, 1000);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start);
  else start();
})();
