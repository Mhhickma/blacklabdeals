/* Mobile-only deal list fixes for Black Lab Deals. */
(function () {
  const mq = window.matchMedia('(max-width: 760px)');
  const PAGE_SIZE = 25;
  const DEALS_URL = '/deals.json';
  const BEST_SELLER_URL = '/best_seller_deals.json';
  const state = new WeakMap();

  function isMobile() { return mq.matches; }
  function qs(sel, root = document) { return root.querySelector(sel); }
  function qsa(sel, root = document) { return Array.from(root.querySelectorAll(sel)); }
  function esc(value) {
    return String(value || '').replace(/[&<>"']/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]));
  }
  function num(value) { return Number(value || 0) || 0; }
  function pct(deal) { return num(deal.pct ?? deal.drop_percent ?? deal.discount_percent ?? deal.percent_off ?? deal.percentOff); }
  function price(deal) { return num(deal.price_amount ?? deal.current_price ?? deal.price ?? deal.sale_price); }
  function cat(deal) { return String(deal.cat || deal.category || 'Amazon Deals'); }
  function title(deal) { return String(deal.title || deal.name || deal.product_title || 'Amazon Deal'); }
  function link(deal) { return String(deal.link || deal.amazon_url || deal.url || deal.affiliate_url || '#'); }
  function image(deal) { return String(deal.image || deal.image_url || deal.thumbnail || ''); }
  function money(value) { return value ? new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(value) : ''; }
  function discountText(deal) { return deal.discount || (pct(deal) ? `${pct(deal)}% off` : 'Deal'); }
  function cleanPath(path) { return String(path || '').replace(/[#?].*$/, '').replace(/\/+$/, '') || '/'; }

  function sortByDiscount(deals) {
    return [...deals].sort((a, b) => pct(b) - pct(a) || price(a) - price(b) || title(a).localeCompare(title(b)));
  }

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
    appliances: ['appliances'],
    handmade: ['handmade'],
    industrial: ['industrial', 'scientific'],
    arts: ['arts', 'crafts', 'sewing'],
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

  function dealCard(deal, index, ranked) {
    const now = deal.price || money(price(deal)) || 'See deal';
    const was = deal.was || deal.old_price || deal.previous_price || '';
    const img = image(deal)
      ? `<img src="${esc(image(deal))}" alt="${esc(title(deal))}" loading="lazy">`
      : '<div class="img-fallback">Deal image unavailable</div>';
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
    const img = image(deal) ? `<img src="${esc(image(deal))}" alt="${esc(title(deal))}" loading="lazy">` : '🐾';
    const rank = deal.bestSellerRank ? `#${deal.bestSellerRank}` : 'Best Seller';
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
    const next = existing.key === key ? existing.count : PAGE_SIZE;
    state.set(grid, { count: next, key });
    const current = state.get(grid).count;
    grid.dataset.bldMobileFixed = 'true';
    grid.innerHTML = deals.slice(0, current).map(cardFn).join('') || '<div class="empty-state">No matching deals found right now.</div>';

    let wrap = grid.nextElementSibling && grid.nextElementSibling.classList.contains('bld-mobile-load-more-wrap') ? grid.nextElementSibling : null;
    if (!wrap) {
      wrap = document.createElement('div');
      wrap.className = 'bld-mobile-load-more-wrap load-more-wrap';
      wrap.innerHTML = '<button class="load-more-btn" type="button">Load 25 More Deals</button>';
      grid.insertAdjacentElement('afterend', wrap);
      wrap.querySelector('button').addEventListener('click', () => {
        const currentState = state.get(grid) || { count: PAGE_SIZE, key };
        state.set(grid, { count: currentState.count + PAGE_SIZE, key });
        renderPaged(grid, deals, cardFn, key);
      });
    }

    const remaining = Math.max(0, deals.length - current);
    const button = wrap.querySelector('button');
    wrap.hidden = remaining <= 0;
    wrap.classList.toggle('hidden', remaining <= 0);
    if (button) button.textContent = `Load ${Math.min(PAGE_SIZE, remaining)} More Deals${remaining ? ` (${remaining} remaining)` : ''}`;
  }

  function updateCounts(total, shown) {
    ['deal-count', 'count-label'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.textContent = total ? `Showing ${Math.min(shown, total)} of ${total} deals` : '0 deals';
    });
  }

  async function renderDealPages() {
    const response = await fetch(`${DEALS_URL}?mobile=${Date.now()}`, { cache: 'no-store' });
    if (!response.ok) return;
    const data = await response.json();
    const all = Array.isArray(data) ? data : Array.isArray(data.deals) ? data.deals : [];
    const pageList = sortByDiscount(pageDeals(all));
    const hotList = sortByDiscount(pageList.filter(deal => deal.hot || pct(deal) >= 40));
    const isHome = cleanPath(location.pathname) === '/';
    const isTop = (document.body.dataset.mode || '') === 'top100' || location.pathname.includes('top-100-amazon-deals-today');

    if (isHome) {
      renderPaged(qs('#hot-grid'), hotList, (deal, i) => dealCard(deal, i, false), `home-hot-${hotList.length}`);
      renderPaged(qs('#deals-grid'), pageList, (deal, i) => dealCard(deal, i, false), `home-all-${pageList.length}`);
      updateCounts(pageList.length, (state.get(qs('#deals-grid')) || {}).count || PAGE_SIZE);
    } else {
      const grid = qs('#hot-grid') || qs('#deals-grid') || qs('#dealsGrid');
      renderPaged(grid, pageList.slice(0, isTop ? 100 : pageList.length), (deal, i) => dealCard(deal, i, isTop), `page-${location.pathname}-${pageList.length}`);
      updateCounts(pageList.length, (state.get(grid) || {}).count || PAGE_SIZE);
    }
  }

  async function renderBestSellers() {
    const response = await fetch(`${BEST_SELLER_URL}?mobile=${Date.now()}`, { cache: 'no-store' });
    if (!response.ok) return;
    const data = await response.json();
    const all = sortByDiscount(Array.isArray(data.deals) ? data.deals : []);
    const grid = qs('#dealsGrid');
    const search = qs('#searchBox');
    const category = qs('#categoryFilter');
    const sort = qs('#sortFilter');
    if (sort) sort.value = 'discount';

    function filtered() {
      const q = search ? search.value.trim().toLowerCase() : '';
      const c = category ? category.value : 'all';
      let deals = all.filter(deal => {
        const matchesSearch = !q || `${title(deal)} ${deal.brand || ''} ${cat(deal)}`.toLowerCase().includes(q);
        const matchesCat = c === 'all' || cat(deal) === c;
        return matchesSearch && matchesCat;
      });
      if (sort && sort.value === 'price-low') deals = deals.sort((a, b) => price(a) - price(b));
      else if (sort && sort.value === 'rank') deals = deals.sort((a, b) => (a.bestSellerRank || 999999) - (b.bestSellerRank || 999999));
      else deals = sortByDiscount(deals);
      return deals;
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
    qsa('.mobile-deal-nav,.bld-mobile-deal-nav').forEach(el => el.remove());
    const nav = document.createElement('nav');
    nav.className = 'bld-mobile-deal-nav';
    nav.setAttribute('aria-label', 'Mobile deal navigation');
    nav.innerHTML = '<button type="button" data-mobile-target="hot">Hot</button><a href="/categories/">Categories</a><a href="/best-seller-deals.html">Best Seller</a>';
    document.body.appendChild(nav);
    nav.addEventListener('click', event => {
      const button = event.target.closest('button[data-mobile-target="hot"]');
      if (!button) return;
      const target = qs('#hot-section') || qs('#hot-deals') || qs('.hot-strip') || qs('#hot-grid');
      if (target) target.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
  }

  function hideCompetingButtons() {
    qsa('.load-more-wrap:not(.bld-mobile-load-more-wrap),.bld-load-more-wrap,.bld-home-load-more-wrap').forEach(el => {
      if (!el.closest('.bld-mobile-load-more-wrap')) el.style.display = 'none';
    });
  }

  function start() {
    if (!isMobile()) return;
    installMobileNav();
    if (location.pathname.includes('best-seller-deals')) renderBestSellers().then(hideCompetingButtons).catch(console.error);
    else renderDealPages().then(hideCompetingButtons).catch(console.error);
    setTimeout(hideCompetingButtons, 1000);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start);
  else start();
})();
