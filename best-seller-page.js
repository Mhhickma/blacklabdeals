// Best Seller Deals page logic
// Isolated from site-common.js to avoid global variable conflicts.
(function () {
  const DATA_URL = '/best_seller_deals.json';
  const PAGE_SIZE = 25;
  let bestSellerDeals = [];
  let visibleDealsCount = PAGE_SIZE;

  function getEl(id) {
    return document.getElementById(id);
  }

  function money(value) {
    if (value === null || value === undefined || value === '') return '';
    if (typeof value === 'string') return value;
    return `$${Number(value).toFixed(2)}`;
  }

  function fmtDate(value) {
    if (!value) return 'â€”';
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return 'â€”';
    return d.toLocaleString([], { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' });
  }

  function cleanText(value) {
    return String(value || '').replace(/[&<>'"]/g, c => ({
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      "'": '&#39;',
      '"': '&quot;'
    }[c]));
  }

  function optimizeDealImage(src) {
    return String(src || '').replace(/\._SL\d+_\./, '._SL160_.');
  }

  function renderFilters() {
    const categoryFilter = getEl('categoryFilter');
    if (!categoryFilter) return;
    const selected = categoryFilter.value;
    categoryFilter.innerHTML = '<option value="all">All categories</option>';
    const cats = [...new Set(bestSellerDeals.map(d => d.cat).filter(Boolean))].sort();
    cats.forEach(cat => {
      const opt = document.createElement('option');
      opt.value = cat;
      opt.textContent = cat;
      categoryFilter.appendChild(opt);
    });
    categoryFilter.value = selected || 'all';
  }

  function dealCard(deal) {
    const rank = deal.bestSellerRank ? `#${deal.bestSellerRank}` : 'Best Seller';
    const pct = deal.pct ? `${deal.pct}% off` : 'Price Drop';
    const image = deal.image
      ? `<img src="${cleanText(optimizeDealImage(deal.image))}" alt="${cleanText(deal.title)}" loading="lazy" decoding="async">`
      : 'ðŸ¾';
    const was = deal.was ? `<span class="best-seller-was">${cleanText(deal.was)}</span>` : '';

    return `<article class="best-seller-card">
      <div class="best-seller-img">${image}</div>
      <div class="best-seller-body">
        <div class="best-seller-badges"><span class="best-seller-badge">${cleanText(pct)}</span><span class="best-seller-badge rank">${cleanText(rank)}</span></div>
        <div class="best-seller-title">${cleanText(deal.title)}</div>
        <div class="best-seller-category">${cleanText(deal.cat || 'Best Sellers')}</div>
        <div class="best-seller-price-row"><span class="best-seller-price">${cleanText(deal.price || money(deal.price_amount))}</span>${was}</div>
        <a class="best-seller-btn" href="${cleanText(deal.link)}" target="_blank" rel="nofollow sponsored noopener">View on Amazon</a>
      </div>
    </article>`;
  }

  function ensureLoadMoreButton() {
    const grid = getEl('dealsGrid');
    if (!grid) return null;

    let wrap = getEl('best-seller-load-more-wrap');
    if (!wrap) {
      wrap = document.createElement('div');
      wrap.id = 'best-seller-load-more-wrap';
      wrap.className = 'load-more-wrap hidden';
      wrap.innerHTML = '<button id="best-seller-load-more-btn" class="load-more-btn" type="button">Load 25 More Deals</button>';
      grid.insertAdjacentElement('afterend', wrap);
      wrap.querySelector('button').addEventListener('click', function () {
        visibleDealsCount += PAGE_SIZE;
        renderDeals(false);
      });
    }
    return wrap;
  }

  function renderLoadMore(total) {
    const wrap = ensureLoadMoreButton();
    if (!wrap) return;
    const button = getEl('best-seller-load-more-btn');
    const remaining = Math.max(0, total - visibleDealsCount);

    if (remaining > 0) {
      wrap.classList.remove('hidden');
      wrap.hidden = false;
      button.hidden = false;
      button.disabled = false;
      button.textContent = `Load ${Math.min(PAGE_SIZE, remaining)} More Deals (${remaining} remaining)`;
    } else {
      wrap.classList.add('hidden');
      wrap.hidden = true;
    }
  }

  function filteredSortedDeals() {
    const searchBox = getEl('searchBox');
    const categoryFilter = getEl('categoryFilter');
    const sortFilter = getEl('sortFilter');
    const q = searchBox ? searchBox.value.trim().toLowerCase() : '';
    const cat = categoryFilter ? categoryFilter.value : 'all';

    let deals = bestSellerDeals.filter(d => {
      const matchesSearch = !q || `${d.title || ''} ${d.brand || ''} ${d.cat || ''}`.toLowerCase().includes(q);
      const matchesCat = cat === 'all' || d.cat === cat;
      return matchesSearch && matchesCat;
    });

    const sort = sortFilter ? sortFilter.value : 'discount';
    if (sort === 'price-low') deals.sort((a, b) => (a.price_amount || 999999) - (b.price_amount || 999999));
    else if (sort === 'rank') deals.sort((a, b) => (a.bestSellerRank || 999999) - (b.bestSellerRank || 999999));
    else deals.sort((a, b) => (b.pct || 0) - (a.pct || 0));

    return deals;
  }

  function renderDeals(shouldReset = false) {
    const grid = getEl('dealsGrid');
    const statusEl = getEl('status');
    if (!grid || !statusEl) return;

    if (shouldReset) visibleDealsCount = PAGE_SIZE;

    const deals = filteredSortedDeals();
    const visibleDeals = deals.slice(0, visibleDealsCount);

    grid.innerHTML = visibleDeals.map(dealCard).join('');
    const dealCount = getEl('deal-count');
    if (dealCount) dealCount.textContent = deals.length ? `${Math.min(visibleDealsCount, deals.length)} of ${deals.length} deals` : '0 deals';
    statusEl.textContent = deals.length ? '' : 'No matching best seller deals are showing yet. Check back after the next hourly update.';
    statusEl.className = deals.length ? 'best-seller-status hidden' : 'best-seller-status';
    renderLoadMore(deals.length);
  }

  async function loadDeals() {
    const statusEl = getEl('status');
    try {
      const res = await fetch(DATA_URL, { cache: 'default' });
      if (!res.ok) throw new Error('Missing best_seller_deals.json');
      const data = await res.json();
      bestSellerDeals = Array.isArray(data.deals) ? data.deals : [];
      visibleDealsCount = PAGE_SIZE;

      const sortFilter = getEl('sortFilter');
      if (sortFilter) sortFilter.value = 'discount';

      const dealCount = getEl('dealCount');
      const watchCount = getEl('watchCount');
      const checkedCount = getEl('checkedCount');
      const hotCount = getEl('hotCount');
      const updatedAt = getEl('updatedAt');
      const heroPill = getEl('hero-pill');

      if (dealCount) dealCount.textContent = data.count ?? bestSellerDeals.length;
      if (watchCount) watchCount.textContent = data.watchlistCount ?? 'â€”';
      if (checkedCount) checkedCount.textContent = data.asinsCheckedThisRun ?? 'â€”';
      if (hotCount) hotCount.textContent = data.hotDeals ?? bestSellerDeals.filter(d => d.hot).length;
      if (updatedAt) updatedAt.textContent = fmtDate(data.updatedAt);
      if (heroPill) heroPill.textContent = `${data.count ?? bestSellerDeals.length} best seller deals found`;

      renderFilters();
      renderDeals(true);
    } catch (err) {
      if (statusEl) {
        statusEl.textContent = 'Best seller deals are not loaded yet. Try refreshing in a few minutes.';
        statusEl.className = 'best-seller-status';
      }
      console.error('Best Seller Deals load failed:', err);
    }
  }

  function init() {
    const searchBox = getEl('searchBox');
    const categoryFilter = getEl('categoryFilter');
    const sortFilter = getEl('sortFilter');
    if (searchBox) searchBox.addEventListener('input', () => renderDeals(true));
    if (categoryFilter) categoryFilter.addEventListener('change', () => renderDeals(true));
    if (sortFilter) sortFilter.addEventListener('change', () => renderDeals(true));
    loadDeals();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
