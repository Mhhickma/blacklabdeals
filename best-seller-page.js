// Best Seller Deals page logic
// Isolated from site-common.js to avoid global variable conflicts.
(function () {
  const DATA_URL = 'https://raw.githubusercontent.com/Mhhickma/blacklabdeals/main/best_seller_deals.json';
  let bestSellerDeals = [];

  function getEl(id) {
    return document.getElementById(id);
  }

  function money(value) {
    if (value === null || value === undefined || value === '') return '';
    if (typeof value === 'string') return value;
    return `$${Number(value).toFixed(2)}`;
  }

  function fmtDate(value) {
    if (!value) return '—';
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return '—';
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
      ? `<img src="${cleanText(deal.image)}" alt="${cleanText(deal.title)}" loading="lazy">`
      : '🐾';
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

  function renderDeals() {
    const grid = getEl('dealsGrid');
    const statusEl = getEl('status');
    const searchBox = getEl('searchBox');
    const categoryFilter = getEl('categoryFilter');
    const sortFilter = getEl('sortFilter');
    if (!grid || !statusEl || !searchBox || !categoryFilter || !sortFilter) return;

    const q = searchBox.value.trim().toLowerCase();
    const cat = categoryFilter.value;
    let deals = bestSellerDeals.filter(d => {
      const matchesSearch = !q || `${d.title || ''} ${d.brand || ''} ${d.cat || ''}`.toLowerCase().includes(q);
      const matchesCat = cat === 'all' || d.cat === cat;
      return matchesSearch && matchesCat;
    });

    const sort = sortFilter.value;
    if (sort === 'discount') deals.sort((a, b) => (b.pct || 0) - (a.pct || 0));
    if (sort === 'price-low') deals.sort((a, b) => (a.price_amount || 999999) - (b.price_amount || 999999));
    if (sort === 'rank') deals.sort((a, b) => (a.bestSellerRank || 999999) - (b.bestSellerRank || 999999));
    if (sort === 'newest') deals.sort((a, b) => String(b.updated_at || '').localeCompare(String(a.updated_at || '')));

    grid.innerHTML = deals.map(dealCard).join('');
    const dealCount = getEl('deal-count');
    if (dealCount) dealCount.textContent = `${deals.length} deals`;
    statusEl.textContent = deals.length ? '' : 'No matching best seller deals are showing yet. Check back after the next hourly update.';
    statusEl.className = deals.length ? 'best-seller-status hidden' : 'best-seller-status';
  }

  async function loadDeals() {
    const statusEl = getEl('status');
    try {
      const res = await fetch(DATA_URL + '?ts=' + Date.now(), { cache: 'no-store' });
      if (!res.ok) throw new Error('Missing best_seller_deals.json');
      const data = await res.json();
      bestSellerDeals = Array.isArray(data.deals) ? data.deals : [];

      const dealCount = getEl('dealCount');
      const watchCount = getEl('watchCount');
      const checkedCount = getEl('checkedCount');
      const hotCount = getEl('hotCount');
      const updatedAt = getEl('updatedAt');
      const heroPill = getEl('hero-pill');

      if (dealCount) dealCount.textContent = data.count ?? bestSellerDeals.length;
      if (watchCount) watchCount.textContent = data.watchlistCount ?? '—';
      if (checkedCount) checkedCount.textContent = data.asinsCheckedThisRun ?? '—';
      if (hotCount) hotCount.textContent = data.hotDeals ?? bestSellerDeals.filter(d => d.hot).length;
      if (updatedAt) updatedAt.textContent = fmtDate(data.updatedAt);
      if (heroPill) heroPill.textContent = `${data.count ?? bestSellerDeals.length} best seller deals found`;

      renderFilters();
      renderDeals();
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
    if (searchBox) searchBox.addEventListener('input', renderDeals);
    if (categoryFilter) categoryFilter.addEventListener('change', renderDeals);
    if (sortFilter) sortFilter.addEventListener('change', renderDeals);
    loadDeals();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
