// Best Seller Product Picks page logic.
(function () {
  const DATA_URL = '/best_seller_deals.json';
  const PAGE_SIZE = 25;
  let products = [];
  let visibleCount = PAGE_SIZE;
  const getEl = id => document.getElementById(id);
  const clean = value => String(value || '').replace(/[&<>'"]/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;'
  }[c]));
  const money = value => value ? new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(value) : '';

  function renderFilters() {
    const filter = getEl('categoryFilter');
    if (!filter) return;
    const selected = filter.value;
    filter.innerHTML = '<option value="all">All categories</option>';
    [...new Set(products.map(product => product.cat).filter(Boolean))].sort().forEach(category => {
      const option = document.createElement('option');
      option.value = category;
      option.textContent = category;
      filter.appendChild(option);
    });
    filter.value = selected || 'all';
  }

  function filteredProducts() {
    const query = (getEl('searchBox')?.value || '').trim().toLowerCase();
    const category = getEl('categoryFilter')?.value || 'all';
    const sort = getEl('sortFilter')?.value || 'newest';
    const result = products.filter(product => {
      const matchesQuery = !query || `${product.title || ''} ${product.brand || ''} ${product.cat || ''}`.toLowerCase().includes(query);
      return matchesQuery && (category === 'all' || product.cat === category);
    });
    if (sort === 'price-low') result.sort((a, b) => (a.price_amount || 999999) - (b.price_amount || 999999));
    else if (sort === 'price-high') result.sort((a, b) => (b.price_amount || 0) - (a.price_amount || 0));
    else result.sort((a, b) => Date.parse(b.updated_at || b.seen_at || 0) - Date.parse(a.updated_at || a.seen_at || 0));
    return result;
  }

  function render(reset = false) {
    if (reset) visibleCount = PAGE_SIZE;
    const grid = getEl('dealsGrid');
    if (!grid) return;
    const filtered = filteredProducts();
    const visible = filtered.slice(0, visibleCount);
    const featured = visible.slice(0, 8);
    const additional = visible.slice(8);
    const renderCard = window.bldProductCard || (product => `<article class="product-card"><a class="product-card-link" href="${clean(product.link)}" target="_blank" rel="nofollow sponsored noopener"><div class="product-card-body"><div class="product-card-meta">${clean(product.brand || product.cat || 'Amazon product')}</div><h3 class="product-card-title">${clean(product.title)}</h3><div class="product-card-price"><span>Current Amazon price</span><strong>${clean(product.price || money(product.price_amount) || 'See current price on Amazon')}</strong></div><span class="product-card-button">View on Amazon</span></div></a></article>`);
    grid.classList.add('product-grid', 'product-grid-featured');
    grid.innerHTML = featured.map(renderCard).join('');
    if (window.bldEnsureProductPageOrder) window.bldEnsureProductPageOrder(grid, additional, filtered.length);
    const count = getEl('deal-count');
    if (count) count.textContent = filtered.length ? `${Math.min(visibleCount, filtered.length)} of ${filtered.length} product picks` : '0 product picks';
    const status = getEl('status');
    if (status) {
      status.textContent = filtered.length ? '' : 'No matching best seller product picks are showing yet.';
      status.className = filtered.length ? 'best-seller-status hidden' : 'best-seller-status';
    }
    const existing = getEl('best-seller-load-more-wrap');
    if (existing) existing.remove();
    if (visibleCount < filtered.length) {
      const wrap = document.createElement('div');
      wrap.id = 'best-seller-load-more-wrap';
      wrap.className = 'load-more-wrap';
      wrap.innerHTML = `<button class="load-more-btn" type="button">Keep Browsing Product Picks (${filtered.length - visibleCount} remaining)</button>`;
      grid.insertAdjacentElement('afterend', wrap);
      wrap.querySelector('button').addEventListener('click', () => {
        visibleCount += PAGE_SIZE;
        render();
      });
    }
  }

  async function loadProducts() {
    try {
      const response = await fetch(DATA_URL, { cache: 'default' });
      if (!response.ok) throw new Error('Missing best_seller_deals.json');
      const data = await response.json();
      products = Array.isArray(data.deals) ? data.deals : [];
      const sort = getEl('sortFilter');
      if (sort) sort.value = 'newest';
      if (getEl('dealCount')) getEl('dealCount').textContent = data.count ?? products.length;
      if (getEl('hotCount')) getEl('hotCount').textContent = data.totalProducts ?? products.length;
      if (getEl('hero-pill')) getEl('hero-pill').textContent = `${data.count ?? products.length} best seller product picks`;
      renderFilters();
      render(true);
    } catch (error) {
      const status = getEl('status');
      if (status) status.textContent = 'Best seller product picks are not loaded yet. Try refreshing in a few minutes.';
      console.error('Best Seller Product Picks load failed:', error);
    }
  }

  function init() {
    if (window.bldSimplifyPageCopy) window.bldSimplifyPageCopy();
    if (window.bldNormalizeProductPageLayout) window.bldNormalizeProductPageLayout();
    ['searchBox', 'categoryFilter', 'sortFilter'].forEach(id => {
      const element = getEl(id);
      if (element) element.addEventListener(element.tagName === 'INPUT' ? 'input' : 'change', () => render(true));
    });
    loadProducts();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
