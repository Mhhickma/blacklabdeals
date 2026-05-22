(() => {
  if ((window.location.pathname || '').split('/').filter(Boolean)[0] !== 'amazon-deal-event') return;

  const FEED_URL = '/amazon-sales-event-deals.json';
  const DEALS_PER_PAGE = 50;
  let allDeals = [];
  let visibleCount = DEALS_PER_PAGE;

  const $ = id => document.getElementById(id);
  const esc = value => String(value || '').replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' }[char]));
  const money = value => Number(value || 0) ? new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(Number(value)) : '';
  const price = deal => Number(deal.price_amount || deal.current_price || deal.price || 0) || 0;
  const pct = deal => Number(deal.pct || deal.discount_percent || deal.percent_off || 0) || 0;
  const updated = deal => Date.parse(deal.updated_at || deal.updatedAt || deal.seen_at || 0) || 0;
  const title = deal => deal.title || deal.name || 'Amazon Sales Event Deal';
  const category = deal => deal.cat || deal.category || 'Amazon Deals';
  const image = deal => String(deal.image || deal.image_url || deal.thumbnail || '').replace(/\._SL\d+_\./, '._SL160_.');
  const hasCoupon = deal => Boolean(deal.hasCoupon || deal.has_coupon || deal.couponDisplay || deal.coupon);
  const isHot = deal => Boolean(deal.hot || pct(deal) >= 30);
  const score = deal => (isHot(deal) ? 1000 : 0) + (hasCoupon(deal) ? 180 : 0) + pct(deal) * 12 + Math.max(0, 80 - price(deal)) + updated(deal) / 1000000000;

  function ribbonText(deal) {
    const discount = pct(deal);
    if (discount > 0) return `${Math.round(discount)}% OFF`;
    if (hasCoupon(deal)) return 'COUPON';
    return 'DEAL';
  }

  function savingsText(deal) {
    const discount = pct(deal);
    if (discount > 0) return `Save ${Math.round(discount)}% today`;
    if (hasCoupon(deal)) return deal.couponDisplay || 'Coupon available';
    if (deal.was) return 'Limited-time deal price';
    return 'Check current deal';
  }

  function ago(timestamp) {
    if (!timestamp) return '-';
    const minutes = Math.floor(Math.max(0, Date.now() - timestamp) / 60000);
    if (minutes < 60) return `${minutes}m ago`;
    const hours = Math.floor(minutes / 60);
    return hours < 24 ? `${hours}h ago` : `${Math.floor(hours / 24)}d ago`;
  }

  function average(values) {
    const nums = values.map(Number).filter(Boolean);
    return nums.length ? nums.reduce((sum, value) => sum + value, 0) / nums.length : 0;
  }

  function uniqueDeals(deals) {
    const seen = new Set();
    return deals.filter(deal => {
      const key = String(deal.asin || deal.link || title(deal)).toUpperCase();
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }

  function filteredDeals() {
    const categoryFilter = $('event-category-filter')?.value || 'all';
    const sortMode = $('event-sort-select')?.value || 'featured';
    let deals = uniqueDeals(allDeals.slice());
    if (categoryFilter !== 'all') {
      deals = deals.filter(deal => Array.isArray(deal.pages) && deal.pages.includes(categoryFilter));
    }
    deals.sort((a, b) => {
      if (sortMode === 'discount-desc') return pct(b) - pct(a) || score(b) - score(a);
      if (sortMode === 'price-asc') return price(a) - price(b) || score(b) - score(a);
      if (sortMode === 'price-desc') return price(b) - price(a) || score(b) - score(a);
      if (sortMode === 'newest') return updated(b) - updated(a) || score(b) - score(a);
      return score(b) - score(a);
    });
    return deals;
  }

  function updateStats(deals) {
    const newest = Math.max(...deals.map(updated), 0);
    if ($('hero-pill')) $('hero-pill').textContent = `${deals.length} Amazon Sales Event deals loaded`;
    if ($('deal-count')) $('deal-count').textContent = `Showing ${Math.min(visibleCount, deals.length)} of ${deals.length} deals`;
    if ($('stat-active')) $('stat-active').textContent = deals.length;
    if ($('stat-hot')) $('stat-hot').textContent = deals.filter(isHot).length;
    if ($('stat-price')) $('stat-price').textContent = average(deals.map(price)) ? money(average(deals.map(price))) : '-';
    if ($('stat-discount')) $('stat-discount').textContent = average(deals.map(pct)) ? `${Math.round(average(deals.map(pct)))}% off` : '-';
    if ($('stat-updated')) $('stat-updated').textContent = ago(newest);
  }

  function render() {
    const grid = $('hot-grid');
    const status = $('status-line');
    if (!grid) return;
    const deals = filteredDeals();
    updateStats(deals);
    if (status) status.textContent = 'Showing all preloaded Amazon Sales Event ASINs. Use the controls to filter or sort.';
    if (!deals.length) {
      grid.innerHTML = '<div class="empty-state">No Amazon Sales Event deals match this filter yet.</div>';
      return;
    }
    grid.innerHTML = deals.slice(0, visibleCount).map((deal, index) => {
      const amount = price(deal);
      const discount = pct(deal);
      const dealTitle = title(deal);
      const src = image(deal);
      const link = deal.link || deal.url || '#';
      const primaryBadge = discount ? `${Math.round(discount)}% off` : hasCoupon(deal) ? 'Coupon' : 'Deal';
      return `<a class="best-seller-card deal-card-unified bld-clickable-card" href="${esc(link)}" target="_blank" rel="nofollow sponsored noopener" data-asin="${esc(deal.asin || '')}" data-deal-title="${esc(dealTitle)}" data-deal-category="${esc(category(deal))}" data-deal-price="${esc(amount)}" data-deal-discount="${esc(discount)}" aria-label="View ${esc(dealTitle)} on Amazon"><span class="deal-ribbon">${esc(ribbonText(deal))}</span><div class="best-seller-img">${src ? `<img src="${esc(src)}" alt="${esc(dealTitle)}" width="160" height="160" loading="lazy" decoding="async">` : '<div class="img-fallback">Deal image unavailable</div>'}</div><div class="best-seller-body"><div class="best-seller-badges"><span class="best-seller-badge">${esc(primaryBadge)}</span><span class="best-seller-badge rank">#${index + 1}</span></div><div class="best-seller-title">${esc(dealTitle)}</div><div class="best-seller-category">${esc(category(deal))}</div><div class="best-seller-price-row"><span class="best-seller-price">${amount ? money(amount) : esc(deal.price || 'See deal')}</span>${deal.was ? `<span class="best-seller-was">${esc(deal.was)}</span>` : ''}</div><div class="deal-savings-line">${esc(savingsText(deal))}</div><span class="best-seller-btn">View on Amazon</span></div></a>`;
    }).join('');
  }

  async function initHubFilters() {
    try {
      const response = await fetch(`${FEED_URL}?v=${Date.now()}`, { cache: 'no-store' });
      const data = await response.json();
      allDeals = Array.isArray(data) ? data : Array.isArray(data.deals) ? data.deals : [];
      visibleCount = DEALS_PER_PAGE;
      $('event-category-filter')?.addEventListener('change', () => { visibleCount = DEALS_PER_PAGE; render(); });
      $('event-sort-select')?.addEventListener('change', () => { visibleCount = DEALS_PER_PAGE; render(); });
      render();
    } catch (error) {
      console.error(error);
    }
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', initHubFilters);
  else initHubFilters();
})();
