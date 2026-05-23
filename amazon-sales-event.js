(() => {
  const EVENT_FEED_URL = '/amazon-sales-event-deals.json';
  const AFFILIATE_TAG = 'blacklabdealsprime-20';
  const EVENT_PAGE_SLUGS = new Set([
    'amazon-deal-event',
    'amazon-tool-deals',
    'amazon-electronics-deals',
    'amazon-home-kitchen-deals',
    'amazon-device-deals',
    'amazon-deals-under-50',
    'amazon-household-essentials-deals',
    'amazon-gaming-deals',
    'amazon-outdoor-garden-deals',
    'amazon-pet-deals'
  ]);
  const DEALS_PER_PAGE = 50;
  let allDeals = [];
  let visibleDealsCount = DEALS_PER_PAGE;

  const $ = id => document.getElementById(id);
  const esc = value => String(value || '').replace(/[&<>"']/g, char => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#039;'
  }[char]));

  function currentSlug() {
    return String(window.location.pathname || '')
      .split('/')
      .filter(Boolean)[0] || 'amazon-deal-event';
  }

  function money(value) {
    const amount = Number(value || 0);
    return amount ? new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(amount) : '';
  }

  function price(deal) {
    return Number(deal.price_amount ?? deal.current_price ?? deal.currentPrice ?? deal.price ?? 0) || 0;
  }

  function pct(deal) {
    return Number(deal.pct ?? deal.discount_percent ?? deal.discountPercent ?? deal.percent_off ?? 0) || 0;
  }

  function updated(deal) {
    return Date.parse(deal.updated_at || deal.updatedAt || deal.seen_at || 0) || 0;
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

  function optimizeDealImage(src) {
    return String(src || '').replace(/\._SL\d+_\./, '._SL160_.');
  }

  function asinImageUrl(asin) {
    const value = String(asin || '').trim().toUpperCase();
    return /^[A-Z0-9]{10}$/.test(value) ? `https://images-na.ssl-images-amazon.com/images/P/${value}.01._SL160_.jpg` : '';
  }

  function image(deal) {
    return optimizeDealImage(deal.image || deal.image_url || deal.imageUrl || deal.thumbnail || asinImageUrl(deal.asin));
  }

  function hasImage(deal) {
    return Boolean(image(deal));
  }

  function cardImage(deal, title) {
    const src = image(deal);
    return src ? `<img src="${esc(src)}" alt="${esc(title)}" width="160" height="160" loading="lazy" decoding="async">` : '<div class="img-fallback">Deal image unavailable</div>';
  }

  function title(deal) {
    return deal.title || deal.name || deal.product_title || 'Amazon Sales Event Deal';
  }

  function addAffiliateTag(url) {
    const raw = String(url || '').trim();
    if (!raw) return raw;
    try {
      const parsed = new URL(raw, window.location.origin);
      if (parsed.hostname.includes('amazon.')) {
        parsed.searchParams.set('tag', AFFILIATE_TAG);
      }
      return parsed.href;
    } catch (error) {
      if (raw.includes('amazon.com')) {
        const clean = raw.replace(/([?&])tag=[^&]*&?/, '$1').replace(/[?&]$/, '');
        return `${clean}${clean.includes('?') ? '&' : '?'}tag=${AFFILIATE_TAG}`;
      }
      return raw;
    }
  }

  function link(deal) {
    const supplied = deal.link || deal.url || deal.amazon_url || deal.affiliate_url;
    const fallback = deal.asin ? `https://www.amazon.com/dp/${encodeURIComponent(deal.asin)}?tag=${AFFILIATE_TAG}` : '#';
    return addAffiliateTag(supplied || fallback);
  }

  function category(deal) {
    return deal.cat || deal.category || 'Amazon Deals';
  }

  function isHot(deal) {
    return Boolean(deal.hot || pct(deal) >= 30);
  }

  function hasCoupon(deal) {
    return Boolean(deal.hasCoupon || deal.has_coupon || deal.couponDisplay || deal.coupon);
  }

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

  function score(deal) {
    return (isHot(deal) ? 1000 : 0) + (hasCoupon(deal) ? 180 : 0) + pct(deal) * 12 + Math.max(0, 80 - price(deal)) + updated(deal) / 1000000000;
  }

  function eventDealsForPage(deals, slug) {
    const filtered = deals.filter(deal => Array.isArray(deal.pages) && deal.pages.includes(slug));
    return filtered.sort((a, b) => score(b) - score(a));
  }

  function ensureLoadMoreButton(grid) {
    let wrap = $('amazon-sales-event-load-more-wrap');
    let button = $('amazon-sales-event-load-more-btn');
    if (!wrap) {
      wrap = document.createElement('div');
      wrap.id = 'amazon-sales-event-load-more-wrap';
      wrap.className = 'load-more-wrap hidden';
      wrap.innerHTML = '<button id="amazon-sales-event-load-more-btn" class="load-more-btn" type="button">Load 50 More Deals</button>';
      grid.insertAdjacentElement('afterend', wrap);
      button = $('amazon-sales-event-load-more-btn');
      button.addEventListener('click', () => {
        visibleDealsCount += DEALS_PER_PAGE;
        render();
      });
    }
    return { wrap, button };
  }

  function updateStats(deals) {
    const total = deals.length;
    const avgPrice = average(deals.map(price));
    const avgDiscount = average(deals.map(pct));
    const newest = Math.max(...deals.map(updated), 0);

    if ($('hero-pill')) $('hero-pill').textContent = `${total} Amazon Sales Event deals loaded`;
    if ($('deal-count')) $('deal-count').textContent = `Showing ${Math.min(visibleDealsCount, total)} of ${total} deals`;
    if ($('stat-active')) $('stat-active').textContent = total;
    if ($('stat-hot')) $('stat-hot').textContent = deals.filter(isHot).length;
    if ($('stat-price')) $('stat-price').textContent = avgPrice ? money(avgPrice) : '-';
    if ($('stat-discount')) $('stat-discount').textContent = avgDiscount ? `${Math.round(avgDiscount)}% off` : '-';
    if ($('stat-updated')) $('stat-updated').textContent = newest ? ago(newest) : '-';
  }

  function render() {
    const slug = currentSlug();
    const grid = $('hot-grid') || document.querySelector('.hot-grid,.deals-grid');
    const status = $('status-line');
    if (!grid) return;

    const pageDeals = eventDealsForPage(allDeals, slug);
    updateStats(pageDeals);

    if (status) status.textContent = `Showing preloaded Amazon Sales Event ASINs with live Amazon pricing.`;

    if (!pageDeals.length) {
      grid.innerHTML = '<div class="empty-state">No Amazon Sales Event deals are loaded for this page yet. Add ASINs to the Google Sheet and run the event feed script.</div>';
      const existingWrap = $('amazon-sales-event-load-more-wrap');
      if (existingWrap) existingWrap.hidden = true;
      return;
    }

    const visibleDeals = pageDeals
      .slice()
      .sort((a, b) => Number(!hasImage(a)) - Number(!hasImage(b)) || score(b) - score(a))
      .slice(0, visibleDealsCount);

    grid.dataset.bldDynamicPager = 'true';
    grid.dataset.bldUniversalPager = 'off';
    grid.innerHTML = visibleDeals.map((deal, index) => {
      const dealTitle = title(deal);
      const amount = price(deal);
      const discount = pct(deal);
      const destination = link(deal);
      const primaryBadge = discount ? `${Math.round(discount)}% off` : isHot(deal) ? 'Hot Deal' : hasCoupon(deal) ? 'Coupon' : 'Deal';
      const secondaryBadge = slug === 'amazon-deal-event' ? `#${index + 1}` : 'Amazon Sales Event';
      return `<a class="best-seller-card deal-card-unified bld-clickable-card" href="${esc(destination)}" target="_blank" rel="nofollow sponsored noopener" data-asin="${esc(deal.asin || '')}" data-deal-title="${esc(dealTitle)}" data-deal-category="${esc(category(deal))}" data-deal-price="${esc(amount)}" data-deal-discount="${esc(discount)}" aria-label="View ${esc(dealTitle)} on Amazon"><span class="deal-ribbon">${esc(ribbonText(deal))}</span><div class="best-seller-img">${cardImage(deal, dealTitle)}</div><div class="best-seller-body"><div class="best-seller-badges"><span class="best-seller-badge">${esc(primaryBadge)}</span><span class="best-seller-badge rank">${esc(secondaryBadge)}</span></div><div class="best-seller-title">${esc(dealTitle)}</div><div class="best-seller-category">${esc(category(deal))}</div><div class="best-seller-price-row"><span class="best-seller-price">${amount ? money(amount) : esc(deal.price || 'See deal')}</span>${deal.was ? `<span class="best-seller-was">${esc(deal.was)}</span>` : ''}</div><div class="deal-savings-line">${esc(savingsText(deal))}</div>${deal.couponDisplay ? `<div class="best-seller-category">${esc(deal.couponDisplay)}</div>` : ''}<span class="best-seller-btn">View on Amazon</span></div></a>`;
    }).join('');

    const { wrap, button } = ensureLoadMoreButton(grid);
    const remaining = Math.max(0, pageDeals.length - visibleDealsCount);
    if (remaining > 0) {
      wrap.hidden = false;
      wrap.classList.remove('hidden');
      button.disabled = false;
      button.textContent = `Load ${Math.min(DEALS_PER_PAGE, remaining)} More Deals (${remaining} remaining)`;
    } else {
      wrap.hidden = true;
      wrap.classList.add('hidden');
    }
  }

  async function init() {
    const slug = currentSlug();
    if (!EVENT_PAGE_SLUGS.has(slug)) return;

    const status = $('status-line');
    if (status) status.textContent = 'Loading Amazon Sales Event ASINs with live Amazon pricing.';

    try {
      const response = await fetch(`${EVENT_FEED_URL}?v=${Date.now()}`, { cache: 'no-store' });
      if (!response.ok) throw new Error(`Could not load ${EVENT_FEED_URL}`);
      const data = await response.json();
      allDeals = Array.isArray(data) ? data : Array.isArray(data.deals) ? data.deals : [];
      visibleDealsCount = DEALS_PER_PAGE;
      render();
    } catch (error) {
      console.error(error);
      if (status) status.textContent = 'Amazon Sales Event feed is not available yet.';
      const grid = $('hot-grid') || document.querySelector('.hot-grid,.deals-grid');
      if (grid) grid.innerHTML = '<div class="empty-state">The Amazon Sales Event feed has not been generated yet. Add ASINs to the Google Sheet, then run the event feed script.</div>';
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
