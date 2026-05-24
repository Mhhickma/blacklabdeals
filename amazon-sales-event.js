(() => {
  const EVENT_FEED_URL = '/amazon-sales-event-deals.json';
  const AFFILIATE_TAG = 'blacklabdealsprime-20';

  const CATEGORIES = [
    ['amazon-electronics-deals', 'Amazon Electronics Deals', 'Tech accessories, audio gear, charging items, gadgets, and electronics finds.', '🎧'],
    ['amazon-furniture-deals', 'Amazon Furniture Deals', 'Storage, shelves, desks, chairs, and home furniture finds.', '🪑'],
    ['amazon-health-personal-care-deals', 'Amazon Health & Personal Care Deals', 'Personal care, grooming, health, and everyday wellness deals.', '💚'],
    ['amazon-home-deals', 'Amazon Home Deals', 'General home finds, storage, decor, cleaning, and everyday essentials.', '🏠'],
    ['amazon-home-improvement-deals', 'Amazon Home Improvement Deals', 'DIY, repair, hardware, and project supplies.', '🔨'],
    ['amazon-home-entertainment-deals', 'Amazon Home Entertainment Deals', 'Streaming, audio, TV accessories, and entertainment items.', '📺'],
    ['amazon-lawn-garden-deals', 'Amazon Lawn and Garden Deals', 'Lawn, garden, patio, and backyard items.', '🍃'],
    ['amazon-office-products-deals', 'Amazon Office Products Deals', 'Desk gear, office supplies, printers, and work-from-home items.', '💼'],
    ['amazon-outdoors-deals', 'Amazon Outdoors Deals', 'Outdoor gear, camping, sports, and adventure finds.', '⛺'],
    ['amazon-pc-deals', 'Amazon PC Deals', 'Computer accessories, PC gear, monitors, and peripherals.', '🖥️'],
    ['amazon-kitchen-deals', 'Amazon Kitchen Deals', 'Kitchen tools, cookware, storage, and small appliances.', '🍲'],
    ['amazon-pet-products-deals', 'Amazon Pet Products Deals', 'Dog, cat, grooming, beds, toys, and pet essentials.', '🐾'],
    ['amazon-sports-deals', 'Amazon Sports Deals', 'Fitness, training, sports gear, and outdoor activity deals.', '🏃'],
    ['amazon-tool-deals', 'Amazon Tool Deals', 'Power tools, hand tools, shop accessories, DIY tools, and project gear.', '🔧'],
    ['amazon-toys-deals', 'Amazon Toys Deals', 'Toys, games, gifts, and family finds.', '🧸'],
    ['amazon-video-devices-deals', 'Amazon Video Devices Deals', 'Streaming devices, video gear, and TV accessories.', '▶️'],
    ['amazon-wireless-deals', 'Amazon Wireless Deals', 'Wireless tech, phones, earbuds, chargers, and accessories.', '📱'],
    ['amazon-device-deals', 'Amazon Device Deals', 'Echo, Fire TV, Kindle, Ring, Blink, eero, and Alexa-enabled finds.', '🔊'],
    ['amazon-deals-under-50', 'Amazon Deals Under $50', 'Budget-friendly deals across popular Amazon categories.', '🏷️'],
    ['amazon-household-essentials-deals', 'Amazon Household Essentials Deals', 'Cleaning supplies, laundry, paper goods, storage bags, and household basics.', '🧺']
  ];

  const EVENT_PAGE_SLUGS = new Set(['amazon-deal-event', ...CATEGORIES.map(([slug]) => slug)]);
  const DEALS_PER_PAGE = 50;
  let allDeals = [];
  let visibleDealsCount = DEALS_PER_PAGE;

  const $ = id => document.getElementById(id);
  const esc = value => String(value || '').replace(/[&<>"']/g, char => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;'
  }[char]));

  function currentSlug() {
    return String(window.location.pathname || '').split('/').filter(Boolean)[0] || 'amazon-deal-event';
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

  function category(deal) {
    return deal.cat || deal.category || 'Amazon Deals';
  }

  function addAffiliateTag(url) {
    const raw = String(url || '').trim();
    if (!raw) return raw;
    try {
      const parsed = new URL(raw, window.location.origin);
      if (parsed.hostname.includes('amazon.')) parsed.searchParams.set('tag', AFFILIATE_TAG);
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

  function hasCoupon(deal) {
    return Boolean(deal.hasCoupon || deal.has_coupon || deal.couponDisplay || deal.coupon);
  }

  function isHot(deal) {
    return Boolean(deal.hot || pct(deal) >= 30);
  }

  function score(deal) {
    return (isHot(deal) ? 1000 : 0) + (hasCoupon(deal) ? 180 : 0) + pct(deal) * 12 + Math.max(0, 80 - price(deal)) + updated(deal) / 1000000000;
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

  function matchesSearch(deal, query) {
    if (!query) return true;
    const haystack = [title(deal), deal.brand, category(deal), deal.asin, deal.couponDisplay, Array.isArray(deal.pages) ? deal.pages.join(' ') : ''].join(' ').toLowerCase();
    return query.split(/\s+/).filter(Boolean).every(term => haystack.includes(term));
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

  function defaultFilterForPage() {
    const slug = currentSlug();
    return slug === 'amazon-deal-event' ? 'all' : slug;
  }

  function selectedFilter() {
    return $('event-category-filter')?.value || defaultFilterForPage();
  }

  function filteredDeals() {
    const categoryFilter = selectedFilter();
    const sortMode = $('event-sort-select')?.value || 'featured';
    const searchQuery = String($('event-search-input')?.value || '').trim().toLowerCase();
    let deals = uniqueDeals(allDeals.slice());

    if (categoryFilter !== 'all') deals = deals.filter(deal => Array.isArray(deal.pages) && deal.pages.includes(categoryFilter));
    deals = deals.filter(deal => matchesSearch(deal, searchQuery));

    deals.sort((a, b) => {
      if (sortMode === 'discount-desc') return pct(b) - pct(a) || score(b) - score(a);
      if (sortMode === 'price-asc') return price(a) - price(b) || score(b) - score(a);
      if (sortMode === 'price-desc') return price(b) - price(a) || score(b) - score(a);
      if (sortMode === 'newest') return updated(b) - updated(a) || score(b) - score(a);
      return score(b) - score(a);
    });

    return deals;
  }

  function ensureSharedNavigation() {
    const slug = currentSlug();
    const stats = document.querySelector('.stats-bar');
    let grid = document.querySelector('.event-category-grid');
    if (!grid && stats) {
      grid = document.createElement('section');
      grid.className = 'event-category-grid';
      grid.setAttribute('aria-label', 'Amazon Sales Event categories');
      stats.insertAdjacentElement('beforebegin', grid);
    }
    if (grid) {
      grid.innerHTML = CATEGORIES.map(([catSlug, label, desc, icon]) => `<a class="event-category-card ${catSlug === slug ? 'is-active' : ''}" href="/${catSlug}/" data-event-category-card="${esc(catSlug)}"><span class="event-category-icon" aria-hidden="true">${esc(icon)}</span><span class="event-category-copy"><strong>${esc(label)}</strong><em>${esc(desc)}</em></span><span class="event-category-arrow" aria-hidden="true">›</span></a>`).join('');
    }
  }

  function ensureControls() {
    const sectionHead = document.querySelector('.section-head');
    const hotStrip = document.querySelector('.hot-strip');
    let controls = $('event-controls');
    if (!controls) {
      controls = document.createElement('section');
      controls.className = 'event-controls';
      controls.id = 'event-controls';
      controls.setAttribute('aria-label', 'Filter, search, and sort Amazon Sales Event deals');
      if (hotStrip) hotStrip.insertAdjacentElement('beforebegin', controls);
      else if (sectionHead) sectionHead.insertAdjacentElement('afterend', controls);
    }

    const options = CATEGORIES.map(([catSlug, label]) => `<option value="${esc(catSlug)}">${esc(label.replace(/^Amazon\s+/i, ''))}</option>`).join('');
    controls.innerHTML = `<div class="event-control"><label for="event-category-filter">Filter by category</label><select id="event-category-filter"><option value="all">All deal categories</option>${options}</select></div><div class="event-control event-search-control"><label for="event-search-input">Search deals</label><input id="event-search-input" type="search" placeholder="Search by product, brand, ASIN, or category" autocomplete="off" inputmode="search"></div><div class="event-control"><label for="event-sort-select">Sort deals</label><select id="event-sort-select"><option value="featured">Featured / best deals</option><option value="discount-desc">Biggest % drop</option><option value="price-asc">Price: low to high</option><option value="price-desc">Price: high to low</option><option value="newest">Newest updated</option></select></div>`;

    const filter = $('event-category-filter');
    if (filter) filter.value = defaultFilterForPage();
    filter?.addEventListener('change', () => { visibleDealsCount = DEALS_PER_PAGE; render(); });
    $('event-sort-select')?.addEventListener('change', () => { visibleDealsCount = DEALS_PER_PAGE; render(); });
    $('event-search-input')?.addEventListener('input', () => { visibleDealsCount = DEALS_PER_PAGE; render(); });
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
      button.addEventListener('click', () => { visibleDealsCount += DEALS_PER_PAGE; render(); });
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
    const grid = $('hot-grid') || document.querySelector('.hot-grid,.deals-grid');
    const status = $('status-line');
    if (!grid) return;
    const deals = filteredDeals();
    const searchQuery = String($('event-search-input')?.value || '').trim();
    updateStats(deals);
    if (status) status.textContent = searchQuery ? `Showing deals matching “${searchQuery}”.` : 'Showing preloaded Amazon Sales Event ASINs with live Amazon pricing.';

    if (!deals.length) {
      grid.innerHTML = '<div class="empty-state">No Amazon Sales Event deals match this search or filter yet.</div>';
      const existingWrap = $('amazon-sales-event-load-more-wrap');
      if (existingWrap) existingWrap.hidden = true;
      return;
    }

    const visibleDeals = deals.slice().sort((a, b) => Number(!hasImage(a)) - Number(!hasImage(b)) || score(b) - score(a)).slice(0, visibleDealsCount);
    grid.dataset.bldDynamicPager = 'true';
    grid.dataset.bldUniversalPager = 'off';
    grid.innerHTML = visibleDeals.map((deal, index) => {
      const dealTitle = title(deal);
      const amount = price(deal);
      const discount = pct(deal);
      const destination = link(deal);
      const primaryBadge = discount ? `${Math.round(discount)}% off` : isHot(deal) ? 'Hot Deal' : hasCoupon(deal) ? 'Coupon' : 'Deal';
      return `<a class="best-seller-card deal-card-unified bld-clickable-card" href="${esc(destination)}" target="_blank" rel="nofollow sponsored noopener" data-asin="${esc(deal.asin || '')}" data-deal-title="${esc(dealTitle)}" data-deal-category="${esc(category(deal))}" data-deal-price="${esc(amount)}" data-deal-discount="${esc(discount)}" aria-label="View ${esc(dealTitle)} on Amazon"><span class="deal-ribbon">${esc(ribbonText(deal))}</span><div class="best-seller-img">${cardImage(deal, dealTitle)}</div><div class="best-seller-body"><div class="best-seller-badges"><span class="best-seller-badge">${esc(primaryBadge)}</span><span class="best-seller-badge rank">#${index + 1}</span></div><div class="best-seller-title">${esc(dealTitle)}</div><div class="best-seller-category">${esc(category(deal))}</div><div class="best-seller-price-row"><span class="best-seller-price">${amount ? money(amount) : esc(deal.price || 'See deal')}</span>${deal.was ? `<span class="best-seller-was">${esc(deal.was)}</span>` : ''}</div><div class="deal-savings-line">${esc(savingsText(deal))}</div>${deal.couponDisplay ? `<div class="best-seller-category">${esc(deal.couponDisplay)}</div>` : ''}<span class="best-seller-btn">View on Amazon</span></div></a>`;
    }).join('');

    const { wrap, button } = ensureLoadMoreButton(grid);
    const remaining = Math.max(0, deals.length - visibleDealsCount);
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
    ensureSharedNavigation();
    ensureControls();
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

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
