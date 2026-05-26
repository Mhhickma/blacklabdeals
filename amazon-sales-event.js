(() => {
  const EVENT_FEED_URL = '/amazon-sales-event-deals.json';
  const AFFILIATE_TAG = 'blacklabdealsprime-20';
  const PRICE_DISCLAIMER = 'Product prices and availability are accurate as of the date/time indicated and are subject to change. Any price and availability information displayed on Amazon at the time of purchase will apply to the purchase of this product.';

  const CATEGORIES = [
    ['amazon-electronics-deals', 'Amazon Electronics Product Picks', 'Tech accessories, audio gear, charging items, gadgets, and electronics finds.', '🎧'],
    ['amazon-furniture-deals', 'Amazon Furniture Product Picks', 'Storage, shelves, desks, chairs, and home furniture finds.', '🪑'],
    ['amazon-health-personal-care-deals', 'Amazon Health & Personal Care Product Picks', 'Personal care, grooming, health, and everyday wellness items.', '💚'],
    ['amazon-home-deals', 'Amazon Home Product Picks', 'General home finds, storage, decor, cleaning, and everyday essentials.', '🏠'],
    ['amazon-home-improvement-deals', 'Amazon Home Improvement Product Picks', 'DIY, repair, hardware, and project supplies.', '🔨'],
    ['amazon-home-entertainment-deals', 'Amazon Home Entertainment Product Picks', 'Streaming, audio, TV accessories, and entertainment items.', '📺'],
    ['amazon-lawn-garden-deals', 'Amazon Lawn and Garden Product Picks', 'Lawn, garden, patio, and backyard items.', '🍃'],
    ['amazon-office-products-deals', 'Amazon Office Product Picks', 'Desk gear, office supplies, printers, and work-from-home items.', '💼'],
    ['amazon-outdoors-deals', 'Amazon Outdoors Product Picks', 'Outdoor gear, camping, sports, and adventure finds.', '⛺'],
    ['amazon-pc-deals', 'Amazon PC Product Picks', 'Computer accessories, PC gear, monitors, and peripherals.', '🖥️'],
    ['amazon-kitchen-deals', 'Amazon Kitchen Product Picks', 'Kitchen tools, cookware, storage, and small appliances.', '🍲'],
    ['amazon-pet-products-deals', 'Amazon Pet Product Picks', 'Dog, cat, grooming, beds, toys, and pet essentials.', '🐾'],
    ['amazon-sports-deals', 'Amazon Sports Product Picks', 'Fitness, training, sports gear, and outdoor activity items.', '🏃'],
    ['amazon-tool-deals', 'Amazon Tool Product Picks', 'Power tools, hand tools, shop accessories, DIY tools, and project gear.', '🔧'],
    ['amazon-toys-deals', 'Amazon Toys Product Picks', 'Toys, games, gifts, and family finds.', '🧸'],
    ['amazon-video-devices-deals', 'Amazon Video Device Product Picks', 'Streaming devices, video gear, and TV accessories.', '▶️'],
    ['amazon-wireless-deals', 'Amazon Wireless Product Picks', 'Wireless tech, phones, earbuds, chargers, and accessories.', '📱'],
    ['amazon-device-deals', 'Amazon Device Product Picks', 'Echo, Fire TV, Kindle, Ring, Blink, eero, and Alexa-enabled finds.', '🔊'],
    ['amazon-deals-under-50', 'Amazon Product Picks Under $50', 'Budget-friendly product picks across popular Amazon categories.', '🏷️'],
    ['amazon-household-essentials-deals', 'Amazon Household Essentials Product Picks', 'Cleaning supplies, laundry, paper goods, storage bags, and household basics.', '🧺']
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

  function updated(deal) {
    return Date.parse(deal.price_fetched_at || deal.updated_at || deal.updatedAt || deal.seen_at || 0) || 0;
  }

  function stamp(timestamp) {
    try {
      return new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit', timeZoneName: 'short' }).format(new Date(timestamp || Date.now()));
    } catch (error) {
      return new Date(timestamp || Date.now()).toLocaleString();
    }
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
    return src ? `<img src="${esc(src)}" alt="${esc(title)}" width="160" height="160" loading="lazy" decoding="async">` : '<div class="img-fallback">Product image unavailable</div>';
  }

  function title(deal) {
    return deal.title || deal.name || deal.product_title || 'Amazon product pick';
  }

  function category(deal) {
    return deal.cat || deal.category || 'Amazon Product Picks';
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

  function score(deal) {
    return (hasCoupon(deal) ? 180 : 0) + Math.max(0, 80 - price(deal)) + updated(deal) / 1000000000;
  }

  function badgeText(deal) {
    if (hasCoupon(deal)) return 'Coupon may be available';
    return 'Product Pick';
  }

  function noteText(deal) {
    if (hasCoupon(deal)) return deal.couponDisplay || 'Coupon status must be confirmed on Amazon.';
    return 'Confirm final price and availability on Amazon.';
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
      controls.setAttribute('aria-label', 'Filter, search, and sort Amazon Sales Event product picks');
      if (hotStrip) hotStrip.insertAdjacentElement('beforebegin', controls);
      else if (sectionHead) sectionHead.insertAdjacentElement('afterend', controls);
    }

    const options = CATEGORIES.map(([catSlug, label]) => `<option value="${esc(catSlug)}">${esc(label.replace(/^Amazon\s+/i, ''))}</option>`).join('');
    controls.innerHTML = `<div class="event-control"><label for="event-category-filter">Filter by category</label><select id="event-category-filter"><option value="all">All product categories</option>${options}</select></div><div class="event-control event-search-control"><label for="event-search-input">Search product picks</label><input id="event-search-input" type="search" placeholder="Search by product, brand, ASIN, or category" autocomplete="off" inputmode="search"></div><div class="event-control"><label for="event-sort-select">Sort product picks</label><select id="event-sort-select"><option value="featured">Featured product picks</option><option value="price-asc">Price: low to high</option><option value="price-desc">Price: high to low</option><option value="newest">Newest updated</option></select></div>`;

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
      wrap.innerHTML = '<button id="amazon-sales-event-load-more-btn" class="load-more-btn" type="button">Load 50 More Product Picks</button>';
      grid.insertAdjacentElement('afterend', wrap);
      button = $('amazon-sales-event-load-more-btn');
      button.addEventListener('click', () => { visibleDealsCount += DEALS_PER_PAGE; render(); });
    }
    return { wrap, button };
  }

  function updateStats(deals) {
    const total = deals.length;
    const avgPrice = average(deals.map(price));
    const newest = Math.max(...deals.map(updated), 0);
    if ($('hero-pill')) $('hero-pill').textContent = total ? `${total} Amazon product picks loaded` : 'Amazon product picks loading';
    if ($('stat-active')) $('stat-active').textContent = total;
    if ($('stat-hot')) $('stat-hot').textContent = deals.filter(hasCoupon).length;
    if ($('stat-price')) $('stat-price').textContent = avgPrice ? money(avgPrice) : '-';
    if ($('stat-discount')) $('stat-discount').textContent = 'Product Picks';
    if ($('stat-updated')) $('stat-updated').textContent = newest ? stamp(newest) : '-';
  }

  function render() {
    const grid = $('hot-grid') || document.querySelector('.hot-grid,.deals-grid');
    const status = $('status-line');
    if (!grid) return;
    const deals = filteredDeals();
    const searchQuery = String($('event-search-input')?.value || '').trim();
    updateStats(deals);
    if (status) status.textContent = searchQuery ? `Showing product picks matching “${searchQuery}”.` : 'Showing preloaded Amazon Sales Event product picks with current Amazon product information.';

    if (!deals.length) {
      grid.innerHTML = '<div class="empty-state">No Amazon Sales Event product picks match this search or filter yet.</div>';
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
      const destination = link(deal);
      const shownAt = updated(deal) || Date.now();
      return `<a class="best-seller-card deal-card-unified bld-clickable-card" href="${esc(destination)}" target="_blank" rel="nofollow sponsored noopener" data-asin="${esc(deal.asin || '')}" data-deal-title="${esc(dealTitle)}" data-deal-category="${esc(category(deal))}" data-deal-price="${esc(amount)}" aria-label="View ${esc(dealTitle)} on Amazon"><span class="deal-ribbon">${esc(badgeText(deal))}</span><div class="best-seller-img">${cardImage(deal, dealTitle)}</div><div class="best-seller-body"><div class="best-seller-badges"><span class="best-seller-badge">${esc(badgeText(deal))}</span><span class="best-seller-badge rank">#${index + 1}</span></div><div class="best-seller-title">${esc(dealTitle)}</div><div class="best-seller-category">${esc(category(deal))}</div><div class="best-seller-price-row"><span class="best-seller-price">${amount ? money(amount) : esc(deal.price || 'Check price on Amazon')}</span></div><div class="bld-price-timestamp">Price shown as of ${esc(stamp(shownAt))}.</div><div class="deal-savings-line">${esc(noteText(deal))}</div><div class="bld-card-price-disclaimer">${PRICE_DISCLAIMER}</div><span class="best-seller-btn">View on Amazon</span></div></a>`;
    }).join('');

    const { wrap, button } = ensureLoadMoreButton(grid);
    const remaining = Math.max(0, deals.length - visibleDealsCount);
    if (remaining > 0) {
      wrap.hidden = false;
      wrap.classList.remove('hidden');
      button.disabled = false;
      button.textContent = `Load ${Math.min(DEALS_PER_PAGE, remaining)} More Product Picks (${remaining} remaining)`;
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
    if (status) status.textContent = 'Loading Amazon Sales Event product picks with current Amazon product information.';
    try {
      const response = await fetch(`${EVENT_FEED_URL}?v=${Date.now()}`, { cache: 'no-store' });
      if (!response.ok) throw new Error(`Could not load ${EVENT_FEED_URL}`);
      const data = await response.json();
      allDeals = Array.isArray(data) ? data : Array.isArray(data.deals) ? data.deals : [];
      visibleDealsCount = DEALS_PER_PAGE;
      render();
    } catch (error) {
      console.error(error);
      if (status) status.textContent = 'Amazon Sales Event product pick feed is not available yet.';
      const grid = $('hot-grid') || document.querySelector('.hot-grid,.deals-grid');
      if (grid) grid.innerHTML = '<div class="empty-state">The Amazon Sales Event product pick feed has not been generated yet.</div>';
    }
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();