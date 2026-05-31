(() => {
  const EVENT_FEED_URL = '/amazon-sales-event-deals.json';
  const AFFILIATE_TAG = 'blacklabdealsprime-20';
  const PRICE_DISCLAIMER = 'Product prices and availability are accurate as of the date/time indicated and are subject to change. Any price and availability information displayed on Amazon at the time of purchase will apply to the purchase of this product.';

  const CATEGORIES = [
    ['amazon-electronics-deals', 'Amazon Electronics Product Picks', 'Tech accessories, audio gear, charging items, gadgets, and electronics product picks.', '🎧'],
    ['amazon-furniture-deals', 'Amazon Furniture Product Picks', 'Storage, shelves, desks, chairs, and home furniture product picks.', '🪑'],
    ['amazon-health-personal-care-deals', 'Amazon Health & Personal Care Product Picks', 'Personal care, grooming, health, and everyday wellness product picks.', '💚'],
    ['amazon-home-deals', 'Amazon Home Product Picks', 'General home, storage, decor, cleaning, and everyday product picks.', '🏠'],
    ['amazon-home-improvement-deals', 'Amazon Home Improvement Product Picks', 'DIY, repair, hardware, and project supply product picks.', '🔨'],
    ['amazon-home-entertainment-deals', 'Amazon Home Entertainment Product Picks', 'Streaming, audio, TV accessories, and entertainment product picks.', '📺'],
    ['amazon-lawn-garden-deals', 'Amazon Lawn and Garden Product Picks', 'Lawn, garden, patio, and backyard product picks.', '🍃'],
    ['amazon-office-products-deals', 'Amazon Office Product Picks', 'Desk gear, office supplies, printers, and work-from-home product picks.', '💼'],
    ['amazon-outdoors-deals', 'Amazon Outdoors Product Picks', 'Outdoor gear, camping, sports, and activity product picks.', '⛺'],
    ['amazon-pc-deals', 'Amazon PC Product Picks', 'Computer accessories, PC gear, monitors, and peripheral product picks.', '🖥️'],
    ['amazon-kitchen-deals', 'Amazon Kitchen Product Picks', 'Kitchen tools, cookware, storage, and small appliance product picks.', '🍲'],
    ['amazon-pet-products-deals', 'Amazon Pet Product Picks', 'Dog, cat, grooming, beds, toys, and pet supply product picks.', '🐾'],
    ['amazon-sports-deals', 'Amazon Sports Product Picks', 'Fitness, training, sports gear, and outdoor activity product picks.', '🏃'],
    ['amazon-tool-deals', 'Amazon Tool Product Picks', 'Power tools, hand tools, shop accessories, DIY tools, and project gear.', '🔧'],
    ['amazon-toys-deals', 'Amazon Toys Product Picks', 'Toys, games, gifts, and family product picks.', '🧸'],
    ['amazon-video-devices-deals', 'Amazon Video Device Product Picks', 'Streaming devices, video gear, and TV accessory product picks.', '▶️'],
    ['amazon-wireless-deals', 'Amazon Wireless Product Picks', 'Wireless tech, phones, earbuds, chargers, and accessory product picks.', '📱'],
    ['amazon-device-deals', 'Amazon Device Product Picks', 'Echo, Fire TV, Kindle, Ring, Blink, eero, and Alexa-enabled product picks.', '🔊'],
    ['amazon-deals-under-50', 'Amazon Product Picks Under $50', 'Product picks under $50 across popular Amazon categories when last checked.', '🏷️'],
    ['amazon-household-essentials-deals', 'Amazon Household Essentials Product Picks', 'Cleaning supplies, laundry, paper goods, storage bags, and household basics.', '🧺']
  ];

  const EVENT_PAGE_SLUGS = new Set(['amazon-deal-event', ...CATEGORIES.map(([slug]) => slug)]);
  const DEALS_PER_PAGE = 50;
  const APPROVED_PRODUCT_KEYS = ['asin', 'title', 'brand', 'cat', 'image', 'price', 'price_amount', 'currency', 'availability', 'link', 'desc', 'seen_at', 'updated_at', 'pages'];
  let allProducts = [];
  let visibleProductCount = DEALS_PER_PAGE;

  const $ = id => document.getElementById(id);
  const esc = value => String(value || '').replace(/[&<>"']/g, char => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;'
  }[char]));

  function currentSlug() {
    return String(window.location.pathname || '').split('/').filter(Boolean)[0] || 'amazon-deal-event';
  }

  function money(value, currency = 'USD') {
    const amount = Number(value || 0);
    const code = /^[A-Z]{3}$/.test(String(currency || 'USD')) ? String(currency || 'USD') : 'USD';
    return amount ? new Intl.NumberFormat('en-US', { style: 'currency', currency: code }).format(amount) : '';
  }

  function parsePrice(value) {
    if (typeof value === 'number') return value;
    return Number(String(value || '').replace(/[^0-9.]/g, '')) || 0;
  }

  function productPrice(product) {
    return parsePrice(product.price_amount || product.price);
  }

  function productUpdated(product) {
    return Date.parse(product.updated_at || product.seen_at || 0) || 0;
  }

  function stamp(timestamp) {
    try {
      return new Intl.DateTimeFormat('en-US', {
        month: 'short',
        day: 'numeric',
        year: 'numeric',
        hour: 'numeric',
        minute: '2-digit',
        timeZoneName: 'short'
      }).format(new Date(timestamp || Date.now()));
    } catch (error) {
      return new Date(timestamp || Date.now()).toLocaleString();
    }
  }

  function average(values) {
    const nums = values.map(Number).filter(Boolean);
    return nums.length ? nums.reduce((sum, value) => sum + value, 0) / nums.length : 0;
  }

  function optimizeProductImage(src) {
    return String(src || '').replace(/\._SL\d+_\./, '._SL160_.').replace(/\._AC_SL\d+_\./, '._AC_SL160_.');
  }

  function asinImageUrl(asin) {
    const value = String(asin || '').trim().toUpperCase();
    return /^[A-Z0-9]{10}$/.test(value) ? `https://images-na.ssl-images-amazon.com/images/P/${value}.01._SL160_.jpg` : '';
  }

  function productImage(product) {
    return optimizeProductImage(product.image || asinImageUrl(product.asin));
  }

  function hasImage(product) {
    return Boolean(productImage(product));
  }

  function cardImage(product, title) {
    const src = productImage(product);
    return src ? `<img src="${esc(src)}" alt="${esc(title)}" width="160" height="160" loading="lazy" decoding="async">` : '<div class="img-fallback">Product image unavailable</div>';
  }

  function productTitle(product) {
    return product.title || 'Amazon product pick';
  }

  function productCategory(product) {
    return product.cat || 'Amazon Product Picks';
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

  function productLink(product) {
    const supplied = product.link;
    const fallback = product.asin ? `https://www.amazon.com/dp/${encodeURIComponent(product.asin)}?tag=${AFFILIATE_TAG}` : '#';
    return addAffiliateTag(supplied || fallback);
  }

  function productScore(product) {
    return Math.max(0, 80 - productPrice(product)) + productUpdated(product) / 1000000000;
  }

  function safeProduct(rawProduct) {
    const safe = {};
    for (const key of APPROVED_PRODUCT_KEYS) {
      if (Object.prototype.hasOwnProperty.call(rawProduct || {}, key)) safe[key] = rawProduct[key];
    }
    if (!safe.asin && rawProduct?.ASIN) safe.asin = rawProduct.ASIN;
    if (!safe.title && rawProduct?.name) safe.title = rawProduct.name;
    if (!safe.price_amount) safe.price_amount = parsePrice(safe.price);
    if (!safe.price && safe.price_amount) safe.price = money(safe.price_amount, safe.currency || 'USD');
    if (!safe.currency) safe.currency = 'USD';
    if (!Array.isArray(safe.pages)) safe.pages = [];
    return safe;
  }

  function matchesSearch(product, query) {
    if (!query) return true;
    const haystack = [productTitle(product), product.brand, productCategory(product), product.asin, Array.isArray(product.pages) ? product.pages.join(' ') : ''].join(' ').toLowerCase();
    return query.split(/\s+/).filter(Boolean).every(term => haystack.includes(term));
  }

  function uniqueProducts(products) {
    const seen = new Set();
    return products.filter(product => {
      const key = String(product.asin || product.link || productTitle(product)).toUpperCase();
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

  function matchesSlug(product, slug) {
    if (slug === 'all') return true;
    if (Array.isArray(product.pages) && product.pages.includes(slug)) return true;
    if (slug === 'amazon-deals-under-50') return productPrice(product) > 0 && productPrice(product) <= 50;
    const categoryText = productCategory(product).toLowerCase();
    return slug.replace(/^amazon-/, '').replace(/-deals$/, '').replace(/-/g, ' ').split(' ').every(word => categoryText.includes(word));
  }

  function filteredProducts() {
    const categoryFilter = selectedFilter();
    const sortMode = $('event-sort-select')?.value || 'featured';
    const searchQuery = String($('event-search-input')?.value || '').trim().toLowerCase();
    let products = uniqueProducts(allProducts.slice());

    products = products.filter(product => matchesSlug(product, categoryFilter));
    products = products.filter(product => matchesSearch(product, searchQuery));

    products.sort((a, b) => {
      if (sortMode === 'price-asc') return productPrice(a) - productPrice(b) || productScore(b) - productScore(a);
      if (sortMode === 'price-desc') return productPrice(b) - productPrice(a) || productScore(b) - productScore(a);
      if (sortMode === 'newest') return productUpdated(b) - productUpdated(a) || productScore(b) - productScore(a);
      return productScore(b) - productScore(a);
    });

    return products;
  }

  function ensureSharedNavigation() {
    const slug = currentSlug();
    const stats = document.querySelector('.stats-bar');
    let grid = document.querySelector('.event-category-grid');
    if (!grid && stats) {
      grid = document.createElement('section');
      grid.className = 'event-category-grid';
      grid.setAttribute('aria-label', 'Amazon product pick categories');
      stats.insertAdjacentElement('beforebegin', grid);
    }
    if (grid) {
      grid.innerHTML = CATEGORIES.map(([catSlug, label, desc, icon]) => `<a class="event-category-card ${catSlug === slug ? 'is-active' : ''}" href="/${catSlug}/" data-event-category-card="${esc(catSlug)}"><span class="event-category-icon" aria-hidden="true">${esc(icon)}</span><span class="event-category-copy"><strong>${esc(label)}</strong><em>${esc(desc)}</em></span><span class="event-category-arrow" aria-hidden="true">›</span></a>`).join('');
    }
  }

  function ensureControls() {
    const sectionHead = document.querySelector('.section-head');
    const productGrid = document.querySelector('.hot-strip');
    let controls = $('event-controls');
    if (!controls) {
      controls = document.createElement('section');
      controls.className = 'event-controls';
      controls.id = 'event-controls';
      controls.setAttribute('aria-label', 'Filter, search, and sort Amazon product picks');
      if (productGrid) productGrid.insertAdjacentElement('beforebegin', controls);
      else if (sectionHead) sectionHead.insertAdjacentElement('afterend', controls);
    }

    const options = CATEGORIES.map(([catSlug, label]) => `<option value="${esc(catSlug)}">${esc(label.replace(/^Amazon\s+/i, ''))}</option>`).join('');
    controls.innerHTML = `<div class="event-control"><label for="event-category-filter">Filter by category</label><select id="event-category-filter"><option value="all">All product categories</option>${options}</select></div><div class="event-control event-search-control"><label for="event-search-input">Search product picks</label><input id="event-search-input" type="search" placeholder="Search by product, brand, ASIN, or category" autocomplete="off" inputmode="search"></div><div class="event-control"><label for="event-sort-select">Sort product picks</label><select id="event-sort-select"><option value="featured">Featured product picks</option><option value="price-asc">Price: low to high</option><option value="price-desc">Price: high to low</option><option value="newest">Newest updated</option></select></div>`;

    const filter = $('event-category-filter');
    if (filter) filter.value = defaultFilterForPage();
    filter?.addEventListener('change', () => { visibleProductCount = DEALS_PER_PAGE; render(); });
    $('event-sort-select')?.addEventListener('change', () => { visibleProductCount = DEALS_PER_PAGE; render(); });
    $('event-search-input')?.addEventListener('input', () => { visibleProductCount = DEALS_PER_PAGE; render(); });
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
      button.addEventListener('click', () => { visibleProductCount += DEALS_PER_PAGE; render(); });
    }
    return { wrap, button };
  }

  function updateStats(products) {
    const total = products.length;
    const avgPrice = average(products.map(productPrice));
    const newest = Math.max(...products.map(productUpdated), 0);
    if ($('hero-pill')) $('hero-pill').textContent = total ? `${total} Amazon product picks loaded` : 'Amazon product picks loading';
    if ($('stat-active')) $('stat-active').textContent = total;
    if ($('stat-hot')) $('stat-hot').textContent = CATEGORIES.length;
    if ($('stat-price')) $('stat-price').textContent = avgPrice ? money(avgPrice) : '-';
    if ($('stat-discount')) $('stat-discount').textContent = 'Product Picks';
    if ($('stat-updated')) $('stat-updated').textContent = newest ? stamp(newest) : '-';
  }

  function render() {
    const grid = $('hot-grid') || document.querySelector('.hot-grid,.products-grid,.deals-grid');
    const status = $('status-line');
    if (!grid) return;
    const products = filteredProducts();
    const searchQuery = String($('event-search-input')?.value || '').trim();
    updateStats(products);
    if (status) status.textContent = searchQuery ? `Showing product picks matching “${searchQuery}”.` : 'Showing Amazon product picks with current Amazon product information.';

    if (!products.length) {
      grid.innerHTML = '<div class="empty-state">No Amazon product picks match this search or filter yet.</div>';
      const existingWrap = $('amazon-sales-event-load-more-wrap');
      if (existingWrap) existingWrap.hidden = true;
      return;
    }

    const visibleProducts = products.slice().sort((a, b) => Number(!hasImage(a)) - Number(!hasImage(b)) || productScore(b) - productScore(a)).slice(0, visibleProductCount);
    grid.dataset.bldDynamicPager = 'true';
    grid.dataset.bldUniversalPager = 'off';
    grid.innerHTML = visibleProducts.map((product, index) => {
      const title = productTitle(product);
      const amount = productPrice(product);
      const destination = productLink(product);
      const shownAt = productUpdated(product) || Date.now();
      const shownPrice = amount ? money(amount, product.currency || 'USD') : esc(product.price || 'Check price on Amazon');
      return `<a class="best-seller-card deal-card-unified bld-clickable-card" href="${esc(destination)}" target="_blank" rel="nofollow sponsored noopener" data-asin="${esc(product.asin || '')}" data-product-title="${esc(title)}" data-product-category="${esc(productCategory(product))}" data-product-price="${esc(amount)}" aria-label="View ${esc(title)} on Amazon"><span class="deal-ribbon">Product Pick</span><div class="best-seller-img">${cardImage(product, title)}</div><div class="best-seller-body"><div class="best-seller-badges"><span class="best-seller-badge">Product Pick</span><span class="best-seller-badge rank">#${index + 1}</span></div><div class="best-seller-title">${esc(title)}</div><div class="best-seller-category">${esc(productCategory(product))}</div><div class="best-seller-price-row"><span class="best-seller-price">${shownPrice}</span></div><div class="bld-price-timestamp">Product information shown as of ${esc(stamp(shownAt))}. Confirm final price and availability on Amazon.</div><div class="bld-card-price-disclaimer">${PRICE_DISCLAIMER}</div><span class="best-seller-btn">View on Amazon</span></div></a>`;
    }).join('');

    const { wrap, button } = ensureLoadMoreButton(grid);
    const remaining = Math.max(0, products.length - visibleProductCount);
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
    if (status) status.textContent = 'Loading Amazon product picks with current Amazon product information.';
    try {
      const response = await fetch(`${EVENT_FEED_URL}?v=${Date.now()}`, { cache: 'no-store' });
      if (!response.ok) throw new Error(`Could not load ${EVENT_FEED_URL}`);
      const data = await response.json();
      const rawProducts = Array.isArray(data) ? data : Array.isArray(data.deals) ? data.deals : [];
      allProducts = rawProducts.map(safeProduct).filter(product => product.asin || product.title || product.link);
      visibleProductCount = DEALS_PER_PAGE;
      render();
    } catch (error) {
      console.error(error);
      if (status) status.textContent = 'Amazon product pick feed is not available yet.';
      const grid = $('hot-grid') || document.querySelector('.hot-grid,.products-grid,.deals-grid');
      if (grid) grid.innerHTML = '<div class="empty-state">The Amazon product pick feed has not been generated yet.</div>';
    }
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
