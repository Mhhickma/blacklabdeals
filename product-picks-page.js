/* Shared conservative Product Picks page renderer. Public cards only use the approved feed fields. */
(function () {
  const PRICE_DISCLAIMER = 'Product prices and availability are accurate as of the date/time indicated and are subject to change. Any price and availability information displayed on Amazon at the time of purchase will apply to the purchase of this product.';
  const PAGE_SIZE = 50;
  const APPROVED_FIELDS = new Set([
    'asin',
    'title',
    'brand',
    'cat',
    'image',
    'price',
    'price_amount',
    'currency',
    'availability',
    'link',
    'desc',
    'seen_at',
    'updated_at'
  ]);

  const cfg = window.BLD_PAGE_CONFIG || {};
  let all = [];
  let visible = cfg.limit || PAGE_SIZE;
  let query = new URLSearchParams(window.location.search).get('q')?.trim() || '';
  let sort = 'best';
  let lastSearchTracked = '';

  const esc = value => String(value ?? '').replace(/[&<>"']/g, ch => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;'
  }[ch]));

  const attr = value => esc(value).replace(/`/g, '&#96;');
  const track = (name, params) => {
    try {
      if (typeof window.BLDTrack === 'function') window.BLDTrack(name, params || {});
      else if (typeof window.gtag === 'function') window.gtag('event', name, params || {});
    } catch (error) {}
  };

  const safeValue = (item, key) => {
    if (!APPROVED_FIELDS.has(key)) return '';
    const value = item && item[key];
    return value === undefined || value === null ? '' : String(value);
  };

  const safeNumber = value => {
    const n = Number(String(value ?? '').replace(/[^0-9.]/g, ''));
    return Number.isFinite(n) ? n : 0;
  };

  const money = value => value
    ? new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(value)
    : '';

  function stamp(value) {
    const parsed = Date.parse(value || '');
    const date = Number.isFinite(parsed) ? new Date(parsed) : new Date();
    return new Intl.DateTimeFormat('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
      timeZoneName: 'short'
    }).format(date);
  }

  function asinImage(asin) {
    const value = String(asin || '').trim().toUpperCase();
    return /^[A-Z0-9]{10}$/.test(value)
      ? 'https://images-na.ssl-images-amazon.com/images/P/' + value + '.01._SL160_.jpg'
      : '';
  }

  function normalize(rawItems) {
    return rawItems
      .filter(item => item && typeof item === 'object')
      .map(item => {
        const priceNum = safeNumber(safeValue(item, 'price_amount'));
        const asin = safeValue(item, 'asin');
        const title = safeValue(item, 'title') || 'Amazon product';
        const brand = safeValue(item, 'brand');
        const category = safeValue(item, 'cat') || 'Everything Else';
        const image = safeValue(item, 'image') || asinImage(asin);
        const link = safeValue(item, 'link') || '#';
        const price = safeValue(item, 'price') || money(priceNum) || 'Check current price on Amazon';
        const updatedValue = safeValue(item, 'updated_at') || safeValue(item, 'seen_at');

        return {
          asin,
          title,
          brand,
          category,
          image,
          link,
          price,
          priceNum,
          updated: Date.parse(updatedValue || '') || Date.now()
        };
      })
      .filter(product => product.title && product.link && product.link !== '#');
  }

  function matchesCategory(product) {
    if (!cfg.category || cfg.category === 'all') return true;
    return product.category.toLowerCase() === String(cfg.category).toLowerCase();
  }

  function normalizeSortSelect() {
    const select = document.getElementById('sort-select');
    if (!select) return;
    select.innerHTML = '<option value="best">Product Picks</option><option value="newest">Newest First</option><option value="featured">Featured Picks</option>';
    select.value = sort;
  }

  function filtered() {
    let list = all.filter(matchesCategory);

    if (cfg.maxPrice) {
      list = list.filter(product => product.priceNum && product.priceNum <= cfg.maxPrice);
    }

    if (query) {
      const q = query.toLowerCase();
      list = list.filter(product => (
        product.title + ' ' + product.brand + ' ' + product.category + ' ' + product.asin
      ).toLowerCase().includes(q));
    }

    if (sort === 'newest') {
      list.sort((a, b) => b.updated - a.updated);
    } else {
      list.sort((a, b) => a.category.localeCompare(b.category) || a.title.localeCompare(b.title));
    }

    return cfg.limit ? list.slice(0, cfg.limit) : list;
  }

  function card(product) {
    const image = product.image
      ? '<img src="' + esc(product.image) + '" alt="' + esc(product.title) + '" loading="lazy">'
      : '';

    return '<article class="bld-product-card" data-asin="' + attr(product.asin) + '" data-title="' + attr(product.title) + '" data-category="' + attr(product.category) + '">'
      + '<div class="bld-product-img">' + image + '</div>'
      + '<div class="bld-product-body">'
      + '<div class="bld-product-title">' + esc(product.title) + '</div>'
      + '<div class="bld-product-category">' + esc(product.category) + '</div>'
      + '<div class="bld-product-price">' + esc(product.price) + '</div>'
      + '<div class="bld-price-stamp">Product information shown as of ' + esc(stamp(product.updated)) + '. Confirm final price and availability on Amazon.</div>'
      + '<div class="bld-card-disclaimer">' + PRICE_DISCLAIMER + '</div>'
      + '<a class="bld-view-btn" href="' + esc(product.link) + '" target="_blank" rel="nofollow sponsored noopener" data-asin="' + attr(product.asin) + '" data-title="' + attr(product.title) + '" data-category="' + attr(product.category) + '">View on Amazon</a>'
      + '</div>'
      + '</article>';
  }

  function render() {
    const list = filtered();
    const shown = list.slice(0, visible);
    const countEl = document.getElementById('deal-count');
    const gridEl = document.getElementById('products-grid');
    const moreEl = document.getElementById('load-more');

    if (countEl) {
      countEl.textContent = list.length
        ? 'Showing ' + Math.min(visible, list.length) + ' of ' + list.length + ' Product Picks'
        : '0 Product Picks';
    }

    if (gridEl) {
      gridEl.innerHTML = shown.length
        ? shown.map(card).join('')
        : '<div class="bld-empty">No matching product picks found right now.</div>';
    }

    if (moreEl) {
      moreEl.hidden = !!cfg.limit || visible >= list.length;
      moreEl.textContent = 'Show 50 More Product Picks';
    }
  }

  function applySearch(value, shouldUpdateUrl) {
    query = String(value || '').trim();
    visible = cfg.limit || PAGE_SIZE;
    const input = document.getElementById('search-input');
    if (input) input.value = query;
    if (shouldUpdateUrl) {
      const url = new URL(window.location.href);
      if (query) url.searchParams.set('q', query);
      else url.searchParams.delete('q');
      url.hash = 'deals-section';
      history.replaceState(null, '', url);
    }
    render();
    document.getElementById('deals-section')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  window.BLDApplyProductSearch = applySearch;

  async function load() {
    try {
      const feedPath = cfg.feedPath || '/deals.json';
      const response = await fetch(feedPath + '?pp=' + Date.now(), { cache: 'no-store' });
      if (!response.ok) throw new Error('Feed unavailable');
      const data = await response.json();
      all = normalize(Array.isArray(data) ? data : (data.deals || []));
      track('product_feed_loaded', {
        feed_path: feedPath,
        feed_count: all.length,
        page_category: cfg.category || 'all',
        page_limit: cfg.limit || ''
      });
      render();
      if (query) applySearch(query, false);
    } catch (error) {
      track('product_feed_error', { page_category: cfg.category || 'all' });
      const gridEl = document.getElementById('products-grid');
      const countEl = document.getElementById('deal-count');
      if (gridEl) gridEl.innerHTML = '<div class="bld-empty">Product picks are temporarily unavailable. Please check back soon.</div>';
      if (countEl) countEl.textContent = 'Product Picks unavailable';
    }
  }

  document.addEventListener('DOMContentLoaded', () => {
    normalizeSortSelect();

    const input = document.getElementById('search-input');
    if (input && query) input.value = query;

    input?.addEventListener('input', event => {
      query = event.target.value.trim();
      visible = cfg.limit || PAGE_SIZE;
      render();
      if (query.length >= 3 && query !== lastSearchTracked) {
        lastSearchTracked = query;
        track('product_search', {
          search_term: query.slice(0, 120),
          search_length: query.length,
          result_count: filtered().length,
          page_category: cfg.category || 'all'
        });
      }
    });

    document.getElementById('sort-select')?.addEventListener('change', event => {
      sort = event.target.value;
      visible = cfg.limit || PAGE_SIZE;
      track('product_sort_change', { sort_value: sort, page_category: cfg.category || 'all' });
      render();
    });

    document.getElementById('load-more')?.addEventListener('click', () => {
      visible += PAGE_SIZE;
      track('product_load_more', { visible_count: visible, page_category: cfg.category || 'all' });
      render();
    });

    load();
  });
})();
