const MODE = document.body.dataset.mode || 'all';
const PAGE_CATEGORY = (document.body.dataset.category || '').toLowerCase().trim();
const PAGE_CATEGORY_LABEL = document.body.dataset.categoryLabel || '';
const DEALS_PER_PAGE = 50;
const DEALS_LIMIT = 100;
const FEATURED_PRODUCT_COUNT = 8;
const DEAL_FEED_URL = '/deals.json';
const AMAZON_DISCLAIMER = 'Product prices and availability are accurate as of the date/time indicated and are subject to change. Any price and availability information displayed on Amazon at the time of purchase will apply to the purchase of this product.';

let allDeals = [];
let visibleDealsCount = DEALS_PER_PAGE;
let currentFilter = 'all';

const $ = id => document.getElementById(id);

function bldTrack(eventName, params = {}) {
  try {
    if (typeof window.gtag !== 'function') return;
    window.gtag('event', eventName, {
      page_location: window.location.href,
      page_path: window.location.pathname,
      page_title: document.title,
      ...params
    });
  } catch (e) {}
}

function title(d) { return d.title || d.name || d.product_title || d.productTitle || 'Amazon product'; }
function optimizeDealImage(src) {
  return String(src || '').replace(/\._SL\d+_\./, '._SL160_.');
}
function img(d) { return optimizeDealImage(d.image || d.image_url || d.imageUrl || d.img || d.thumbnail || ''); }
function link(d) { return d.amazon_url || d.url || d.link || d.affiliate_url || d.affiliateUrl || d.product_url || '#'; }
function price(d) { return Number(d.price_amount ?? d.current_price ?? d.currentPrice ?? d.price ?? d.sale_price ?? 0) || 0; }
function cat(d) { return String(d.cat || d.category || d.product_category || 'Amazon products'); }
function updated(d) { return Date.parse(d.updated_at || d.updatedAt || d.posted_at || d.first_seen_at || d.checked_at || d.seen_at || d.seenAt || 0) || 0; }
function money(v) { return v ? new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(v) : ''; }
function ago(ts) {
  if (!ts) return 'â€”';
  const m = Math.floor(Math.max(0, Date.now() - ts) / 60000);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  return h < 24 ? `${h}h ago` : `${Math.floor(h / 24)}d ago`;
}
function timestamp(d) {
  const value = updated(d);
  if (!value) return 'Product information shown from the current Amazon feed.';
  return `Product information shown as of ${new Date(value).toLocaleString([], { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' })}.`;
}
function avg(a) { return a.length ? a.reduce((s, v) => s + v, 0) / a.length : 0; }
function esc(s) {
  return String(s || '').replace(/[&<>"']/g, m => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' }[m]));
}
function norm(s) {
  return String(s || '').toLowerCase().replace(/&/g, 'and').replace(/[^a-z0-9]+/g, ' ').trim();
}

const CATEGORY_KEYWORDS = {
  electronics: ['electronics', 'cell phones', 'cell phone', 'computers', 'computer', 'camera', 'audio', 'headphones', 'tablet', 'tv', 'television'],
  automotive: ['automotive', 'car', 'truck', 'vehicle', 'garage'],
  patio: ['patio', 'lawn', 'garden', 'outdoor', 'yard'],
  sports: ['sports', 'outdoors', 'outdoor', 'camping', 'fitness', 'hunting', 'fishing'],
  pet: ['pet', 'pets', 'pet supplies', 'dog', 'cat'],
  toys: ['toys', 'games', 'toy', 'game'],
  office: ['office', 'office products', 'school supplies'],
  health: ['health', 'household', 'beauty', 'personal care', 'cleaning'],
  baby: ['baby', 'baby products'],
  music: ['musical instruments', 'music', 'instrument'],
  appliances: ['appliances', 'appliance'],
  handmade: ['handmade'],
  industrial: ['industrial', 'scientific'],
  arts: ['arts', 'crafts', 'sewing', 'craft'],
  tools: ['tools', 'home improvement', 'tool'],
  devices: ['amazon device', 'amazon devices', 'echo', 'kindle', 'fire tv', 'fire tablet', 'ring', 'blink', 'eero', 'alexa'],
  home: ['home', 'kitchen']
};

function matchCategory(d, key) {
  const c = norm(cat(d));
  const k = norm(key);
  const words = CATEGORY_KEYWORDS[k] || [k];
  return words.some(w => c.includes(norm(w))) || norm(title(d)).includes(k);
}

function pageMatch(d) {
  const c = cat(d).toLowerCase().trim();
  const p = price(d);
  if (PAGE_CATEGORY) return matchCategory(d, PAGE_CATEGORY);
  if (MODE === 'tools') return c === 'tools & home improvement' || c.includes('tool') || c.includes('home improvement') || norm(title(d)).includes('tool');
  if (MODE === 'home') return c === 'home & kitchen' || c.includes('home') || c.includes('kitchen');
  if (MODE === 'under50') return p > 0 && p <= 50;
  return p > 0 || title(d);
}

function topFilter(d) {
  const c = cat(d).toLowerCase();
  const p = price(d);
  if (currentFilter === 'under50') return p > 0 && p <= 50;
  if (currentFilter === 'home') return c.includes('home') || c.includes('kitchen');
  if (currentFilter === 'electronics') return matchCategory(d, 'electronics');
  if (currentFilter === 'tools') return matchCategory(d, 'tools');
  return true;
}

function score(d) { return (img(d) ? 1000 : 0) + updated(d) / 1000000000 + Math.max(0, 80 - price(d)); }
function sorted(a) { return [...a].sort((x, y) => score(y) - score(x)); }
function shown() {
  let d = allDeals;
  if (MODE === 'top100') d = d.filter(topFilter).slice(0, DEALS_LIMIT);
  return sorted(d);
}

function ensureDealCount() {
  let count = $('deal-count');
  if (count) return count;
  const head = document.querySelector('.section-head');
  if (!head) return null;
  [...head.childNodes].forEach(node => {
    if (node.nodeType === Node.TEXT_NODE && /\bof\s+\d+\s+deals\b/i.test(node.textContent || '')) node.remove();
  });
  count = document.createElement('div');
  count.className = 'deal-count';
  count.id = 'deal-count';
  head.appendChild(count);
  return count;
}

function dealCountText(total) {
  return `Showing ${Math.min(visibleDealsCount, total)} of ${total} product picks`;
}

function stats(d) {
  const label = PAGE_CATEGORY_LABEL || 'product';
  if ($('hero-pill')) $('hero-pill').textContent = PAGE_CATEGORY ? `${d.length} ${label} product picks` : `${d.length} product picks`;
  if ($('hero-pill-text')) $('hero-pill-text').textContent = `${d.length} product picks loaded`;
  if ($('stat-active')) $('stat-active').textContent = d.length;
  if ($('stat-total')) $('stat-total').textContent = d.length;
  if ($('stat-hot')) $('stat-hot').textContent = d.length;
  const ap = avg(d.map(price).filter(Boolean));
  if ($('stat-price')) $('stat-price').textContent = ap ? money(ap) : 'â€”';
  if ($('stat-avg-price')) $('stat-avg-price').textContent = ap ? money(ap) : 'â€”';
  if ($('stat-discount')) $('stat-discount').textContent = 'Current';
  if ($('stat-avg-discount')) $('stat-avg-discount').textContent = 'Current';
  const n = Math.max(...d.map(updated), 0);
  if ($('stat-updated')) $('stat-updated').textContent = n ? ago(n) : 'â€”';
}

function cardImage(d, t) {
  const i = img(d);
  return i ? `<img src="${esc(i)}" alt="${esc(t)}" loading="lazy" decoding="async" onerror="this.outerHTML='&lt;div class=\\'img-fallback\\'&gt;Product image unavailable&lt;/div&gt;'">` : `<div class="img-fallback">Product image unavailable</div>`;
}

function productCard(d, index = 0) {
  const t = title(d);
  const p = money(price(d));
  return `<article class="product-card">
    <a class="product-card-link" href="${esc(link(d))}" target="_blank" rel="nofollow sponsored noopener" data-asin="${esc(d.asin || '')}" data-deal-title="${esc(t)}" data-deal-category="${esc(cat(d))}" data-deal-price="${esc(price(d))}">
      <div class="product-card-image">${cardImage(d, t)}${MODE === 'top100' ? `<div class="rank-badge">#${index + 1}</div>` : ''}</div>
      <div class="product-card-body">
        <div class="product-card-meta">${esc(d.brand || '')}${d.brand && cat(d) ? '<span aria-hidden="true">·</span>' : ''}${esc(cat(d))}</div>
        <h3 class="product-card-title">${esc(t)}</h3>
        <div class="product-card-price"><span>Current Amazon price</span><strong>${p || 'See current price on Amazon'}</strong></div>
        <p class="product-card-time">${esc(timestamp(d))}</p>
        <span class="product-card-button">View on Amazon</span>
      </div>
    </a>
  </article>`;
}

function findDealsGrid() {
  return $('hot-grid') || $('deals-grid') || $('dealsGrid') || document.querySelector('.hot-grid,.deals-grid,[id*="hot"][id*="grid"],[class*="hot"][class*="grid"],[id*="deal"][id*="grid"],[class*="deal"][class*="grid"]');
}

function bindLoadMoreButton(button) {
  if (!button || button.dataset.bldLoadMoreBound) return;
  button.dataset.bldLoadMoreBound = 'true';
  button.addEventListener('click', () => {
    visibleDealsCount += DEALS_PER_PAGE;
    render();
  });
}

function ensureLoadMoreButton() {
  let wrap = $('load-more-wrap');
  let button = $('load-more-btn');
  if (wrap && button) {
    bindLoadMoreButton(button);
    return { wrap, button };
  }
  const grid = findDealsGrid();
  if (!grid) return { wrap: null, button: null };
  wrap = document.createElement('div');
  wrap.id = 'load-more-wrap';
  wrap.className = 'load-more-wrap hidden';
  wrap.innerHTML = '<button id="load-more-btn" class="load-more-btn" type="button">Keep Browsing Product Picks</button>';
  grid.insertAdjacentElement('afterend', wrap);
  button = $('load-more-btn');
  bindLoadMoreButton(button);
  return { wrap, button };
}

function more(c) {
  const { wrap, button } = ensureLoadMoreButton();
  if (!wrap || !button) return;
  const remaining = Math.max(0, c - visibleDealsCount);
  if (remaining > 0) {
    wrap.classList.remove('hidden');
    wrap.hidden = false;
    button.hidden = false;
    button.disabled = false;
    button.textContent = `Keep Browsing Product Picks (${remaining} remaining)`;
  } else {
    wrap.classList.add('hidden');
    wrap.hidden = true;
  }
}

function removeCompetingLoadMoreButtons(grid) {
  if (!grid) return;
  let next = grid.nextElementSibling;
  while (next && next.classList && (next.classList.contains('bld-load-more-wrap') || next.classList.contains('bld-home-load-more-wrap'))) {
    const current = next;
    next = next.nextElementSibling;
    current.remove();
  }
}

function render() {
  const g = findDealsGrid();
  const s = $('status-line');
  const f = shown();
  stats(f);
  const count = ensureDealCount();
  if (count) count.textContent = dealCountText(f.length);
  if (s) s.textContent = PAGE_CATEGORY ? `Showing current ${PAGE_CATEGORY_LABEL || 'category'} product picks.` : 'Showing current Amazon product picks.';
  if (!g) return;
  g.dataset.bldDynamicPager = 'true';
  g.dataset.bldUniversalPager = 'off';
  g.dataset.bldPagerOff = 'true';
  removeCompetingLoadMoreButtons(g);
  if (!f.length) {
    g.innerHTML = '<div class="empty-state">No matching product picks found right now.</div>';
    more(0);
    return;
  }
  const list = f.slice(0, visibleDealsCount);
  const featured = list.slice(0, FEATURED_PRODUCT_COUNT);
  const additional = list.slice(FEATURED_PRODUCT_COUNT);
  g.classList.add('product-grid', 'product-grid-featured');
  g.innerHTML = featured.map(productCard).join('');
  ensureProductPageOrder(g, additional, f.length);
  more(f.length);
}

const POPULAR_CATEGORY_LINKS = [
  { href: '/top-100-amazon-deals-today/', title: 'Top 100 Amazon Product Picks', desc: 'Current product picks across popular categories.' },
  { href: '/best-amazon-tool-deals/', title: 'Amazon Tool Product Picks', desc: 'Power tools, hand tools, and workshop products.' },
  { href: '/best-amazon-home-kitchen-deals/', title: 'Amazon Home & Kitchen Product Picks', desc: 'Kitchen, storage, bedding, and home essentials.' },
  { href: '/best-amazon-deals-under-50/', title: 'Amazon Product Picks Under $50', desc: 'Products under $50 across popular categories.' },
  { href: '/best-amazon-electronics-deals/', title: 'Amazon Electronics Product Picks', desc: 'Tech accessories, audio, smart home, and gadgets.' },
  { href: '/best-amazon-health-household-deals/', title: 'Amazon Health & Household Product Picks', desc: 'Cleaning, personal care, and household basics.' },
  { href: '/best-amazon-patio-lawn-garden-deals/', title: 'Amazon Patio, Lawn & Garden Product Picks', desc: 'Outdoor tools, yard care, patio, and garden finds.' },
  { href: '/best-amazon-pet-supplies-deals/', title: 'Amazon Pet Supplies Product Picks', desc: 'Pet essentials, grooming, toys, beds, and cleanup.' },
  { href: '/best-amazon-sports-outdoors-deals/', title: 'Amazon Sports & Outdoors Product Picks', desc: 'Fitness, camping, outdoor, and recreation products.' },
  { href: '/best-amazon-automotive-deals/', title: 'Amazon Automotive Product Picks', desc: 'Car care, garage, tools, and vehicle accessories.' },
  { href: '/best-amazon-toys-games-deals/', title: 'Amazon Toys & Games Product Picks', desc: 'Toys, games, puzzles, gifts, and learning finds.' },
  { href: '/best-amazon-office-products-deals/', title: 'Amazon Office Product Picks', desc: 'Desk supplies, printer items, school, and workspace gear.' },
  { href: '/best-amazon-baby-products-deals/', title: 'Amazon Baby Product Picks', desc: 'Nursery, feeding, bath, travel, and family supplies.' },
  { href: '/best-amazon-musical-instruments-deals/', title: 'Amazon Musical Instrument Product Picks', desc: 'Music accessories, stands, strings, and audio gear.' }
];

function cleanPath(path) {
  return (`/${String(path || '').split('?')[0].split('#')[0].replace(/^\/+|\/+$/g, '')}/`).replace('//', '/');
}

function renderPopularCategoryNav() {
  const currentPath = cleanPath(window.location.pathname || '/');
  const links = POPULAR_CATEGORY_LINKS.map(item => {
    const itemPath = cleanPath(item.href);
    const isCurrent = currentPath === itemPath;
    return `<a class="popular-category-link${isCurrent ? ' is-current' : ''}" href="${esc(item.href)}"${isCurrent ? ' aria-current="page"' : ''}><span>${esc(item.title)}${isCurrent ? '<em class="current-page-label">Current page</em>' : ''}</span><small>${esc(item.desc)}</small></a>`;
  }).join('');
  return `<!-- BLD POPULAR CATEGORY NAV START --><section class="popular-category-nav" aria-labelledby="popular-category-nav-title"><div class="popular-category-nav-head"><h2 id="popular-category-nav-title">Popular Amazon Product Categories</h2><p>Browse current product picks by category and price range.</p></div><div class="popular-category-grid">${links}</div></section><!-- BLD POPULAR CATEGORY NAV END -->`;
}

function markCurrentPopularCategoryNav(nav) {
  if (!nav) return;
  const currentPath = cleanPath(window.location.pathname || '/');
  nav.querySelectorAll('.popular-category-link').forEach(link => {
    const isCurrent = cleanPath(link.getAttribute('href')) === currentPath;
    link.classList.toggle('is-current', isCurrent);
    if (isCurrent) {
      link.setAttribute('aria-current', 'page');
      const title = link.querySelector('span');
      if (title && !title.querySelector('.current-page-label')) title.insertAdjacentHTML('beforeend', '<em class="current-page-label">Current page</em>');
    } else {
      link.removeAttribute('aria-current');
      link.querySelectorAll('.current-page-label').forEach(label => label.remove());
    }
  });
}

function ensureBrowseSection() {
  if (document.body.dataset.bldHiddenEvent === 'true') return;
  const main = document.querySelector('main.page-shell') || document.querySelector('main');
  if (!main) return;

  let nav = document.querySelector('.popular-category-nav');
  if (!nav) {
    const holder = document.createElement('div');
    holder.innerHTML = renderPopularCategoryNav();
    nav = holder.firstElementChild;
  }

  markCurrentPopularCategoryNav(nav);

  const dealSection = main.querySelector('.hot-strip');
  const intro = main.querySelector('.category-intro-section,.best-seller-seo-intro,.seo-content');
  if (dealSection && dealSection.parentNode) {
    dealSection.insertAdjacentElement('afterend', nav);
    if (intro) main.appendChild(intro);
    return;
  }

  const fallback = main.querySelector('.section-head') || main.querySelector('.filter-row');
  if (fallback && fallback.parentNode) {
    fallback.parentNode.insertBefore(nav, fallback);
  } else {
    main.appendChild(nav);
  }
}

function ensureProductPageOrder(featuredGrid, additional, total) {
  const main = featuredGrid.closest('main') || document.querySelector('main');
  if (!main) return;
  ensureBrowseSection();
  const nav = main.querySelector('.popular-category-nav');
  let additionalSection = main.querySelector('.bld-additional-products');
  if (!additionalSection) {
    additionalSection = document.createElement('section');
    additionalSection.className = 'bld-additional-products';
    additionalSection.innerHTML = '<div class="section-head"><h2>More Current Product Picks</h2></div><div class="product-grid bld-additional-product-grid"></div>';
  }
  const additionalGrid = additionalSection.querySelector('.bld-additional-product-grid');
  additionalGrid.innerHTML = additional.map((product, index) => productCard(product, index + FEATURED_PRODUCT_COUNT)).join('');
  if (nav) nav.insertAdjacentElement('afterend', additionalSection);
  else featuredGrid.parentElement.insertAdjacentElement('afterend', additionalSection);
  additionalSection.hidden = !additional.length;

  let disclaimer = main.querySelector('.bld-amazon-disclaimer');
  if (!disclaimer) {
    disclaimer = document.createElement('aside');
    disclaimer.className = 'bld-amazon-disclaimer';
    disclaimer.setAttribute('aria-label', 'Amazon price and availability disclaimer');
  }
  disclaimer.textContent = AMAZON_DISCLAIMER;

  const helpfulSections = Array.from(main.querySelectorAll(
    '.category-intro-section,.best-seller-seo-intro,.seo-content,.related-deal-pages,.event-related,.event-legal'
  ));
  helpfulSections.forEach(section => main.appendChild(section));
  main.appendChild(disclaimer);
}

function simplifyPageCopy() {
  document.querySelectorAll('.hero').forEach(hero => {
    Array.from(hero.querySelectorAll('p')).slice(2).forEach(paragraph => paragraph.remove());
  });

  let keptConfirmation = false;
  document.querySelectorAll('p,.disclaimer,.amazon-disclaimer,.price-disclaimer').forEach(node => {
    if (node.closest('.bld-amazon-disclaimer')) return;
    const text = node.textContent.replace(/\s+/g, ' ').trim().toLowerCase();
    if (text.includes('product prices and availability are accurate as of the date/time indicated')) {
      node.remove();
      return;
    }
    if (text.includes('confirm final price and availability')) {
      if (keptConfirmation) node.remove();
      keptConfirmation = true;
    }
  });
}

function normalizeProductPageLayout() {
  const main = document.querySelector('main.page-shell') || document.querySelector('main');
  const products = main?.querySelector('.hot-strip,.deals-section,.best-seller-section');
  if (!main || !products) return;
  const anchor = main.querySelector('.hero') || main.querySelector('.breadcrumbs,.breadcrumb');
  if (anchor && products !== anchor.nextElementSibling) anchor.insertAdjacentElement('afterend', products);
  const productHeading = main.querySelector('.section-head');
  if (productHeading && productHeading.parentElement !== products) products.insertAdjacentElement('afterbegin', productHeading);
  ensureBrowseSection();
}

async function fetchDealsFeed() {
  const r = await fetch(DEAL_FEED_URL, { cache: 'default' });
  if (!r.ok) throw new Error('Could not load Black Lab deals.json');
  const data = await r.json();
  const source = Array.isArray(data) ? data : Array.isArray(data.deals) ? data.deals : [];
  if (!source.length) throw new Error('Black Lab deals.json had no deals');
  return source;
}

async function loadDeals() {
  simplifyPageCopy();
  normalizeProductPageLayout();
  ensureBrowseSection();
  const s = $('status-line');
  try {
    const source = await fetchDealsFeed();
    allDeals = source.filter(pageMatch);
    visibleDealsCount = DEALS_PER_PAGE;
    render();
  } catch (e) {
    console.error(e);
    if (s) s.textContent = 'Could not load current product picks right now.';
    if ($('hero-pill')) $('hero-pill').textContent = 'Product picks unavailable right now';
    if ($('hero-pill-text')) $('hero-pill-text').textContent = 'Product picks unavailable right now';
    if (findDealsGrid()) findDealsGrid().innerHTML = '<div class="empty-state">Current product information could not be loaded right now.</div>';
  }
}

function initFilters() {
  document.querySelectorAll('.filter-btn').forEach(b => b.addEventListener('click', () => {
    document.querySelectorAll('.filter-btn').forEach(x => x.classList.remove('active'));
    b.classList.add('active');
    currentFilter = b.dataset.filter;
    visibleDealsCount = DEALS_PER_PAGE;
    bldTrack('category_click', { category_name: currentFilter || b.textContent.trim() || 'all', button_text: b.textContent.trim(), page_mode: MODE, page_category: PAGE_CATEGORY || 'all' });
    render();
  }));
}

function initUniversalDealPagination() {
  const state = new WeakMap();
  const gridSelector = '.hot-grid,.deals-grid,.product-grid,.products-grid,.best-seller-grid,#hot-grid,#deals-grid,#dealsGrid,#products-grid,#hot-deals-grid,#hotDealsGrid,#hot-deals-list,#hotDealsList,#deals-list,#dealsList,[id*="hot"][id*="grid"],[class*="hot"][class*="grid"],[id*="hot"][id*="list"],[class*="hot"][class*="list"],[id*="deal"][id*="grid"],[class*="deal"][class*="grid"],[id*="deal"][id*="list"],[class*="deal"][class*="list"],[id*="product"][id*="grid"],[class*="product"][class*="grid"]';
  const cardSelector = '.hot-card,.deal-card,.product-card,.best-seller-card,.amazon-card,a[href*="amazon.com"],a[href*="amzn.to"],a[href*="joylink.io"]';

  function isCard(el) {
    if (!el || !el.matches || !el.matches(cardSelector)) return false;
    if (el.closest('header,nav,footer,.bld-header-shell,.bld-mobile-drawer,.bld-mega-menu,.browse-pages-section,.browse-pages-grid,.panel,.link-list,.seo-info-section,.seo-content')) return false;
    if (el.matches('.share-btn,.copy-btn,[data-share],[data-copy]')) return false;
    return true;
  }

  function getCards(grid) {
    const direct = [...grid.children].filter(isCard);
    if (direct.length) return direct;
    return [...grid.querySelectorAll(cardSelector)].filter(card => {
      if (!isCard(card)) return false;
      const parentGrid = card.parentElement && card.parentElement.closest(gridSelector);
      return !parentGrid || parentGrid === grid;
    });
  }

  function applyGrid(grid) {
    if (!grid || grid.dataset.bldUniversalPager === 'off') return;
    if (grid.dataset.bldDynamicPager === 'true') {
      removeCompetingLoadMoreButtons(grid);
      return;
    }
    const cards = getCards(grid);
    if (cards.length <= DEALS_PER_PAGE) return;
    let current = state.get(grid) || DEALS_PER_PAGE;
    current = Math.min(current, cards.length);
    state.set(grid, current);
    cards.forEach((card, index) => { card.style.display = index < current ? '' : 'none'; });
    let wrap = grid.nextElementSibling && grid.nextElementSibling.classList && grid.nextElementSibling.classList.contains('bld-load-more-wrap') ? grid.nextElementSibling : null;
    if (!wrap) {
      wrap = document.createElement('div');
      wrap.className = 'bld-load-more-wrap load-more-wrap';
      wrap.innerHTML = '<button class="bld-load-more-btn load-more-btn" type="button">Keep Browsing Product Picks</button>';
      grid.insertAdjacentElement('afterend', wrap);
      wrap.querySelector('button').addEventListener('click', () => {
        const before = state.get(grid) || DEALS_PER_PAGE;
        state.set(grid, Math.min(before + DEALS_PER_PAGE, getCards(grid).length));
        applyGrid(grid);
      });
    }
    const button = wrap.querySelector('button');
    const remaining = cards.length - current;
    if (remaining > 0) {
      wrap.classList.remove('hidden');
      wrap.hidden = false;
      button.hidden = false;
      button.disabled = false;
      button.textContent = `Keep Browsing Product Picks (${remaining} remaining)`;
    } else {
      wrap.classList.add('hidden');
      wrap.hidden = true;
    }
  }

  function applyAll() { document.querySelectorAll(gridSelector).forEach(applyGrid); }
  applyAll();
  window.addEventListener('load', applyAll);
  [300, 1000, 2500, 5000, 8000].forEach(ms => setTimeout(applyAll, ms));
  const observer = new MutationObserver(() => applyAll());
  observer.observe(document.body, { childList: true, subtree: true });
}

function initDealClickTracking() {
  document.addEventListener('click', event => {
    const dealLink = event.target.closest('a.hot-card, a.deal-card, a.product-card, a.card, a[href*="amazon.com"], a[href*="joylink.io"]');
    if (!dealLink) return;
    const href = dealLink.href || '';
    if (!href.includes('amazon.com') && !href.includes('amzn.to') && !href.includes('joylink.io')) return;
    bldTrack('deal_click', { deal_title: dealLink.dataset.dealTitle || dealLink.textContent.trim().slice(0, 120), outbound_url: href, page_mode: MODE, page_category: PAGE_CATEGORY || 'all' });
  }, true);
}

function initSearchTracking() {
  let searchTimer;
  document.addEventListener('input', event => {
    const input = event.target;
    if (!input || !input.matches('input[type="search"], input[placeholder*="Search"], input[placeholder*="search"], #site-search, #search, #searchBox')) return;
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      const term = input.value.trim();
      if (term.length < 2 || input.dataset.lastTrackedSearch === term) return;
      input.dataset.lastTrackedSearch = term;
      bldTrack('site_search', { search_term: term.slice(0, 100), page_mode: MODE, page_category: PAGE_CATEGORY || 'all' });
    }, 900);
  });
}

function initScrollDepthTracking() {
  const marks = [25, 50, 75, 90];
  const tracked = new Set();
  function checkScroll() {
    const doc = document.documentElement;
    const height = Math.max(doc.scrollHeight, document.body.scrollHeight) - window.innerHeight;
    if (height <= 0) return;
    const percent = Math.round((window.scrollY / height) * 100);
    marks.forEach(mark => {
      if (percent >= mark && !tracked.has(mark)) {
        tracked.add(mark);
        bldTrack('scroll_depth', { percent_scrolled: mark, page_mode: MODE, page_category: PAGE_CATEGORY || 'all' });
      }
    });
  }
  window.addEventListener('scroll', checkScroll, { passive: true });
  window.addEventListener('load', checkScroll);
}

function initTimeOnPageTracking() {
  [30, 60, 120, 300].forEach(seconds => setTimeout(() => bldTrack('time_on_page', { seconds_on_page: seconds, page_mode: MODE, page_category: PAGE_CATEGORY || 'all' }), seconds * 1000));
}

function ensureMobileDealNav() {}

window.bldProductCard = productCard;
window.bldEnsureProductPageOrder = ensureProductPageOrder;
window.bldSimplifyPageCopy = simplifyPageCopy;
window.bldNormalizeProductPageLayout = normalizeProductPageLayout;

simplifyPageCopy();

const SHOULD_RENDER_SHARED_DEALS = Boolean(document.body.dataset.mode)
  && !['search', 'best-sellers'].includes(MODE);

if (SHOULD_RENDER_SHARED_DEALS) {
  initFilters();
  ensureMobileDealNav();
  loadDeals();
  initUniversalDealPagination();
}
initDealClickTracking();
initSearchTracking();
initScrollDepthTracking();
initTimeOnPageTracking();
