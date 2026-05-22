const MODE = document.body.dataset.mode || 'all';
const PAGE_CATEGORY = (document.body.dataset.category || '').toLowerCase().trim();
const PAGE_CATEGORY_LABEL = document.body.dataset.categoryLabel || '';
const DEALS_PER_PAGE = 50;
const DEALS_LIMIT = 100;
const DEAL_FEED_URL = '/deals.json';

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

function title(d) { return d.title || d.name || d.product_title || d.productTitle || 'Amazon Deal'; }
function optimizeDealImage(src) {
  return String(src || '').replace(/\._SL\d+_\./, '._SL160_.');
}
function asinImageUrl(asin) {
  const value = String(asin || '').trim().toUpperCase();
  return /^[A-Z0-9]{10}$/.test(value) ? `https://images-na.ssl-images-amazon.com/images/P/${value}.01._SL160_.jpg` : '';
}
function img(d) { return optimizeDealImage(d.image || d.image_url || d.imageUrl || d.img || d.thumbnail || asinImageUrl(d.asin)); }
function hasDealImage(d) { return Boolean(img(d)); }
function imageFirst(a) { return [...a].sort((x, y) => Number(!hasDealImage(x)) - Number(!hasDealImage(y))); }
function link(d) { return d.amazon_url || d.url || d.link || d.affiliate_url || d.affiliateUrl || d.product_url || '#'; }
function price(d) { return Number(d.price_amount ?? d.current_price ?? d.currentPrice ?? d.price ?? d.sale_price ?? 0) || 0; }
function pct(d) { return Number(d.pct ?? d.drop_percent ?? d.discount_percent ?? d.discountPercent ?? d.percent_off ?? d.percentOff ?? 0) || 0; }
function was(d) { return d.was || d.old_price || d.previous_price || d.previousPrice || (d.avg_30_price ? `$${d.avg_30_price}` : null); }
function cat(d) { return String(d.cat || d.category || d.product_category || 'Amazon Deals'); }
function hot(d) { return Boolean(d.hot || d.is_hot || d.isHot || pct(d) >= 30); }
function coupon(d) { return Boolean(d.hasCoupon || d.has_coupon || d.couponDisplay || d.coupon); }
function updated(d) { return Date.parse(d.updated_at || d.updatedAt || d.posted_at || d.first_seen_at || d.checked_at || d.seen_at || d.seenAt || 0) || 0; }
function money(v) { return v ? new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(v) : ''; }
function ago(ts) {
  if (!ts) return '-';
  const m = Math.floor(Math.max(0, Date.now() - ts) / 60000);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  return h < 24 ? `${h}h ago` : `${Math.floor(h / 24)}d ago`;
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
  if (currentFilter === 'hot') return hot(d);
  if (currentFilter === 'coupon') return coupon(d);
  if (currentFilter === 'under50') return p > 0 && p <= 50;
  if (currentFilter === 'home') return c.includes('home') || c.includes('kitchen');
  if (currentFilter === 'electronics') return matchCategory(d, 'electronics');
  if (currentFilter === 'tools') return matchCategory(d, 'tools');
  return true;
}

function score(d) {
  return (hot(d) ? 1000 : 0) + (coupon(d) ? 180 : 0) + pct(d) * 12 + (was(d) ? 60 : 0) + updated(d) / 1000000000 + Math.max(0, 80 - price(d));
}
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
  return `Showing ${Math.min(visibleDealsCount, total)} of ${total} deals`;
}

function displayableDeals(deals) {
  const imageDeals = deals.filter(hasDealImage);
  return imageDeals.length ? imageDeals : deals;
}

function stats(d) {
  const label = PAGE_CATEGORY_LABEL || 'deals';
  if ($('hero-pill')) $('hero-pill').textContent = PAGE_CATEGORY ? `${d.length} ${label} deals live right now` : `${d.length} deals live right now`;
  if ($('hero-pill-text')) $('hero-pill-text').textContent = `${d.length} deals loaded`;
  if ($('stat-active')) $('stat-active').textContent = d.length;
  if ($('stat-total')) $('stat-total').textContent = d.length;
  if ($('stat-hot')) $('stat-hot').textContent = d.filter(hot).length;
  const ap = avg(d.map(price).filter(Boolean));
  if ($('stat-price')) $('stat-price').textContent = ap ? money(ap) : '-';
  if ($('stat-avg-price')) $('stat-avg-price').textContent = ap ? money(ap) : '-';
  const ad = avg(d.map(pct).filter(Boolean));
  if ($('stat-discount')) $('stat-discount').textContent = ad ? `${Math.round(ad)}% off` : '-';
  if ($('stat-avg-discount')) $('stat-avg-discount').textContent = ad ? `${Math.round(ad)}% off` : '-';
  const n = Math.max(...d.map(updated), 0);
  if ($('stat-updated')) $('stat-updated').textContent = n ? ago(n) : '-';
}

function cardImage(d, t) {
  const i = img(d);
  return i ? `<img src="${esc(i)}" alt="${esc(t)}" width="160" height="160" loading="lazy" decoding="async">` : `<div class="img-fallback">Deal image unavailable</div>`;
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
  wrap.innerHTML = '<button id="load-more-btn" class="load-more-btn" type="button">Load 50 More Deals</button>';
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
    button.textContent = `Load ${Math.min(DEALS_PER_PAGE, remaining)} More Deals (${remaining} remaining)`;
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
  const displayDeals = displayableDeals(f);
  stats(f);
  const count = ensureDealCount();
  if (count) count.textContent = dealCountText(displayDeals.length);
  if (s) s.textContent = PAGE_CATEGORY ? `Showing live ${PAGE_CATEGORY_LABEL || 'category'} deals from the Black Lab Deals feed.` : 'Showing live Black Lab Deals with the same sitewide header and footer.';
  if (!g) return;
  g.dataset.bldDynamicPager = 'true';
  g.dataset.bldUniversalPager = 'off';
  g.dataset.bldPagerOff = 'true';
  removeCompetingLoadMoreButtons(g);
  if (!displayDeals.length) {
    g.innerHTML = '<div class="empty-state">No matching deals found right now.</div>';
    more(0);
    return;
  }
  const list = imageFirst(displayDeals).slice(0, visibleDealsCount);
  g.innerHTML = list.map((d, i) => {
    const t = title(d), p = money(price(d)), w = was(d), off = pct(d), badge = hot(d) ? 'Hot Deal' : coupon(d) ? 'Coupon' : 'Deal';
    const primaryBadge = off ? `${off}% off` : badge;
    const secondaryBadge = MODE === 'top100' ? `#${i + 1}` : badge;
    return `<article class="best-seller-card deal-card-unified" data-asin="${esc(d.asin || '')}" data-deal-title="${esc(t)}" data-deal-category="${esc(cat(d))}" data-deal-price="${esc(price(d))}" data-deal-discount="${esc(off)}"><div class="best-seller-img">${cardImage(d, t)}</div><div class="best-seller-body"><div class="best-seller-badges"><span class="best-seller-badge">${esc(primaryBadge)}</span><span class="best-seller-badge rank">${esc(secondaryBadge)}</span></div><div class="best-seller-title">${esc(t)}</div><div class="best-seller-category">${esc(cat(d))}</div><div class="best-seller-price-row"><span class="best-seller-price">${p || 'See deal'}</span>${w ? `<span class="best-seller-was">${esc(w)}</span>` : ''}</div><a class="best-seller-btn" href="${esc(link(d))}" target="_blank" rel="nofollow sponsored noopener">View on Amazon</a></div></article>`;
  }).join('');
  more(displayDeals.length);
}

const POPULAR_CATEGORY_LINKS = [
  { href: '/top-100-amazon-deals-today/', title: 'Top 100 Deals Found on Amazon Today', desc: 'Ranked deals and current price drops.' },
  { href: '/best-amazon-tool-deals/', title: 'Best Amazon Tool Deals', desc: 'Power tools, hand tools, and workshop finds.' },
  { href: '/best-amazon-home-kitchen-deals/', title: 'Best Amazon Home & Kitchen Deals', desc: 'Kitchen, storage, bedding, and home essentials.' },
  { href: '/best-amazon-deals-under-50/', title: 'Best Amazon Deals Under $50', desc: 'Budget-friendly deals across popular categories.' },
  { href: '/best-amazon-electronics-deals/', title: 'Best Amazon Electronics Deals', desc: 'Tech accessories, audio, smart home, and gadgets.' },
  { href: '/best-amazon-health-household-deals/', title: 'Best Amazon Health & Household Deals', desc: 'Cleaning, personal care, and household basics.' },
  { href: '/best-amazon-patio-lawn-garden-deals/', title: 'Best Amazon Patio, Lawn & Garden Deals', desc: 'Outdoor tools, yard care, patio, and garden finds.' },
  { href: '/best-amazon-pet-supplies-deals/', title: 'Best Amazon Pet Supplies Deals', desc: 'Pet essentials, grooming, toys, beds, and cleanup.' },
  { href: '/best-amazon-sports-outdoors-deals/', title: 'Best Amazon Sports & Outdoors Deals', desc: 'Fitness, camping, outdoor, and recreation deals.' },
  { href: '/best-amazon-automotive-deals/', title: 'Best Amazon Automotive Deals', desc: 'Car care, garage, tools, and vehicle accessories.' },
  { href: '/best-amazon-toys-games-deals/', title: 'Best Amazon Toys & Games Deals', desc: 'Toys, games, puzzles, gifts, and learning finds.' },
  { href: '/best-amazon-office-products-deals/', title: 'Best Amazon Office Products Deals', desc: 'Desk supplies, printer items, school, and workspace gear.' },
  { href: '/best-amazon-baby-products-deals/', title: 'Best Amazon Baby Product Deals', desc: 'Nursery, feeding, bath, travel, and family supplies.' },
  { href: '/best-amazon-musical-instruments-deals/', title: 'Best Amazon Musical Instrument Deals', desc: 'Music accessories, stands, strings, and audio gear.' }
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
  return `<!-- BLD POPULAR CATEGORY NAV START --><section class="popular-category-nav" aria-labelledby="popular-category-nav-title"><div class="popular-category-nav-head"><h2 id="popular-category-nav-title">Popular Amazon Deal Categories</h2><p>Jump to current deal pages by category, price range, and trending finds.</p></div><div class="popular-category-grid">${links}</div></section><!-- BLD POPULAR CATEGORY NAV END -->`;
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
  const intro = main.querySelector('.category-intro-section');
  if (dealSection && dealSection.parentNode) {
    let afterDeals = dealSection;
    if (intro && intro !== dealSection.nextElementSibling) {
      dealSection.insertAdjacentElement('afterend', intro);
      afterDeals = intro;
    } else if (intro) {
      afterDeals = intro;
    }
    afterDeals.insertAdjacentElement('afterend', nav);
    return;
  }

  const fallback = main.querySelector('.section-head') || main.querySelector('.filter-row');
  if (fallback && fallback.parentNode) {
    fallback.parentNode.insertBefore(nav, fallback);
  } else {
    main.appendChild(nav);
  }
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
  ensureBrowseSection();
  const s = $('status-line');
  try {
    const source = await fetchDealsFeed();
    allDeals = source.filter(pageMatch);
    visibleDealsCount = DEALS_PER_PAGE;
    render();
  } catch (e) {
    console.error(e);
    if (s) s.textContent = 'Could not load live deals right now.';
    if ($('hero-pill')) $('hero-pill').textContent = 'Deals unavailable right now';
    if ($('hero-pill-text')) $('hero-pill-text').textContent = 'Deals unavailable right now';
    if (findDealsGrid()) findDealsGrid().innerHTML = '<div class="empty-state">This page is live, but the Black Lab deal feed could not be loaded right now.</div>';
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
      wrap.innerHTML = '<button class="bld-load-more-btn load-more-btn" type="button">Load 50 More Deals</button>';
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
      button.textContent = `Load ${Math.min(DEALS_PER_PAGE, remaining)} More Deals (${remaining} remaining)`;
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

const SHOULD_RENDER_SHARED_DEALS = Boolean(document.body.dataset.mode)
  && !['search', 'best-sellers'].includes(MODE)
  && document.body.dataset.bldHomepage !== 'true';

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
