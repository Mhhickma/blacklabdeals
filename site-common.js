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
function img(d) { return d.image || d.image_url || d.imageUrl || d.img || d.thumbnail || ''; }
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
  return i ? `<img src="${esc(i)}" alt="${esc(t)}" loading="lazy" onerror="this.outerHTML='&lt;div class=\\'img-fallback\\'&gt;Deal image unavailable&lt;/div&gt;'">` : `<div class="img-fallback">Deal image unavailable</div>`;
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
  stats(f);
  const count = ensureDealCount();
  if (count) count.textContent = `Showing ${Math.min(visibleDealsCount, f.length)} of ${f.length} deals`;
  if (s) s.textContent = PAGE_CATEGORY ? `Showing live ${PAGE_CATEGORY_LABEL || 'category'} deals from the Black Lab Deals feed.` : 'Showing live Black Lab Deals with the same sitewide header and footer.';
  if (!g) return;
  g.dataset.bldDynamicPager = 'true';
  g.dataset.bldUniversalPager = 'off';
  g.dataset.bldPagerOff = 'true';
  removeCompetingLoadMoreButtons(g);
  if (!f.length) {
    g.innerHTML = '<div class="empty-state">No matching deals found right now.</div>';
    more(0);
    return;
  }
  const list = f.slice(0, visibleDealsCount);
  g.innerHTML = list.map((d, i) => {
    const t = title(d), p = money(price(d)), w = was(d), off = pct(d), badge = hot(d) ? 'Hot Deal' : coupon(d) ? 'Coupon' : 'Deal';
    return `<a class="hot-card" href="${esc(link(d))}" target="_blank" rel="nofollow sponsored noopener" data-asin="${esc(d.asin || '')}" data-deal-title="${esc(t)}" data-deal-category="${esc(cat(d))}" data-deal-price="${esc(price(d))}" data-deal-discount="${esc(off)}"><div class="hot-card-img">${cardImage(d, t)}${MODE === 'top100' ? `<div class="rank-badge">#${i + 1}</div>` : ''}<div class="hot-card-badge">${badge}</div></div><div class="hot-card-body">${(MODE === 'top100' || PAGE_CATEGORY) ? `<div class="category-pill">${esc(cat(d))}</div>` : ''}<div class="stars">${hot(d) ? '*****' : '****'} ${esc((d.brand || '').slice(0, 24))}</div><div class="hot-card-title">${esc(t)}</div><div class="hot-card-prices"><span class="hot-price-now">${p || 'See deal'}</span>${w ? `<span class="hot-price-was">${esc(w)}</span>` : ''}${off ? `<span class="hot-off">${off}% off</span>` : ''}</div><span class="hot-btn">See Deal on Amazon</span></div></a>`;
  }).join('');
  more(f.length);
}

async function fetchDealsFeed() {
  const cacheBust = DEAL_FEED_URL.includes('?') ? `&v=${Date.now()}` : `?v=${Date.now()}`;
  const r = await fetch(DEAL_FEED_URL + cacheBust, { cache: 'no-store' });
  if (!r.ok) throw new Error('Could not load Black Lab deals.json');
  const data = await r.json();
  const source = Array.isArray(data) ? data : Array.isArray(data.deals) ? data.deals : [];
  if (!source.length) throw new Error('Black Lab deals.json had no deals');
  return source;
}
async function loadDeals() {
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

initFilters();
ensureMobileDealNav();
loadDeals();
initDealClickTracking();
initSearchTracking();
initScrollDepthTracking();
initTimeOnPageTracking();
