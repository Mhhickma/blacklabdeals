/* Black Lab Deals home/product pagination.
   Shows 50 cards in every deal-card section, then adds Load 50 More.
   Header/navigation is not touched. */
(function () {
  const PAGE_SIZE = 50;
  const state = new WeakMap();
  let scheduled = false;

  const CONTAINER_SELECTOR = [
    '.hot-grid', '.deals-grid', '.product-grid', '.products-grid', '.best-seller-grid',
    '#hot-grid', '#deals-grid', '#dealsGrid', '#hot-deals-grid', '#hotDealsGrid',
    '#hot-deals-list', '#hotDealsList', '#deals-list', '#dealsList',
    '[id*="hot"][id*="grid"]', '[class*="hot"][class*="grid"]',
    '[id*="hot"][id*="list"]', '[class*="hot"][class*="list"]',
    '[id*="deal"][id*="grid"]', '[class*="deal"][class*="grid"]',
    '[id*="deal"][id*="list"]', '[class*="deal"][class*="list"]',
    '[id*="product"][id*="grid"]', '[class*="product"][class*="grid"]'
  ].join(',');

  const CARD_SELECTOR = [
    '.hot-card', '.deal-card', '.product-card', '.best-seller-card', '.amazon-card',
    'article[class*="card"]', 'a[href*="amazon.com"]', 'a[href*="amzn.to"]', 'a[href*="joylink.io"]'
  ].join(',');

  function isExcluded(el) {
    return !!el.closest('header, nav, footer, .bld-header-shell, .bld-mobile-drawer, .bld-mega-menu, .browse-pages-section, .browse-pages-grid, .panel, .link-list, .seo-info-section, .seo-content');
  }

  function isCard(el) {
    if (!el || !el.matches || !el.matches(CARD_SELECTOR)) return false;
    if (isExcluded(el)) return false;
    if (el.matches('.share-btn, .copy-btn, [data-share], [data-copy]')) return false;
    return true;
  }

  function cardsFor(container) {
    const direct = Array.from(container.children).filter(isCard);
    if (direct.length) return direct;
    return Array.from(container.querySelectorAll(CARD_SELECTOR)).filter(card => {
      if (!isCard(card)) return false;
      const parentGrid = card.parentElement && card.parentElement.closest(CONTAINER_SELECTOR);
      return !parentGrid || parentGrid === container;
    });
  }

  function findContainers() {
    const containers = new Set();
    document.querySelectorAll(CONTAINER_SELECTOR).forEach(container => {
      if (cardsFor(container).length > PAGE_SIZE) containers.add(container);
    });
    document.querySelectorAll(CARD_SELECTOR).forEach(card => {
      if (!isCard(card)) return;
      const container = card.closest(CONTAINER_SELECTOR) || card.parentElement;
      if (container && cardsFor(container).length > PAGE_SIZE) containers.add(container);
    });
    return Array.from(containers);
  }

  function ensureButton(container) {
    let wrap = container.nextElementSibling;
    if (!wrap || !wrap.classList || !wrap.classList.contains('bld-home-load-more-wrap')) {
      wrap = document.createElement('div');
      wrap.className = 'bld-home-load-more-wrap load-more-wrap';
      wrap.innerHTML = '<button class="bld-home-load-more-btn load-more-btn" type="button">Load 50 More Deals</button>';
      container.insertAdjacentElement('afterend', wrap);
      wrap.querySelector('button').addEventListener('click', function () {
        const cards = cardsFor(container);
        const before = state.get(container) || PAGE_SIZE;
        state.set(container, Math.min(before + PAGE_SIZE, cards.length));
        applyContainer(container);
      });
    }
    return wrap;
  }

  function applyContainer(container) {
    if (!container || container.dataset.bldPagerOff === 'true') return;
    if (container.dataset.bldDynamicPager === 'true') {
      const next = container.nextElementSibling;
      if (next && next.classList && next.classList.contains('bld-home-load-more-wrap')) next.remove();
      return;
    }
    const cards = cardsFor(container);
    if (cards.length <= PAGE_SIZE) return;

    const current = Math.min(state.get(container) || PAGE_SIZE, cards.length);
    state.set(container, current);

    cards.forEach((card, index) => {
      card.style.display = index < current ? '' : 'none';
    });

    const wrap = ensureButton(container);
    const button = wrap.querySelector('button');
    const remaining = cards.length - current;

    if (remaining > 0) {
      wrap.hidden = false;
      wrap.classList.remove('hidden');
      button.hidden = false;
      button.disabled = false;
      button.textContent = `Load ${Math.min(PAGE_SIZE, remaining)} More Deals (${remaining} remaining)`;
    } else {
      wrap.hidden = true;
      wrap.classList.add('hidden');
    }
  }

  function applyAll() {
    scheduled = false;
    findContainers().forEach(applyContainer);
  }

  function scheduleApply() {
    if (scheduled) return;
    scheduled = true;
    requestAnimationFrame(applyAll);
  }

  function start() {
    applyAll();
    [250, 750, 1500, 3000, 5000, 8000].forEach(ms => setTimeout(applyAll, ms));
    const observer = new MutationObserver(scheduleApply);
    observer.observe(document.body, { childList: true, subtree: true });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start);
  else start();
})();
