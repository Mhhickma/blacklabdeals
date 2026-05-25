/* Black Lab Deals Amazon compliance layer
   Public product cards use current Amazon price only, remove deal-tracking display,
   add price timestamp/disclaimer, and hide stale prices after 23 hours. */
(function () {
  const PRICE_MAX_AGE_HOURS = 23;
  const PRICE_DISCLAIMER = 'Product prices and availability are accurate as of the date/time indicated and are subject to change. Any price and availability information displayed on Amazon at the time of purchase will apply to the purchase of this product.';

  function qs(selector, root = document) { return Array.from(root.querySelectorAll(selector)); }
  function parseDate(value) {
    const ts = Date.parse(value || '');
    return Number.isFinite(ts) ? ts : 0;
  }
  function pageUpdatedAt() {
    const candidates = [
      document.querySelector('meta[name="bld-price-fetched-at"]')?.content,
      document.querySelector('meta[name="last-modified"]')?.content,
      document.lastModified
    ];
    for (const value of candidates) {
      const ts = parseDate(value);
      if (ts) return ts;
    }
    return Date.now();
  }
  function formatStamp(ts) {
    try {
      return new Intl.DateTimeFormat('en-US', {
        month: 'short', day: 'numeric', year: 'numeric',
        hour: 'numeric', minute: '2-digit', timeZoneName: 'short'
      }).format(new Date(ts));
    } catch (e) {
      return new Date(ts).toLocaleString();
    }
  }
  function isExpired(ts) {
    return Date.now() - ts > PRICE_MAX_AGE_HOURS * 60 * 60 * 1000;
  }
  function looksLikeOldPrice(el) {
    return el && (el.classList.contains('best-seller-was') || /was|old|previous|strike|compare/i.test(el.className || '') || /line-through/i.test(el.getAttribute('style') || ''));
  }
  function closestCard(el) {
    return el.closest('.best-seller-card,.deal-card,.hot-card,.product-card,.amazon-card,article[class*="card"],a[class*="card"]');
  }
  function cardTitle(card) {
    return (card.querySelector('.best-seller-title,.card-title,.hot-card-title,h2,h3')?.textContent || card.getAttribute('data-deal-title') || '').trim();
  }
  function cleanCard(card, fetchedAt) {
    if (!card || card.dataset.bldComplianceDone === 'true') return;
    card.dataset.bldComplianceDone = 'true';

    card.removeAttribute('data-deal-discount');
    card.removeAttribute('data-discount');
    card.removeAttribute('data-price-drop');
    card.removeAttribute('data-hot');
    card.removeAttribute('data-was-price');
    card.removeAttribute('data-lowest-price');
    card.removeAttribute('data-highest-price');

    qs('.best-seller-badges,.discount-badge,.hot-card-badge,.card-badge-hot,.hot-off,.deal-badge,.coupon-badge', card).forEach(el => el.remove());
    qs('.best-seller-was,.price-was,.hot-price-was,.was-price,.old-price,.compare-price,.list-price', card).forEach(el => el.remove());
    qs('*', card).forEach(el => {
      if (looksLikeOldPrice(el)) el.remove();
    });

    const priceRow = card.querySelector('.best-seller-price-row,.card-footer,.price-block,.hot-card-prices,.price-row') || card.querySelector('.best-seller-body,.card-body,.hot-card-body') || card;
    const priceEl = card.querySelector('.best-seller-price,.price-now,.hot-price-now,.product-price,.current-price');
    if (priceEl) {
      priceEl.textContent = priceEl.textContent.trim();
      priceEl.setAttribute('aria-label', 'Current Amazon price');
      priceEl.classList.add('bld-current-amazon-price');
    }

    qs('*', card).forEach(el => {
      const text = (el.textContent || '').trim();
      if (/^\d+%\s*off$/i.test(text) || /^hot deal$/i.test(text) || /^price drop$/i.test(text)) el.remove();
    });

    const stale = isExpired(fetchedAt);
    if (stale && priceEl) {
      priceEl.textContent = 'Check current price on Amazon';
      priceEl.classList.add('bld-price-expired');
    }

    if (priceRow && !card.querySelector('.bld-price-timestamp')) {
      const stamp = document.createElement('div');
      stamp.className = 'bld-price-timestamp';
      stamp.textContent = stale ? 'Price expired after 23 hours. Confirm current price on Amazon.' : `Price shown as of ${formatStamp(fetchedAt)}.`;
      priceRow.insertAdjacentElement('afterend', stamp);
    }

    if (!card.querySelector('.bld-card-price-disclaimer')) {
      const disc = document.createElement('div');
      disc.className = 'bld-card-price-disclaimer';
      disc.textContent = PRICE_DISCLAIMER;
      const btn = card.querySelector('.best-seller-btn,.hot-btn,.btn-deal,a[href*="amazon.com"]');
      if (btn) btn.insertAdjacentElement('beforebegin', disc);
      else card.appendChild(disc);
    }

    const btn = card.querySelector('.best-seller-btn,.hot-btn,.btn-deal,a[href*="amazon.com"]');
    if (btn) btn.textContent = 'View on Amazon';
  }
  function cleanStatsAndLabels() {
    qs('.stat-label').forEach(el => {
      const t = el.textContent.trim().toLowerCase();
      if (t === 'hot deals' || t === 'avg. discount' || t === 'average discount') el.closest('.stat,div')?.remove();
      if (t === 'active deals') el.textContent = 'Product picks';
      if (t === 'avg. price') el.textContent = 'Avg. current price';
    });
    qs('#deal-count,.deal-count').forEach(el => {
      el.textContent = el.textContent.replace(/deals/gi, 'product picks').replace(/Showing\s+(\d+)\s+of\s+(\d+)/i, 'Showing $1 of $2');
    });
    qs('#status-line,.status-line').forEach(el => {
      el.textContent = 'Showing current Amazon product information. Confirm final price, coupon status, shipping, and availability on Amazon before buying.';
    });
    qs('.hero-pill').forEach(el => { el.textContent = el.textContent.replace(/deals?/gi, 'product picks'); });
  }
  function cleanCopy() {
    const replacements = [
      [/hot deals?/gi, 'product picks'],
      [/price drops?/gi, 'current Amazon product information'],
      [/deal prices?/gi, 'current prices'],
      [/deals found on Amazon/gi, 'product picks on Amazon'],
      [/deal feed/gi, 'product feed'],
      [/stronger deals first/gi, 'useful product picks first'],
      [/discounts/gi, 'product information'],
      [/Avg\. discount/gi, ''],
      [/All Deals/gi, 'Product Picks'],
      [/Load More Deals/gi, 'Load More Product Picks'],
      [/Load 50 More Deals/gi, 'Load 50 More Product Picks']
    ];
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, {
      acceptNode(node) {
        const parent = node.parentElement;
        if (!parent || ['SCRIPT','STYLE','NOSCRIPT'].includes(parent.tagName)) return NodeFilter.FILTER_REJECT;
        return /deal|discount|price drop|hot/i.test(node.nodeValue || '') ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_SKIP;
      }
    });
    const nodes = [];
    while (walker.nextNode()) nodes.push(walker.currentNode);
    nodes.forEach(node => {
      let text = node.nodeValue;
      replacements.forEach(([from, to]) => { text = text.replace(from, to); });
      node.nodeValue = text;
    });
  }
  function addSiteDisclaimer() {
    if (document.querySelector('.bld-sitewide-price-disclaimer')) return;
    const target = document.querySelector('.section-head,.hot-strip,.deals-grid,.hot-grid,main') || document.body;
    const box = document.createElement('div');
    box.className = 'bld-sitewide-price-disclaimer';
    box.textContent = PRICE_DISCLAIMER;
    target.insertAdjacentElement(target.matches('.section-head') ? 'afterend' : 'beforebegin', box);
  }
  function addStyles() {
    if (document.getElementById('bld-compliance-style')) return;
    const style = document.createElement('style');
    style.id = 'bld-compliance-style';
    style.textContent = `
      .best-seller-badges,.discount-badge,.hot-card-badge,.card-badge-hot,.hot-off,.coupon-badge,.best-seller-was,.price-was,.hot-price-was,.was-price,.old-price,.compare-price,.list-price{display:none!important}
      .bld-current-amazon-price{color:#c94040;font-weight:900}
      .bld-price-timestamp{font-size:11px;line-height:1.35;color:#6b6b65;margin-top:4px}
      .bld-card-price-disclaimer{font-size:10px;line-height:1.35;color:#7a6a45;margin:6px 0 8px;background:#fffdf7;border:1px solid #f0e4bd;border-radius:8px;padding:6px}
      .bld-sitewide-price-disclaimer{max-width:1180px;margin:0 auto 14px;background:#fffdf7;border:1px solid #f0e4bd;border-radius:12px;padding:10px 14px;color:#6f5a1c;font-size:12px;line-height:1.45}
      .bld-price-expired{font-size:14px!important;color:#1a3a5c!important}
    `;
    document.head.appendChild(style);
  }
  function applyCompliance() {
    const fetchedAt = pageUpdatedAt();
    addStyles();
    cleanCopy();
    cleanStatsAndLabels();
    qs('.best-seller-card,.deal-card,.hot-card,.product-card,.amazon-card,article[class*="card"],a[class*="card"]').forEach(card => cleanCard(card, fetchedAt));
    addSiteDisclaimer();
  }
  function observe() {
    let timer;
    const observer = new MutationObserver(() => {
      clearTimeout(timer);
      timer = setTimeout(applyCompliance, 50);
    });
    observer.observe(document.body, { childList: true, subtree: true });
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => { applyCompliance(); observe(); });
  } else {
    applyCompliance(); observe();
  }
  window.addEventListener('load', applyCompliance);
})();
