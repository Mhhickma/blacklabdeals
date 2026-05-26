/* Black Lab Deals Amazon compliance layer
   Sitewide emergency safeguard: public cards show current Amazon price only with timestamp/disclaimer.
   The permanent backend fix should still remove was/discount/price-drop fields from generated data. */
(function () {
  const PRICE_MAX_AGE_HOURS = 23;
  const PRICE_DISCLAIMER = 'Product prices and availability are accurate as of the date/time indicated and are subject to change. Any price and availability information displayed on Amazon at the time of purchase will apply to the purchase of this product.';
  const CARD_SELECTOR = '.best-seller-card,.deal-card,.hot-card,.product-card,.amazon-card,.search-card,article[class*="card"],a[class*="card"]';
  const PRICE_SELECTOR = '.best-seller-price,.price-now,.hot-price-now,.product-price,.current-price,.search-price,[class*="price"]';
  const BUTTON_SELECTOR = '.best-seller-btn,.hot-btn,.btn-deal,.search-btn,a[href*="amazon.com"],a[href*="amzn.to"],a[href*="joylink.io"]';

  function qs(selector, root = document) { return Array.from(root.querySelectorAll(selector)); }
  function parseDate(value) { const ts = Date.parse(value || ''); return Number.isFinite(ts) ? ts : 0; }
  function fetchedAt() {
    const candidates = [
      document.querySelector('meta[name="bld-price-fetched-at"]')?.content,
      document.querySelector('meta[name="last-modified"]')?.content,
      document.lastModified
    ];
    for (const value of candidates) { const ts = parseDate(value); if (ts) return ts; }
    return Date.now();
  }
  function formatStamp(ts) {
    try {
      return new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit', timeZoneName: 'short' }).format(new Date(ts));
    } catch (e) { return new Date(ts).toLocaleString(); }
  }
  function isExpired(ts) { return Date.now() - ts > PRICE_MAX_AGE_HOURS * 60 * 60 * 1000; }
  function isPriceText(text) { return /^\$\s*\d/.test(String(text || '').trim()) || /^Check current price/i.test(String(text || '').trim()); }

  function addStyles() {
    if (document.getElementById('bld-amazon-compliance-style')) return;
    const style = document.createElement('style');
    style.id = 'bld-amazon-compliance-style';
    style.textContent = `
      .best-seller-badges,.best-seller-badge,.discount-badge,.hot-card-badge,.card-badge-hot,.hot-off,.deal-badge,.coupon-badge,.rank-badge,.percent-off,.savings-badge,.savings-pill,.best-seller-was,.price-was,.hot-price-was,.was-price,.old-price,.compare-price,.list-price,.original-price,.strike-price,.compare-at-price{display:none!important}
      [data-deal-discount],[data-discount],[data-price-drop],[data-hot],[data-was-price],[data-lowest-price],[data-highest-price]{--bld-compliance:1}
      .bld-current-amazon-price{color:#c94040!important;font-weight:900!important}
      .bld-price-timestamp{display:block!important;font-size:11px!important;line-height:1.35!important;color:#6b6b65!important;margin-top:4px!important}
      .bld-card-price-disclaimer{display:block!important;font-size:10px!important;line-height:1.35!important;color:#7a6a45!important;margin:6px 0 8px!important;background:#fffdf7!important;border:1px solid #f0e4bd!important;border-radius:8px!important;padding:6px!important}
      .bld-sitewide-price-disclaimer{max-width:1180px;margin:0 auto 14px;background:#fffdf7;border:1px solid #f0e4bd;border-radius:12px;padding:10px 14px;color:#6f5a1c;font-size:12px;line-height:1.45}
      .bld-price-expired{font-size:14px!important;color:#1a3a5c!important}
    `;
    document.head.appendChild(style);
  }

  function cleanTextCopy() {
    const replacements = [
      [/All Deals/g, 'All Product Picks'], [/all deals/g, 'all product picks'],
      [/Hot Deals/g, 'Product Picks'], [/hot deals/g, 'product picks'], [/Hot Deal/g, 'Product Pick'], [/hot deal/g, 'product pick'],
      [/price drops/gi, 'current Amazon product information'], [/deal feed/gi, 'product feed'], [/current deal data/gi, 'current Amazon product information'],
      [/stronger deals first/gi, 'useful product picks first'], [/discounts/gi, 'product information'],
      [/Avg\. discount/gi, ''], [/average discount/gi, ''], [/Search deals/gi, 'Search product picks'],
      [/Load\s+50\s+More\s+Deals\s*\([^)]*\)/gi, 'Show 50 More Product Picks'],
      [/Load\s+More\s+Deals\s*\([^)]*\)/gi, 'Show More Product Picks'],
      [/Load\s+More\s+Deals/gi, 'Show More Product Picks'],
      [/Showing\s+(\d+)\s+of\s+(\d+)\s+deals/gi, 'Showing $1 of $2 Product Picks'],
      [/Product Picks\s*-\s*Showing\s+(\d+)\s+of\s+(\d+)\s+deals/gi, 'Showing $1 of $2 Product Picks'],
      [/Product Picks\s*-\s*Showing\s+(\d+)\s+of\s+(\d+)\s+Product Picks/gi, 'Showing $1 of $2 Product Picks']
    ];
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, {
      acceptNode(node) {
        const parent = node.parentElement;
        if (!parent || ['SCRIPT','STYLE','NOSCRIPT'].includes(parent.tagName)) return NodeFilter.FILTER_REJECT;
        return /deals?|discount|price drops?|hot|product picks|load|current deal data|search deals/i.test(node.nodeValue || '') ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_SKIP;
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

  function normalizeHomepageCopy() {
    qs('p,div,span,strong,small').forEach(el => {
      const text = (el.textContent || '').trim();
      if (!text) return;
      if (/How these deals are checked/i.test(text) || /flags strong .*current Amazon product information/i.test(text) || /flags strong .*price drops/i.test(text)) {
        if (el.children.length <= 2 && text.length < 500) {
          el.textContent = 'How product picks are checked: Black Lab Deals refreshes product picks automatically and sends each click to Amazon so you can confirm the final price, shipping, coupon status, and availability before buying.';
        }
      }
    });
    qs('input[placeholder],textarea[placeholder]').forEach(el => {
      if (/search deals/i.test(el.getAttribute('placeholder') || '')) el.setAttribute('placeholder', 'Search product picks');
    });
    qs('.filter-btn,button,a').forEach(el => {
      const text = (el.textContent || '').trim();
      if (/^All Deals$/i.test(text)) el.textContent = 'All Product Picks';
      if (/^Hot Deals$/i.test(text)) el.textContent = 'Product Picks';
    });
  }

  function normalizeSortMenus() {
    qs('select').forEach(select => {
      qs('option', select).forEach(option => {
        const text = (option.textContent || '').trim();
        const value = String(option.value || '');
        if (/biggest\s+discount/i.test(text) || /discount/i.test(value)) {
          option.remove();
        }
      });
      if (/discount/i.test(String(select.value || ''))) {
        const first = qs('option', select)[0];
        if (first) select.value = first.value;
      }
    });
  }

  function normalizeCountsAndButtons() {
    qs('.deal-count,#deal-count,[class*="deal-count"]').forEach(el => {
      const text = (el.textContent || '').trim();
      const match = text.match(/(?:Product Picks\s*-\s*)?Showing\s+(\d+)\s+of\s+(\d+)\s+(?:deals|product picks)/i);
      if (match) {
        el.textContent = `Showing ${match[1]} of ${match[2]} Product Picks`;
      } else {
        el.textContent = text.replace(/\bdeals\b/gi, 'Product Picks').replace(/Product Picks\s*-\s*/i, '');
      }
    });
    qs('button,a').forEach(el => {
      const text = (el.textContent || '').trim();
      if (/load\s+50\s+more\s+deals/i.test(text) || /load\s+50\s+more\s+product picks/i.test(text)) {
        el.textContent = 'Show 50 More Product Picks';
      } else if (/load\s+more\s+deals/i.test(text) || /load\s+more\s+product picks/i.test(text)) {
        el.textContent = 'Show More Product Picks';
      }
    });
  }

  function cleanCard(card, ts) {
    if (!card || card.dataset.bldComplianceApplied === 'true') return;
    const priceEl = qs(PRICE_SELECTOR, card).find(el => isPriceText(el.textContent));
    const btn = card.querySelector(BUTTON_SELECTOR);
    if (!priceEl && !btn) return;

    card.dataset.bldComplianceApplied = 'true';
    ['data-deal-discount','data-discount','data-price-drop','data-hot','data-was-price','data-lowest-price','data-highest-price'].forEach(attr => card.removeAttribute(attr));

    qs('.best-seller-badges,.best-seller-badge,.discount-badge,.hot-card-badge,.card-badge-hot,.hot-off,.deal-badge,.coupon-badge,.rank-badge,.percent-off,.savings-badge,.savings-pill,.best-seller-was,.price-was,.hot-price-was,.was-price,.old-price,.compare-price,.list-price,.original-price,.strike-price,.compare-at-price', card).forEach(el => el.remove());
    qs('*', card).forEach(el => {
      const text = (el.textContent || '').trim();
      const cls = String(el.className || '');
      const style = String(el.getAttribute('style') || '');
      if (/^\d+%\s*off$/i.test(text) || /^hot deal$/i.test(text) || /^price drop$/i.test(text) || /was|old|previous|strike|compare/i.test(cls) || /line-through/i.test(style)) el.remove();
    });

    const stale = isExpired(ts);
    if (priceEl) {
      priceEl.classList.add('bld-current-amazon-price');
      priceEl.setAttribute('aria-label', 'Current Amazon price');
      if (stale) {
        priceEl.textContent = 'Check current price on Amazon';
        priceEl.classList.add('bld-price-expired');
      }
    }

    const priceRow = priceEl?.closest('.best-seller-price-row,.hot-card-prices,.price-block,.price-row,.search-price-row') || priceEl || card.querySelector('.best-seller-body,.hot-card-body,.card-body,.search-body') || card;
    if (!card.querySelector('.bld-price-timestamp')) {
      const stamp = document.createElement('div');
      stamp.className = 'bld-price-timestamp';
      stamp.textContent = stale ? 'Price expired after 23 hours. Confirm current price on Amazon.' : `Price shown as of ${formatStamp(ts)}.`;
      priceRow.insertAdjacentElement('afterend', stamp);
    }
    if (!card.querySelector('.bld-card-price-disclaimer')) {
      const disc = document.createElement('div');
      disc.className = 'bld-card-price-disclaimer';
      disc.textContent = PRICE_DISCLAIMER;
      if (btn) btn.insertAdjacentElement('beforebegin', disc);
      else card.appendChild(disc);
    }
    if (btn && /deal|check|view/i.test(btn.textContent || '')) btn.textContent = 'View on Amazon';
  }

  function addSitewideDisclaimer() {
    if (document.querySelector('.bld-sitewide-price-disclaimer')) return;
    const anchor = document.querySelector('.section-head,.filter-row,.hot-strip,.hot-grid,.deals-grid,main') || document.body;
    const box = document.createElement('div');
    box.className = 'bld-sitewide-price-disclaimer';
    box.textContent = PRICE_DISCLAIMER;
    anchor.insertAdjacentElement(anchor.matches('.section-head,.filter-row') ? 'afterend' : 'beforebegin', box);
  }

  function applyCompliance() {
    const ts = fetchedAt();
    addStyles();
    cleanTextCopy();
    normalizeHomepageCopy();
    normalizeSortMenus();
    normalizeCountsAndButtons();
    qs(CARD_SELECTOR).forEach(card => cleanCard(card, ts));
    normalizeHomepageCopy();
    normalizeSortMenus();
    normalizeCountsAndButtons();
    addSitewideDisclaimer();
  }

  function start() {
    applyCompliance();
    let timer;
    const interval = window.setInterval(function () {
      cleanTextCopy();
      normalizeHomepageCopy();
      normalizeSortMenus();
      normalizeCountsAndButtons();
    }, 1000);
    window.setTimeout(function () { window.clearInterval(interval); }, 30000);
    new MutationObserver(() => {
      clearTimeout(timer);
      timer = setTimeout(() => {
        qs('[data-bld-compliance-applied="true"]').forEach(card => {
          if (!card.querySelector('.bld-price-timestamp') || !card.querySelector('.bld-card-price-disclaimer')) card.dataset.bldComplianceApplied = 'false';
        });
        applyCompliance();
      }, 75);
    }).observe(document.body, { childList: true, subtree: true, characterData: true });
    window.addEventListener('load', applyCompliance);
    setTimeout(applyCompliance, 500);
    setTimeout(applyCompliance, 1500);
    setTimeout(applyCompliance, 3000);
    setTimeout(applyCompliance, 6000);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start);
  else start();
})();