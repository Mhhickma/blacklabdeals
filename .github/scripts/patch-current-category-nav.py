from pathlib import Path

js_path = Path('site-common.js')
css_path = Path('site-common.css')
js = js_path.read_text(encoding='utf-8')
css = css_path.read_text(encoding='utf-8')

start = js.index('function ensureBrowseSection() {')
end = js.index('\n\nasync function fetchDealsFeed()', start)

new_nav = r'''const POPULAR_CATEGORY_LINKS = [
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
  const main = document.querySelector('main.page-shell') || document.querySelector('main');
  if (!main) return;

  let nav = document.querySelector('.popular-category-nav');
  if (!nav) {
    const holder = document.createElement('div');
    holder.innerHTML = renderPopularCategoryNav();
    nav = holder.firstElementChild;
  }

  markCurrentPopularCategoryNav(nav);

  const anchor = main.querySelector('.hot-strip') || main.querySelector('.section-head') || main.querySelector('.filter-row');
  if (anchor && anchor.parentNode) {
    anchor.parentNode.insertBefore(nav, anchor);
  } else {
    main.appendChild(nav);
  }
}'''

js = js[:start] + new_nav + js[end:]
js_path.write_text(js, encoding='utf-8')

css_add = ".popular-category-link.is-current{border-color:#cfe0f3;background:var(--accent-light);box-shadow:inset 0 0 0 1px #cfe0f3}.popular-category-link.is-current span{color:var(--accent)}.current-page-label{display:inline-flex;margin-left:8px;vertical-align:middle;background:var(--accent);color:#fff;border-radius:999px;padding:2px 7px;font-family:'DM Sans',sans-serif;font-size:10px;font-style:normal;font-weight:900;line-height:1.4;white-space:nowrap}@media(max-width:520px){.current-page-label{display:flex;width:max-content;margin:6px 0 0}}"
if '.popular-category-link.is-current' not in css:
    css = css.rstrip() + css_add + '\n'
css_path.write_text(css, encoding='utf-8')
print('Patched shared popular category navigation')
