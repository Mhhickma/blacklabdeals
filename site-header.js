/* Black Lab Deals clean shared header */
(function(){
  function isHome(){return location.pathname==='/'||location.pathname==='/index.html'}
  function normalizeSortLabels(){
    var select=document.getElementById('sort-select');
    if(!select) return;
    var current=select.value;
    select.innerHTML='<option value="best">Product Picks</option><option value="newest">Newest First</option><option value="featured">Featured Picks</option>';
    select.value=(current==='newest')?'newest':(current==='featured'?'featured':'best');
  }
  function addStyles(){
    if(document.getElementById('bld-header-style')) return;
    var s=document.createElement('style');
    s.id='bld-header-style';
    s.textContent='.bld-header-shell{background:#fff;border-bottom:1px solid #e8e6e1;position:sticky;top:0;z-index:300;box-shadow:0 8px 22px rgba(26,58,92,.06)}.bld-header-main{max-width:1180px;margin:0 auto;padding:16px 24px;display:flex;align-items:center;justify-content:space-between;gap:22px}.bld-brand{display:flex;align-items:center;gap:13px;text-decoration:none;color:#1a1a18}.bld-brand-logo{width:76px;height:76px;border-radius:50%;object-fit:cover;border:3px solid #c9a84c}.bld-brand-title{font-family:Georgia,serif;font-size:32px;line-height:1}.bld-brand-title span{color:#c9a84c}.bld-brand-tagline{font-size:13px;color:#777;margin-top:5px}.bld-nav{display:flex;align-items:center;gap:16px;flex-wrap:wrap}.bld-nav a{font-weight:900;text-decoration:none;color:#1a1a18;font-size:14px}.bld-alert-btn{background:#1a3a5c;color:#fff!important;border-radius:999px;padding:9px 14px}.bld-search-row{border-top:1px solid #e8e6e1;padding:10px 16px;background:#fff}.bld-search{max-width:760px;margin:0 auto;display:flex;gap:8px}.bld-search input{flex:1;height:42px;border:1px solid #e8e6e1;border-radius:999px;padding:0 16px;font-weight:800}.bld-search button{height:42px;border:0;border-radius:999px;background:#1a3a5c;color:#fff;font-weight:900;padding:0 18px}@media(max-width:760px){.bld-nav{display:none}.bld-brand-logo{width:58px;height:58px}.bld-brand-title{font-size:24px}.bld-brand-tagline{display:none}}';
    document.head.appendChild(s);
  }
  function addCompliance(){
    if(document.querySelector('script[src^="/site-common.js"]')) return;
    var sc=document.createElement('script');
    sc.src='/site-common.js?v=15';
    sc.defer=true;
    document.body.appendChild(sc);
  }
  function headerHtml(){
    var picks=isHome()?'#deals-section':'/#deals-section';
    var alerts=isHome()?'#alerts-box':'/#alerts-box';
    return '<header class="bld-header-shell"><div class="bld-header-main"><a href="/" class="bld-brand"><img class="bld-brand-logo" src="/logo-128.jpg" alt="Black Lab Deals logo"><div><div class="bld-brand-title">Black Lab <span>Deals</span></div><div class="bld-brand-tagline">Fresh Amazon product picks updated daily</div></div></a><nav class="bld-nav"><a href="/categories/">Categories</a><a href="'+picks+'">All Product Picks</a><a href="/top-100-amazon-deals-today/">Top 100</a><a href="/best-amazon-tool-deals/">Tools</a><a href="/best-amazon-home-kitchen-deals/">Home &amp; Kitchen</a><a class="bld-alert-btn" href="'+alerts+'">Get Alerts</a></nav></div><div class="bld-search-row"><form class="bld-search" action="/search.html" method="get"><input type="search" name="q" placeholder="Search product picks"><button type="submit">Search</button></form></div></header>';
  }
  function mount(){addStyles();var el=document.getElementById('site-header');if(el)el.innerHTML=headerHtml();normalizeSortLabels();addCompliance();}
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',mount);else mount();
})();
