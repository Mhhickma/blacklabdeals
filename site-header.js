/* Black Lab Deals clean shared header */
(function(){
  function isHome(){return location.pathname==='/'||location.pathname==='/index.html'}
  function addAnalytics(){
    var gaId='G-ES3MC2ZTTM';
    var metaPixelId='1642712089209011';
    if(!window.__BLD_ANALYTICS_ADDED__){
      window.__BLD_ANALYTICS_ADDED__=true;
      if(!document.querySelector('script[src="https://www.googletagmanager.com/gtag/js?id='+gaId+'"]')){
        var ga=document.createElement('script');
        ga.async=true;
        ga.src='https://www.googletagmanager.com/gtag/js?id='+gaId;
        document.head.appendChild(ga);
      }
      window.dataLayer=window.dataLayer||[];
      window.gtag=window.gtag||function(){window.dataLayer.push(arguments)};
      window.gtag('js',new Date());
      window.gtag('config',gaId);
      if(!window.fbq){
        var n=window.fbq=function(){n.callMethod?n.callMethod.apply(n,arguments):n.queue.push(arguments)};
        if(!window._fbq)window._fbq=n;
        n.push=n;n.loaded=true;n.version='2.0';n.queue=[];
        var fb=document.createElement('script');
        fb.async=true;
        fb.src='https://connect.facebook.net/en_US/fbevents.js';
        var firstScript=document.getElementsByTagName('script')[0];
        firstScript.parentNode.insertBefore(fb,firstScript);
      }
      window.fbq('init',metaPixelId);
      window.fbq('track','PageView');
    }
  }
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
    s.textContent='.bld-header-shell{background:#fff;border-bottom:1px solid #e8e6e1;position:sticky;top:0;z-index:300;box-shadow:0 8px 22px rgba(26,58,92,.06)}.bld-header-main{max-width:1180px;margin:0 auto;padding:12px 24px;display:flex;align-items:center;justify-content:space-between;gap:18px}.bld-brand{display:flex;align-items:center;gap:12px;text-decoration:none;color:#1a1a18;min-width:250px}.bld-brand-logo{width:64px;height:64px;border-radius:50%;object-fit:cover;border:3px solid #c9a84c}.bld-brand-title{font-family:Georgia,serif;font-size:30px;line-height:1}.bld-brand-title span{color:#c9a84c}.bld-brand-tagline{font-size:12px;color:#777;margin-top:5px}.bld-nav{display:flex;align-items:center;justify-content:flex-end;gap:13px;flex-wrap:wrap}.bld-nav a{font-weight:900;text-decoration:none;color:#1a1a18;font-size:14px;white-space:nowrap}.bld-nav a:hover{color:#1a3a5c}.bld-menu{position:relative}.bld-menu-btn{display:flex;align-items:center;gap:6px;height:38px;border:1px solid #e8e6e1;border-radius:999px;background:#fff;color:#1a1a18;font-size:14px;font-weight:900;padding:0 15px;cursor:pointer}.bld-menu-btn:hover,.bld-menu:focus-within .bld-menu-btn{border-color:#9db5ca;color:#1a3a5c;box-shadow:0 0 0 3px rgba(26,58,92,.08)}.bld-menu-chevron{font-size:11px;line-height:1}.bld-menu-panel{position:absolute;top:calc(100% + 10px);right:0;width:min(88vw,390px);max-height:70vh;overflow:auto;background:#fff;border:1px solid #e8e6e1;border-radius:18px;box-shadow:0 22px 55px rgba(26,58,92,.18);padding:10px;display:none;z-index:500}.bld-menu:hover .bld-menu-panel,.bld-menu:focus-within .bld-menu-panel{display:grid;grid-template-columns:1fr 1fr;gap:4px}.bld-menu-panel a{display:block;border-radius:12px;padding:10px 11px;font-size:13px;line-height:1.2;color:#1a1a18}.bld-menu-panel a:hover,.bld-menu-panel a:focus{background:#f6f4ee;color:#1a3a5c;outline:none}.bld-menu-featured{grid-column:1/-1;background:#f8f1db}.bld-nav-search{display:flex;align-items:center;gap:6px}.bld-nav-search input{width:190px;height:36px;border:1px solid #e8e6e1;border-radius:999px;padding:0 12px;font-size:13px;font-weight:800;outline:none}.bld-nav-search input:focus{border-color:#9db5ca;box-shadow:0 0 0 3px rgba(26,58,92,.08)}.bld-nav-search button{height:36px;border:0;border-radius:999px;background:#1a3a5c;color:#fff;font-size:13px;font-weight:900;padding:0 13px;cursor:pointer}.bld-alert-btn{background:#1a3a5c;color:#fff!important;border-radius:999px;padding:9px 14px}.bld-alert-btn:hover{background:#244f7a}.bld-auto-seo{max-width:1180px;margin:34px auto 0;padding:0 24px}.bld-auto-seo-card{background:#fff;border:1px solid #e6e1d6;border-radius:22px;box-shadow:0 3px 12px rgba(20,20,15,.05);padding:28px 30px}.bld-auto-seo-card h2{font-family:Georgia,serif;font-size:32px;line-height:1.1;margin:0 0 12px;color:#1d1d1a}.bld-auto-seo-card p{color:#4f5660;font-size:16px;line-height:1.55;margin:0 0 10px;max-width:930px}.bld-auto-label{color:#1d1d1a;font-size:13px;font-weight:900;letter-spacing:.08em;margin:20px 0 10px;text-transform:uppercase}.bld-auto-chips,.bld-auto-links{display:flex;flex-wrap:wrap;gap:10px}.bld-auto-chip{background:#eef3f7;border:1px solid #d8e3ec;border-radius:999px;color:#1a3a5c;font-size:13px;font-weight:900;padding:8px 12px}.bld-auto-links{margin-top:18px}.bld-auto-links a{background:#f6f5f1;border:1px solid #e6e1d6;border-radius:999px;color:#0f3355;font-size:13px;font-weight:900;padding:9px 14px;text-decoration:none}.bld-auto-note{border-top:1px solid #eee8dc;color:#7a6a45!important;font-size:12px!important;line-height:1.45!important;margin:18px 0 0!important;padding-top:12px}@media(max-width:820px){.bld-header-main{align-items:flex-start;flex-direction:column}.bld-nav{justify-content:flex-start;width:100%}.bld-menu{width:100%}.bld-menu-btn{width:100%;justify-content:space-between}.bld-menu-panel{position:static;width:100%;max-height:none;margin-top:8px}.bld-menu:hover .bld-menu-panel,.bld-menu:focus-within .bld-menu-panel{grid-template-columns:1fr}.bld-nav-search{width:100%}.bld-nav-search input{width:100%;flex:1}.bld-brand-logo{width:54px;height:54px}.bld-brand-title{font-size:24px}.bld-brand-tagline{display:none}.bld-auto-seo{padding:0 14px}.bld-auto-seo-card{padding:20px}.bld-auto-seo-card h2{font-size:24px}.bld-auto-chip,.bld-auto-links a{width:100%;text-align:center}}';
    document.head.appendChild(s);
  }
  function addCompliance(){
    if(document.querySelector('script[src^="/site-common.js"]')) return;
    var sc=document.createElement('script');
    sc.src='/site-common.js?v=15';
    sc.defer=true;
    document.body.appendChild(sc);
  }
  function addAlertsPopup(){
    if(document.querySelector('script[src^="/alerts-popup.js"]')) return;
    var sc=document.createElement('script');
    sc.src='/alerts-popup.js?v=1';
    sc.defer=true;
    document.body.appendChild(sc);
  }
  function categoryName(){
    var cfg=window.BLD_PAGE_CONFIG||{};
    var value=(cfg.category||'').trim();
    if(!value||value==='all')return '';
    return value;
  }
  function addSharedCategorySeo(){
    if(document.querySelector('.tool-seo-content,.category-seo-content,.bld-auto-seo'))return;
    var name=categoryName();
    if(!name)return;
    var main=document.querySelector('main.page-shell')||document.querySelector('main');
    if(!main)return;
    var safe=name.replace(/&/g,'&amp;');
    var section=document.createElement('section');
    section.className='bld-auto-seo';
    section.innerHTML='<div class="bld-auto-seo-card"><h2>Shop Current Amazon '+safe+' Product Picks</h2><p>Find current Amazon product picks for '+safe.toLowerCase()+' and related items.</p><p>Black Lab Deals helps shoppers browse current product information in one place. Final price, shipping, coupon status, and availability should always be confirmed on Amazon before buying.</p><div class="bld-auto-label">Popular '+safe.toLowerCase()+' topics</div><div class="bld-auto-chips"><span class="bld-auto-chip">'+safe+'</span><span class="bld-auto-chip">Current Product Information</span><span class="bld-auto-chip">Product Picks</span><span class="bld-auto-chip">Category Picks</span></div><div class="bld-auto-links"><a href="/top-100-amazon-deals-today/">View Top 100 Picks</a><a href="/best-amazon-deals-under-50/">Shop Picks Under $50</a><a href="/best-amazon-tool-deals/">Browse Tool Picks</a><a href="/categories/">See All Categories</a></div><p class="bld-auto-note">Product information is refreshed regularly. Confirm final price and availability on Amazon.</p></div>';
    main.appendChild(section);
  }
  function bindSearch(){
    var form=document.querySelector('.bld-nav-search');
    var input=form&&form.querySelector('input[name="q"]');
    if(!form||!input) return;
    form.addEventListener('submit',function(event){
      var q=(input.value||'').trim();
      if(isHome()&&typeof window.BLDApplyProductSearch==='function'){
        event.preventDefault();
        window.BLDApplyProductSearch(q,true);
      }
    });
  }
  function headerHtml(){
    return '<header class="bld-header-shell"><div class="bld-header-main"><a href="/" class="bld-brand"><img class="bld-brand-logo" src="/logo-128.jpg" alt="Black Lab Deals logo"><div><div class="bld-brand-title">Black Lab <span>Deals</span></div><div class="bld-brand-tagline">Fresh Amazon product picks updated daily</div></div></a><nav class="bld-nav"><div class="bld-menu"><button class="bld-menu-btn" type="button" aria-haspopup="true">Product Picks <span class="bld-menu-chevron">▼</span></button><div class="bld-menu-panel"><a class="bld-menu-featured" href="/top-100-amazon-deals-today/">Top 100 Product Picks</a><a class="bld-menu-featured" href="/best-seller-deals.html">Best Seller Product Picks</a><a class="bld-menu-featured" href="/best-amazon-deals-under-50/">Product Picks Under $50</a><a href="/categories/">All Categories</a><a href="/best-amazon-tool-deals/">Tool Product Picks</a><a href="/best-amazon-home-kitchen-deals/">Home & Kitchen Product Picks</a><a href="/best-amazon-electronics-deals/">Electronics Product Picks</a><a href="/best-amazon-automotive-deals/">Automotive Product Picks</a><a href="/best-amazon-patio-lawn-garden-deals/">Patio, Lawn & Garden Product Picks</a><a href="/best-amazon-sports-outdoors-deals/">Sports & Outdoors Product Picks</a><a href="/best-amazon-pet-supplies-deals/">Pet Supplies Product Picks</a><a href="/best-amazon-toys-games-deals/">Toys & Games Product Picks</a><a href="/best-amazon-office-products-deals/">Office Product Picks</a><a href="/best-amazon-health-household-deals/">Health & Household Product Picks</a><a href="/best-amazon-baby-products-deals/">Baby Product Picks</a><a href="/best-amazon-musical-instruments-deals/">Musical Instruments Product Picks</a></div></div><form class="bld-nav-search" action="/" method="get"><input type="search" name="q" placeholder="Search picks" aria-label="Search product picks"><button type="submit">Search</button></form><a class="bld-alert-btn" href="#" data-bld-alert-open>Get Alerts</a></nav></div></header>';
  }
  function mount(){addAnalytics();addStyles();var el=document.getElementById('site-header');if(el)el.innerHTML=headerHtml();normalizeSortLabels();bindSearch();addCompliance();addAlertsPopup();addSharedCategorySeo();}
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',mount);else mount();
})();