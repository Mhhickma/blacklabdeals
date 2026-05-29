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
    s.textContent='.bld-header-shell{background:#fff;border-bottom:1px solid #e8e6e1;position:sticky;top:0;z-index:300;box-shadow:0 8px 22px rgba(26,58,92,.06)}.bld-header-main{max-width:1180px;margin:0 auto;padding:12px 24px;display:flex;align-items:center;justify-content:space-between;gap:18px}.bld-brand{display:flex;align-items:center;gap:12px;text-decoration:none;color:#1a1a18;min-width:250px}.bld-brand-logo{width:64px;height:64px;border-radius:50%;object-fit:cover;border:3px solid #c9a84c}.bld-brand-title{font-family:Georgia,serif;font-size:30px;line-height:1}.bld-brand-title span{color:#c9a84c}.bld-brand-tagline{font-size:12px;color:#777;margin-top:5px}.bld-nav{display:flex;align-items:center;justify-content:flex-end;gap:13px;flex-wrap:wrap}.bld-nav a{font-weight:900;text-decoration:none;color:#1a1a18;font-size:14px;white-space:nowrap}.bld-nav a:hover{color:#1a3a5c}.bld-nav-search{display:flex;align-items:center;gap:6px;margin-left:4px}.bld-nav-search input{width:190px;height:36px;border:1px solid #e8e6e1;border-radius:999px;padding:0 12px;font-size:13px;font-weight:800;outline:none}.bld-nav-search input:focus{border-color:#9db5ca;box-shadow:0 0 0 3px rgba(26,58,92,.08)}.bld-nav-search button{height:36px;border:0;border-radius:999px;background:#1a3a5c;color:#fff;font-size:13px;font-weight:900;padding:0 13px;cursor:pointer}.bld-alert-btn{background:#1a3a5c;color:#fff!important;border-radius:999px;padding:9px 14px}.bld-alert-btn:hover{background:#244f7a}@media(max-width:1020px){.bld-brand{min-width:220px}.bld-brand-logo{width:58px;height:58px}.bld-brand-title{font-size:26px}.bld-nav-search input{width:150px}.bld-nav{gap:10px}.bld-nav a{font-size:13px}}@media(max-width:820px){.bld-header-main{align-items:flex-start;flex-direction:column}.bld-nav{justify-content:flex-start;width:100%}.bld-nav-search{order:20;width:100%;margin-left:0}.bld-nav-search input{width:100%;flex:1}.bld-brand-logo{width:54px;height:54px}.bld-brand-title{font-size:24px}.bld-brand-tagline{display:none}}';
    document.head.appendChild(s);
  }
  function addCompliance(){
    if(document.querySelector('script[src^="/site-common.js"]')) return;
    var sc=document.createElement('script');
    sc.src='/site-common.js?v=15';
    sc.defer=true;
    document.body.appendChild(sc);
  }
  function bindSearch(){
    var forms=document.querySelectorAll('.bld-nav-search');
    forms.forEach(function(form){
      var input=form.querySelector('input[name="q"]');
      if(!input) return;
      form.addEventListener('submit',function(event){
        var q=(input.value||'').trim();
        if(isHome()&&typeof window.BLDApplyProductSearch==='function'){
          event.preventDefault();
          window.BLDApplyProductSearch(q,true);
        }
      });
    });
  }
  function headerHtml(){
    var picks=isHome()?'#deals-section':'/#deals-section';
    var alerts=isHome()?'#alerts-box':'/#alerts-box';
    return '<header class="bld-header-shell"><div class="bld-header-main"><a href="/" class="bld-brand"><img class="bld-brand-logo" src="/logo-128.jpg" alt="Black Lab Deals logo"><div><div class="bld-brand-title">Black Lab <span>Deals</span></div><div class="bld-brand-tagline">Fresh Amazon product picks updated daily</div></div></a><nav class="bld-nav"><a href="/categories/">Categories</a><a href="'+picks+'">All Picks</a><a href="/top-100-amazon-deals-today/">Top 100</a><a href="/best-amazon-tool-deals/">Tools</a><a href="/best-amazon-home-kitchen-deals/">Home</a><form class="bld-nav-search" action="/" method="get"><input type="search" name="q" placeholder="Search picks" aria-label="Search product picks"><button type="submit">Search</button></form><a class="bld-alert-btn" href="'+alerts+'">Get Alerts</a></nav></div></header>';
  }
  function mount(){addAnalytics();addStyles();var el=document.getElementById('site-header');if(el)el.innerHTML=headerHtml();normalizeSortLabels();bindSearch();addCompliance();}
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',mount);else mount();
})();
