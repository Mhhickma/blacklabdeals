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
    s.textContent='.bld-header-shell{background:#fff;border-bottom:1px solid #e8e6e1;position:sticky;top:0;z-index:300;box-shadow:0 8px 22px rgba(26,58,92,.06)}.bld-header-main{max-width:1180px;margin:0 auto;padding:12px 24px;display:flex;align-items:center;justify-content:space-between;gap:18px}.bld-brand{display:flex;align-items:center;gap:12px;text-decoration:none;color:#1a1a18;min-width:250px}.bld-brand-logo{width:64px;height:64px;border-radius:50%;object-fit:cover;border:3px solid #c9a84c}.bld-brand-title{font-family:Georgia,serif;font-size:30px;line-height:1}.bld-brand-title span{color:#c9a84c}.bld-brand-tagline{font-size:12px;color:#777;margin-top:5px}.bld-nav{display:flex;align-items:center;justify-content:flex-end;gap:13px;flex-wrap:wrap}.bld-nav a{font-weight:900;text-decoration:none;color:#1a1a18;font-size:14px;white-space:nowrap}.bld-nav a:hover{color:#1a3a5c}@media(max-width:820px){.bld-header-main{align-items:flex-start;flex-direction:column}.bld-nav{justify-content:flex-start;width:100%}.bld-brand-logo{width:54px;height:54px}.bld-brand-title{font-size:24px}.bld-brand-tagline{display:none}}';
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
    return '<header class="bld-header-shell"><div class="bld-header-main"><a href="/" class="bld-brand"><img class="bld-brand-logo" src="/logo-128.jpg" alt="Black Lab Deals logo"><div><div class="bld-brand-title">Black Lab <span>Deals</span></div><div class="bld-brand-tagline">Fresh Amazon product picks updated daily</div></div></a><nav class="bld-nav"><a href="/categories/">Categories</a></nav></div></header>';
  }
  function mount(){addAnalytics();addStyles();var el=document.getElementById('site-header');if(el)el.innerHTML=headerHtml();normalizeSortLabels();addCompliance();}
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',mount);else mount();
})();
