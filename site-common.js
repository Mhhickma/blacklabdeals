/* Black Lab Deals shared page helpers */
(function(){
  const PRICE_DISCLAIMER='Product prices and availability are accurate as of the date/time indicated and are subject to change. Any price and availability information displayed on Amazon at the time of purchase will apply to the purchase of this product.';
  const SITE_HOST='blacklabdeals.com';
  const AMAZON_HOST_RE=/amazon\.|amzn\.to|a\.co|joylink\.io/i;
  const safeText=value=>String(value||'').replace(/\s+/g,' ').trim().slice(0,120);
  function track(name,params){
    const payload=Object.assign({page_path:location.pathname,page_title:document.title},params||{});
    try{if(typeof window.gtag==='function')window.gtag('event',name,payload)}catch(e){}
    try{if(typeof window.fbq==='function')window.fbq('trackCustom',name,payload)}catch(e){}
  }
  window.BLDTrack=window.BLDTrack||track;
  function addStyles(){
    if(document.getElementById('bld-shared-style'))return;
    const style=document.createElement('style');
    style.id='bld-shared-style';
    style.textContent='.bld-card-price-disclaimer{display:block;font-size:10px;line-height:1.35;color:#7a6a45;margin:6px 0 8px;background:#fffdf7;border:1px solid #f0e4bd;border-radius:8px;padding:6px}.bld-price-timestamp{display:block;font-size:11px;line-height:1.35;color:#6b6b65;margin-top:4px}';
    document.head.appendChild(style);
  }
  function stamp(){
    try{return new Intl.DateTimeFormat('en-US',{month:'short',day:'numeric',year:'numeric',hour:'numeric',minute:'2-digit',timeZoneName:'short'}).format(new Date())}
    catch(e){return new Date().toLocaleString()}
  }
  function enhanceCards(){
    addStyles();
    document.querySelectorAll('.bld-product-card,.product-card,article[class*="card"]').forEach(card=>{
      if(!card.querySelector('.bld-card-price-disclaimer')){
        const disc=document.createElement('div');
        disc.className='bld-card-price-disclaimer';
        disc.textContent=PRICE_DISCLAIMER;
        const btn=card.querySelector('a[href]');
        if(btn)btn.insertAdjacentElement('beforebegin',disc);else card.appendChild(disc);
      }
      if(!card.querySelector('.bld-price-timestamp')){
        const t=document.createElement('div');
        t.className='bld-price-timestamp';
        t.textContent='Price shown as of '+stamp()+'.';
        const body=card.querySelector('.bld-product-body,.product-body')||card;
        body.appendChild(t);
      }
    });
  }
  function linkType(anchor){
    try{
      const url=new URL(anchor.href,location.href);
      if(AMAZON_HOST_RE.test(url.hostname)||AMAZON_HOST_RE.test(anchor.href))return 'amazon_outbound';
      if(url.hostname&&url.hostname!==location.hostname&&!url.hostname.endsWith(SITE_HOST))return 'external_outbound';
      if(anchor.closest('.bld-nav'))return 'navigation';
      if(anchor.closest('footer'))return 'footer';
      if(anchor.closest('.bld-product-card,.product-card,article[class*="card"]'))return 'product_click';
      return 'internal_click';
    }catch(e){return 'link_click'}
  }
  function setupInteractionTracking(){
    if(window.__BLD_INTERACTION_TRACKING__)return;
    window.__BLD_INTERACTION_TRACKING__=true;
    track('bld_page_ready',{page_location:location.href});
    document.addEventListener('click',event=>{
      const anchor=event.target.closest&&event.target.closest('a[href]');
      const button=event.target.closest&&event.target.closest('button');
      if(anchor){
        const card=anchor.closest('.bld-product-card,.product-card,article[class*="card"]');
        const type=linkType(anchor);
        const payload={
          link_type:type,
          link_text:safeText(anchor.textContent||anchor.getAttribute('aria-label')||''),
          link_url:anchor.href,
          product_asin:anchor.dataset.asin||card?.dataset.asin||'',
          product_category:anchor.dataset.category||card?.dataset.category||'',
          product_title:anchor.dataset.title||card?.dataset.title||safeText(card?.querySelector('.bld-product-title,.product-title')?.textContent||'')
        };
        track(type,payload);
      }else if(button){
        track('button_click',{button_id:button.id||'',button_text:safeText(button.textContent||button.getAttribute('aria-label')||''),button_class:button.className||''});
      }
    },true);
    document.addEventListener('submit',event=>{
      const form=event.target;
      if(!form||!form.matches)return;
      const searchInput=form.querySelector('input[type="search"],input[name="q"]');
      if(searchInput){
        track('site_search_submit',{search_term:safeText(searchInput.value),search_length:String(searchInput.value||'').length,form_action:form.getAttribute('action')||''});
      }else{
        track('form_submit',{form_action:form.getAttribute('action')||'',form_id:form.id||'',form_class:form.className||''});
      }
    },true);
    document.addEventListener('change',event=>{
      const target=event.target;
      if(target&&target.matches&&target.matches('select'))track('select_change',{select_id:target.id||'',select_value:safeText(target.value)});
    },true);
    let maxDepth=0;
    let scrollTimer=null;
    window.addEventListener('scroll',()=>{
      if(scrollTimer)return;
      scrollTimer=setTimeout(()=>{
        scrollTimer=null;
        const doc=document.documentElement;
        const scrollable=Math.max(1,doc.scrollHeight-window.innerHeight);
        const pct=Math.min(100,Math.round((window.scrollY/scrollable)*100));
        const bucket=pct>=90?90:pct>=75?75:pct>=50?50:pct>=25?25:0;
        if(bucket>maxDepth){maxDepth=bucket;track('scroll_depth',{percent_scrolled:bucket});}
      },500);
    },{passive:true});
  }
  function init(){enhanceCards();setupInteractionTracking();}
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',init);else init();
})();