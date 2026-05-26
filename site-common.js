/* Black Lab Deals shared page helpers */
(function(){
  const PRICE_DISCLAIMER='Product prices and availability are accurate as of the date/time indicated and are subject to change. Any price and availability information displayed on Amazon at the time of purchase will apply to the purchase of this product.';
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
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',enhanceCards);else enhanceCards();
})();
