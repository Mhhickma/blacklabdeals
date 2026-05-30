/* Black Lab Deals alert signup popup */
(function(){
  var AUTO_POPUP_DELAY_MS=5000;
  var DISMISSED_KEY='bld_alert_popup_dismissed_at';
  var SIGNED_UP_KEY='bld_alert_popup_signed_up';
  var DISMISS_COOLDOWN_DAYS=7;
  function q(sel,root){return (root||document).querySelector(sel)}
  function addStyles(){
    if(q('#bld-alert-popup-style')) return;
    var style=document.createElement('style');
    style.id='bld-alert-popup-style';
    style.textContent='.bld-alert-overlay{position:fixed;inset:0;background:rgba(10,20,30,.55);z-index:1000;display:none;align-items:center;justify-content:center;padding:18px}.bld-alert-overlay.open{display:flex}.bld-alert-modal{width:min(94vw,460px);background:#fff;border:1px solid #e8e6e1;border-radius:22px;box-shadow:0 22px 70px rgba(0,0,0,.26);position:relative;overflow:hidden}.bld-alert-modal:before{content:"";display:block;height:8px;background:#1a3a5c}.bld-alert-content{padding:24px}.bld-alert-close{position:absolute;top:14px;right:14px;border:0;background:#f4f2eb;border-radius:999px;width:34px;height:34px;font-size:22px;line-height:1;cursor:pointer}.bld-alert-kicker{font-size:12px;font-weight:900;text-transform:uppercase;letter-spacing:.12em;color:#1a3a5c;margin:0 0 6px}.bld-alert-content h2{font-family:Georgia,serif;font-size:31px;line-height:1.05;margin:0 42px 10px 0;color:#1a1a18}.bld-alert-content p{margin:0 0 16px;color:#62625c;line-height:1.45}.bld-alert-field{display:grid;gap:6px;margin-bottom:11px}.bld-alert-field label{font-size:13px;font-weight:900;color:#333}.bld-alert-field input{height:46px;border:1px solid #e1ddd3;border-radius:12px;padding:0 13px;font-size:16px;font-weight:700;outline:none}.bld-alert-field input:focus{border-color:#9db5ca;box-shadow:0 0 0 3px rgba(26,58,92,.08)}.bld-alert-submit{width:100%;height:46px;border:0;border-radius:999px;background:#1a3a5c;color:#fff;font-weight:900;font-size:15px;cursor:pointer;margin-top:4px}.bld-alert-submit:hover{background:#244f7a}.bld-alert-submit:disabled{opacity:.75;cursor:not-allowed}.bld-alert-note{font-size:11px!important;color:#777!important;margin:11px 0 0!important}.bld-alert-status{font-size:13px;font-weight:900;margin-top:12px;min-height:18px}.bld-alert-status.ok{color:#16703a}.bld-alert-status.err{color:#b23333}@media(max-width:520px){.bld-alert-content{padding:21px}.bld-alert-content h2{font-size:27px}}';
    document.head.appendChild(style);
  }
  function html(){
    return '<div class="bld-alert-overlay" id="bld-alert-overlay" role="dialog" aria-modal="true" aria-labelledby="bld-alert-title"><div class="bld-alert-modal"><button class="bld-alert-close" type="button" aria-label="Close">&times;</button><div class="bld-alert-content"><div class="bld-alert-kicker">Get Alerts</div><h2 id="bld-alert-title">Get Black Lab Deals alerts</h2><p>Enter your email or phone number to get updates when new product picks are posted.</p><form id="bld-alert-form"><div class="bld-alert-field"><label for="bld-alert-email">Email address</label><input id="bld-alert-email" name="email" type="email" placeholder="you@example.com" autocomplete="email"></div><div class="bld-alert-field"><label for="bld-alert-phone">Phone number</label><input id="bld-alert-phone" name="phone" type="tel" placeholder="555-555-5555" autocomplete="tel"></div><button class="bld-alert-submit" type="submit">Sign Up for Alerts</button><p class="bld-alert-note">You can use email, phone, or both. You can opt out anytime.</p><div class="bld-alert-status" aria-live="polite"></div></form></div></div></div>';
  }
  function storageGet(key){try{return localStorage.getItem(key)}catch(e){return null}}
  function storageSet(key,value){try{localStorage.setItem(key,value)}catch(e){}}
  function recentlyDismissed(){
    var value=Number(storageGet(DISMISSED_KEY)||0);
    if(!value) return false;
    return Date.now()-value < DISMISS_COOLDOWN_DAYS*24*60*60*1000;
  }
  function shouldAutoOpen(){
    if(storageGet(SIGNED_UP_KEY)==='1') return false;
    if(recentlyDismissed()) return false;
    if(q('#bld-alert-overlay.open')) return false;
    return true;
  }
  function open(){var o=q('#bld-alert-overlay');if(o){o.classList.add('open');setTimeout(function(){var e=q('#bld-alert-email');if(e)e.focus();},50)}}
  function close(markDismissed){
    var o=q('#bld-alert-overlay');
    if(o)o.classList.remove('open');
    if(markDismissed) storageSet(DISMISSED_KEY,String(Date.now()));
  }
  function scheduleAutoOpen(){
    setTimeout(function(){if(shouldAutoOpen()) open()},AUTO_POPUP_DELAY_MS);
  }
  function bind(){
    document.addEventListener('click',function(e){
      var trigger=e.target.closest&&e.target.closest('.bld-alert-btn,[data-bld-alert-open]');
      if(trigger){e.preventDefault();open();return}
      if(e.target.matches&&e.target.matches('.bld-alert-overlay,.bld-alert-close')) close(true);
    });
    document.addEventListener('keydown',function(e){if(e.key==='Escape') close(true)});
    var form=q('#bld-alert-form');
    if(!form) return;
    form.addEventListener('submit',async function(e){
      e.preventDefault();
      var email=q('#bld-alert-email',form).value.trim();
      var phone=q('#bld-alert-phone',form).value.trim();
      var status=q('.bld-alert-status',form);
      var btn=q('.bld-alert-submit',form);
      if(!email&&!phone){status.textContent='Enter an email address or phone number.';status.className='bld-alert-status err';return}
      status.textContent='Saving...';status.className='bld-alert-status';btn.disabled=true;
      try{
        var res=await fetch('/api/subscribe',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({email:email,phone:phone,source:location.pathname,createdAt:new Date().toISOString()})});
        var data={};try{data=await res.json()}catch(_e){}
        if(!res.ok||!data.ok) throw new Error('save failed');
        storageSet(SIGNED_UP_KEY,'1');
        form.reset();status.textContent='You are signed up!';status.className='bld-alert-status ok';
        setTimeout(function(){close(false)},650);
      }catch(err){status.textContent='Could not save yet. Try again soon.';status.className='bld-alert-status err'}
      btn.disabled=false;
    });
  }
  function init(){addStyles();if(!q('#bld-alert-overlay'))document.body.insertAdjacentHTML('beforeend',html());bind();scheduleAutoOpen()}
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',init);else init();
})();