// ════════════════════════════════════════════════════════════
// PORRA: pronósticos de la cartelera con cuenta y clasificación
// Base de datos en Supabase (supabase/porra.sql). Los partidos los sube y
// los cierra el workflow de datos (tools/porra.py); los puntos los calcula
// la base de datos. La librería de Supabase solo se carga al entrar aquí.
// ════════════════════════════════════════════════════════════
const PORRA = {
  url: 'https://nckeadvyeymewibrqpuj.supabase.co',
  key: 'sb_publishable_bVJumfuxrHV_CH3NScZF6Q_qSN2I7-2',
};
let _sb = null, _porraSesion = null, _porraPerfil = undefined;
let _porraTab = 'pronosticar', _porraClasif = 'general';
// Vuelta del inicio de sesión (Google o enlace del correo): /?porra=1&code=…
const _porraVuelta = /[?&](porra|code)=/.test(location.search);

function porraCont(){ return document.getElementById('porraContent'); }

function cargarSupabase(){
  if(window.supabase?.createClient) return Promise.resolve();
  return new Promise((ok, mal)=>{
    const s = document.createElement('script');
    s.src = 'vendor/supabase/supabase.js';
    s.onload = ok; s.onerror = mal;
    document.head.appendChild(s);
  });
}

async function porraInit(){
  const cont = porraCont();
  if(!cont) return;
  if(!_sb){
    cont.innerHTML = `<div class="cart-loading">⟳ ${tx('Cargando la porra…','Porra kargatzen…')}</div>`;
    try{
      await cargarSupabase();
      _sb = window.supabase.createClient(PORRA.url, PORRA.key, {
        auth: {flowType: 'pkce', detectSessionInUrl: true, persistSession: true, autoRefreshToken: true},
      });
      _sb.auth.onAuthStateChange((ev, sesion)=>{
        const cambio = (sesion?.user?.id||null) !== (_porraSesion?.user?.id||null);
        _porraSesion = sesion;
        if(cambio){ _porraPerfil = undefined; if(document.getElementById('sec-porra')?.classList.contains('active')) porraRender(); }
      });
      const {data} = await _sb.auth.getSession();
      _porraSesion = data.session;
    }catch(e){
      cont.innerHTML = `<div class="nodata">${tx('No se ha podido cargar la porra. Inténtalo más tarde.','Ezin izan da porra kargatu. Saiatu geroago.')}</div>`;
      return;
    }
    // Quita ?porra=1&code=… de la dirección después de entrar
    if(_porraVuelta) history.replaceState(null, '', location.pathname + '#/porra');
  }
  porraRender();
}

async function porraRender(){
  const cont = porraCont();
  if(!cont || !_sb) return;
  if(!_porraSesion){ cont.innerHTML = htmlPorraEntrar(); porraPintarClasif('prClasifPublica'); return; }
  if(_porraPerfil === undefined){
    const {data} = await _sb.from('perfiles').select('alias').eq('id', _porraSesion.user.id).maybeSingle();
    _porraPerfil = data;
  }
  if(!_porraPerfil){ cont.innerHTML = htmlPorraAlias(); return; }
  const tabs = [['pronosticar', tx('Pronosticar','Iragarri')], ['mios', tx('Mis pronósticos','Nire iragarpenak')],
    ['clasificacion', tx('Clasificación','Sailkapena')], ['normas', tx('Normas','Arauak')]];
  cont.innerHTML = `
    <div class="pr-user"><span>👤 <b>${h(_porraPerfil.alias)}</b></span>
      <button class="btn-ghost pr-salir" onclick="porraSalir()">${tx('Salir','Irten')}</button></div>
    <div class="rk-tabs" role="tablist">${tabs.map(([k,l])=>
      `<button class="rk-tab${_porraTab===k?' on':''}" role="tab" aria-selected="${_porraTab===k}" onclick="porraSetTab('${k}')">${l}</button>`).join('')}</div>
    <div id="prPanel"><div class="cart-loading">⟳</div></div>`;
  const panel = document.getElementById('prPanel');
  if(_porraTab==='pronosticar') porraPintarPronosticar(panel);
  else if(_porraTab==='mios') porraPintarMios(panel);
  else if(_porraTab==='clasificacion'){ panel.innerHTML = `<div id="prClasif"></div>`; porraPintarClasif('prClasif'); }
  else panel.innerHTML = htmlPorraNormas(true);
}

function porraSetTab(k){ _porraTab = k; porraRender(); }

// ── Entrar ──────────────────────────────────────────────────
function htmlPorraEntrar(){
  return `
  <div class="pr-hero">
    <h3>${tx('¿Quién ganará? Demuéstralo.','Nork irabaziko du? Erakutsi.')}</h3>
    <p>${tx('Pronostica los partidos de la cartelera, suma puntos cuando aciertes y compite en la clasificación con el resto de aficionados. Gratis y sin dinero de por medio.',
            'Iragarri kartelerako partidak, asmatzen duzunean puntuak batu eta lehiatu sailkapenean beste zaleekin. Doan eta dirurik gabe.')}</p>
    <button class="btn pr-google" onclick="porraEntrarGoogle()">
      <svg viewBox="0 0 48 48" aria-hidden="true"><path fill="#FFC107" d="M43.6 20.5H42V20H24v8h11.3C33.7 32.7 29.2 36 24 36c-6.6 0-12-5.4-12-12s5.4-12 12-12c3.1 0 5.8 1.2 7.9 3.1l5.7-5.7C34 6.1 29.3 4 24 4 12.9 4 4 12.9 4 24s8.9 20 20 20 20-8.9 20-20c0-1.3-.1-2.4-.4-3.5z"/><path fill="#FF3D00" d="m6.3 14.7 6.6 4.8C14.7 15.1 19 12 24 12c3.1 0 5.8 1.2 7.9 3.1l5.7-5.7C34 6.1 29.3 4 24 4 16.3 4 9.7 8.3 6.3 14.7z"/><path fill="#4CAF50" d="M24 44c5.2 0 9.9-2 13.4-5.2l-6.2-5.2C29.2 35.1 26.7 36 24 36c-5.2 0-9.6-3.3-11.3-8l-6.5 5C9.5 39.6 16.2 44 24 44z"/><path fill="#1976D2" d="M43.6 20.5H42V20H24v8h11.3c-.8 2.2-2.2 4.2-4.1 5.6l6.2 5.2C37 39.2 44 34 44 24c0-1.3-.1-2.4-.4-3.5z"/></svg>
      ${tx('Entrar con Google','Google-rekin sartu')}</button>
    <details class="pr-email"><summary>${tx('Prefiero entrar con mi correo','Nire posta elektronikoarekin sartu nahi dut')}</summary>
      <form onsubmit="porraEntrarEmail(event)">
        <input type="email" id="prEmail" required placeholder="tu@email.com" aria-label="Email">
        <button class="btn-ghost" type="submit">${tx('Enviarme el enlace','Esteka bidali')}</button>
      </form>
      <p class="pr-help" id="prEmailMsg">${tx('Te llegará un enlace para entrar, sin contraseña. Ábrelo en este mismo navegador.','Sartzeko esteka bat jasoko duzu, pasahitzik gabe. Ireki nabigatzaile honetan bertan.')}</p>
    </details>
  </div>
  <div class="pr-card"><h4>${tx('Clasificación general','Sailkapen orokorra')}</h4><div id="prClasifPublica"></div></div>
  ${htmlPorraNormas(false)}`;
}

function porraRedirect(){ return location.origin + location.pathname + '?porra=1'; }

async function porraEntrarGoogle(){
  const {error} = await _sb.auth.signInWithOAuth({provider: 'google', options: {redirectTo: porraRedirect()}});
  if(error) alert(tx('No se ha podido entrar con Google: ','Ezin izan da Google-rekin sartu: ') + error.message);
}

async function porraEntrarEmail(ev){
  ev.preventDefault();
  const email = document.getElementById('prEmail').value.trim();
  const msg = document.getElementById('prEmailMsg');
  const {error} = await _sb.auth.signInWithOtp({email, options: {emailRedirectTo: porraRedirect()}});
  msg.textContent = error
    ? tx('No se ha podido enviar: ','Ezin izan da bidali: ') + error.message
    : tx('✓ Enlace enviado. Mira tu correo (y la carpeta de spam).','✓ Esteka bidalita. Begiratu zure posta (eta spam karpeta).');
}

async function porraSalir(){ await _sb.auth.signOut(); _porraSesion = null; _porraPerfil = undefined; porraRender(); }

// ── Alias ───────────────────────────────────────────────────
function htmlPorraAlias(){
  return `<div class="pr-hero">
    <h3>${tx('Elige tu nombre en la porra','Aukeratu zure izena porran')}</h3>
    <p>${tx('Es el que verán los demás en la clasificación (de 3 a 20 letras o números).','Besteek sailkapenean ikusiko dutena (3tik 20ra hizki edo zenbaki).')}</p>
    <form onsubmit="porraGuardarAlias(event)" class="pr-alias">
      <input id="prAlias" required minlength="3" maxlength="20" pattern="[A-Za-z0-9ÁÉÍÓÚÜÑáéíóúüñ _.\\-]+" aria-label="Alias">
      <button class="btn" type="submit">${tx('Empezar','Hasi')}</button>
    </form>
    <p class="pr-help" id="prAliasMsg"></p>
    <p class="pr-help">${tx('Has entrado como','Honela sartu zara:')} ${h(_porraSesion.user.email||'')} · <a href="#" onclick="porraSalir();return false;">${tx('Salir','Irten')}</a></p>
  </div>`;
}

async function porraGuardarAlias(ev){
  ev.preventDefault();
  const alias = document.getElementById('prAlias').value.trim();
  const {error} = await _sb.from('perfiles').insert({id: _porraSesion.user.id, alias});
  if(error){
    document.getElementById('prAliasMsg').textContent = error.code==='23505'
      ? tx('Ese nombre ya está cogido. Prueba con otro.','Izen hori hartuta dago. Saiatu beste batekin.')
      : tx('No se ha podido guardar: ','Ezin izan da gorde: ') + error.message;
    return;
  }
  _porraPerfil = {alias};
  porraRender();
}

// ── Pronosticar ─────────────────────────────────────────────
function porraEquipo(eq){ return eq.map(n=>h(n)).join(' – '); }
function porraFecha(iso){
  return new Date(iso).toLocaleString(LANG==='eu'?'eu-ES':'es-ES', {weekday:'short', day:'numeric', month:'short', hour:'2-digit', minute:'2-digit'});
}

async function porraPintarPronosticar(panel){
  const ahora = new Date().toISOString();
  const {data: partidos, error} = await _sb.from('porra_partidos').select('*')
    .eq('estado','abierto').gt('inicio', ahora).order('inicio').limit(60);
  if(error){ panel.innerHTML = `<div class="nodata">${h(error.message)}</div>`; return; }
  if(!partidos.length){
    panel.innerHTML = `<div class="nodata">${tx('Ahora mismo no hay partidos abiertos. Vuelve cuando salga la próxima cartelera.','Une honetan ez dago partida irekirik. Itzuli hurrengo kartelera ateratzen denean.')}</div>`;
    return;
  }
  const {data: mios} = await _sb.from('porra_pronosticos').select('partido,ganador,tantos_perdedor')
    .eq('usuario', _porraSesion.user.id).in('partido', partidos.map(p=>p.id));
  const mio = Object.fromEntries((mios||[]).map(x=>[x.partido, x]));
  let html = `<p class="pr-help">${tx('Elige el ganador y, si quieres, los tantos del perdedor. Puedes cambiarlo hasta la hora de la velada.','Aukeratu irabazlea eta, nahi baduzu, galtzailearen tantoak. Jaialdiaren ordura arte alda dezakezu.')}</p>`;
  let velada = '';
  partidos.forEach(p=>{
    const v = p.inicio + p.fronton;
    if(v !== velada){
      velada = v;
      html += `<div class="pr-velada">${h(porraFecha(p.inicio))} · ${h(p.fronton||'')}</div>`;
    }
    const m = mio[p.id] || {};
    const prob = probVictoria(p.eq1.map(resolverPelotari).filter(Boolean), p.eq2.map(resolverPelotari).filter(Boolean));
    const pct = prob===null ? null : Math.round(prob*100);
    const opciones = ['<option value="">—</option>'].concat([...Array(22).keys()].map(i=>
      `<option value="${i}"${m.tantos_perdedor===i?' selected':''}>22 – ${i}</option>`)).join('');
    html += `<div class="pr-partido" data-id="${h(p.id)}">
      <div class="pr-comp">${h(tComp(p.competicion||''))}${p.fase?` · ${h(p.fase)}`:''}</div>
      <div class="pr-elige">
        <button class="pr-eq${m.ganador===1?' on':''}" onclick="porraElegir('${esc(p.id)}',1)">${porraEquipo(p.eq1)}${pct!==null?`<small>Elo ${pct}%</small>`:''}</button>
        <span class="an-muted">vs</span>
        <button class="pr-eq${m.ganador===2?' on':''}" onclick="porraElegir('${esc(p.id)}',2)">${porraEquipo(p.eq2)}${pct!==null?`<small>Elo ${100-pct}%</small>`:''}</button>
      </div>
      <label class="pr-tantos">${tx('Resultado','Emaitza')}
        <select onchange="porraTantos('${esc(p.id)}',this.value)"${m.ganador?'':' disabled'}>${opciones}</select></label>
      <span class="pr-ok" aria-live="polite"></span>
    </div>`;
  });
  panel.innerHTML = html;
}

async function porraGuardar(id, cambios){
  const fila = document.querySelector(`.pr-partido[data-id="${CSS.escape(id)}"]`);
  const ok = fila?.querySelector('.pr-ok');
  const {error} = await _sb.from('porra_pronosticos').upsert({partido: id, ...cambios}, {onConflict: 'usuario,partido'});
  if(ok){
    ok.textContent = error ? tx('✗ Cerrado o sin conexión','✗ Itxita edo konexiorik gabe') : tx('✓ Guardado','✓ Gordeta');
    ok.classList.toggle('mal', !!error);
  }
  return !error;
}

async function porraElegir(id, ganador){
  const fila = document.querySelector(`.pr-partido[data-id="${CSS.escape(id)}"]`);
  const sel = fila.querySelector('select');
  const tantos = sel.value==='' ? null : +sel.value;
  if(await porraGuardar(id, {ganador, tantos_perdedor: tantos})){
    fila.querySelectorAll('.pr-eq').forEach((b,i)=>b.classList.toggle('on', i+1===ganador));
    sel.disabled = false;
  }
}

async function porraTantos(id, valor){
  const fila = document.querySelector(`.pr-partido[data-id="${CSS.escape(id)}"]`);
  const ganador = [...fila.querySelectorAll('.pr-eq')].findIndex(b=>b.classList.contains('on')) + 1;
  if(ganador) await porraGuardar(id, {ganador, tantos_perdedor: valor==='' ? null : +valor});
}

// ── Mis pronósticos ─────────────────────────────────────────
async function porraPintarMios(panel){
  const {data, error} = await _sb.from('porra_puntuados').select('*')
    .eq('usuario', _porraSesion.user.id).order('inicio', {ascending: false}).limit(80);
  if(error){ panel.innerHTML = `<div class="nodata">${h(error.message)}</div>`; return; }
  if(!data.length){ panel.innerHTML = `<div class="nodata">${tx('Todavía no has pronosticado ningún partido.','Oraindik ez duzu partidarik iragarri.')}</div>`; return; }
  const total = data.reduce((s,x)=>s+(x.puntos||0), 0);
  const jugados = data.filter(x=>x.puntos!==null);
  const aciertos = jugados.filter(x=>x.puntos>=3).length;
  panel.innerHTML = `
    <div class="an-kpis">
      <div><div class="an-kpi-v">${total}</div><div class="an-kpi-l">${tx('Puntos','Puntuak')}</div></div>
      <div><div class="an-kpi-v">${aciertos}/${jugados.length}</div><div class="an-kpi-l">${tx('Ganadores acertados','Asmatutako irabazleak')}</div></div>
    </div>
    <div class="pr-wrap"><table class="pr-tabla"><tbody>${data.map(x=>{
      const estado = x.estado==='anulado' ? `<span class="an-muted">${tx('Anulado','Baliogabea')}</span>`
        : x.puntos===null ? `<span class="an-muted">${tx('Pendiente','Zain')}</span>`
        : `<b class="pr-pts p${x.puntos}">+${x.puntos}</b>`;
      const res = x.puntos1!==null && x.puntos1!==undefined ? `${x.puntos1}–${x.puntos2}` : '';
      const tantos = x.tantos_perdedor!==null ? ` (22–${x.tantos_perdedor})` : '';
      return `<tr><td class="pr-f">${h(porraFecha(x.inicio))}</td>
        <td><span class="${x.ganador===1?'pr-elegido':''}">${porraEquipo(x.eq1)}</span> <span class="an-muted">vs</span>
            <span class="${x.ganador===2?'pr-elegido':''}">${porraEquipo(x.eq2)}</span><span class="an-muted">${tantos}</span></td>
        <td class="pr-n">${res}</td><td class="pr-n">${estado}</td></tr>`;
    }).join('')}</tbody></table></div>`;
}

// ── Clasificación ───────────────────────────────────────────
function porraDesde(){
  const d = new Date(); d.setHours(0,0,0,0);
  if(_porraClasif==='semana') d.setDate(d.getDate() - ((d.getDay()+6)%7));      // lunes
  else if(_porraClasif==='mes') d.setDate(1);
  else return null;
  return d.toISOString();
}

function porraSetClasif(k, id){ _porraClasif = k; porraPintarClasif(id); }

async function porraPintarClasif(id){
  const cont = document.getElementById(id);
  if(!cont) return;
  const filtros = [['general', tx('Temporada','Denboraldia')], ['mes', tx('Este mes','Hilabete hau')], ['semana', tx('Esta semana','Aste hau')]];
  cont.innerHTML = `<div class="pr-filtros">${filtros.map(([k,l])=>
    `<button class="pill${_porraClasif===k?' on':''}" onclick="porraSetClasif('${k}','${id}')">${l}</button>`).join('')}</div>
    <div class="cart-loading">⟳</div>`;
  const {data, error} = await _sb.rpc('porra_clasificacion', {desde: porraDesde()});
  const tabla = cont.querySelector('.cart-loading');
  if(error){ tabla.outerHTML = `<div class="nodata">${h(error.message)}</div>`; return; }
  if(!data.length){ tabla.outerHTML = `<div class="nodata">${tx('Aún no hay partidos puntuados en este periodo.','Oraindik ez dago puntuatutako partidarik aldi honetan.')}</div>`; return; }
  const yo = _porraSesion?.user?.id;
  let pos = 0, prev = null;
  tabla.outerHTML = `<div class="pr-wrap"><table class="pr-tabla pr-clasif">
    <thead><tr><th>#</th><th>${tx('Nombre','Izena')}</th><th class="pr-n">${tx('Pts','Ptu')}</th>
      <th class="pr-n" title="${tx('Ganadores acertados','Asmatutako irabazleak')}">✓</th>
      <th class="pr-n" title="${tx('Resultados exactos','Emaitza zehatzak')}">🎯</th><th class="pr-n">PJ</th></tr></thead>
    <tbody>${data.map((r,i)=>{
      if(r.puntos!==prev){ pos = i+1; prev = r.puntos; }
      return `<tr class="${r.usuario===yo?'pr-yo':''}"><td>${pos<=3?['🥇','🥈','🥉'][pos-1]:pos}</td><td>${h(r.alias)}</td>
        <td class="pr-n"><b>${r.puntos}</b></td><td class="pr-n">${r.aciertos}</td><td class="pr-n">${r.exactos}</td><td class="pr-n">${r.jugados}</td></tr>`;
    }).join('')}</tbody></table></div>`;
}

// ── Normas y privacidad ─────────────────────────────────────
function htmlPorraNormas(conCuenta){
  return `<div class="pr-card pr-normas">
    <h4>${tx('Cómo se puntúa','Nola puntuatzen den')}</h4>
    <ul>
      <li>${tx('<b>3 puntos</b> por acertar el ganador.','<b>3 puntu</b> irabazlea asmatzeagatik.')}</li>
      <li>${tx('<b>+3</b> si además aciertas los tantos exactos del perdedor (6 en total), o <b>+1</b> si te quedas a 2 tantos o menos.',
               '<b>+3</b> galtzailearen tanto zehatzak ere asmatzen badituzu (6 guztira), edo <b>+1</b> 2 tanto edo gutxiagora geratzen bazara.')}</li>
      <li>${tx('Se puede pronosticar y cambiar hasta la hora de inicio de la velada. Los pronósticos de los demás se ven cuando empieza.',
               'Jaialdiaren hasiera ordura arte iragarri eta alda daiteke. Besteen iragarpenak hasten denean ikusten dira.')}</li>
      <li>${tx('Si cambia el cartel o el partido no se juega, ese pronóstico se anula y no cuenta.',
               'Kartela aldatzen bada edo partida ez bada jokatzen, iragarpen hori baliogabetu egiten da eta ez da kontatzen.')}</li>
      <li>${tx('Es un juego gratuito entre aficionados: no hay apuestas ni dinero.','Zaleen arteko doako jokoa da: ez dago apusturik ez dirurik.')}</li>
    </ul>
    <h4>${tx('Tus datos','Zure datuak')}</h4>
    <p class="pr-help">${tx('Solo guardamos tu correo (para entrar), el nombre que elijas y tus pronósticos, en servidores de la Unión Europea (Supabase, Fráncfort). No se comparten con nadie ni se usan para publicidad. Los demás solo ven tu nombre y tus puntos. Puedes borrar tu cuenta y todos tus datos cuando quieras.',
      'Zure posta (sartzeko), aukeratzen duzun izena eta zure iragarpenak bakarrik gordetzen ditugu, Europar Batasuneko zerbitzarietan (Supabase, Frankfurt). Ez dira inorekin partekatzen ez publizitaterako erabiltzen. Besteek zure izena eta puntuak bakarrik ikusten dituzte. Zure kontua eta datu guztiak nahi duzunean ezaba ditzakezu.')}</p>
    ${conCuenta ? `<button class="btn-ghost pr-borrar" onclick="porraBorrarCuenta()">${tx('Borrar mi cuenta','Nire kontua ezabatu')}</button>` : ''}
  </div>`;
}

async function porraBorrarCuenta(){
  if(!confirm(tx('¿Seguro? Se borrarán tu cuenta, tu nombre y todos tus pronósticos. No se puede deshacer.',
                 'Ziur zaude? Zure kontua, izena eta iragarpen guztiak ezabatuko dira. Ezin da desegin.'))) return;
  const {error} = await _sb.rpc('porra_borrar_cuenta');
  if(error){ alert(error.message); return; }
  await porraSalir();
}
