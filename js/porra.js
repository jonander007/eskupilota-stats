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
let _porraVista = 'general', _porraSub = 'pronosticar', _porraLigaSub = 'pronosticar';
let _porraLigaSel = null, _porraSel = null, _porraClasifCtx = {};
let _porraOpc = null;            // meses, años y torneos con partidos en la porra
let _porraLigas = [];            // mis ligas: {id, nombre, codigo, alcance, creador}
// Vuelta del inicio de sesión (Google o enlace del correo): /?porra=1&code=…
const _porraVuelta = /[?&](porra|code|liga)=/.test(location.search);
// Enlace de invitación a una liga (/?porra=1&liga=ABC123): se guarda por si
// antes hay que entrar con Google, que vuelve sin ese parámetro.
(()=>{
  const cod = new URLSearchParams(location.search).get('liga');
  if(cod) try{ localStorage.setItem('porra_liga_pendiente', cod.toUpperCase()); }catch(e){}
})();
function porraLeer(k, def){ try{ const v = localStorage.getItem(k); return v===null ? def : v; }catch(e){ return def; } }
function porraGuardarPref(k, v){ try{ localStorage.setItem(k, v); }catch(e){} }

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
  if(!_porraSesion){ cont.innerHTML = htmlPorraEntrar(); porraPintarClasif('prClasifPublica', {publico: true}); return; }
  if(_porraPerfil === undefined){
    // select('*'): no depende de columnas nuevas que la base de datos aún no tenga
    const {data, error} = await _sb.from('perfiles').select('*').eq('id', _porraSesion.user.id).maybeSingle();
    if(error){
      cont.innerHTML = `<div class="nodata">${tx('No se ha podido cargar tu perfil: ','Ezin izan da zure profila kargatu: ')}${h(porraError(error))}</div>`;
      return;
    }
    _porraPerfil = data;
  }
  if(!_porraPerfil){ cont.innerHTML = htmlPorraAlias(); return; }
  await porraCargarLigas();
  // Invitación pendiente: unirse y abrir esa liga
  const pendiente = porraLeer('porra_liga_pendiente', '');
  if(pendiente){
    try{ localStorage.removeItem('porra_liga_pendiente'); }catch(e){}
    const {data: lid, error} = await _sb.rpc('porra_unirse', {codigo: pendiente});
    if(error) alert(porraError(error));
    else { await porraCargarLigas(); _porraVista = 'ligas'; _porraLigaSel = lid; _porraLigaSub = 'pronosticar'; }
  }
  const vistas = [['general', tx('Porra general','Porra orokorra')],
                  ['ligas', tx('Mis ligas','Nire ligak') + (_porraLigas.length ? ` (${_porraLigas.length})` : '')]];
  cont.innerHTML = `
    <div class="pr-user"><span>👤 <b>${h(_porraPerfil.alias)}</b></span>
      <button class="btn-ghost pr-salir" onclick="porraSalir()">${tx('Salir','Irten')}</button></div>
    <div class="rk-tabs pr-vistas" role="tablist">${vistas.map(([k,l])=>
      `<button class="rk-tab${_porraVista===k?' on':''}" role="tab" aria-selected="${_porraVista===k}" onclick="porraSetVista('${k}')">${l}</button>`).join('')}</div>
    <div id="prPanel"><div class="cart-loading">⟳</div></div>
    <details class="pr-normas-det"><summary>${tx('Normas, puntuación y tus datos','Arauak, puntuazioa eta zure datuak')}</summary>${htmlPorraNormas(true)}</details>`;
  const panel = document.getElementById('prPanel');
  if(_porraVista==='ligas') porraPintarMisLigas(panel); else porraPintarGeneral(panel);
}

function porraSetVista(v){ _porraVista = v; _porraLigaSel = null; porraRender(); }
function porraPanel(){ return document.getElementById('prPanel'); }
function htmlSubtabs(subs, actual, fn){
  return `<div class="pr-subtabs" role="tablist">${subs.map(([k,l])=>
    `<button class="pill${actual===k?' on':''}" role="tab" aria-selected="${actual===k}" onclick="${fn}('${k}')">${l}</button>`).join('')}</div>`;
}

// ── Porra general ───────────────────────────────────────────
// Meses (hora de España) y nombres
function porraMesDe(iso){ return new Date(iso).toLocaleDateString('sv-SE', {timeZone: 'Europe/Madrid'}).slice(0, 7); }
function porraMesActual(){ return porraMesDe(new Date().toISOString()); }
const MESES_EU = ['urtarrila','otsaila','martxoa','apirila','maiatza','ekaina','uztaila','abuztua','iraila','urria','azaroa','abendua'];
function porraNombreMes(m){
  const [y, mm] = m.split('-');
  if(LANG==='eu') return `${y}ko ${MESES_EU[+mm - 1]}`;
  const t = new Date(+y, +mm - 1, 15).toLocaleDateString('es-ES', {month: 'long', year: 'numeric'});
  return t.charAt(0).toUpperCase() + t.slice(1);
}
// Fase del partido en el idioma de la web («octavos» → «Final-zortzirenak»)
function porraFase(f){ const k = 'fase_' + f, v = t(k); return v===k ? f : v; }

// Mensajes de la base de datos (en castellano) traducidos al euskera
const PORRA_ERRORES_EU = {
  'Primero elige tu alias': 'Lehenik aukeratu zure aliasa',
  'Como mucho puedes crear 10 ligas': 'Gehienez 10 liga sor ditzakezu',
  'No existe ninguna liga con ese código': 'Ez dago kode hori duen ligarik',
  'La liga está completa (20 personas)': 'Liga beteta dago (20 lagun)',
};
function porraError(e){
  const m = e?.message || String(e);
  if(LANG!=='eu') return m;
  const k = Object.keys(PORRA_ERRORES_EU).find(x=>m.includes(x));
  return k ? PORRA_ERRORES_EU[k] : m;
}
function porraNombreComp(c){ return `${tComp(c)} ${(c.match(/\b20\d\d\b/)||[''])[0]}`.trim(); }
function porraNombreAlcance(a){ return a.startsWith('mes:') ? porraNombreMes(a.slice(4)) : porraNombreComp(a); }
const SERIE = /\bSerie [AB]\b/;

// Porras de la general: ranking anual, una por mes y una por torneo (serie A, B o entero)
async function porraOpciones(){
  if(_porraOpc) return _porraOpc;
  const {data} = await _sb.from('porra_partidos').select('competicion,categoria,inicio').limit(5000);
  const meses = new Set([porraMesActual()]), comps = new Set();
  (data||[]).forEach(p=>{ meses.add(porraMesDe(p.inicio)); if(p.competicion && p.categoria!=='festival') comps.add(p.competicion); });
  const anio = n => (n.match(/\b(20\d\d)\b/)||[0,''])[1];
  const grupos = {};
  [...comps].forEach(c=>{ const base = c.replace(SERIE, '').replace(/\s+/g, ' ').trim(); (grupos[base] = grupos[base] || []).push(c); });
  const torneos = [];
  Object.keys(grupos).sort((a,b)=>anio(b).localeCompare(anio(a)) || a.localeCompare(b)).forEach(base=>{
    const cs = grupos[base];
    // La etiqueta se calcula al pintar, en el idioma de ese momento
    if(SERIE.test(cs[0])){
      const A = cs[0].replace(SERIE, 'Serie A'), B = cs[0].replace(SERIE, 'Serie B');
      torneos.push({v: 'comp:'+A, l: ()=>porraNombreComp(A)}, {v: 'comp:'+B, l: ()=>porraNombreComp(B)},
                   {v: 'comp:'+A+'|'+B, l: ()=>`${porraNombreComp(base)} · ${tx('entero (A y B)','osoa (A eta B)')}`});
    } else cs.forEach(c=>torneos.push({v: 'comp:'+c, l: ()=>porraNombreComp(c)}));
  });
  const ms = [...meses].sort().reverse();
  const {data: pod} = await _sb.from('porra_podio_torneos').select('competicion,cierre');
  _porraOpc = {meses: ms, anios: [...new Set(ms.map(m=>m.slice(0, 4)))], torneos,
               podios: Object.fromEntries((pod||[]).map(t=>[t.competicion, t.cierre]))};
  return _porraOpc;
}

function porraSelActual(){ return _porraSel || 'mes:' + porraMesActual(); }
function porraSelCtx(sel){
  const [tipo, ...resto] = sel.split(':'); const v = resto.join(':');
  return tipo==='mes' ? {mes: v} : tipo==='anual' ? {anio: +v} : {comps: v.split('|')};
}

function htmlSelPorra(opc, fn){
  const sel = porraSelActual(), actual = porraMesActual();
  const op = (v, l) => `<option value="${h(v)}"${v===sel?' selected':''}>${h(l)}</option>`;
  return `<select class="pr-sel-liga pr-sel-porra" onchange="${fn}(this.value)" aria-label="Porra">
    <optgroup label="${tx('Porras por mes','Hilabeteko porrak')}">${opc.meses.map(m=>op('mes:'+m, porraNombreMes(m) + (m===actual ? tx(' · en curso',' · martxan') : ''))).join('')}</optgroup>
    <optgroup label="${tx('Ranking anual','Urteko sailkapena')}">${opc.anios.map(a=>op('anual:'+a, tx('Ranking anual ','Urteko sailkapena ') + a)).join('')}</optgroup>
    ${opc.torneos.length ? `<optgroup label="${tx('Porras por torneo','Txapelketako porrak')}">${opc.torneos.map(t=>op(t.v, t.l())).join('')}</optgroup>` : ''}
  </select>`;
}

async function porraPintarGeneral(panel){
  const opc = await porraOpciones();
  const subs = [['pronosticar', tx('Pronosticar','Iragarri')], ['clasificacion', tx('Clasificación','Sailkapena')],
                ['mios', tx('Cerrados','Itxitakoak')]];
  panel.innerHTML = `<div class="pr-barra">${htmlSubtabs(subs, _porraSub, 'porraSetSub')}
      ${_porraSub!=='mios' ? htmlSelPorra(opc, 'porraSetSel') : ''}</div>
    <div id="prSub"><div class="cart-loading">⟳</div></div>`;
  const sub = panel.querySelector('#prSub');
  const ctx = porraSelCtx(porraSelActual());
  if(_porraSub==='clasificacion'){ sub.innerHTML = '<div id="prClasif"></div>'; porraPintarClasif(sub.firstChild, ctx); }
  else if(_porraSub==='mios') porraPintarMios(sub);
  else porraPintarPronosticar(sub, {liga: null, ...ctx});
}
function porraSetSub(k){ _porraSub = k; porraPintarGeneral(porraPanel()); }
function porraSetSel(v){ _porraSel = v; porraPintarGeneral(porraPanel()); }

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
const DIAS_EU = ['ig.','al.','ar.','az.','og.','or.','lr.'];
function porraHora(iso){ return new Date(iso).toLocaleTimeString('es-ES', {hour:'2-digit', minute:'2-digit', timeZone:'Europe/Madrid'}); }
function porraFecha(iso){
  const d = new Date(iso);
  const hora = d.toLocaleTimeString('es-ES', {hour:'2-digit', minute:'2-digit', timeZone:'Europe/Madrid'});
  if(LANG==='eu'){
    const [y, m, dd] = d.toLocaleDateString('sv-SE', {timeZone:'Europe/Madrid'}).split('-');
    const dia = DIAS_EU[new Date(+y, +m - 1, +dd).getDay()];
    return `${dia} ${MESES_EU[+m - 1]}k ${+dd}, ${hora}`;
  }
  return d.toLocaleString('es-ES', {weekday:'short', day:'numeric', month:'short', hour:'2-digit', minute:'2-digit', timeZone:'Europe/Madrid'});
}

async function porraCargarLigas(){
  const {data} = await _sb.from('porra_ligas').select('id,nombre,codigo,alcance,creador').order('creada');
  _porraLigas = data || [];
}

function porraEnLiga(l, p){ const a = l.alcance || []; return a.includes(p.competicion) || a.includes('mes:' + porraMesDe(p.inicio)); }

// Dónde cuenta un partido: la general (si está abierto en ella) y mis ligas de esa competición
function porraAmbitos(p){
  const amb = p.pronosticable ? [{liga: null, nombre: tx('General','Orokorra')}] : [];
  _porraLigas.forEach(l=>{ if(porraEnLiga(l, p)) amb.push({liga: l.id, nombre: l.nombre}); });
  return amb;
}

function porraSetMismo(v){ porraGuardarPref('porra_mismo', v ? '1' : '0'); porraRender(); }

// ctx: {liga: null (general) | id de liga, comp: competición para filtrar la general}
async function porraPintarPronosticar(panel, ctx){
  const liga = ctx.liga ? _porraLigas.find(l=>l.id===ctx.liga) : null;
  const admin = !!_porraPerfil?.admin && !liga;
  const {data, error} = await _sb.from('porra_abiertos').select('*').order('inicio').limit(100);
  if(error){ panel.innerHTML = `<div class="nodata">${h(porraError(error))}</div>`; return; }
  const partidos = data.filter(p=> liga ? porraEnLiga(liga, p)
    : (admin || p.pronosticable) && (ctx.mes ? porraMesDe(p.inicio)===ctx.mes : ctx.comps ? ctx.comps.includes(p.competicion) : true));
  let html = admin ? await htmlPorraAdmin() : '';
  const opc = await porraOpciones();
  const compsPodio = (liga ? (liga.alcance||[]) : (ctx.comps||[])).filter(c=>opc.podios[c]);
  if(compsPodio.length) html += await htmlPodios(ctx, compsPodio);
  if(!partidos.length){
    panel.innerHTML = html + `<div class="nodata">${tx('Ahora mismo no hay partidos abiertos. Vuelve cuando salga la próxima cartelera.','Une honetan ez dago partida irekirik. Itzuli hurrengo kartelera ateratzen denean.')}</div>`;
    return;
  }
  const {data: mios} = await _sb.from('porra_pronosticos').select('partido,liga,ganador,tantos_perdedor')
    .eq('usuario', _porraSesion.user.id).in('partido', partidos.map(p=>p.id));
  const mio = Object.fromEntries((mios||[]).map(x=>[x.partido+'|'+(x.liga||''), x]));
  const mismo = porraLeer('porra_mismo', '1') === '1';
  html += `<p class="pr-help">${tx('Elige el ganador y, si quieres, los tantos del perdedor. Puedes cambiarlo hasta 1 hora antes de que empiece.','Aukeratu irabazlea eta, nahi baduzu, galtzailearen tantoak. Hasi baino ordubete lehenago arte alda dezakezu.')}</p>`;
  if(_porraLigas.length) html += `<label class="pr-mismo"><input type="checkbox"${mismo?' checked':''} onchange="porraSetMismo(this.checked)">
    ${tx('Mismo pronóstico para la general y mis ligas','Iragarpen bera orokorrerako eta nire ligetarako')}</label>`;
  const aqui = liga ? liga.id : null;
  let velada = '';
  partidos.forEach(p=>{
    const v = p.inicio + p.fronton;
    if(v !== velada){
      velada = v;
      html += `<div class="pr-velada">${h(porraFecha(p.inicio))} · ${h(p.fronton||'')}
        <span class="pr-cierre">${tx('Cierra','Ixtea')} ${h(porraHora(p.cierre || p.inicio))}</span></div>`;
    }
    const amb = porraAmbitos(p);
    const prob = probVictoria(p.eq1.map(resolverPelotari).filter(Boolean), p.eq2.map(resolverPelotari).filter(Boolean));
    const pct = prob===null ? null : Math.round(prob*100);
    const nombreComp = p.categoria==='festival' ? tx('Festival','Jaialdia') : tComp(p.competicion||'');
    const interruptor = admin ? `<label class="pr-switch" title="${tx('Solo lo ves tú (administrador): abre o cierra este partido en la porra general','Zuk bakarrik ikusten duzu (administratzailea): partida hau porra orokorrean ireki edo itxi')}">
        <input type="checkbox"${p.pronosticable?' checked':''} onchange="porraActivar('${esc(p.id)}',this.checked)">
        <span>⚙️ ${tx('Abierto','Irekita')}</span></label>` : '';
    // Con «mismo pronóstico», una fila que guarda en todos los ámbitos; si no, solo el de esta vista
    const propio = amb.filter(a=>a.liga===aqui);
    const grupos = [mismo ? amb : propio];
    const filas = grupos.map(g=>{
      const m = g.map(a=>mio[p.id+'|'+(a.liga||'')]).find(Boolean) || {};
      const cerrado = !g.length;
      const etiqueta = g.length > 1
        ? `<div class="pr-ambitos">${g.map(a=>`<span class="${a.liga?'pr-liga':'pr-gen'}">${h(a.nombre)}</span>`).join('')}</div>` : '';
      return `<div class="pr-fila" data-partido="${h(p.id)}" data-ligas="${h(g.map(a=>a.liga||'').join(','))}">
        ${etiqueta}
        <div class="pr-elige">
          <button class="pr-eq${m.ganador===1?' on':''}" onclick="porraElegir(this,1)"${cerrado?' disabled':''}>${porraEquipo(p.eq1)}${pct!==null?`<small>Elo ${pct}%</small>`:''}</button>
          <span class="an-muted">vs</span>
          <button class="pr-eq${m.ganador===2?' on':''}" onclick="porraElegir(this,2)"${cerrado?' disabled':''}>${porraEquipo(p.eq2)}${pct!==null?`<small>Elo ${100-pct}%</small>`:''}</button>
          <div class="pr-marca">${porraMarca(1, m.ganador, m.tantos_perdedor, cerrado)}</div><span></span>
          <div class="pr-marca">${porraMarca(2, m.ganador, m.tantos_perdedor, cerrado)}</div>
        </div>
        <div class="pr-pie"><span class="pr-help">${m.ganador || cerrado ? '' : tx('Pulsa quién gana y luego los tantos del otro.','Sakatu nork irabazten duen eta gero bestearen tantoak.')}</span>
          <span class="pr-ok" aria-live="polite"></span></div>
      </div>`;
    }).join('');
    html += `<div class="pr-partido${amb.length?'':' pr-cerrado'}" data-id="${h(p.id)}">
      <div class="pr-comp"><span>${h(nombreComp)}${p.fase?` · ${h(porraFase(p.fase))}`:''}</span>${interruptor}</div>
      ${filas}
    </div>`;
  });
  panel.innerHTML = html;
}

// ── Podio de los torneos individuales ───────────────────────
// Campeón, subcampeón y los dos semifinalistas, hasta 1 hora antes del primer partido.
function porraCandidatos(comp){
  const en = new Set();
  PARTIDOS.filter(p=>p.competicion===comp).forEach(p=>{ [p.equipo1.delantero, p.equipo2.delantero].forEach(n=>n && en.add(n)); });
  (window._porraEnCartel?.[comp] || []).forEach(n=>en.add(n));
  const resto = Object.keys(PELOTARIS).filter(n=>!en.has(n)).sort((a,b)=>(ELO[b]||0)-(ELO[a]||0));
  return {en: [...en].sort((a,b)=>(ELO[b]||0)-(ELO[a]||0)), resto};
}

function htmlSelPelotari(cand, valor, rol, cerrado){
  const op = n => `<option value="${h(n)}"${porraClaveN(n)===porraClaveN(valor)?' selected':''}>${h(n)}</option>`;
  const extra = valor && ![...cand.en, ...cand.resto].some(n=>porraClaveN(n)===porraClaveN(valor)) ? op(valor) : '';
  return `<select data-rol="${rol}"${cerrado?' disabled':''}><option value="">—</option>${extra}
    ${cand.en.length ? `<optgroup label="${tx('En el torneo','Txapelketan')}">${cand.en.map(op).join('')}</optgroup>` : ''}
    <optgroup label="${tx('Otros','Besteak')}">${cand.resto.map(op).join('')}</optgroup></select>`;
}
function porraClaveN(n){ return (n||'').normalize('NFD').replace(/[̀-ͯ]/g,'').toLowerCase().replace(/[^a-z0-9]/g,''); }

async function htmlPodios(ctx, comps){
  const opc = await porraOpciones();
  const mismo = porraLeer('porra_mismo', '1') === '1';
  const {data: mios} = await _sb.from('porra_podios_puntuados').select('*').eq('usuario', _porraSesion.user.id).in('competicion', comps);
  // Pelotaris anunciados en la cartelera de cada torneo (para la lista)
  const {data: cartel} = await _sb.from('porra_partidos').select('competicion,eq1,eq2').in('competicion', comps);
  window._porraEnCartel = {};
  (cartel||[]).forEach(p=>[...p.eq1, ...p.eq2].forEach(n=>{
    const r = resolverPelotari(n); if(r) (window._porraEnCartel[p.competicion] = window._porraEnCartel[p.competicion] || new Set()).add(r);
  }));
  return comps.map(comp=>{
    const cierre = opc.podios[comp], cerrado = new Date(cierre) <= new Date();
    // Dónde se guarda: este ámbito y, con «mismo pronóstico», también la general y las ligas de este torneo
    const aqui = ctx.liga || null;
    let ligas = [aqui];
    if(mismo) ligas = [...new Set([null, ..._porraLigas.filter(l=>(l.alcance||[]).includes(comp)).map(l=>l.id), aqui])];
    const m = (mios||[]).find(x=>(x.liga||null)===aqui) || (mios||[]).find(x=>ligas.includes(x.liga||null)) || {};
    const cand = porraCandidatos(comp);
    const fila = (rol, etiqueta, valor) => `<label class="pr-podio-f"><span>${etiqueta}</span>${htmlSelPelotari(cand, valor, rol, cerrado)}</label>`;
    const puntos = m.puntos!==null && m.puntos!==undefined
      ? `<div class="pr-podio-res">${tx('Podio real','Benetako podioa')}: 🥇 ${h(m.real_campeon)} · 🥈 ${h(m.real_subcampeon)}${(m.real_semis||[]).length ? ` · ${(m.real_semis||[]).map(h).join(', ')}` : ''}
          <b class="pr-pts p${m.puntos>=15?6:m.puntos>0?3:0}">+${m.puntos}</b></div>` : '';
    return `<div class="pr-card pr-podio" data-comp="${h(comp)}" data-ligas="${h(ligas.map(l=>l||'').join(','))}">
      <div class="pr-podio-top"><h4>🏆 ${tx('Podio','Podioa')} · ${h(porraNombreComp(comp))}</h4>
        <span class="pr-cierre">${cerrado ? tx('Cerrado','Itxita') : `${tx('Cierra','Ixtea')} ${h(porraFecha(cierre))}`}</span></div>
      <div class="pr-podio-g">
        ${fila('campeon', '🥇 ' + tx('Campeón','Txapelduna'), m.campeon)}
        ${fila('subcampeon', '🥈 ' + tx('Subcampeón','Txapeldunordea'), m.subcampeon)}
        ${fila('semi1', '🥉 ' + tx('Semifinalista','Finalerdilaria'), m.semi1)}
        ${fila('semi2', '🥉 ' + tx('Semifinalista','Finalerdilaria'), m.semi2)}
      </div>
      ${cerrado ? '' : `<div class="pr-pie"><span class="pr-help">${tx('Campeón 15 · subcampeón 9 · finalista cambiado 5 · semifinalista 3 · pleno +6',
        'Txapelduna 15 · txapeldunordea 9 · finalista trukatua 5 · finalerdilaria 3 · betea +6')}</span>
        <button class="btn" onclick="porraGuardarPodio(this)">${tx('Guardar podio','Podioa gorde')}</button></div>
        <span class="pr-ok" aria-live="polite"></span>`}
      ${puntos}
    </div>`;
  }).join('');
}

async function porraGuardarPodio(btn){
  const card = btn.closest('.pr-podio');
  const v = r => card.querySelector(`select[data-rol="${r}"]`).value || null;
  const [c, s, s1, s2] = ['campeon', 'subcampeon', 'semi1', 'semi2'].map(v);
  const ok = card.querySelector('.pr-ok');
  const nombres = [c, s, s1, s2].filter(Boolean).map(porraClaveN);
  if(!c || !s){ ok.textContent = tx('Elige al menos campeón y subcampeón.','Aukeratu gutxienez txapelduna eta txapeldunordea.'); ok.classList.add('mal'); return; }
  if(new Set(nombres).size !== nombres.length){ ok.textContent = tx('No repitas pelotari.','Ez errepikatu pilotaria.'); ok.classList.add('mal'); return; }
  const filas = card.dataset.ligas.split(',').map(l=>({competicion: card.dataset.comp, liga: l || null,
    campeon: c, subcampeon: s, semi1: s1, semi2: s2}));
  const {error} = await _sb.from('porra_podios').upsert(filas, {onConflict: 'usuario,competicion,liga'});
  ok.textContent = error ? tx('✗ Cerrado o sin conexión','✗ Itxita edo konexiorik gabe') : tx('✓ Podio guardado','✓ Podioa gordeta');
  ok.classList.toggle('mal', !!error);
}

// ── Administración: qué partidos entran en la porra ─────────
async function htmlPorraAdmin(){
  const {data} = await _sb.from('porra_config').select('modo').maybeSingle();
  const modo = data?.modo || 'oficiales';
  const modos = [['oficiales', tx('Solo campeonatos y torneos','Txapelketak eta torneoak bakarrik')],
                 ['todos', tx('Todos los partidos (también festivales)','Partida guztiak (jaialdiak ere)')],
                 ['manual', tx('Elegir a mano','Eskuz aukeratu')]];
  return `<div class="pr-admin">
    <b>⚙️ ${tx('Administración','Administrazioa')}</b>
    <label>${tx('Partidos en la porra','Porrako partidak')}
      <select onchange="porraSetModo(this.value)">${modos.map(([k,l])=>`<option value="${k}"${k===modo?' selected':''}>${l}</option>`).join('')}</select></label>
    <p class="pr-help">${tx('Con el interruptor «⚙️ Abierto» de cada partido lo abres o lo cierras aunque el modo diga otra cosa. Solo tú ves este panel y los partidos cerrados.',
      'Partida bakoitzeko «⚙️ Irekita» etengailuarekin ireki edo itxi egiten duzu, moduak beste zerbait esan arren. Zuk bakarrik ikusten dituzu panel hau eta itxitako partidak.')}</p>
  </div>`;
}

async function porraSetModo(modo){
  const {error} = await _sb.from('porra_config').update({modo}).eq('id', 1);
  if(error){ alert(porraError(error)); return; }
  porraPintarGeneral(porraPanel());
}

async function porraActivar(id, activo){
  const {error} = await _sb.from('porra_partidos').update({activo}).eq('id', id);
  if(error){ alert(porraError(error)); return; }
  porraPintarGeneral(porraPanel());
}

function porraMarca(lado, ganador, tantos, cerrado){
  if(!ganador) return '';
  if(lado===ganador) return '<b class="pr-22">22</b>';
  const opciones = ['<option value="">—</option>'].concat([...Array(22).keys()].map(i=>
    `<option value="${i}"${tantos===i?' selected':''}>${i}</option>`)).join('');
  return `<select onchange="porraTantos(this)" aria-label="${tx('Tantos del perdedor','Galtzailearen tantoak')}"${cerrado?' disabled':''}>${opciones}</select>`;
}

async function porraGuardar(fila, ganador, tantos){
  const partido = fila.dataset.partido;
  const filas = fila.dataset.ligas.split(',').map(l=>({partido, liga: l || null, ganador, tantos_perdedor: tantos}));
  const {error} = await _sb.from('porra_pronosticos').upsert(filas, {onConflict: 'usuario,partido,liga'});
  const ok = fila.querySelector('.pr-ok');
  ok.textContent = error ? tx('✗ Cerrado o sin conexión','✗ Itxita edo konexiorik gabe') : tx('✓ Guardado','✓ Gordeta');
  ok.classList.toggle('mal', !!error);
  return !error;
}

async function porraElegir(btn, ganador){
  const fila = btn.closest('.pr-fila');
  const sel = fila.querySelector('.pr-marca select');
  const tantos = sel && sel.value!=='' ? +sel.value : null;
  if(await porraGuardar(fila, ganador, tantos)){
    fila.querySelectorAll('.pr-eq').forEach((b,i)=>b.classList.toggle('on', i+1===ganador));
    fila.querySelectorAll('.pr-marca').forEach((c,i)=>{ c.innerHTML = porraMarca(i+1, ganador, tantos, false); });
    const ayuda = fila.querySelector('.pr-pie .pr-help'); if(ayuda) ayuda.textContent = '';
  }
}

async function porraTantos(sel){
  const fila = sel.closest('.pr-fila');
  const ganador = [...fila.querySelectorAll('.pr-eq')].findIndex(b=>b.classList.contains('on')) + 1;
  if(ganador) await porraGuardar(fila, ganador, sel.value==='' ? null : +sel.value);
}

// ── Mis pronósticos ─────────────────────────────────────────
// Mis pronósticos ya cerrados de la porra general (los de cada liga se ven en su liga)
async function porraPintarMios(panel){
  const limite = new Date(Date.now() + 3600e3).toISOString();      // se cierran 1 hora antes
  const {data, error} = await _sb.from('porra_puntuados').select('*')
    .eq('usuario', _porraSesion.user.id).is('liga', null).lte('inicio', limite)
    .order('inicio', {ascending: false}).limit(80);
  if(error){ panel.innerHTML = `<div class="nodata">${h(porraError(error))}</div>`; return; }
  const ligas = {};
  const opc = await porraOpciones();
  const {data: todos} = await _sb.from('porra_podios_puntuados').select('*').eq('usuario', _porraSesion.user.id).is('liga', null);
  const pods = (todos||[]).filter(x=>opc.podios[x.competicion] && new Date(opc.podios[x.competicion]) <= new Date());
  if(!data.length && !pods.length){
    panel.innerHTML = `<div class="nodata">${tx('Todavía no tienes pronósticos cerrados en la porra general.','Oraindik ez duzu iragarpen itxirik porra orokorrean.')}</div>`;
    return;
  }
  const htmlPods = (pods||[]).length ? `<h4 class="pr-sub-h">🏆 ${tx('Podios','Podioak')}</h4>
    <div class="pr-wrap"><table class="pr-tabla"><tbody>${pods.map(x=>`<tr>
      <td>${h(porraNombreComp(x.competicion))}</td>
      <td>🥇 ${h(x.campeon)} · 🥈 ${h(x.subcampeon)}${x.semi1||x.semi2 ? ` · 🥉 ${[x.semi1, x.semi2].filter(Boolean).map(h).join(', ')}` : ''}</td>
      <td class="pr-n">${x.puntos===null ? `<span class="an-muted">${tx('Pendiente','Zain')}</span>` : `<b class="pr-pts">+${x.puntos}</b>`}</td></tr>`).join('')}</tbody></table></div>
    <h4 class="pr-sub-h">${tx('Partidos','Partidak')}</h4>` : '';
  const general = data.filter(x=>!x.liga);
  const total = general.reduce((s,x)=>s+(x.puntos||0), 0);
  const jugados = general.filter(x=>x.puntos!==null);
  const aciertos = jugados.filter(x=>x.puntos>=3).length;
  panel.innerHTML = `
    <div class="an-kpis">
      <div><div class="an-kpi-v">${total}</div><div class="an-kpi-l">${tx('Puntos en la general','Puntuak orokorrean')}</div></div>
      <div><div class="an-kpi-v">${aciertos}/${jugados.length}</div><div class="an-kpi-l">${tx('Ganadores acertados','Asmatutako irabazleak')}</div></div>
    </div>
    ${htmlPods}
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

function porraSetSelPublica(v){ _porraSel = v; porraPintarClasif(_porraClasifCtx.id, {publico: true}); }

// ctx: {liga} | {mes} | {comps} | {anio} | {publico} (sin cuenta: se elige la porra aquí mismo)
async function porraPintarClasif(id, ctx){
  const cont = typeof id === 'string' ? document.getElementById(id) : id;
  if(!cont) return;
  _porraClasifCtx = {...ctx, id: cont};
  if(ctx.publico) ctx = porraSelCtx(porraSelActual());
  const sel = _porraClasifCtx.publico ? `<div class="pr-filtros">${htmlSelPorra(await porraOpciones(), 'porraSetSelPublica')}</div>` : '';
  const anual = !!ctx.anio;
  const nota = anual
    ? tx('Cada mes, los 50 primeros de la porra del mes suman de 50 a 1 puntos. Cuentan los meses ya cerrados.',
         'Hilero, hilabeteko porrako lehen 50ek 50etik 1era puntu batzen dituzte. Itxitako hilabeteak kontatzen dira.')
    : ctx.mes ? tx('Al cerrar el mes, los 50 primeros suman de 50 a 1 puntos al ranking anual (columna 🏆). Los empates los deshacen los puntos en partidos oficiales.',
                   'Hilabetea ixtean, lehen 50ek 50etik 1era puntu batzen dituzte urteko sailkapenean (🏆 zutabea). Berdinketak partida ofizialetako puntuek hausten dituzte.')
    : tx('Los empates los deshacen los puntos en partidos oficiales (sin festivales).','Berdinketak partida ofizialetako puntuek hausten dituzte (jaialdirik gabe).');
  cont.innerHTML = `${sel}${nota ? `<p class="pr-help">${nota}</p>` : ''}<div class="cart-loading">⟳</div>`;
  const {data, error} = anual
    ? await _sb.rpc('porra_ranking_anual', {anio: ctx.anio})
    : await _sb.rpc('porra_clasificacion', {liga: ctx.liga || null, competiciones: ctx.comps || null, mes: ctx.mes || null});
  const tabla = cont.querySelector('.cart-loading');
  if(!tabla) return;
  if(error){ tabla.outerHTML = `<div class="nodata">${h(porraError(error))}</div>`; return; }
  if(!data.length){ tabla.outerHTML = `<div class="nodata">${tx('Aún no hay partidos puntuados aquí.','Oraindik ez dago puntuatutako partidarik hemen.')}</div>`; return; }
  const yo = _porraSesion?.user?.id;
  let pos = 0, prev = null;
  const medalla = p => p<=3 ? ['🥇','🥈','🥉'][p-1] : p;
  const cab = anual
    ? `<th>#</th><th>${tx('Nombre','Izena')}</th><th class="pr-n">${tx('Pts','Ptu')}</th>
       <th class="pr-n" title="${tx('Meses ganados','Irabazitako hilabeteak')}">🏆</th>
       <th class="pr-n" title="${tx('Mejor puesto en un mes','Hilabete bateko posturik onena')}">${tx('Mejor','Onena')}</th>
       <th class="pr-n" title="${tx('Meses jugados','Jokatutako hilabeteak')}">${tx('Meses','Hil.')}</th>`
    : `<th>#</th><th>${tx('Nombre','Izena')}</th><th class="pr-n">${tx('Pts','Ptu')}</th>
       <th class="pr-n" title="${tx('Ganadores acertados','Asmatutako irabazleak')}">✓</th>
       <th class="pr-n" title="${tx('Resultados exactos','Emaitza zehatzak')}">🎯</th><th class="pr-n">PJ</th>
       ${ctx.mes ? `<th class="pr-n" title="${tx('Puntos para el ranking anual','Urteko sailkapenerako puntuak')}">🏆</th>` : ''}`;
  tabla.outerHTML = `<div class="pr-wrap"><table class="pr-tabla pr-clasif"><thead><tr>${cab}</tr></thead>
    <tbody>${data.map((r,i)=>{
      // Empate a puntos: decide lo sumado en partidos oficiales
      const clave = anual ? r.puntos : `${r.puntos}|${r.oficiales ?? ''}`;
      if(clave!==prev){ pos = i+1; prev = clave; }
      const celdas = anual
        ? `<td class="pr-n">${r.ganados}</td><td class="pr-n">${r.mejor}${LANG==='eu'?'.':'º'}</td><td class="pr-n">${r.meses}</td>`
        : `<td class="pr-n">${r.aciertos}</td><td class="pr-n">${r.exactos}</td><td class="pr-n">${r.jugados}</td>
           ${ctx.mes ? `<td class="pr-n pr-anual">${pos<=50 ? '+'+(51-pos) : ''}</td>` : ''}`;
      return `<tr class="${r.usuario===yo?'pr-yo':''}"><td>${medalla(pos)}</td><td>${h(r.alias)}</td>
        <td class="pr-n"><b>${r.puntos}</b></td>${celdas}</tr>`;
    }).join('')}</tbody></table></div>`;
}

// ── Ligas privadas ──────────────────────────────────────────
// Competiciones para una liga: las que están en juego o por empezar
// (en la cartelera o de este año sin final), agrupadas sin la serie.
async function porraCompeticionesLiga(){
  const nombres = new Set();
  const {data} = await _sb.from('porra_abiertos').select('competicion,categoria').limit(200);
  (data||[]).forEach(p=>{ if(p.competicion && p.categoria!=='festival') nombres.add(p.competicion); });
  const anio = String(new Date().getFullYear());
  const conFinal = new Set(PARTIDOS.filter(p=>p.fase==='final').map(p=>p.competicion));
  PARTIDOS.forEach(p=>{
    if(['campeonato','torneo','desafio'].includes(p.categoria) && p.competicion.includes(anio) && !conFinal.has(p.competicion))
      nombres.add(p.competicion);
  });
  const grupos = {};
  [...nombres].forEach(n=>{
    const base = n.replace(/\s*\bSerie [AB]\b/, '');
    const g = grupos[base] = grupos[base] || {base, series: /\bSerie [AB]\b/.test(n), nombres: new Set()};
    g.nombres.add(n);
  });
  // Con series: siempre A, B y entero, aunque una de las dos aún no haya empezado
  Object.values(grupos).forEach(g=>{
    if(!g.series) return;
    const n = [...g.nombres][0];
    g.A = n.replace(/\bSerie [AB]\b/, 'Serie A');
    g.B = n.replace(/\bSerie [AB]\b/, 'Serie B');
  });
  return Object.values(grupos).sort((a,b)=>a.base.localeCompare(b.base));
}

function porraEnlaceLiga(cod){ return `${location.origin}/?porra=1&liga=${cod}`; }

async function porraPintarMisLigas(panel){
  if(_porraLigaSel && _porraLigas.some(l=>l.id===_porraLigaSel)) return porraPintarLiga(panel, _porraLigaSel);
  _porraLigaSel = null;
  const lista = _porraLigas.length
    ? `<div class="pr-ligas">${_porraLigas.map(l=>`
        <button class="pr-liga-item" onclick="porraAbrirLiga('${l.id}')">
          <b>${h(l.nombre)}</b><span>${(l.alcance||[]).map(c=>h(porraNombreAlcance(c))).join(' · ')}</span><span class="pr-flecha" aria-hidden="true">›</span>
        </button>`).join('')}</div>`
    : `<p class="pr-help">${tx('Juega con tu cuadrilla: crea una liga privada de una competición (hasta 20 personas) o únete con un código.',
        'Jokatu zure koadrilarekin: sortu txapelketa bateko liga pribatu bat (20 lagun arte) edo batu kode batekin.')}</p>`;
  const grupos = await porraCompeticionesLiga();
  window._porraGrupos = grupos;
  panel.innerHTML = lista + `
    <div class="pr-card"><h4>${tx('Unirme a una liga','Liga batera batu')}</h4>
      <form class="pr-alias" onsubmit="porraUnirse(event)">
        <input id="prCodigo" required maxlength="6" placeholder="ABC123" aria-label="${tx('Código','Kodea')}" style="text-transform:uppercase">
        <button class="btn" type="submit">${tx('Unirme','Batu')}</button></form></div>
    <div class="pr-card"><h4>${tx('Crear una liga','Liga bat sortu')}</h4>
      <form class="pr-crear" onsubmit="porraCrearLiga(event)">
        <input id="prLigaNombre" required minlength="3" maxlength="40" placeholder="${tx('Nombre de la liga','Ligaren izena')}" aria-label="${tx('Nombre de la liga','Ligaren izena')}">
        <div class="pr-series" role="radiogroup">
          <label><input type="radio" name="prLigaTipo" value="torneo" checked onchange="porraPintarSeries()"> ${tx('De un torneo','Txapelketa batekoa')}</label>
          <label><input type="radio" name="prLigaTipo" value="mes" onchange="porraPintarSeries()"> ${tx('De un mes','Hilabete batekoa')}</label>
        </div>
        <select id="prLigaComp" onchange="porraPintarSeries()" aria-label="${tx('Competición','Txapelketa')}">
          ${grupos.map((g,i)=>`<option value="${i}">${h(porraNombreComp(g.base))}</option>`).join('')}</select>
        <select id="prLigaMes" aria-label="${tx('Mes','Hilabetea')}" hidden>
          ${porraProximosMeses().map(m=>`<option value="${m}">${h(porraNombreMes(m))}</option>`).join('')}</select>
        <div id="prLigaSeries" class="pr-series"></div>
        <button class="btn" type="submit">${tx('Crear liga','Liga sortu')}</button>
        <p class="pr-help" id="prLigaMsg"></p></form>
    </div>`;
  porraPintarSeries();
}

function porraAbrirLiga(id){ _porraLigaSel = id; _porraLigaSub = 'pronosticar'; porraPintarMisLigas(porraPanel()); }
function porraCerrarLiga(){ _porraLigaSel = null; porraPintarMisLigas(porraPanel()); }
function porraSetLigaSub(k){ _porraLigaSub = k; porraPintarMisLigas(porraPanel()); }

async function porraPintarLiga(panel, id){
  const l = _porraLigas.find(x=>x.id===id);
  const {data: miembros} = await _sb.from('porra_miembros').select('usuario,perfiles(alias)').eq('liga', id);
  const gente = (miembros||[]).map(m=>m.perfiles?.alias || '—');
  const texto = encodeURIComponent(tx(`Únete a mi liga «${l.nombre}» en la porra de EskupilotaStats: `, `Batu nire «${l.nombre}» ligara EskupilotaStatsen porran: `) + porraEnlaceLiga(l.codigo));
  const subs = [['pronosticar', tx('Pronosticar','Iragarri')], ['cerrados', tx('Cerrados','Itxitakoak')],
                ['clasificacion', tx('Clasificación','Sailkapena')], ['info', tx('Invitar y miembros','Gonbidatu eta kideak')]];
  let cuerpo;
  if(_porraLigaSub==='info') cuerpo = `<div class="pr-card pr-liga-card">
      <div class="pr-codigo">${tx('Código','Kodea')}: <b>${h(l.codigo)}</b>
        <button class="btn-ghost" onclick="navigator.clipboard?.writeText('${porraEnlaceLiga(l.codigo)}');this.textContent='✓'">${tx('Copiar enlace','Esteka kopiatu')}</button>
        <a class="btn-ghost" href="https://wa.me/?text=${texto}" target="_blank" rel="noopener">WhatsApp</a></div>
      <h4>${tx('Miembros','Kideak')} (${gente.length}/20)</h4>
      <p class="pr-help">${gente.map(a=>h(a)).join(', ')}</p>
      <div class="pr-liga-acc">${l.creador===_porraSesion.user.id
        ? `<button class="btn-ghost pr-borrar" onclick="porraBorrarLiga('${l.id}')">${tx('Borrar liga','Liga ezabatu')}</button>`
        : `<button class="btn-ghost pr-borrar" onclick="porraSalirLiga('${l.id}')">${tx('Salir de la liga','Ligatik irten')}</button>`}</div>
    </div>`;
  else cuerpo = '<div id="prSub"><div class="cart-loading">⟳</div></div>';
  panel.innerHTML = `
    <button class="pr-volver" onclick="porraCerrarLiga()">‹ ${tx('Mis ligas','Nire ligak')}</button>
    <div class="pr-liga-top"><h4>${h(l.nombre)}</h4><span class="an-muted">${gente.length}/20</span></div>
    <div class="pr-help">${(l.alcance||[]).map(c=>h(porraNombreAlcance(c))).join(' · ')}</div>
    <div class="pr-barra">${htmlSubtabs(subs, _porraLigaSub, 'porraSetLigaSub')}</div>
    ${cuerpo}`;
  const sub = panel.querySelector('#prSub');
  if(_porraLigaSub==='clasificacion'){ sub.innerHTML = '<div id="prClasif"></div>'; porraPintarClasif(sub.firstChild, {liga: id}); }
  else if(_porraLigaSub==='pronosticar') porraPintarPronosticar(sub, {liga: id});
  else if(_porraLigaSub==='cerrados') porraPintarCerrados(sub, l, miembros||[]);
}

// Partidos ya cerrados de la liga; al pulsar uno, lo que puso cada miembro
async function porraPintarCerrados(panel, l, miembros){
  const limite = new Date(Date.now() + 3600e3).toISOString();
  const {data, error} = await _sb.from('porra_partidos').select('*').lte('inicio', limite).neq('estado', 'anulado')
    .order('inicio', {ascending: false}).limit(300);
  if(error){ panel.innerHTML = `<div class="nodata">${h(porraError(error))}</div>`; return; }
  const ps = data.filter(p=>porraEnLiga(l, p)).slice(0, 60);
  window._porraAlias = Object.fromEntries(miembros.map(m=>[m.usuario, m.perfiles?.alias || '—']));
  if(!ps.length){ panel.innerHTML = `<div class="nodata">${tx('Todavía no se ha cerrado ningún partido de esta liga.','Oraindik ez da liga honetako partidarik itxi.')}</div>`; return; }
  panel.innerHTML = `<p class="pr-help">${tx('Pulsa un partido para ver lo que puso cada uno.','Sakatu partida bat bakoitzak zer jarri zuen ikusteko.')}</p>` + ps.map(p=>`
    <details class="pr-cerrado-item" ontoggle="if(this.open) porraVerPronosticos(this, '${l.id}', '${esc(p.id)}')">
      <summary><span class="pr-f">${h(porraFecha(p.inicio))}</span>
        <span>${porraEquipo(p.eq1)} <span class="an-muted">vs</span> ${porraEquipo(p.eq2)}</span>
        <b class="pr-n">${p.estado==='jugado' ? `${p.puntos1}–${p.puntos2}` : `<span class="an-muted">${tx('Pendiente','Zain')}</span>`}</b></summary>
      <div class="pr-cerrado-lista"><div class="cart-loading">⟳</div></div>
    </details>`).join('');
}

async function porraVerPronosticos(det, liga, partido){
  const cont = det.querySelector('.pr-cerrado-lista');
  if(cont.dataset.cargado) return;
  const {data, error} = await _sb.from('porra_puntuados').select('usuario,ganador,tantos_perdedor,puntos,eq1,eq2')
    .eq('liga', liga).eq('partido', partido);
  if(error){ cont.innerHTML = `<div class="nodata">${h(porraError(error))}</div>`; return; }
  cont.dataset.cargado = '1';
  if(!data.length){ cont.innerHTML = `<p class="pr-help">${tx('Nadie lo pronosticó.','Inork ez zuen iragarri.')}</p>`; return; }
  const yo = _porraSesion.user.id;
  data.sort((a,b)=>(b.puntos??-1)-(a.puntos??-1) || (window._porraAlias[a.usuario]||'').localeCompare(window._porraAlias[b.usuario]||''));
  cont.innerHTML = `<table class="pr-tabla"><tbody>${data.map(x=>`
    <tr class="${x.usuario===yo?'pr-yo':''}"><td>${h(window._porraAlias[x.usuario] || '—')}</td>
      <td>${porraEquipo(x.ganador===1 ? x.eq1 : x.eq2)}${x.tantos_perdedor!==null ? ` <span class="an-muted">(22–${x.tantos_perdedor})</span>` : ''}</td>
      <td class="pr-n">${x.puntos===null ? '' : `<b class="pr-pts p${x.puntos}">+${x.puntos}</b>`}</td></tr>`).join('')}</tbody></table>`;
}

// Este mes y los dos siguientes
function porraProximosMeses(){
  const [y, m] = porraMesActual().split('-').map(Number);
  return [0, 1, 2].map(i=>{ const d = new Date(y, m - 1 + i, 1); return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`; });
}

function porraPintarSeries(){
  const cont = document.getElementById('prLigaSeries');
  const sel = document.getElementById('prLigaComp'), mes = document.getElementById('prLigaMes');
  if(!cont || !sel) return;
  const porMes = document.querySelector('input[name="prLigaTipo"]:checked')?.value === 'mes';
  sel.hidden = porMes || !window._porraGrupos.length; mes.hidden = !porMes;
  const g = window._porraGrupos[+sel.value];
  cont.innerHTML = !porMes && g?.series ? [['A', tx('Serie A','A seriea')], ['B', tx('Serie B','B seriea')], ['AB', tx('Entero (A y B)','Osoa (A eta B)')]]
    .map(([k,l],i)=>`<label><input type="radio" name="prSerie" value="${k}"${i===0?' checked':''}> ${l}</label>`).join('') : '';
}

async function porraCrearLiga(ev){
  ev.preventDefault();
  const porMes = document.querySelector('input[name="prLigaTipo"]:checked')?.value === 'mes';
  let alcance;
  if(porMes) alcance = ['mes:' + document.getElementById('prLigaMes').value];
  else {
    const g = window._porraGrupos[+document.getElementById('prLigaComp').value];
    if(!g){ document.getElementById('prLigaMsg').textContent = tx('No hay torneos en juego: crea una liga de un mes.','Ez dago txapelketarik jokoan: sortu hilabete bateko liga.'); return; }
    const serie = document.querySelector('input[name="prSerie"]:checked')?.value;
    alcance = !g.series ? [...g.nombres] : serie==='A' ? [g.A] : serie==='B' ? [g.B] : [g.A, g.B];
  }
  const nombre = document.getElementById('prLigaNombre').value.trim();
  const {data, error} = await _sb.rpc('porra_crear_liga', {nombre, alcance});
  if(error){ document.getElementById('prLigaMsg').textContent = porraError(error); return; }
  await porraCargarLigas();
  _porraLigaSel = data?.[0]?.id || null; _porraLigaSub = 'info';
  porraRender();
}

async function porraUnirse(ev){
  ev.preventDefault();
  const {data: lid, error} = await _sb.rpc('porra_unirse', {codigo: document.getElementById('prCodigo').value});
  if(error){ alert(porraError(error)); return; }
  await porraCargarLigas();
  _porraLigaSel = lid; _porraLigaSub = 'pronosticar';
  porraRender();
}

async function porraSalirLiga(id){
  if(!confirm(tx('¿Salir de la liga? Tus pronósticos de esta liga dejarán de contar.','Ligatik irten? Liga honetako zure iragarpenek ez dute balioko.'))) return;
  const {error} = await _sb.from('porra_miembros').delete().eq('liga', id).eq('usuario', _porraSesion.user.id);
  if(error){ alert(porraError(error)); return; }
  await porraCargarLigas();
  _porraLigaSel = null;
  porraRender();
}

async function porraBorrarLiga(id){
  if(!confirm(tx('¿Borrar la liga para todos sus miembros? No se puede deshacer.','Liga kide guztientzat ezabatu? Ezin da desegin.'))) return;
  const {error} = await _sb.from('porra_ligas').delete().eq('id', id);
  if(error){ alert(porraError(error)); return; }
  await porraCargarLigas();
  _porraLigaSel = null;
  porraRender();
}

// ── Normas y privacidad ─────────────────────────────────────
function htmlPorraNormas(conCuenta){
  return `<div class="pr-card pr-normas">
    <h4>${tx('Cómo se puntúa','Nola puntuatzen den')}</h4>
    <ul>
      <li>${tx('<b>3 puntos</b> por acertar el ganador.','<b>3 puntu</b> irabazlea asmatzeagatik.')}</li>
      <li>${tx('<b>+3</b> si además aciertas los tantos exactos del perdedor (6 en total), o <b>+1</b> si te quedas a 2 tantos o menos.',
               '<b>+3</b> galtzailearen tanto zehatzak ere asmatzen badituzu (6 guztira), edo <b>+1</b> 2 tanto edo gutxiagora geratzen bazara.')}</li>
      <li>${tx('Se puede pronosticar y cambiar hasta <b>1 hora antes</b> del inicio. Entonces el partido pasa a «Cerrados»: en la general ves lo que pusiste tú y, en cada liga, lo que puso cada miembro.',
               'Hasiera baino <b>ordubete lehenago</b> arte iragarri eta alda daiteke. Orduan partida «Itxitakoak» atalera pasatzen da: orokorrean zuk jarritakoa ikusten duzu eta, liga bakoitzean, kide bakoitzak jarritakoa.')}</li>
      <li>${tx('<b>Podio</b> en los torneos de mano a mano y 4 y medio (hasta 1 hora antes del primer partido): campeón 15, subcampeón 9, finalista en el puesto cambiado 5, cada semifinalista 3 y +6 por el pleno. Suma en la porra del torneo y en sus ligas.',
               '<b>Podioa</b> buruz buruko eta lau t\'erdiko txapelketetan (lehen partida baino ordubete lehenago arte): txapelduna 15, txapeldunordea 9, finalista trukatua 5, finalerdilari bakoitza 3 eta +6 betea. Txapelketako porran eta bere ligetan batzen da.')}</li>
      <li>${tx('Si cambia el cartel o el partido no se juega, ese pronóstico se anula y no cuenta.',
               'Kartela aldatzen bada edo partida ez bada jokatzen, iragarpen hori baliogabetu egiten da eta ez da kontatzen.')}</li>
      <li>${tx('En la porra general hay una porra por mes y una por torneo (serie A, B o entero). Un mismo pronóstico cuenta para las dos.',
               'Porra orokorrean hilabeteko porra bat eta txapelketako bat daude (A, B seriea edo osoa). Iragarpen berak bietan balio du.')}</li>
      <li>${tx('<b>Ranking anual:</b> al cerrar cada mes, los 50 primeros de la porra del mes suman de 50 a 1 puntos (1º 50, 2º 49…).',
               '<b>Urteko sailkapena:</b> hilabete bakoitza ixtean, hilabeteko porrako lehen 50ek 50etik 1era puntu batzen dituzte (1.a 50, 2.a 49…; berdinduek, berdin).')}</li>
      <li>${tx('<b>Empates:</b> a igualdad de puntos, va delante quien más puntos tenga en partidos oficiales (campeonatos, torneos y desafíos, sin festivales).',
               '<b>Berdinketak:</b> puntu berdinekin, partida ofizialetan (txapelketak, torneoak eta desafioak, jaialdirik gabe) puntu gehien dituena doa aurretik.')}</li>
      <li>${tx('Puedes crear ligas privadas de un torneo o de un mes (hasta 20 personas) y pronosticar lo mismo que en la general o distinto.',
               'Txapelketa edo hilabete bateko liga pribatuak sor ditzakezu (20 lagun arte) eta orokorrean bezala edo desberdin iragarri.')}</li>
      <li>${tx('Es un juego gratuito entre aficionados: no hay apuestas ni dinero.','Zaleen arteko doako jokoa da: ez dago apusturik ez dirurik.')}</li>
    </ul>
    <h4>${tx('Tus datos','Zure datuak')}</h4>
    <p class="pr-help">${tx('Solo guardamos tu correo (para entrar), el nombre que elijas y tus pronósticos, en servidores de la Unión Europea (Supabase, Fráncfort). No se comparten con nadie ni se usan para publicidad. Los demás solo ven tu nombre y tus puntos. Puedes borrar tu cuenta y todos tus datos cuando quieras.',
      'Zure posta (sartzeko), aukeratzen duzun izena eta zure iragarpenak bakarrik gordetzen ditugu, Europar Batasuneko zerbitzarietan (Supabase, Frankfurt). Ez dira inorekin partekatzen ez publizitaterako erabiltzen. Besteek zure izena eta puntuak bakarrik ikusten dituzte. Zure kontua eta datu guztiak nahi duzunean ezaba ditzakezu.')}</p>
    <p class="pr-help"><a href="${LANG==='eu'?'/eu':''}/porra/">${tx('Qué es la porra','Zer da porra')}</a> ·
      <a href="${LANG==='eu'?'/eu':''}/privacidad/">${tx('Política de privacidad completa','Pribatutasun politika osoa')}</a></p>
    ${conCuenta ? `<button class="btn-ghost pr-borrar" onclick="porraBorrarCuenta()">${tx('Borrar mi cuenta','Nire kontua ezabatu')}</button>` : ''}
  </div>`;
}

async function porraBorrarCuenta(){
  if(!confirm(tx('¿Seguro? Se borrarán tu cuenta, tu nombre y todos tus pronósticos. No se puede deshacer.',
                 'Ziur zaude? Zure kontua, izena eta iragarpen guztiak ezabatuko dira. Ezin da desegin.'))) return;
  const {error} = await _sb.rpc('porra_borrar_cuenta');
  if(error){ alert(porraError(error)); return; }
  await porraSalir();
}
