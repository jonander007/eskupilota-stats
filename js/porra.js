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
let _porraTab = 'pronosticar', _porraClasif = 'general', _porraClasifLiga = null;
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
  if(!_porraSesion){ cont.innerHTML = htmlPorraEntrar(); porraPintarClasif('prClasifPublica'); return; }
  if(_porraPerfil === undefined){
    // select('*'): no depende de columnas nuevas que la base de datos aún no tenga
    const {data, error} = await _sb.from('perfiles').select('*').eq('id', _porraSesion.user.id).maybeSingle();
    if(error){
      cont.innerHTML = `<div class="nodata">${tx('No se ha podido cargar tu perfil: ','Ezin izan da zure profila kargatu: ')}${h(error.message)}</div>`;
      return;
    }
    _porraPerfil = data;
  }
  if(!_porraPerfil){ cont.innerHTML = htmlPorraAlias(); return; }
  await porraCargarLigas();
  // Invitación pendiente: unirse y abrir la pestaña de ligas
  const pendiente = porraLeer('porra_liga_pendiente', '');
  if(pendiente){
    try{ localStorage.removeItem('porra_liga_pendiente'); }catch(e){}
    const {error} = await _sb.rpc('porra_unirse', {codigo: pendiente});
    if(error) alert(error.message); else { await porraCargarLigas(); _porraTab = 'ligas'; }
  }
  const tabs = [['pronosticar', tx('Pronosticar','Iragarri')], ['mios', tx('Mis pronósticos','Nire iragarpenak')],
    ['clasificacion', tx('Clasificación','Sailkapena')], ['ligas', tx('Ligas','Ligak')], ['normas', tx('Normas','Arauak')]];
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
  else if(_porraTab==='ligas') porraPintarLigas(panel);
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

async function porraCargarLigas(){
  const {data} = await _sb.from('porra_ligas').select('id,nombre,codigo,alcance,creador').order('creada');
  _porraLigas = data || [];
}

// Dónde cuenta un partido: la general (si está abierto en ella) y mis ligas de esa competición
function porraAmbitos(p){
  const amb = p.pronosticable ? [{liga: null, nombre: tx('General','Orokorra')}] : [];
  _porraLigas.forEach(l=>{ if((l.alcance||[]).includes(p.competicion)) amb.push({liga: l.id, nombre: l.nombre}); });
  return amb;
}

function porraSetMismo(v){ porraGuardarPref('porra_mismo', v ? '1' : '0'); porraPintarPronosticar(document.getElementById('prPanel')); }

async function porraPintarPronosticar(panel){
  const admin = !!_porraPerfil?.admin;
  const {data, error} = await _sb.from('porra_abiertos').select('*').order('inicio').limit(100);
  if(error){ panel.innerHTML = `<div class="nodata">${h(error.message)}</div>`; return; }
  const partidos = data.filter(p=>admin || porraAmbitos(p).length);
  let html = admin ? await htmlPorraAdmin() : '';
  if(!partidos.length){
    panel.innerHTML = html + `<div class="nodata">${tx('Ahora mismo no hay partidos abiertos. Vuelve cuando salga la próxima cartelera.','Une honetan ez dago partida irekirik. Itzuli hurrengo kartelera ateratzen denean.')}</div>`;
    return;
  }
  const {data: mios} = await _sb.from('porra_pronosticos').select('partido,liga,ganador,tantos_perdedor')
    .eq('usuario', _porraSesion.user.id).in('partido', partidos.map(p=>p.id));
  const mio = Object.fromEntries((mios||[]).map(x=>[x.partido+'|'+(x.liga||''), x]));
  const mismo = porraLeer('porra_mismo', '1') === '1';
  html += `<p class="pr-help">${tx('Elige el ganador y, si quieres, los tantos del perdedor. Puedes cambiarlo hasta la hora de la velada.','Aukeratu irabazlea eta, nahi baduzu, galtzailearen tantoak. Jaialdiaren ordura arte alda dezakezu.')}</p>`;
  if(_porraLigas.length) html += `<label class="pr-mismo"><input type="checkbox"${mismo?' checked':''} onchange="porraSetMismo(this.checked)">
    ${tx('Mismo pronóstico para la general y mis ligas','Iragarpen bera orokorrerako eta nire ligetarako')}</label>`;
  let velada = '';
  partidos.forEach(p=>{
    const v = p.inicio + p.fronton;
    if(v !== velada){
      velada = v;
      html += `<div class="pr-velada">${h(porraFecha(p.inicio))} · ${h(p.fronton||'')}</div>`;
    }
    const amb = porraAmbitos(p);
    const prob = probVictoria(p.eq1.map(resolverPelotari).filter(Boolean), p.eq2.map(resolverPelotari).filter(Boolean));
    const pct = prob===null ? null : Math.round(prob*100);
    const nombreComp = p.categoria==='festival' ? tx('Festival','Jaialdia') : tComp(p.competicion||'');
    const interruptor = admin ? `<label class="pr-switch" title="${tx('Abierto en la general','Orokorrean irekita')}">
        <input type="checkbox"${p.pronosticable?' checked':''} onchange="porraActivar('${esc(p.id)}',this.checked)">
        <span>${tx('En la general','Orokorrean')}</span></label>` : '';
    // Una fila para todos los ámbitos, o una por ámbito
    const grupos = !amb.length ? [[]] : (mismo || amb.length===1) ? [amb] : amb.map(a=>[a]);
    const filas = grupos.map(g=>{
      const m = g.map(a=>mio[p.id+'|'+(a.liga||'')]).find(Boolean) || {};
      const cerrado = !g.length;
      const opciones = ['<option value="">—</option>'].concat([...Array(22).keys()].map(i=>
        `<option value="${i}"${m.tantos_perdedor===i?' selected':''}>22 – ${i}</option>`)).join('');
      const etiqueta = g.length && (_porraLigas.length || !p.pronosticable)
        ? `<div class="pr-ambitos">${g.map(a=>`<span class="${a.liga?'pr-liga':'pr-gen'}">${h(a.nombre)}</span>`).join('')}</div>` : '';
      return `<div class="pr-fila" data-partido="${h(p.id)}" data-ligas="${h(g.map(a=>a.liga||'').join(','))}">
        ${etiqueta}
        <div class="pr-elige">
          <button class="pr-eq${m.ganador===1?' on':''}" onclick="porraElegir(this,1)"${cerrado?' disabled':''}>${porraEquipo(p.eq1)}${pct!==null?`<small>Elo ${pct}%</small>`:''}</button>
          <span class="an-muted">vs</span>
          <button class="pr-eq${m.ganador===2?' on':''}" onclick="porraElegir(this,2)"${cerrado?' disabled':''}>${porraEquipo(p.eq2)}${pct!==null?`<small>Elo ${100-pct}%</small>`:''}</button>
        </div>
        <div class="pr-pie"><label class="pr-tantos">${tx('Resultado','Emaitza')}
          <select onchange="porraTantos(this)"${m.ganador && !cerrado?'':' disabled'}>${opciones}</select></label>
          <span class="pr-ok" aria-live="polite"></span></div>
      </div>`;
    }).join('');
    html += `<div class="pr-partido${amb.length?'':' pr-cerrado'}" data-id="${h(p.id)}">
      <div class="pr-comp"><span>${h(nombreComp)}${p.fase?` · ${h(p.fase)}`:''}</span>${interruptor}</div>
      ${filas}
    </div>`;
  });
  panel.innerHTML = html;
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
    <p class="pr-help">${tx('Con el interruptor «En la porra» de cada partido lo abres o lo cierras aunque el modo diga otra cosa. Solo tú ves este panel y los partidos cerrados.',
      'Partida bakoitzeko «Porran» etengailuarekin ireki edo itxi egiten duzu, moduak beste zerbait esan arren. Zuk bakarrik ikusten dituzu panel hau eta itxitako partidak.')}</p>
  </div>`;
}

async function porraSetModo(modo){
  const {error} = await _sb.from('porra_config').update({modo}).eq('id', 1);
  if(error){ alert(error.message); return; }
  porraPintarPronosticar(document.getElementById('prPanel'));
}

async function porraActivar(id, activo){
  const {error} = await _sb.from('porra_partidos').update({activo}).eq('id', id);
  if(error){ alert(error.message); return; }
  porraPintarPronosticar(document.getElementById('prPanel'));
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
  const sel = fila.querySelector('select');
  if(await porraGuardar(fila, ganador, sel.value==='' ? null : +sel.value)){
    fila.querySelectorAll('.pr-eq').forEach((b,i)=>b.classList.toggle('on', i+1===ganador));
    sel.disabled = false;
  }
}

async function porraTantos(sel){
  const fila = sel.closest('.pr-fila');
  const ganador = [...fila.querySelectorAll('.pr-eq')].findIndex(b=>b.classList.contains('on')) + 1;
  if(ganador) await porraGuardar(fila, ganador, sel.value==='' ? null : +sel.value);
}

// ── Mis pronósticos ─────────────────────────────────────────
async function porraPintarMios(panel){
  const {data, error} = await _sb.from('porra_puntuados').select('*')
    .eq('usuario', _porraSesion.user.id).order('inicio', {ascending: false}).limit(80);
  if(error){ panel.innerHTML = `<div class="nodata">${h(error.message)}</div>`; return; }
  if(!data.length){ panel.innerHTML = `<div class="nodata">${tx('Todavía no has pronosticado ningún partido.','Oraindik ez duzu partidarik iragarri.')}</div>`; return; }
  const ligas = Object.fromEntries(_porraLigas.map(l=>[l.id, l.nombre]));
  const general = data.filter(x=>!x.liga);
  const total = general.reduce((s,x)=>s+(x.puntos||0), 0);
  const jugados = general.filter(x=>x.puntos!==null);
  const aciertos = jugados.filter(x=>x.puntos>=3).length;
  panel.innerHTML = `
    <div class="an-kpis">
      <div><div class="an-kpi-v">${total}</div><div class="an-kpi-l">${tx('Puntos en la general','Puntuak orokorrean')}</div></div>
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
            <span class="${x.ganador===2?'pr-elegido':''}">${porraEquipo(x.eq2)}</span><span class="an-muted">${tantos}</span>
            <div class="pr-ambitos"><span class="${x.liga?'pr-liga':'pr-gen'}">${h(x.liga ? (ligas[x.liga]||tx('Liga','Liga')) : tx('General','Orokorra'))}</span></div></td>
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
function porraSetClasifLiga(lid, id){ _porraClasifLiga = lid || null; porraPintarClasif(id); }

async function porraPintarClasif(id){
  const cont = document.getElementById(id);
  if(!cont) return;
  const filtros = [['general', tx('Temporada','Denboraldia')], ['mes', tx('Este mes','Hilabete hau')], ['semana', tx('Esta semana','Aste hau')]];
  const ligas = _porraSesion ? _porraLigas : [];
  if(_porraClasifLiga && !ligas.some(l=>l.id===_porraClasifLiga)) _porraClasifLiga = null;
  const selLiga = ligas.length ? `<select class="pr-sel-liga" onchange="porraSetClasifLiga(this.value,'${id}')" aria-label="${tx('Clasificación','Sailkapena')}">
      <option value="">${tx('General','Orokorra')}</option>${ligas.map(l=>`<option value="${l.id}"${l.id===_porraClasifLiga?' selected':''}>${h(l.nombre)}</option>`).join('')}</select>` : '';
  cont.innerHTML = `<div class="pr-filtros">${selLiga}${filtros.map(([k,l])=>
    `<button class="pill${_porraClasif===k?' on':''}" onclick="porraSetClasif('${k}','${id}')">${l}</button>`).join('')}</div>
    <div class="cart-loading">⟳</div>`;
  const {data, error} = await _sb.rpc('porra_clasificacion', {desde: porraDesde(), liga: _porraClasifLiga});
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

async function porraPintarLigas(panel){
  const {data: miembros} = _porraLigas.length
    ? await _sb.from('porra_miembros').select('liga,usuario,perfiles(alias)').in('liga', _porraLigas.map(l=>l.id))
    : {data: []};
  const porLiga = {};
  (miembros||[]).forEach(m=>(porLiga[m.liga] = porLiga[m.liga] || []).push(m.perfiles?.alias || '—'));
  const yo = _porraSesion.user.id;
  let html = _porraLigas.map(l=>{
    const gente = porLiga[l.id] || [];
    const texto = encodeURIComponent(tx(`Únete a mi liga «${l.nombre}» en la porra de EskupilotaStats: `, `Batu nire «${l.nombre}» ligara EskupilotaStatsen porran: `) + porraEnlaceLiga(l.codigo));
    return `<div class="pr-card pr-liga-card">
      <div class="pr-liga-top"><h4>${h(l.nombre)}</h4><span class="an-muted">${gente.length}/20</span></div>
      <div class="pr-help">${(l.alcance||[]).map(c=>h(tComp(c))).join(' · ')}</div>
      <div class="pr-codigo">${tx('Código','Kodea')}: <b>${h(l.codigo)}</b>
        <button class="btn-ghost" onclick="navigator.clipboard?.writeText('${porraEnlaceLiga(l.codigo)}');this.textContent='✓'">${tx('Copiar enlace','Esteka kopiatu')}</button>
        <a class="btn-ghost" href="https://wa.me/?text=${texto}" target="_blank" rel="noopener">WhatsApp</a></div>
      <div class="pr-help">${gente.map(a=>h(a)).join(', ')}</div>
      <div class="pr-liga-acc">
        <button class="btn-ghost" onclick="_porraClasifLiga='${l.id}';porraSetTab('clasificacion')">${tx('Ver clasificación','Sailkapena ikusi')}</button>
        ${l.creador===yo
          ? `<button class="btn-ghost pr-borrar" onclick="porraBorrarLiga('${l.id}')">${tx('Borrar liga','Liga ezabatu')}</button>`
          : `<button class="btn-ghost pr-borrar" onclick="porraSalirLiga('${l.id}')">${tx('Salir de la liga','Ligatik irten')}</button>`}
      </div></div>`;
  }).join('');
  if(!_porraLigas.length) html = `<p class="pr-help">${tx('Juega con tu cuadrilla: crea una liga privada de una competición (hasta 20 personas) o únete con un código.',
    'Jokatu zure koadrilarekin: sortu txapelketa bateko liga pribatu bat (20 lagun arte) edo batu kode batekin.')}</p>`;
  const grupos = await porraCompeticionesLiga();
  window._porraGrupos = grupos;
  html += `
    <div class="pr-card"><h4>${tx('Unirme a una liga','Liga batera batu')}</h4>
      <form class="pr-alias" onsubmit="porraUnirse(event)">
        <input id="prCodigo" required maxlength="6" placeholder="ABC123" aria-label="${tx('Código','Kodea')}" style="text-transform:uppercase">
        <button class="btn" type="submit">${tx('Unirme','Batu')}</button></form></div>
    <div class="pr-card"><h4>${tx('Crear una liga','Liga bat sortu')}</h4>
      ${grupos.length ? `<form class="pr-crear" onsubmit="porraCrearLiga(event)">
        <input id="prLigaNombre" required minlength="3" maxlength="40" placeholder="${tx('Nombre de la liga','Ligaren izena')}" aria-label="${tx('Nombre de la liga','Ligaren izena')}">
        <select id="prLigaComp" onchange="porraPintarSeries()" aria-label="${tx('Competición','Txapelketa')}">
          ${grupos.map((g,i)=>`<option value="${i}">${h(tComp(g.base))}</option>`).join('')}</select>
        <div id="prLigaSeries" class="pr-series"></div>
        <button class="btn" type="submit">${tx('Crear liga','Liga sortu')}</button>
        <p class="pr-help" id="prLigaMsg"></p></form>`
      : `<p class="pr-help">${tx('Ahora mismo no hay ninguna competición en juego o por empezar.','Une honetan ez dago txapelketarik jokoan edo hastear.')}</p>`}
    </div>`;
  panel.innerHTML = html;
  porraPintarSeries();
}

function porraPintarSeries(){
  const cont = document.getElementById('prLigaSeries');
  const sel = document.getElementById('prLigaComp');
  if(!cont || !sel) return;
  const g = window._porraGrupos[+sel.value];
  cont.innerHTML = g.series ? [['A', tx('Serie A','A seriea')], ['B', tx('Serie B','B seriea')], ['AB', tx('Entero (A y B)','Osoa (A eta B)')]]
    .map(([k,l],i)=>`<label><input type="radio" name="prSerie" value="${k}"${i===0?' checked':''}> ${l}</label>`).join('') : '';
}

async function porraCrearLiga(ev){
  ev.preventDefault();
  const g = window._porraGrupos[+document.getElementById('prLigaComp').value];
  const serie = document.querySelector('input[name="prSerie"]:checked')?.value;
  const alcance = !g.series ? [...g.nombres] : serie==='A' ? [g.A] : serie==='B' ? [g.B] : [g.A, g.B];
  const nombre = document.getElementById('prLigaNombre').value.trim();
  const {error} = await _sb.rpc('porra_crear_liga', {nombre, alcance});
  if(error){ document.getElementById('prLigaMsg').textContent = error.message; return; }
  await porraCargarLigas();
  porraRender();
}

async function porraUnirse(ev){
  ev.preventDefault();
  const {error} = await _sb.rpc('porra_unirse', {codigo: document.getElementById('prCodigo').value});
  if(error){ alert(error.message); return; }
  await porraCargarLigas();
  porraRender();
}

async function porraSalirLiga(id){
  if(!confirm(tx('¿Salir de la liga? Tus pronósticos de esta liga dejarán de contar.','Ligatik irten? Liga honetako zure iragarpenek ez dute balioko.'))) return;
  const {error} = await _sb.from('porra_miembros').delete().eq('liga', id).eq('usuario', _porraSesion.user.id);
  if(error){ alert(error.message); return; }
  await porraCargarLigas();
  porraRender();
}

async function porraBorrarLiga(id){
  if(!confirm(tx('¿Borrar la liga para todos sus miembros? No se puede deshacer.','Liga kide guztientzat ezabatu? Ezin da desegin.'))) return;
  const {error} = await _sb.from('porra_ligas').delete().eq('id', id);
  if(error){ alert(error.message); return; }
  await porraCargarLigas();
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
      <li>${tx('Se puede pronosticar y cambiar hasta la hora de inicio de la velada. Los pronósticos de los demás se ven cuando empieza.',
               'Jaialdiaren hasiera ordura arte iragarri eta alda daiteke. Besteen iragarpenak hasten denean ikusten dira.')}</li>
      <li>${tx('Si cambia el cartel o el partido no se juega, ese pronóstico se anula y no cuenta.',
               'Kartela aldatzen bada edo partida ez bada jokatzen, iragarpen hori baliogabetu egiten da eta ez da kontatzen.')}</li>
      <li>${tx('Hay una clasificación general y puedes crear ligas privadas de una competición (hasta 20 personas). Puedes pronosticar lo mismo para todas o distinto en cada una.',
               'Sailkapen orokor bat dago eta txapelketa bateko liga pribatuak sor ditzakezu (20 lagun arte). Guztietarako gauza bera edo bakoitzean desberdina iragar dezakezu.')}</li>
      <li>${tx('Es un juego gratuito entre aficionados: no hay apuestas ni dinero.','Zaleen arteko doako jokoa da: ez dago apusturik ez dirurik.')}</li>
    </ul>
    <h4>${tx('Tus datos','Zure datuak')}</h4>
    <p class="pr-help">${tx('Solo guardamos tu correo (para entrar), el nombre que elijas y tus pronósticos, en servidores de la Unión Europea (Supabase, Fráncfort). No se comparten con nadie ni se usan para publicidad. Los demás solo ven tu nombre y tus puntos. Puedes borrar tu cuenta y todos tus datos cuando quieras.',
      'Zure posta (sartzeko), aukeratzen duzun izena eta zure iragarpenak bakarrik gordetzen ditugu, Europar Batasuneko zerbitzarietan (Supabase, Frankfurt). Ez dira inorekin partekatzen ez publizitaterako erabiltzen. Besteek zure izena eta puntuak bakarrik ikusten dituzte. Zure kontua eta datu guztiak nahi duzunean ezaba ditzakezu.')}</p>
    <p class="pr-help"><a href="${LANG==='eu'?'/eu':''}/privacidad/">${tx('Política de privacidad completa','Pribatutasun politika osoa')}</a></p>
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
