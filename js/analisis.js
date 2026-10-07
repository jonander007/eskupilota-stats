// ════════════════════════════════════════════════════════════
// ANÁLISIS: enlaces directos, Elo, campeonatos, gráficos del perfil,
// pronóstico de la cartelera y ficha de frontón.
// Se carga después de app.js y usa sus datos (PARTIDOS, CAT_*) y utilidades.
// ════════════════════════════════════════════════════════════

function tx(es, eu){ return LANG==='eu' ? eu : es; }

function slugify(s){
  return (s||'').normalize('NFD').replace(/[̀-ͯ]/g,'')
    .toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/^-|-$/g,'');
}

// Escapa texto para meterlo en HTML
function h(s){
  return String(s??'').replace(/[&<>"']/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

// ════════════════════════════════════════════════════════════
// ELO
// Cada pelotari empieza con 1500. En cada partido la fuerza de un equipo es
// la media de sus pelotaris; el ganador suma y el perdedor resta
// K·(resultado − esperado). Los festivales cuentan la mitad.
// Probado con los partidos desde 2025: acierta ~56% de los resultados y las
// probabilidades se encogen un 20% hacia el 50% porque salen algo optimistas.
// ════════════════════════════════════════════════════════════
const ELO_BASE = 1500, ELO_K = 24, ELO_ENCOGE = 0.8;
let ELO = {}, ELO_HIST = {};

function calcElo(){
  ELO = {}; ELO_HIST = {};
  const orden = [...PARTIDOS].sort((a,b)=>parseDate(a.fecha)-parseDate(b.fecha));
  const r = n => ELO[n] ?? ELO_BASE;
  orden.forEach(p=>{
    const t1 = pels(p.equipo1), t2 = pels(p.equipo2);
    if(!t1.length || !t2.length || !p.ganador) return;
    const R1 = t1.reduce((s,n)=>s+r(n),0)/t1.length;
    const R2 = t2.reduce((s,n)=>s+r(n),0)/t2.length;
    const E = 1/(1+Math.pow(10,(R2-R1)/400));
    const S = p.ganador==='equipo1' ? 1 : 0;
    const K = (p.tipo||'').startsWith('festival') ? ELO_K/2 : ELO_K;
    const d = K*(S-E);
    t1.forEach(n=>{ ELO[n]=r(n)+d; (ELO_HIST[n]=ELO_HIST[n]||[]).push([p.fecha, ELO[n]]); });
    t2.forEach(n=>{ ELO[n]=r(n)-d; (ELO_HIST[n]=ELO_HIST[n]||[]).push([p.fecha, ELO[n]]); });
  });
}

// Probabilidad (0-1) de que gane el equipo 1, o null si algún pelotari no tiene historial
function probVictoria(eq1, eq2){
  if(!eq1.length || !eq2.length || [...eq1,...eq2].some(n=>ELO[n]===undefined)) return null;
  const R1 = eq1.reduce((s,n)=>s+ELO[n],0)/eq1.length;
  const R2 = eq2.reduce((s,n)=>s+ELO[n],0)/eq2.length;
  const E = 1/(1+Math.pow(10,(R2-R1)/400));
  return 0.5 + (E-0.5)*ELO_ENCOGE;
}

// Nombre del catálogo a partir de cómo lo escribe la cartelera ("p.etxeberria", "DARIO")
let _PEL_POR_CLAVE = null;
function resolverPelotari(nombre){
  if(!nombre || /^(\?|X+)$/i.test(nombre.trim())) return null;
  if(!_PEL_POR_CLAVE){
    _PEL_POR_CLAVE = {};
    Object.keys(PELOTARIS).forEach(n=>{ _PEL_POR_CLAVE[slugify(n).replace(/-/g,'')] = n; });
  }
  const limpio = nombre.replace(/\(.*?\)/g,'').split(/\s+o\s+/i)[0];
  return _PEL_POR_CLAVE[slugify(limpio).replace(/-/g,'')] || null;
}

// Resultados (V/D) de los últimos n partidos de un pelotari, del más antiguo al más reciente
function formaReciente(nombre, n=5){
  const res = [];
  for(let i=0;i<PARTIDOS.length && res.length<n;i++){   // PARTIDOS está ordenado del más reciente al más antiguo
    const p = PARTIDOS[i];
    const en1 = pels(p.equipo1).includes(nombre), en2 = pels(p.equipo2).includes(nombre);
    if(!en1 && !en2) continue;
    res.push((en1 && p.ganador==='equipo1') || (en2 && p.ganador==='equipo2') ? 'V' : 'D');
  }
  return res.reverse();
}

function chipsForma(forma){
  if(!forma.length) return '<span class="an-muted">—</span>';
  return `<span class="an-forma">${forma.map(r=>`<span class="an-chip ${r==='V'?'v':'d'}" title="${r==='V'?tx('Victoria','Garaipena'):tx('Derrota','Porrota')}">${r==='V'?t('abbr_v'):t('abbr_d')}</span>`).join('')}</span>`;
}

// Cara a cara entre dos equipos: cuenta partidos en los que todos los de eq1
// jugaron juntos contra todos los de eq2
function caraACara(eq1, eq2, filtro=null){
  let g1=0, g2=0;
  PARTIDOS.forEach(p=>{
    if(filtro && !filtro(p)) return;
    const a = pels(p.equipo1), b = pels(p.equipo2);
    const incl = (eq, lado) => eq.every(n=>lado.includes(n));
    if(incl(eq1,a) && incl(eq2,b)){ p.ganador==='equipo1'?g1++:g2++; }
    else if(incl(eq1,b) && incl(eq2,a)){ p.ganador==='equipo2'?g1++:g2++; }
  });
  return {g1, g2};
}

// ════════════════════════════════════════════════════════════
// ENLACES DIRECTOS (#/pelotari/jaka, #/campeonato/COMP026…)
// ════════════════════════════════════════════════════════════
const SEC_SLUG = {partidos:'resultados', cartelera:'cartelera', comparador:'comparador', pelotaris:'pelotaris',
  frontones:'frontones', ranking:'ranking', campeonatos:'campeonatos', porra:'porra', contacto:'contacto'};
const SLUG_SEC = Object.fromEntries(Object.entries(SEC_SLUG).map(([k,v])=>[v,k]));
let _routing = false;

function setHash(hash, replace){
  if(_routing || location.hash===hash) return;
  history[replace?'replaceState':'pushState'](null, '', hash);
}

function secBtn(id){ return document.querySelector(`header nav button[onclick*="showSec('${id}'"]`); }

function irASeccion(id){
  const btn = secBtn(id);
  if(btn && !document.getElementById('sec-'+id).classList.contains('active')) showSec(id, btn, true);
}

function route(){
  const partes = location.hash.replace(/^#\/?/,'').split('/').map(decodeURIComponent);
  const [tipo, arg] = partes;
  _routing = true;
  try{
    if(tipo==='pelotari' && arg){
      const nombre = Object.keys(PELOTARIS).find(n=>slugify(n)===arg);
      irASeccion('pelotaris');
      if(nombre) openPerfil(nombre); else closePerfil();
    } else if(tipo==='fronton' && arg){
      const nombre = [...new Set(PARTIDOS.map(p=>p.fronton))].find(f=>slugify(f)===arg);
      irASeccion('frontones');
      if(nombre) abrirFronton(nombre, false);
    } else if(tipo==='campeonato' && arg){
      irASeccion('campeonatos');
      renderCampeonato(arg);
    } else {
      const sec = SLUG_SEC[tipo] || 'partidos';
      irASeccion(sec);
      if(sec==='pelotaris') closePerfil();
      if(sec==='frontones') cerrarFronton();
    }
  } finally { _routing = false; }
}

function initRouter(){
  window.addEventListener('popstate', route);
  window.addEventListener('hashchange', route);
  route();
}

async function compartirEnlace(titulo){
  const url = location.href;
  try{
    if(navigator.share){ await navigator.share({title:titulo, url}); return; }
    await navigator.clipboard.writeText(url);
    avisoBreve(tx('Enlace copiado','Esteka kopiatuta'));
  }catch(e){ /* el usuario canceló */ }
}

function avisoBreve(msg){
  let el = document.getElementById('anToast');
  if(!el){ el = document.createElement('div'); el.id='anToast'; el.className='an-toast'; el.setAttribute('role','status'); document.body.appendChild(el); }
  el.textContent = msg; el.classList.add('on');
  clearTimeout(el._t); el._t = setTimeout(()=>el.classList.remove('on'), 1800);
}

function botonCompartir(titulo){
  return `<button class="btn-ghost an-share" onclick="compartirEnlace('${h(esc(titulo))}')" aria-label="${tx('Compartir enlace','Esteka partekatu')}">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="18" cy="5" r="3"/><circle cx="6" cy="12" r="3"/><circle cx="18" cy="19" r="3"/><line x1="8.6" y1="13.5" x2="15.4" y2="17.5"/><line x1="15.4" y1="6.5" x2="8.6" y2="10.5"/></svg>
    <span>${tx('Compartir','Partekatu')}</span></button>`;
}

// ════════════════════════════════════════════════════════════
// FICHA DEL PELOTARI EN JPG (con los filtros de la sección Pelotaris)
// ════════════════════════════════════════════════════════════
// Texto del filtro aplicado: "Parejas · 2025", "Todos los partidos"...
function textoFiltroPelotaris(){
  const partes = [];
  if(activeTipo !== 'todos') partes.push({campeonato:t('tipo_parejas'), manomanista:t('tipo_mano'), cuatro:t('tipo_cuatro')}[activeTipo] || activeTipo);
  if(dateRangePel.desde || dateRangePel.hasta){
    const f = s => s ? s.split('-').reverse().join('/') : '…';
    partes.push(`${f(dateRangePel.desde)} – ${f(dateRangePel.hasta)}`);
  } else if(activeYearPel !== 'todos') partes.push(tx('Temporada ','') + activeYearPel + tx('',' denboraldia'));
  if(typeof pfExtra !== 'undefined'){
    if(pfExtra.fronton) partes.push(pfExtra.fronton);
    if(pfExtra.comp) partes.push(tx('Con ','') + pfExtra.comp + tx('','-rekin'));
    if(pfExtra.rival) partes.push(tx('Contra ','') + pfExtra.rival + tx('','-ren aurka'));
  }
  return partes.length ? partes.join(' · ') : tx('Todos los partidos','Partida guztiak');
}

// ════════════════════════════════════════════════════════════
// PALMARÉS: finales de campeonato y torneo de un pelotari
// ════════════════════════════════════════════════════════════
function palmares(nombre, parts){
  const finales = parts.filter(p => p.fase==='final' && (p.categoria==='campeonato' || p.categoria==='torneo') &&
      (pels(p.equipo1).includes(nombre) || pels(p.equipo2).includes(nombre)))
    .sort((a,b) => parseDate(b.fecha)-parseDate(a.fecha));
  const gana = p => p.ganador === (pels(p.equipo1).includes(nombre) ? 'equipo1' : 'equipo2');
  return {finales, ganadas: finales.filter(gana), perdidas: finales.filter(p => !gana(p))};
}

function htmlPalmares(nombre, parts){
  const pa = palmares(nombre, parts);
  if(!pa.finales.length) return '';
  const rival = p => neq(pels(p.equipo1).includes(nombre) ? p.equipo2 : p.equipo1);
  const chip = (p, ok) => `<span class="rk-txapela ${ok ? (p.categoria==='campeonato' ? 'camp' : 'torn') : 'fin'}"
    title="${h(tComp(p.competicion))} · ${p.fecha} · ${p.puntos1}-${p.puntos2} ${h(tx('contra','-ren aurka'))} ${h(rival(p))}">${ok ? '🏆 ' : ''}${h(nombreCortoComp(p.competicion))}</span>`;
  const camp = pa.ganadas.filter(p => p.categoria==='campeonato').length;
  return `<div class="ch-card pf-palmares">
    <h3>${tx('Palmarés','Palmaresa')}</h3>
    <div class="pf-palm-cifras">
      <div><b>${pa.ganadas.length}</b><span>${tx('Txapelas','Txapelak')}</span></div>
      <div><b>${camp}</b><span>${tx('De campeonato','Txapelketakoak')}</span></div>
      <div><b>${pa.finales.length}</b><span>${tx('Finales jugadas','Jokatutako finalak')}</span></div>
    </div>
    ${pa.ganadas.length ? `<div class="an-sub">${tx('Campeón','Txapelduna')}</div><div class="pf-palm-chips">${pa.ganadas.map(p => chip(p, true)).join('')}</div>` : ''}
    ${pa.perdidas.length ? `<div class="an-sub">${tx('Finalista','Finalista')}</div><div class="pf-palm-chips">${pa.perdidas.map(p => chip(p, false)).join('')}</div>` : ''}
    <p class="an-nota">${tx('Según los filtros de arriba. Solo cuentan las competiciones con la final registrada, desde noviembre de 2022.','Goiko iragazkien arabera. Finala erregistratuta duten lehiaketak bakarrik, 2022ko azarotik aurrera.')}</p>
  </div>`;
}

function datosFicha(nombre){
  const parts = partidosPerfil(nombre);
  const lado = p => pels(p.equipo1).includes(nombre) ? 'equipo1' : 'equipo2';
  const gana = p => p.ganador === lado(p);
  const pg = parts.filter(gana).length;
  const pf = parts.reduce((s,p) => s + (lado(p)==='equipo1' ? p.puntos1 : p.puntos2), 0);
  const pc = parts.reduce((s,p) => s + (lado(p)==='equipo1' ? p.puntos2 : p.puntos1), 0);
  const cuenta = f => { const c = {}; parts.forEach(p => f(p).forEach(k => { c[k] = c[k] || {pj:0, pg:0}; c[k].pj++; if(gana(p)) c[k].pg++; })); return c; };
  const top = (c, n) => Object.entries(c).sort((a,b) => b[1].pj-a[1].pj || b[1].pg-a[1].pg).slice(0, n);
  const hist = ELO_HIST[nombre] || [];
  const activos = getActivePlayers();
  const ranking = Object.keys(ELO).filter(n => activos.has(n.toUpperCase())).sort((a,b) => ELO[b]-ELO[a]);
  return {
    pj: parts.length, pg, pp: parts.length-pg, pf, pc,
    ultimos: parts.slice(0, 10).map(p => gana(p) ? 'V' : 'D').reverse(),   // PARTIDOS va del más reciente al más antiguo
    companeros: top(cuenta(p => pels(p[lado(p)]).filter(n => n !== nombre)), 3),
    frontones: top(cuenta(p => [p.fronton]), 3),
    ...(pa => ({finales: [...pa.ganadas, ...pa.perdidas], txapelas: pa.ganadas.length}))(palmares(nombre, parts)),
    elo: ELO[nombre], eloMax: hist.length ? Math.max(...hist.map(x => x[1])) : null,
    pos: ranking.indexOf(nombre) + 1,
  };
}

function cargarImagen(src){
  return new Promise(ok => { const img = new Image(); img.onload = () => ok(img); img.onerror = () => ok(null); img.src = src; });
}

async function descargarFicha(nombre){
  const d = datosFicha(nombre);
  const W = 1080, H = 1350, M = 64;
  const cv = document.createElement('canvas'); cv.width = W; cv.height = H;
  const c = cv.getContext('2d');
  try{ await document.fonts.ready; }catch(e){}
  const logo = await cargarImagen('logo-header.png');
  const VERDE = '#007A3D', VERDE_OSC = '#005a2c', ROJO = '#C8102E', TEXTO = '#1A1A1A', GRIS = '#5e6d63', FONDO = '#f5f7f4';
  const disp = (px, w=700) => `${w} ${px}px "Barlow Condensed", "Arial Narrow", sans-serif`;
  const mono = px => `600 ${px}px "JetBrains Mono", monospace`;
  const sans = (px, w=500) => `${w} ${px}px Inter, system-ui, sans-serif`;
  const texto = (s, x, y, font, color, align='left') => { c.font = font; c.fillStyle = color; c.textAlign = align; c.fillText(s, x, y); };
  const ajustar = (s, font, max) => { c.font = font; while(s.length > 3 && c.measureText(s).width > max) s = s.slice(0, -2) + '…'; return s; };
  const caja = (x, y, w, h, r=18, color='#fff') => { c.fillStyle = color; c.beginPath(); c.roundRect(x, y, w, h, r); c.fill(); };

  // Fondo y cabecera verde
  c.fillStyle = FONDO; c.fillRect(0, 0, W, H);
  const g = c.createLinearGradient(0, 0, W, 360); g.addColorStop(0, VERDE); g.addColorStop(1, VERDE_OSC);
  c.fillStyle = g; c.fillRect(0, 0, W, 360);
  if(logo) c.drawImage(logo, M, 44, 240, 240 * logo.height / logo.width);
  texto(textoFiltroPelotaris().toUpperCase(), W-M, 92, mono(24), 'rgba(255,255,255,.85)', 'right');
  texto(ajustar(nombre, disp(118, 800), W-2*M), M, 262, disp(118, 800), '#fff');
  const rol = getRol(nombre) === 'zaguero' ? tx('Zaguero','Atzelaria') : getRol(nombre) === 'delantero' ? tx('Delantero','Aurrelaria') : '';
  const emp = getEmpresa(nombre) ? NOMBRE_EMPRESA[getEmpresa(nombre)] : '';
  texto(`${rol ? rol.toUpperCase() + ' · ' : ''}${emp ? emp.toUpperCase() + ' · ' : ''}${tx('PELOTA A MANO','ESKU PILOTA')}`, M, 318, mono(24), 'rgba(255,255,255,.85)');

  // Cifras (3 x 2)
  const pct = d.pj ? Math.round(d.pg / d.pj * 100) : 0;
  const dif = d.pf - d.pc;
  const kpis = [
    [d.pj, t('lbl_partidos'), TEXTO], [d.pg, t('lbl_victorias'), VERDE], [d.pp, t('lbl_derrotas'), ROJO],
    [pct + '%', t('lbl_pct_vic'), pct >= 50 ? VERDE : ROJO], [d.pj ? (d.pf/d.pj).toFixed(1) : '—', t('lbl_pts_p'), TEXTO],
    [(dif > 0 ? '+' : '') + dif, t('lbl_diferencia'), dif >= 0 ? VERDE : ROJO],
  ];
  const kw = (W - 2*M - 2*24) / 3, kh = 150;
  kpis.forEach(([v, l, col], i) => {
    const x = M + (i % 3) * (kw + 24), y = 400 + Math.floor(i / 3) * (kh + 24);
    caja(x, y, kw, kh);
    texto(String(v), x + 28, y + 88, disp(80), col);
    texto(String(l).toUpperCase(), x + 28, y + 128, mono(20), GRIS);
  });

  // Elo y últimos 10
  let y = 400 + 2*(kh+24) + 8;
  caja(M, y, W - 2*M, 170);
  texto('ELO', M + 28, y + 50, mono(20), GRIS);
  texto(d.elo ? String(Math.round(d.elo)) : '—', M + 28, y + 128, disp(84), TEXTO);
  texto(`${tx('Máx.','Gor.')} ${d.eloMax ? Math.round(d.eloMax) : '—'}${d.pos ? ` · ${d.pos}º ${tx('activos','aktiboak')}` : ''}`, M + 28, y + 156, mono(19), GRIS);
  texto(tx('ÚLTIMOS','AZKENAK').toUpperCase() + ` ${d.ultimos.length}`, M + 400, y + 50, mono(20), GRIS);
  d.ultimos.forEach((r, i) => {
    const x = M + 400 + i * 50, yy = y + 78;
    caja(x, yy, 42, 58, 8, r === 'V' ? VERDE : '#e3e8e1');
    texto(r === 'V' ? t('abbr_v') : t('abbr_d'), x + 21, yy + 41, disp(34), r === 'V' ? '#fff' : GRIS, 'center');
  });
  if(!d.ultimos.length) texto('—', M + 400, y + 120, disp(40), GRIS);

  // Compañeros / frontones (o palmarés si hay finales)
  y += 170 + 24;
  const colW = (W - 2*M - 24) / 2, colH = H - y - 110;
  // Texto que cabe en 'max': baja el tamaño de letra (hasta 26 px) y, si aun
  // así no cabe, lo corta con '…'
  const encajar = (s, max, px=36) => {
    for(; px > 26; px -= 2){ c.font = disp(px); if(c.measureText(s).width <= max) return [s, disp(px)]; }
    return [ajustar(s, disp(px), max), disp(px)];
  };
  const lista = (x, titulo, filas) => {
    caja(x, y, colW, colH);
    texto(titulo.toUpperCase(), x + 28, y + 50, mono(20), GRIS);
    if(!filas.length) texto('—', x + 28, y + 110, disp(40), GRIS);
    filas.forEach(([n, s, color], i) => {
      if(color){   // dos líneas: nombre y, debajo, el detalle (finales)
        const yy = y + 100 + i * 72;
        const [txt, font] = encajar(n, colW - 56, 34);
        texto(txt, x + 28, yy, font, TEXTO);
        texto(s, x + 28, yy + 28, mono(19), color);
        return;
      }
      const yy = y + 108 + i * 64;
      c.font = mono(22); const ancho = c.measureText(s).width;
      const [txt, font] = encajar(n, colW - 56 - ancho - 16);
      texto(txt, x + 28, yy, font, TEXTO);
      texto(s, x + colW - 28, yy, mono(22), GRIS, 'right');
    });
  };
  const fila = ([n, s]) => [n, `${s.pg}/${s.pj} · ${Math.round(s.pg / s.pj * 100)}%`];
  const izq = d.finales.length
    ? [`${tx('Palmarés','Palmaresa')} · ${d.txapelas} ${d.txapelas===1 ? 'txapela' : tx('txapelas','txapela')}`, d.finales.slice(0, 3).map(p => {
        const g2 = p.ganador === (pels(p.equipo1).includes(nombre) ? 'equipo1' : 'equipo2');
        // Nombre corto: 'Parejas Serie A 2026', 'Binakako A Seriea 2026'
        const corto = tComp(p.competicion).replace(/^(Campeonato|Torneo)\s+/, '').replace(/\s*Txapelketa\b/, '');
        const anio = (p.competicion.match(/\b20\d\d\b/)||[''])[0];
        return [corto, `${anio} · ${(g2 ? tx('Campeón','Txapelduna') : tx('Finalista','Finalista')).toUpperCase()}`, g2 ? VERDE : GRIS];
      })]
    : [t('lbl_compañeros'), d.companeros.map(fila)];
  lista(M, izq[0], izq[1]);
  lista(M + colW + 24, tx('Frontones','Frontoiak'), d.frontones.map(fila));

  // Pie
  c.fillStyle = VERDE; c.fillRect(0, H - 80, W, 80);
  texto('eskupilotastats.com', M, H - 30, sans(28, 700), '#fff');
  const hoy = PARTIDOS.length ? PARTIDOS[0].fecha : '';
  texto(`${tx('Datos hasta el','Datuak')} ${hoy}${tx('','ra arte')}`, W - M, H - 30, mono(20), 'rgba(255,255,255,.85)', 'right');

  const filtro = [activeTipo !== 'todos' ? activeTipo : '', activeYearPel !== 'todos' ? activeYearPel : '',
    pfExtra.fronton, pfExtra.comp ? 'con-' + pfExtra.comp : '', pfExtra.rival ? 'vs-' + pfExtra.rival : ''].filter(Boolean).map(slugify).join('-');
  entregarImagen(cv, `ficha-${slugify(nombre)}${filtro ? '-' + filtro : ''}.jpg`, `${nombre} · EskupilotaStats`, tx('Ficha descargada','Fitxa deskargatuta'));
}

// JPG del lienzo: en el móvil, menú de compartir del sistema (guardar en la
// galería, WhatsApp...); en el ordenador, o si no se puede, se descarga
function entregarImagen(cv, fichero, titulo, aviso){
  cv.toBlob(async blob => {
    if(!blob) return;
    const file = new File([blob], fichero, {type: 'image/jpeg'});
    const tactil = window.matchMedia && matchMedia('(pointer: coarse)').matches;
    if(tactil && navigator.canShare && navigator.canShare({files: [file]})){
      try{ await navigator.share({files: [file], title: titulo}); return; }
      catch(e){ if(e && e.name === 'AbortError') return; }
    }
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = fichero;
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(() => URL.revokeObjectURL(a.href), 1000);
    avisoBreve(aviso || tx('Imagen descargada','Irudia deskargatuta'));
  }, 'image/jpeg', 0.92);
}

// ════════════════════════════════════════════════════════════
// GRÁFICOS SVG (una sola serie cada uno, con tooltip al pasar el ratón)
// ════════════════════════════════════════════════════════════
function tooltipEl(){
  let el = document.getElementById('anTip');
  if(!el){ el = document.createElement('div'); el.id='anTip'; el.className='an-tip'; document.body.appendChild(el); }
  return el;
}
function mostrarTip(ev, html){
  const el = tooltipEl(); el.innerHTML = html; el.style.display='block';
  const x = Math.min(ev.clientX+14, window.innerWidth-el.offsetWidth-8);
  el.style.left = x+'px'; el.style.top = (ev.clientY+14)+'px';
}
function ocultarTip(){ const el=document.getElementById('anTip'); if(el) el.style.display='none'; }

// Barras verticales: datos [{etiqueta, valor(0-100), detalle}]
function graficoBarras(datos){
  if(!datos.length) return `<div class="an-muted">${tx('Sin datos','Daturik ez')}</div>`;
  const W=Math.max(260, datos.length*56), H=170, m={t:18,r:8,b:24,l:30};
  const iw=W-m.l-m.r, ih=H-m.t-m.b, bw=Math.min(28, iw/datos.length*0.55);
  const y=v=>m.t+ih-(v/100)*ih;
  const grid=[0,50,100].map(v=>`<line x1="${m.l}" x2="${W-m.r}" y1="${y(v)}" y2="${y(v)}" class="an-grid${v===50?' mid':''}"/><text x="${m.l-6}" y="${y(v)+3}" class="an-axis" text-anchor="end">${v}%</text>`).join('');
  const bars=datos.map((d,i)=>{
    const cx=m.l+iw*(i+0.5)/datos.length, top=y(d.valor), hgt=Math.max(0,m.t+ih-top);
    const r=Math.min(4,hgt/2, bw/2);
    const path=`M${cx-bw/2},${m.t+ih} V${top+r} Q${cx-bw/2},${top} ${cx-bw/2+r},${top} H${cx+bw/2-r} Q${cx+bw/2},${top} ${cx+bw/2},${top+r} V${m.t+ih} Z`;
    const tip=`<b>${h(d.etiqueta)}</b><br>${Math.round(d.valor)}% · ${h(d.detalle)}`;
    return `<g class="an-hit" onmousemove="mostrarTip(event,'${h(esc(tip))}')" onmouseleave="ocultarTip()">
      <rect x="${cx-iw/datos.length/2}" y="${m.t}" width="${iw/datos.length}" height="${ih}" fill="transparent"/>
      <path d="${path}" class="an-bar"/>
      <text x="${cx}" y="${top-5}" class="an-val" text-anchor="middle">${Math.round(d.valor)}%</text>
      <text x="${cx}" y="${H-8}" class="an-axis" text-anchor="middle">${h(d.etiqueta)}</text></g>`;
  }).join('');
  return `<div class="an-chart-scroll"><svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" role="img" aria-label="${tx('Porcentaje de victorias por temporada','Garaipen ehunekoa denboraldika')}">${grid}${bars}</svg></div>`;
}

// Línea temporal: puntos [[fecha dd/mm/yyyy, valor]]
function graficoLinea(puntos, etiqueta){
  if(puntos.length<2) return `<div class="an-muted">${tx('Pocos partidos todavía','Partida gutxiegi oraindik')}</div>`;
  const W=640, H=190, m={t:14,r:12,b:24,l:40};
  const iw=W-m.l-m.r, ih=H-m.t-m.b;
  const xs=puntos.map(p=>parseDate(p[0]).getTime()), vs=puntos.map(p=>p[1]);
  const x0=Math.min(...xs), x1=Math.max(...xs);
  let v0=Math.min(...vs, ELO_BASE), v1=Math.max(...vs, ELO_BASE);
  const pad=Math.max(10,(v1-v0)*0.1); v0-=pad; v1+=pad;
  const X=t=>m.l+(x1===x0?iw/2:(t-x0)/(x1-x0)*iw), Y=v=>m.t+ih-(v-v0)/(v1-v0)*ih;
  const d=puntos.map((p,i)=>`${i?'L':'M'}${X(xs[i]).toFixed(1)},${Y(vs[i]).toFixed(1)}`).join(' ');
  const ticksY=[v0+pad, ELO_BASE, v1-pad].filter((v,i,a)=>a.findIndex(w=>Math.abs(w-v)<15)===i)
    .map(v=>`<line x1="${m.l}" x2="${W-m.r}" y1="${Y(v)}" y2="${Y(v)}" class="an-grid${v===ELO_BASE?' mid':''}"/><text x="${m.l-6}" y="${Y(v)+3}" class="an-axis" text-anchor="end">${Math.round(v)}</text>`).join('');
  const anios=[...new Set(puntos.map(p=>p[0].slice(-4)))];
  const ticksX=anios.map(a=>{ const t=new Date(+a,0,1).getTime(); return t>=x0&&t<=x1?`<text x="${X(t)}" y="${H-8}" class="an-axis" text-anchor="middle">${a}</text>`:''; }).join('');
  const datos=encodeURIComponent(JSON.stringify(puntos.map((p,i)=>[X(xs[i]),Y(vs[i]),p[0],Math.round(vs[i])])));
  return `<div class="an-chart-scroll"><svg viewBox="0 0 ${W} ${H}" class="an-line-svg" role="img" aria-label="${h(etiqueta)}"
      data-puntos="${datos}" onmousemove="tipLinea(event,this)" onmouseleave="tipLineaFuera(this)">
    ${ticksY}${ticksX}<path d="${d}" class="an-line"/>
    <line class="an-cross" y1="${m.t}" y2="${m.t+ih}" x1="-10" x2="-10"/><circle class="an-dot" r="4.5" cx="-10" cy="-10"/>
    <rect x="${m.l}" y="${m.t}" width="${iw}" height="${ih}" fill="transparent"/></svg></div>`;
}

function tipLinea(ev, svg){
  const pts = svg._pts || (svg._pts = JSON.parse(decodeURIComponent(svg.dataset.puntos)));
  const r = svg.getBoundingClientRect(), vb = svg.viewBox.baseVal;
  const x = (ev.clientX-r.left)*vb.width/r.width;
  let best = pts[0]; for(const p of pts){ if(Math.abs(p[0]-x)<Math.abs(best[0]-x)) best=p; }
  svg.querySelector('.an-cross').setAttribute('x1',best[0]); svg.querySelector('.an-cross').setAttribute('x2',best[0]);
  const c = svg.querySelector('.an-dot'); c.setAttribute('cx',best[0]); c.setAttribute('cy',best[1]);
  mostrarTip(ev, `<b>${best[2]}</b><br>Elo ${best[3]}`);
}
function tipLineaFuera(svg){ ocultarTip(); svg.querySelector('.an-dot')?.setAttribute('cx',-10); svg.querySelector('.an-cross')?.setAttribute('x1',-10); svg.querySelector('.an-cross')?.setAttribute('x2',-10); }

// ════════════════════════════════════════════════════════════
// PERFIL: evolución, forma, mejor frontón
// ════════════════════════════════════════════════════════════
function htmlEvolucionPerfil(nombre){
  const suyos = PARTIDOS.filter(p=>pels(p.equipo1).includes(nombre)||pels(p.equipo2).includes(nombre));
  const gano = p => (pels(p.equipo1).includes(nombre)&&p.ganador==='equipo1')||(pels(p.equipo2).includes(nombre)&&p.ganador==='equipo2');

  const porAnio = {};
  suyos.forEach(p=>{ const a=getYear(p); porAnio[a]=porAnio[a]||{pj:0,pg:0}; porAnio[a].pj++; if(gano(p)) porAnio[a].pg++; });
  const barras = Object.keys(porAnio).sort().map(a=>({etiqueta:a, valor:porAnio[a].pg/porAnio[a].pj*100,
    detalle:`${porAnio[a].pg}${t('abbr_v')}–${porAnio[a].pj-porAnio[a].pg}${t('abbr_d')}`}));

  // Rendimiento por frontón: todos con al menos 2 partidos, de más a menos jugados
  const porFronton = {};
  suyos.forEach(p=>{ const f=porFronton[p.fronton]=porFronton[p.fronton]||{pj:0,pg:0,tf:0,tc:0};
    const en1=pels(p.equipo1).includes(nombre); f.pj++; if(gano(p)) f.pg++;
    f.tf+=en1?p.puntos1:p.puntos2; f.tc+=en1?p.puntos2:p.puntos1; });
  const frontones = Object.entries(porFronton).filter(([,s])=>s.pj>=2).sort((a,b)=>b[1].pj-a[1].pj||b[1].pg-a[1].pg);
  const conMin = frontones.filter(([,s])=>s.pj>=5);
  const pctF = s=>s.pg/s.pj;
  const mejorF = conMin.length>1 ? [...conMin].sort((a,b)=>pctF(b[1])-pctF(a[1])||b[1].pj-a[1].pj)[0] : null;
  const peorF = conMin.length>1 ? [...conMin].sort((a,b)=>pctF(a[1])-pctF(b[1])||b[1].pj-a[1].pj)[0] : null;
  const VISIBLES = 8;
  const filaF = ([f,s],i)=>{ const pc=Math.round(pctF(s)*100);
    return `<tr class="${i>=VISIBLES?'an-fr-extra':''}"><td><span class="clk" onclick="abrirFronton('${esc(f)}')">${h(f)}</span>${mejorF&&mejorF[0]===f?' ▲':''}${peorF&&peorF[0]===f?' ▼':''}</td>
      <td class="an-num">${s.pj}</td><td class="an-num an-up">${s.pg}</td><td class="an-num an-down">${s.pj-s.pg}</td>
      <td class="an-num" style="white-space:nowrap"><span class="an-fr-bar ${pc<50?'bajo':''}" style="width:${Math.max(4,pc*0.5)}px" aria-hidden="true"></span>${pc}%</td>
      <td class="an-num ${s.tf-s.tc>=0?'an-up':'an-down'}">${s.tf-s.tc>0?'+':''}${s.tf-s.tc}</td></tr>`; };

  const hist = ELO_HIST[nombre]||[];
  const elo = ELO[nombre];
  const maxElo = hist.length ? Math.max(...hist.map(x=>x[1])) : null;
  const activos = getActivePlayers();
  const rankingElo = Object.keys(ELO).filter(n=>activos.has(n.toUpperCase())).sort((a,b)=>ELO[b]-ELO[a]);
  const pos = rankingElo.indexOf(nombre);

  return `
    <div class="ch-card an-perfil">
      <div class="an-head">
        <h3>${tx('Evolución histórica','Bilakaera historikoa')}</h3>
        ${botonCompartir(nombre)}
      </div>
      <div class="an-kpis">
        <div><div class="an-kpi-v">${elo?Math.round(elo):'—'}</div><div class="an-kpi-l">${tx('Elo actual','Oraingo Elo')}</div></div>
        <div><div class="an-kpi-v">${maxElo?Math.round(maxElo):'—'}</div><div class="an-kpi-l">${tx('Elo máximo','Elo gorena')}</div></div>
        <div><div class="an-kpi-v">${pos>=0?pos+1+'º':'—'}</div><div class="an-kpi-l">${tx('Ranking Elo (activos)','Elo sailkapena (aktiboak)')}</div></div>
        <div><div class="an-kpi-v">${chipsForma(formaReciente(nombre,10))}</div><div class="an-kpi-l">${tx('Últimos 10','Azken 10ak')}</div></div>
      </div>
      <div class="an-sub">${tx('Elo a lo largo del tiempo','Elo denboran zehar')} <span class="an-help" title="${tx('Puntuación de fuerza: sube al ganar y baja al perder, más cuanto más fuerte es el rival. Todos empiezan en 1500.','Indar puntuazioa: irabaztean igo eta galtzean jaisten da, arerioa zenbat eta indartsuago orduan eta gehiago. Denek 1500ean hasten dute.')}">?</span></div>
      ${graficoLinea(hist, tx('Evolución del Elo de ','Elo-aren bilakaera: ')+nombre)}
      <div class="an-sub">${tx('% de victorias por temporada','Garaipen % denboraldika')}</div>
      ${graficoBarras(barras)}
      <table class="comp-table an-table-sr"><caption>${tx('Victorias por temporada','Garaipenak denboraldika')}</caption>
        <thead><tr><th>${tx('Año','Urtea')}</th><th>${t('abbr_pj')}</th><th>${t('abbr_v')}</th><th>%</th></tr></thead>
        <tbody>${barras.map(b=>`<tr><td>${b.etiqueta}</td><td>${porAnio[b.etiqueta].pj}</td><td>${porAnio[b.etiqueta].pg}</td><td>${Math.round(b.valor)}%</td></tr>`).join('')}</tbody></table>
      ${frontones.length?`<div class="an-sub">${tx('Rendimiento por frontón','Errendimendua frontoika')}</div>
      ${mejorF?`<p class="an-nota">▲ ${tx('Mejor','Onena')}: <b>${h(mejorF[0])}</b> ${Math.round(pctF(mejorF[1])*100)}% · ▼ ${tx('Peor','Txarrena')}: <b>${h(peorF[0])}</b> ${Math.round(pctF(peorF[1])*100)}% <span class="an-muted">(${tx('mín. 5 partidos','gutx. 5 partida')})</span></p>`:''}
      <div class="an-table-wrap"><table class="comp-table an-fr-tabla">
        <thead><tr><th>${tx('Frontón','Frontoia')}</th><th class="an-num">${t('abbr_pj')}</th><th class="an-num">${t('abbr_v')}</th><th class="an-num">${t('abbr_d')}</th><th class="an-num">%</th><th class="an-num" title="${tx('Diferencia de tantos','Tanto aldea')}">${tx('Dif','Alde')}</th></tr></thead>
        <tbody>${frontones.map(filaF).join('')}</tbody></table></div>
      ${frontones.length>VISIBLES?`<button class="btn-ghost an-fr-mas" onclick="this.previousElementSibling.querySelector('table').classList.add('todos');this.remove()">${t('c4_ver_mas').replace('{n}',frontones.length-VISIBLES)}</button>`:''}`:''}
    </div>`;
}

// ════════════════════════════════════════════════════════════
// RANKING: clasificación completa (tabla ordenable)
// ════════════════════════════════════════════════════════════
const RK_MIN_PCT = 10;          // para ordenar por %, los de menos partidos van al final
let _rkOrden = {col:'pg', asc:false};

function ordenarRk(col){
  _rkOrden = _rkOrden.col===col ? {col, asc:!_rkOrden.asc} : {col, asc: col==='nombre'};
  buildRanking();
}

function htmlRankingTabla(parts, activos){
  const st = calcStats(parts);
  const filas = Object.entries(st)
    .filter(([n])=>!activos || activos.has(n.toUpperCase()))
    .map(([n,s])=>({nombre:n, ...s, pct: s.pj ? s.pg/s.pj : 0, dif: s.pf-s.pc, elo: ELO[n] ?? null}));
  if(!filas.length) return `<div class="nodata"><div class="ic">📭</div>${t('sin_datos')}</div>`;
  const {col, asc} = _rkOrden;
  const val = f => col==='nombre' ? f.nombre : (f[col] ?? -Infinity);
  filas.sort((a,b)=>{
    if(col==='pct'){ const pa=a.pj>=RK_MIN_PCT, pb=b.pj>=RK_MIN_PCT; if(pa!==pb) return pa?-1:1; }
    const va=val(a), vb=val(b);
    const c = col==='nombre' ? va.localeCompare(vb) : va-vb;
    return (asc ? c : -c) || (b.pg-a.pg) || (b.dif-a.dif);
  });
  const th = (c, txt, titulo, cls='an-num') => {
    const on = col===c;
    return `<th class="${cls} rk-th ${on?'on':''}" aria-sort="${on?(asc?'ascending':'descending'):'none'}">
      <button onclick="ordenarRk('${c}')" title="${h(titulo)}">${txt}${on?(asc?' ▲':' ▼'):''}</button></th>`;
  };
  const fila = (f,i) => `<tr>
    <td class="an-num rk-n">${i+1}</td>
    <td><span class="clk" onclick="goToPel('${esc(f.nombre)}')">${h(f.nombre)}</span>
      ${getRol(f.nombre)!=='otro'?`<span class="rol-badge ${getRol(f.nombre)==='zaguero'?'zag':'del'}">${getRol(f.nombre)==='zaguero'?t('rol_zag'):t('rol_del')}</span>`:''}</td>
    <td class="an-num">${f.pj}</td>
    <td class="an-num an-up"><b>${f.pg}</b></td>
    <td class="an-num an-down">${f.pp}</td>
    <td class="an-num ${f.pj<RK_MIN_PCT?'an-muted':''}">${Math.round(f.pct*100)}%</td>
    <td class="an-num ${f.dif>=0?'an-up':'an-down'}">${f.dif>0?'+':''}${f.dif}</td>
    <td class="an-num">${f.elo===null?'—':Math.round(f.elo)}</td>
    <td class="rk-forma">${chipsForma(formaReciente(f.nombre,5))}</td></tr>`;
  return `<div class="ch-card">
    <p class="an-nota">${tx(`Pulsa en una columna para ordenar. Al ordenar por %, los que tienen menos de ${RK_MIN_PCT} partidos van al final. El Elo y la forma (últimos 5) tienen en cuenta todos los partidos.`,
      `Sakatu zutabe batean ordenatzeko. %-ka ordenatzean, ${RK_MIN_PCT} partida baino gutxiago dituztenak amaieran doaz. Eloak eta formak (azken 5ak) partida guztiak hartzen dituzte kontuan.`)}</p>
    <div class="an-table-wrap"><table class="comp-table rk-tabla">
      <thead><tr><th class="an-num">#</th>${th('nombre', tx('Pelotari','Pilotaria'), tx('Nombre','Izena'), '')}
        ${th('pj', t('abbr_pj'), tx('Partidos jugados','Jokatutako partidak'))}
        ${th('pg', t('abbr_v'), tx('Victorias','Garaipenak'))}
        ${th('pp', t('abbr_d'), tx('Derrotas','Porrotak'))}
        ${th('pct', '%', tx('Porcentaje de victorias','Garaipen ehunekoa'))}
        ${th('dif', tx('Dif','Alde'), tx('Diferencia de tantos','Tanto aldea'))}
        ${th('elo', 'Elo', 'Elo')}
        <th class="rk-forma">${tx('Forma','Forma')}</th></tr></thead>
      <tbody>${filas.map(fila).join('')}</tbody></table></div></div>`;
}

// ════════════════════════════════════════════════════════════
// RANKING: títulos (finales de campeonatos y torneos)
// ════════════════════════════════════════════════════════════
// 'Campeonato Parejas Serie A 2025' -> 'Parejas A 2025'; 'Torneo San Fermin 4 y Medio 2026' -> 'San Fermín 4½ 2026'
function nombreCortoComp(nombre){
  return (nombre||'').replace(/^(Campeonato|Torneo)\s+/i,'').replace(/\s*Serie\s+([AB])\b/i,' $1')
    .replace(/\s*4 y Medio/i,' 4½').replace(/Fermin\b/,'Fermín').replace(/Manomanista/,'Mano');
}

function htmlRankingTitulos(parts, activos){
  const finales = parts.filter(p=>p.fase==='final' && (p.categoria==='campeonato' || p.categoria==='torneo'))
    .sort((a,b)=>parseDate(b.fecha)-parseDate(a.fecha));
  const st = {};
  const de = n => st[n] = st[n] || {titulos:[], finales:0, camp:0};
  finales.forEach(p=>{
    const gan = p.ganador==='equipo1' ? p.equipo1 : p.equipo2;
    const per = p.ganador==='equipo1' ? p.equipo2 : p.equipo1;
    pels(gan).forEach(n=>{ const s=de(n); s.finales++; s.titulos.push(p); if(p.categoria==='campeonato') s.camp++; });
    pels(per).forEach(n=>{ de(n).finales++; });
  });
  const filas = Object.entries(st).filter(([n])=>!activos || activos.has(n.toUpperCase()))
    .sort((a,b)=>b[1].titulos.length-a[1].titulos.length || b[1].camp-a[1].camp || b[1].finales-a[1].finales || a[0].localeCompare(b[0]));
  if(!filas.length) return `<div class="nodata"><div class="ic">🏆</div>${tx('No hay finales de campeonato o torneo con estos filtros.','Ez dago txapelketa edo torneo finalik iragazki hauekin.')}</div>`;
  const chip = p => `<span class="rk-txapela ${p.categoria==='campeonato'?'camp':'torn'}" title="${h(tComp(p.competicion))} · ${p.fecha}">${h(nombreCortoComp(p.competicion))}</span>`;
  return `<div class="ch-card">
    <p class="an-nota">${tx('Finales de campeonatos (verde) y torneos (azul) ganadas por cada pelotari. Solo cuentan las competiciones con la final registrada, desde noviembre de 2022.',
      'Pilotari bakoitzak irabazitako txapelketa (berdea) eta torneo (urdina) finalak. Finala erregistratuta duten lehiaketak bakarrik, 2022ko azarotik aurrera.')}</p>
    <div class="an-table-wrap"><table class="comp-table rk-tabla">
      <thead><tr><th class="an-num">#</th><th>${tx('Pelotari','Pilotaria')}</th>
        <th class="an-num" title="${tx('Finales ganadas','Irabazitako finalak')}">🏆</th>
        <th class="an-num" title="${tx('De ellas, campeonatos','Horietatik, txapelketak')}">${tx('Camp.','Txap.')}</th>
        <th class="an-num" title="${tx('Finales jugadas','Jokatutako finalak')}">${tx('Finales','Finalak')}</th>
        <th>${tx('Títulos','Txapelak')}</th></tr></thead>
      <tbody>${filas.map(([n,s],i)=>`<tr>
        <td class="an-num rk-n">${i+1}</td>
        <td><span class="clk" onclick="goToPel('${esc(n)}')">${h(n)}</span></td>
        <td class="an-num"><b>${s.titulos.length}</b></td>
        <td class="an-num">${s.camp}</td>
        <td class="an-num">${s.finales}</td>
        <td class="rk-titulos">${s.titulos.map(chip).join('')||'<span class="an-muted">—</span>'}</td></tr>`).join('')}</tbody></table></div></div>`;
}

// ════════════════════════════════════════════════════════════
// RANKING ELO (pestaña del ranking): tabla con la variación del último mes
// y gráfico de la evolución de los cinco primeros en el último año
// ════════════════════════════════════════════════════════════
function eloEnFecha(n, fecha){
  // Elo que tenía un pelotari al terminar el día `fecha` (Date)
  let v = ELO_BASE;
  for(const [f, e] of (ELO_HIST[n]||[])){ if(parseDate(f) > fecha) break; v = e; }
  return v;
}

function htmlGraficoElo(nombres, desde, hasta){
  const W=720, H=250, M={l:44, r:12, t:12, b:26};
  const colores = ['var(--green)','var(--blue)','var(--red)','#d99a00','#8a5cc7'];
  const series = nombres.map(n=>{
    const pts = [[desde, eloEnFecha(n, desde)]];
    (ELO_HIST[n]||[]).forEach(([f,e])=>{ const d=parseDate(f); if(d>desde && d<=hasta) pts.push([d,e]); });
    pts.push([hasta, ELO[n]]);
    return pts;
  });
  const vals = series.flat().map(p=>p[1]);
  const lo = Math.floor((Math.min(...vals)-10)/25)*25, hi = Math.ceil((Math.max(...vals)+10)/25)*25;
  const x = d => M.l + (d-desde)/(hasta-desde)*(W-M.l-M.r);
  const y = v => M.t + (hi-v)/(hi-lo)*(H-M.t-M.b);
  const paso = (hi-lo) > 200 ? 100 : 50;
  const ticks = []; for(let v=Math.ceil(lo/paso)*paso; v<=hi; v+=paso) ticks.push(v);
  const meses = [];
  for(let d=new Date(desde.getFullYear(), desde.getMonth()+1, 1); d<hasta; d=new Date(d.getFullYear(), d.getMonth()+3, 1)) meses.push(d);
  const camino = pts => pts.map(([d,v],i)=> i ? `H${x(d).toFixed(1)}V${y(v).toFixed(1)}` : `M${x(d).toFixed(1)} ${y(v).toFixed(1)}`).join('');
  return `<div class="rk-elo-graf">
    <svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${tx('Evolución del Elo en el último año','Eloaren bilakaera azken urtean')}">
      ${ticks.map(v=>`<line x1="${M.l}" x2="${W-M.r}" y1="${y(v)}" y2="${y(v)}" class="rk-eje"/><text x="${M.l-6}" y="${y(v)+3}" text-anchor="end" class="rk-eje-txt">${v}</text>`).join('')}
      ${meses.map(d=>`<text x="${x(d)}" y="${H-8}" text-anchor="middle" class="rk-eje-txt">${String(d.getMonth()+1).padStart(2,'0')}/${String(d.getFullYear()).slice(2)}</text>`).join('')}
      ${series.map((pts,i)=>`<path d="${camino(pts)}" fill="none" stroke="${colores[i]}" stroke-width="2.2" stroke-linejoin="round"/>`).join('')}
    </svg>
    <div class="rk-leyenda">${nombres.map((n,i)=>`<span><i style="background:${colores[i]}"></i>${h(n)}</span>`).join('')}</div>
  </div>`;
}

function htmlRankingElo(){
  const activos = getActivePlayers();
  const lista = Object.keys(ELO)
    .filter(n=>!filterActivos || activos.has(n.toUpperCase()))
    .sort((a,b)=>ELO[b]-ELO[a]).slice(0,30);
  const hasta = PARTIDOS.reduce((mx,p)=>{ const d=parseDate(p.fecha); return d>mx?d:mx; }, new Date(0));
  const haceMes = new Date(hasta); haceMes.setDate(haceMes.getDate()-30);
  const haceAnio = new Date(hasta); haceAnio.setFullYear(haceAnio.getFullYear()-1);
  const filas = lista.map((n,i)=>{
    const dif = Math.round(ELO[n]-eloEnFecha(n, haceMes));
    return `<tr><td class="an-num">${i+1}</td><td><span class="clk" onclick="goToPel('${esc(n)}')">${h(n)}</span> <span class="rol-badge ${getRol(n)==='zaguero'?'zag':'del'}">${getRol(n)==='zaguero'?t('rol_zag'):t('rol_del')}</span></td>
      <td class="an-num"><b>${Math.round(ELO[n])}</b></td>
      <td class="an-num ${dif>0?'an-up':dif<0?'an-down':'an-muted'}">${dif>0?'▲ +'+dif:dif<0?'▼ '+dif:'='}</td>
      <td class="rk-forma">${chipsForma(formaReciente(n,5))}</td></tr>`;
  }).join('');
  return `<div class="ch-card">
    <h3>${tx('Ranking Elo','Elo sailkapena')}</h3>
    <p class="an-nota">${tx('Puntuación de fuerza calculada con todos los partidos: sube al ganar y baja al perder, más cuanto más fuerte es el rival. Todos empiezan en 1500. La columna "1 mes" es lo que ha subido o bajado en los últimos 30 días.',
      'Partida guztiekin kalkulatutako indar puntuazioa: irabaztean igo eta galtzean jaisten da, arerioa zenbat eta indartsuago orduan eta gehiago. Denek 1500ean hasten dute. "Hilabete" zutabea azken 30 egunetan igo edo jaitsi dena da.')}</p>
    ${lista.length>=2 ? `<div class="an-sub">${tx('Evolución de los cinco primeros en el último año','Lehen bosten bilakaera azken urtean')}</div>${htmlGraficoElo(lista.slice(0,5), haceAnio, hasta)}` : ''}
    <div class="an-table-wrap"><table class="comp-table">
      <thead><tr><th>#</th><th>${tx('Pelotari','Pilotaria')}</th><th class="an-num">Elo</th><th class="an-num">${tx('1 mes','Hilabete')}</th><th class="rk-forma">${tx('Forma','Forma')}</th></tr></thead>
      <tbody>${filas||`<tr><td colspan="5" class="nodata">${tx('Sin datos','Daturik ez')}</td></tr>`}</tbody></table></div></div>`;
}

// ════════════════════════════════════════════════════════════
// CAMPEONATOS: clasificación y partidos de cada competición
// ════════════════════════════════════════════════════════════
let _campModo = 'equipos';
let _frontonActual = null;
let _campActual = null;

function competicionesOficiales(){
  // Campeonatos, torneos y desafíos (no festivales), de la más reciente a la más antigua
  const ultimas = {};
  PARTIDOS.forEach(p=>{
    if(!p.competicion || p.categoria==='festival') return;
    const d = parseDate(p.fecha);
    if(!ultimas[p.competicion] || d>ultimas[p.competicion]) ultimas[p.competicion]=d;
  });
  const idPorNombre = Object.fromEntries(Object.values(CAT_COMPETICIONES).map(c=>[c.nombre,c.id]));
  return Object.entries(ultimas).filter(([n])=>idPorNombre[n])
    .map(([nombre,ultima])=>({id:idPorNombre[nombre], nombre, ultima}))
    .sort((a,b)=>b.ultima-a.ultima);
}

// Dos filtros: la competición (sin año) y el año. Al cambiar de competición
// se muestra su año más reciente.
// (la clave ignora tildes: 'Torneo San Fermin' y 'Torneo San Fermín' son la misma)
const baseComp = nombre => slugify(nombre.replace(/\s*\b20\d\d\b/, ''));
const anioComp = nombre => (nombre.match(/\b(20\d\d)\b/)||[])[1] || '';

function buildCampeonatos(){
  const sel = document.getElementById('campSel');
  if(!sel) return;
  const comps = competicionesOficiales();
  const bases = {};
  comps.forEach(c => (bases[baseComp(c.nombre)] = bases[baseComp(c.nombre)] || []).push(c));
  const cat = b => (CAT_COMPETICIONES[bases[b][0].id]||{}).categoria || 'campeonato';
  const GRUPOS = [['campeonato', tx('Campeonatos','Txapelketak')], ['torneo', tx('Torneos','Torneoak')], ['desafio', tx('Desafíos','Desafioak')]];
  sel.innerHTML = GRUPOS.map(([k, titulo]) => {
    const nombre = b => tComp(bases[b][0].nombre);
    const suyas = Object.keys(bases).filter(b => cat(b)===k).sort((x,y) => nombre(x).localeCompare(nombre(y)));
    return suyas.length ? `<optgroup label="${h(titulo)}">${suyas.map(b =>
      `<option value="${h(b)}">${h(nombre(b))}</option>`).join('')}</optgroup>` : '';
  }).join('');
  _campBases = bases;
  if(!_campActual && comps.length){
    // Por defecto, el campeonato (no torneo) que se jugó más recientemente
    _campActual = (comps.find(c=>c.nombre.startsWith('Campeonato'))||comps[0]).id;
  }
  if(_campActual) sincronizarFiltrosCamp(_campActual);
}

let _campBases = {};

// Pone los dos filtros (competición y año) en la competición id
function sincronizarFiltrosCamp(id){
  const comp = CAT_COMPETICIONES[id];
  const sel = document.getElementById('campSel'), anio = document.getElementById('campAnio');
  if(!comp || !sel) return;
  const base = baseComp(comp.nombre);
  sel.value = base;
  if(anio){
    const lista = (_campBases[base] || []).slice().sort((a,b) => anioComp(b.nombre).localeCompare(anioComp(a.nombre)));
    anio.innerHTML = lista.map(c => `<option value="${c.id}">${anioComp(c.nombre) || '—'}</option>`).join('');
    anio.value = comp.id;
    anio.disabled = lista.length < 2;
  }
}

// Al elegir competición: su año más reciente
function elegirCompeticion(base){
  const lista = (_campBases[base] || []).slice().sort((a,b) => anioComp(b.nombre).localeCompare(anioComp(a.nombre)));
  if(lista.length) renderCampeonato(lista[0].id);
}

// Campeones: ganadores de la final (si la conocemos). Si la final se ha
// deducido (último partido del campeonato), se indica.
function htmlCampeon(parts){
  const final = parts.find(p=>p.fase==='final');
  if(!final) return '';
  const gan = final.ganador==='equipo1' ? final.equipo1 : final.equipo2;
  return `<div class="ch-card an-campeon">
    <div class="an-campeon-ic" aria-hidden="true">🏆</div>
    <div>
      <div class="an-kpi-l">${t('lbl_campeones')}</div>
      <div class="an-campeon-nombre">${pels(gan).map(n=>`<span class="clk" onclick="goToPel('${esc(n)}')">${h(n)}</span>`).join(' / ')}</div>
      <div class="an-muted">${t('fase_final')} · ${final.fecha} · ${h(final.fronton)} · ${final.puntos1}–${final.puntos2}
        ${final.fase_deducida?` · <span title="${h(t('lbl_final_deducida'))}">${tx('deducida','ondorioztatua')} ⓘ</span>`:''}</div>
    </div>
  </div>`;
}

// Cuadro de fases: una columna por fase. Las eliminatorias directas se
// dibujan como cruces (cada partido a la altura del de la ronda siguiente al
// que da paso su ganador) y las liguillas (fase con grupos), como una
// pequeña clasificación por grupo. Solo entran las fases coherentes: en una
// eliminatoria cada equipo una vez y no más partidos de los que caben; en
// una liguilla, grupos de hasta 6 equipos que juegan al menos 2 partidos.
const RONDAS_KO = ['eliminatoria','octavos','cuartos','semifinal','final'];
const MAX_POR_RONDA = {eliminatoria:16, octavos:8, cuartos:4, semifinal:2, final:1};

function htmlCuadro(parts){
  const clave = eq => pels(eq).join(' / ');
  const ganadorDe = p => clave(p.ganador==='equipo1' ? p.equipo1 : p.equipo2);
  const koOk = (f, ps) => ps.length <= MAX_POR_RONDA[f] &&
    new Set(ps.flatMap(p => [clave(p.equipo1), clave(p.equipo2)])).size === ps.length*2;
  const grupos = ps => {
    const g = {};
    ps.forEach(p => (g[p.grupo] = g[p.grupo] || []).push(p));
    return g;
  };
  const ligaOk = ps => Object.values(grupos(ps)).every(gp => {
    const n = {};
    gp.forEach(p => [clave(p.equipo1), clave(p.equipo2)].forEach(k => n[k] = (n[k]||0)+1));
    return Object.keys(n).length <= 6 && Object.values(n).every(x => x >= 2);
  });
  const fases = [];
  RONDAS_KO.forEach(f => {
    const ps = parts.filter(p => p.fase===f);
    if(!ps.length) return;
    const conGrupo = ps.filter(p => p.grupo), sinGrupo = ps.filter(p => !p.grupo);
    if(conGrupo.length && !sinGrupo.length && ligaOk(conGrupo)) fases.push({f, liga:true, ps:conGrupo});
    else if(!conGrupo.length && koOk(f, sinGrupo)) fases.push({f, liga:false, ps:sinGrupo});
  });
  if(fases.length < 2) return '';
  // Orden de los cruces según la ronda siguiente (si también es eliminatoria)
  for(let i = fases.length-2; i >= 0; i--){
    if(fases[i].liga || fases[i+1].liga) continue;
    const sig = fases[i+1].ps;
    const pos = p => {
      const g = ganadorDe(p);
      const j = sig.findIndex(q => clave(q.equipo1)===g || clave(q.equipo2)===g);
      return j < 0 ? 99 : j*2 + (clave(sig[j].equipo1)===g ? 0 : 1);
    };
    fases[i].ps = [...fases[i].ps].sort((a,b) => pos(a)-pos(b) || parseDate(a.fecha)-parseDate(b.fecha));
  }
  const tercero = parts.filter(p => p.fase==='tercero');
  const deducidas = parts.some(p => p.fase_deducida && RONDAS_KO.includes(p.fase));
  const nombres = eq => pels(eq).map(n=>`<span class="clk" onclick="goToPel('${esc(n)}')">${h(n)}</span>`).join(' / ');
  const lado = (p, eq, pts) => `<div class="an-ko-eq ${p.ganador===eq?'gana':''}"><span>${nombres(p[eq])}</span><b>${p[pts]}</b></div>`;
  const partido = p => `<div class="an-ko-m">
      ${lado(p,'equipo1','puntos1')}${lado(p,'equipo2','puntos2')}
      <div class="an-ko-info">${p.fecha} · ${h(p.fronton)}</div></div>`;
  // Clasificados de una liguilla: los que juegan la fase siguiente
  const enFase = i => new Set((fases[i]?.ps || []).flatMap(p => [clave(p.equipo1), clave(p.equipo2)]));
  const tablaGrupo = (g, ps, pasan) => {
    const tabla = {};
    ps.forEach(p => ['equipo1','equipo2'].forEach(eq => {
      const k = clave(p[eq]); const f = tabla[k] = tabla[k] || {eq:p[eq], v:0, d:0};
      p.ganador===eq ? f.v++ : f.d++;
    }));
    const filas = Object.values(tabla).sort((a,b) => b.v-a.v || a.d-b.d);
    return `<div class="an-ko-m an-ko-grupo">
      <div class="an-ko-gtit">${t('lbl_grupo').replace('{g}', h(g))}</div>
      ${filas.map((f,i) => `<div class="an-ko-eq ${(pasan.size ? pasan.has(clave(f.eq)) : i===0)?'gana':''}"><span>${nombres(f.eq)}</span><b>${f.v}–${f.d}</b></div>`).join('')}
    </div>`;
  };
  const columna = (fase, idx) => {
    const cuerpo = fase.liga
      ? Object.entries(grupos(fase.ps)).sort().map(([g, ps]) => tablaGrupo(g, ps, enFase(idx+1))).join('')
      : fase.ps.map(partido).join('');
    const extra = idx === fases.length-1 && tercero.length
      ? `<div class="an-ko-tit an-ko-tit2">${t('fase_tercero')}</div>${tercero.map(partido).join('')}` : '';
    return `<div class="an-ko-col ${fase.liga?'liga':''}">
      <div class="an-ko-tit">${fase.liga ? tx('Liguilla de ','') : ''}${t('fase_'+fase.f).toLowerCase().replace(/^./, c=>c.toUpperCase())}${fase.liga ? tx('',' (liga)') : ''}</div>
      <div class="an-ko-lista">${cuerpo}${extra}</div></div>`;
  };
  return `<div class="ch-card">
    <h3>${tx('Fases','Faseak')}</h3>
    ${deducidas ? `<p class="an-nota">${tx('ⓘ Algunas rondas están deducidas del calendario: el partido anterior de cada clasificado, cuando cuadra como eliminatoria.',
      'ⓘ Kanporaketa batzuk egutegitik ondorioztatuak dira: sailkatu bakoitzaren aurreko partida, kanporaketa gisa bat datorrenean.')}</p>` : ''}
    <div class="an-ko" style="--rondas:${fases.length}">${fases.map(columna).join('')}</div></div>`;
}

function setCampModo(modo){ _campModo = modo; renderCampeonato(_campActual); }

function renderCampeonato(id){
  const cont = document.getElementById('campContent');
  if(!cont) return;
  if(!document.getElementById('campSel').options.length) buildCampeonatos();
  const comp = CAT_COMPETICIONES[id] || CAT_COMPETICIONES[_campActual];
  if(!comp){ cont.innerHTML = `<div class="nodata">${tx('Sin competiciones','Txapelketarik ez')}</div>`; return; }
  _campActual = comp.id;
  sincronizarFiltrosCamp(comp.id);
  // Sustituye la entrada del historial: así "atrás" vuelve a la sección anterior
  setHash('#/campeonato/'+comp.id, true);

  const parts = PARTIDOS.filter(p=>p.competicion===comp.nombre).sort((a,b)=>parseDate(b.fecha)-parseDate(a.fecha));
  if(!parts.length){ cont.innerHTML = `<div class="nodata">${tx('Sin partidos','Partidarik ez')}</div>`; return; }
  const parejas = parts.some(p=>p.equipo1.zaguero);
  const modo = parejas ? _campModo : 'pelotaris';

  const tabla = {};
  const sumar = (clave, integrantes, pf, pc, gana) => {
    const f = tabla[clave] = tabla[clave] || {integrantes, pj:0, g:0, p:0, tf:0, tc:0};
    f.pj++; f.tf+=pf; f.tc+=pc; gana?f.g++:f.p++;
  };
  parts.forEach(p=>{
    [['equipo1','puntos1','puntos2'],['equipo2','puntos2','puntos1']].forEach(([eq,a,b])=>{
      const inte = pels(p[eq]); const gana = p.ganador===eq;
      if(modo==='equipos') sumar(inte.join(' / '), inte, p[a], p[b], gana);
      else inte.forEach(n=>sumar(n, [n], p[a], p[b], gana));
    });
  });
  const filas = Object.entries(tabla).sort((x,y)=>y[1].g-x[1].g || (y[1].tf-y[1].tc)-(x[1].tf-x[1].tc) || y[1].tf-x[1].tf);

  const ultimo = parts[0];
  const ganUlt = ultimo.ganador==='equipo1' ? ultimo.equipo1 : ultimo.equipo2;
  const fechaIni = parts[parts.length-1].fecha, fechaFin = parts[0].fecha;

  const fila = ([clave,f],i)=>`<tr>
    <td class="an-num">${i+1}</td>
    <td>${f.integrantes.map(n=>`<span class="clk" onclick="goToPel('${esc(n)}')">${h(n)}</span>`).join(' / ')}</td>
    <td class="an-num">${f.pj}</td><td class="an-num an-up">${f.g}</td><td class="an-num an-down">${f.p}</td>
    <td class="an-num">${f.tf}</td><td class="an-num">${f.tc}</td>
    <td class="an-num ${f.tf-f.tc>=0?'an-up':'an-down'}">${f.tf-f.tc>0?'+':''}${f.tf-f.tc}</td></tr>`;

  cont.innerHTML = `
    <div class="an-head">
      <div>
        <div class="an-camp-nombre">${h(tComp(comp.nombre))} ${(comp.nombre.match(/\b20\d\d\b/)||[''])[0]}</div>
        <div class="an-muted">${nPartidos(parts.length)} · ${fechaIni} → ${fechaFin}</div>
      </div>
      ${botonCompartir(comp.nombre)}
    </div>
    ${htmlCampeon(parts)}
    ${htmlCuadro(parts)}
    <div class="ch-card an-ultimo">
      <h3>${tx('Último partido disputado','Jokatutako azken partida')} · ${ultimo.fecha} · ${h(ultimo.fronton)}${ultimo.fase?' · '+textoFase(ultimo):''}</h3>
      <div class="an-ultimo-res">
        <span class="${ultimo.ganador==='equipo1'?'an-win':''}">${h(neq(ultimo.equipo1))}</span>
        <span class="an-marcador">${ultimo.puntos1} – ${ultimo.puntos2}</span>
        <span class="${ultimo.ganador==='equipo2'?'an-win':''}">${h(neq(ultimo.equipo2))}</span>
      </div>
      <div class="an-muted">${tx('Ganan','Irabazleak')}: <b>${h(neq(ganUlt))}</b></div>
    </div>
    <div class="ch-card">
      <div class="an-head"><h3>${tx('Clasificación','Sailkapena')}</h3>
        ${parejas?`<div class="an-toggle" role="group">
          <button class="rk-tab ${modo==='equipos'?'on':''}" onclick="setCampModo('equipos')">${tx('Por parejas','Bikoteka')}</button>
          <button class="rk-tab ${modo==='pelotaris'?'on':''}" onclick="setCampModo('pelotaris')">${tx('Por pelotari','Pilotariko')}</button></div>`:''}
      </div>
      <p class="an-nota">${tx('Ordenada por victorias y, a igualdad, por diferencia de tantos. Incluye todas las fases; las parejas que jugaron con un sustituto aparecen aparte.',
        'Garaipenen arabera ordenatua eta, berdinketan, tanto diferentziaren arabera. Fase guztiak barne; ordezko batekin jokatu zuten bikoteak bereizita agertzen dira.')}</p>
      <div class="an-table-wrap"><table class="comp-table">
        <thead><tr><th>#</th><th>${modo==='equipos'?tx('Pareja','Bikotea'):tx('Pelotari','Pilotaria')}</th><th class="an-num" title="${tx('Partidos jugados','Jokatutako partidak')}">${t('abbr_pj')}</th><th class="an-num" title="${tx('Victorias','Garaipenak')}">${t('abbr_v')}</th><th class="an-num" title="${tx('Derrotas','Porrotak')}">${t('abbr_d')}</th><th class="an-num" title="${tx('Tantos a favor','Aldeko tantoak')}">${tx('TF','AT')}</th><th class="an-num" title="${tx('Tantos en contra','Kontrako tantoak')}">${tx('TC','KT')}</th><th class="an-num" title="${tx('Diferencia','Aldea')}">${tx('Dif','Alde')}</th></tr></thead>
        <tbody>${filas.map(fila).join('')}</tbody></table></div>
    </div>
    <div class="ch-card">
      <h3>${tx('Partidos','Partidak')}</h3>
      <div class="an-table-wrap"><table class="comp-table an-partidos">
        <tbody>${parts.map(p=>`<tr>
          <td class="an-fecha">${p.fecha}${p.fase?`<br><span class="fase-lbl">${textoFase(p)}</span>`:''}</td><td class="an-fron">${h(p.fronton)}</td>
          <td class="${p.ganador==='equipo1'?'an-win':''}">${h(neq(p.equipo1))}</td>
          <td class="an-num an-marcador">${p.puntos1}–${p.puntos2}</td>
          <td class="${p.ganador==='equipo2'?'an-win':''}">${h(neq(p.equipo2))}</td></tr>`).join('')}</tbody></table></div>
    </div>`;
}

// ════════════════════════════════════════════════════════════
// CARTELERA: cara a cara, forma y pronóstico de cada partido
// ════════════════════════════════════════════════════════════
function htmlPrevia(p){
  const eq1 = (p.eq1||[]).map(resolverPelotari), eq2 = (p.eq2||[]).map(resolverPelotari);
  if(!eq1.length || !eq2.length || eq1.includes(null) || eq2.includes(null)) return '';
  const prob = probVictoria(eq1, eq2);
  // Mano a mano o 4 y medio: el cara a cara solo en esa modalidad, y solo si
  // se han enfrentado en ella
  const individual = eq1.length===1 && eq2.length===1 && (p.modalidad==='mano' || p.modalidad==='cuatro');
  const cc = individual ? caraACara(eq1, eq2, x => x.modalidad===p.modalidad) : caraACara(eq1, eq2);
  const nombreMod = p.modalidad==='cuatro' ? '4½' : tx('mano a mano','buruz buru');
  const filaCC = individual
    ? (cc.g1+cc.g2 ? `<div class="an-previa-row"><span class="an-prob-l">${tx('Cara a cara','Aurrez aurre')} (${nombreMod})</span><span><b>${cc.g1}</b> – <b>${cc.g2}</b></span></div>` : '')
    : `<div class="an-previa-row"><span class="an-prob-l">${tx('Cara a cara','Aurrez aurre')}</span>
      <span>${cc.g1+cc.g2 ? `<b>${cc.g1}</b> – <b>${cc.g2}</b>` : tx('Nunca se han enfrentado','Ez dira inoiz aurrez aurre aritu')}</span></div>`;
  const forma = eq => chipsForma(formaReciente(eq[0],5));
  const p1 = prob==null ? null : Math.round(prob*100);
  return `<div class="an-previa">
    ${p1==null?'':`<div class="an-prob" title="${tx('Estimación orientativa según el Elo de cada pelotari. En partidos pasados acertó el ganador en torno al 56% de las veces: los partidos suelen estar muy igualados.','Pilotari bakoitzaren Elo-aren araberako estimazio orientagarria. Iraganeko partidetan irabazlea %56 inguru asmatu zuen: partidak oso parekatuak izan ohi dira.')}">
      <span class="an-prob-l">${tx('Pronóstico','Pronostikoa')}</span>
      <span class="an-prob-n">${p1}%</span>
      <span class="an-prob-bar" aria-hidden="true"><span style="width:${p1}%"></span></span>
      <span class="an-prob-n">${100-p1}%</span>
    </div>`}
    ${filaCC}
    ${eq1.length + eq2.length > 2
      // Parejas: solo el mejor y el peor en forma de los cuatro
      ? htmlExtremosForma([...eq1, ...eq2])
      // Mano a mano: la forma de los dos
      : `<div class="an-previa-row"><span class="an-prob-l">${tx('Forma','Forma')} (${h(eq1[0])} · ${h(eq2[0])})</span>
      <span>${forma(eq1)} <span class="an-muted">·</span> ${forma(eq2)}</span></div>`}
  </div>`;
}

// De los cuatro pelotaris de un partido de parejas, el que llega en mejor
// y en peor forma: victorias en sus últimos 5 partidos (a igualdad, en los
// últimos 10). Si todos están igual, no se muestra.
function htmlExtremosForma(jugadores){
  if(jugadores.length < 3) return '';
  const v = (n, k) => formaReciente(n, k).filter(r => r==='V').length;
  const datos = jugadores.map(n => ({n, v5:v(n,5), j5:formaReciente(n,5).length, v10:v(n,10)}))
    .filter(d => d.j5 > 0)
    .sort((a,b) => b.v5-a.v5 || b.v10-a.v10);
  if(datos.length < 2) return '';
  const mejor = datos[0], peor = datos[datos.length-1];
  if(mejor.v5 === peor.v5 && mejor.v10 === peor.v10) return '';
  const lado = eq => `${h(eq.n)} <span class="an-muted">${eq.v5}${t('abbr_v')}–${eq.j5-eq.v5}${t('abbr_d')}</span>`;
  return `<div class="an-previa-row"><span class="an-prob-l">${tx('De los cuatro','Lauetatik')}</span>
    <span class="an-extremos">
      <span class="an-mejor" title="${tx('Mejor forma: más victorias en sus últimos 5 partidos','Formarik onena: azken 5 partidetan garaipen gehien')}">▲ ${tx('Mejor forma','Formarik onena')}: ${lado(mejor)}</span>
      <span class="an-peor" title="${tx('Peor forma: menos victorias en sus últimos 5 partidos','Formarik txarrena: azken 5 partidetan garaipen gutxien')}">▼ ${tx('Peor forma','Formarik txarrena')}: ${lado(peor)}</span>
    </span></div>`;
}

// ════════════════════════════════════════════════════════════
// FICHA DE FRONTÓN
// ════════════════════════════════════════════════════════════
function abrirFronton(nombre, scroll=true, navegar=true){
  if(navegar && !document.getElementById('sec-frontones').classList.contains('active')){
    const btn = secBtn('frontones'); if(btn) showSec('frontones', btn, true);
  }
  const det = document.getElementById('frontonDetail');
  const parts = PARTIDOS.filter(p=>p.fronton===nombre);
  if(!det || !parts.length) return;
  _frontonActual = nombre;
  setHash('#/fronton/'+slugify(nombre));

  const info = Object.values(CAT_FRONTONES).find(f=>(f.nombre||'').toUpperCase()===nombre.toUpperCase()) || {};
  const ciudad = parts[0].ciudad || '';
  const mapsUrl = info.google_maps_link || `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent('Fronton '+nombre)}`;
  const orden = [...parts].sort((a,b)=>parseDate(a.fecha)-parseDate(b.fecha));

  const st = {};
  parts.forEach(p=>['equipo1','equipo2'].forEach(eq=>pels(p[eq]).forEach(n=>{
    st[n]=st[n]||{pj:0,pg:0}; st[n].pj++; if(p.ganador===eq) st[n].pg++;
  })));
  const top = Object.entries(st).filter(([,s])=>s.pj>=3)
    .sort((a,b)=>b[1].pg-a[1].pg || b[1].pg/b[1].pj-a[1].pg/a[1].pj).slice(0,8);

  const perdedor = parts.map(p=>Math.min(p.puntos1,p.puntos2));
  const mediaPerd = (perdedor.reduce((a,b)=>a+b,0)/parts.length).toFixed(1);
  const igualados = Math.round(parts.filter(p=>Math.abs(p.puntos1-p.puntos2)<=3).length/parts.length*100);
  const oficiales = parts.filter(p=>!(p.tipo||'').startsWith('festival')).length;

  det.innerHTML = `
    <div class="ch-card">
      <div class="an-head">
        <div><div class="an-camp-nombre">${h(nombre)}</div><div class="an-muted">${h(ciudad)}</div></div>
        <div class="an-acciones">
          ${botonCompartir(nombre)}
          <a href="${mapsUrl}" target="_blank" rel="noopener noreferrer" class="btn-ghost">${t('fronton_como_llegar')}</a>
          <button class="btn-ghost" onclick="cerrarFronton()" aria-label="${tx('Cerrar','Itxi')}">✕</button>
        </div>
      </div>
      <div class="an-kpis">
        <div><div class="an-kpi-v">${parts.length}</div><div class="an-kpi-l">${tx('Partidos','Partidak')}</div></div>
        <div><div class="an-kpi-v">${oficiales}</div><div class="an-kpi-l">${tx('Oficiales','Ofizialak')}</div></div>
        <div><div class="an-kpi-v">${mediaPerd}</div><div class="an-kpi-l">${tx('Tantos del perdedor (media)','Galtzailearen tantoak (batez bestekoa)')}</div></div>
        <div><div class="an-kpi-v">${igualados}%</div><div class="an-kpi-l">${tx('Decididos por 3 o menos','3 edo gutxiagoz erabakiak')}</div></div>
      </div>
      <div class="an-muted" style="margin-bottom:1rem">${tx('Desde','Noiztik')} ${orden[0].fecha} · ${tx('último','azkena')} ${orden[orden.length-1].fecha}</div>
      <div class="an-grid2">
        <div>
          <div class="an-sub">${tx('Más victorias aquí (mín. 3 partidos)','Hemen garaipen gehien (gutx. 3 partida)')}</div>
          <table class="comp-table"><thead><tr><th>${tx('Pelotari','Pilotaria')}</th><th class="an-num">${t('abbr_v')}</th><th class="an-num">${t('abbr_pj')}</th><th class="an-num">%</th></tr></thead>
          <tbody>${top.map(([n,s])=>`<tr><td><span class="clk" onclick="goToPel('${esc(n)}')">${h(n)}</span></td><td class="an-num">${s.pg}</td><td class="an-num">${s.pj}</td><td class="an-num">${Math.round(s.pg/s.pj*100)}%</td></tr>`).join('')
            ||`<tr><td colspan="4" class="an-muted">${tx('Pocos partidos','Partida gutxi')}</td></tr>`}</tbody></table>
        </div>
        <div>
          <div class="an-sub">${tx('Últimos partidos','Azken partidak')}</div>
          <table class="comp-table an-partidos"><tbody>${orden.slice(-6).reverse().map(p=>`<tr>
            <td class="an-fecha">${p.fecha}</td>
            <td class="${p.ganador==='equipo1'?'an-win':''}">${h(neq(p.equipo1))}</td>
            <td class="an-num an-marcador">${p.puntos1}–${p.puntos2}</td>
            <td class="${p.ganador==='equipo2'?'an-win':''}">${h(neq(p.equipo2))}</td></tr>`).join('')}</tbody></table>
          <button class="btn" style="margin-top:.8rem" onclick="filtrarPorFronton('${esc(nombre)}')">${tx('Ver todos los partidos','Partida guztiak ikusi')} →</button>
        </div>
      </div>
    </div>
    ${htmlEstadisticasFronton(parts)}`;
  det.classList.add('active');
  if(scroll) det.scrollIntoView({behavior:'smooth', block:'start'});
}

// Más estadísticas del frontón: mejor porcentaje, parejas, por modalidad y
// año, partidos más ajustados y mayores diferencias
function htmlEstadisticasFronton(parts){
  const n = parts.length;
  const MIN_PCT = 5, MIN_PAREJA = 3;
  const tantos = (parts.reduce((s,p)=>s+p.puntos1+p.puntos2,0)/n).toFixed(1);
  const st = {}, parejas = {};
  parts.forEach(p=>['equipo1','equipo2'].forEach(eq=>{
    const gana = p.ganador===eq, js = pels(p[eq]);
    js.forEach(x=>{ st[x]=st[x]||{pj:0,pg:0}; st[x].pj++; if(gana) st[x].pg++; });
    if(js.length===2){ const k=js.join(' / '); parejas[k]=parejas[k]||{pj:0,pg:0,js}; parejas[k].pj++; if(gana) parejas[k].pg++; }
  }));
  const mejorPct = Object.entries(st).filter(([,s])=>s.pj>=MIN_PCT)
    .sort((a,b)=>b[1].pg/b[1].pj-a[1].pg/a[1].pj || b[1].pj-a[1].pj).slice(0,8);
  const topParejas = Object.values(parejas).filter(s=>s.pj>=MIN_PAREJA)
    .sort((a,b)=>b.pg-a.pg || b.pg/b.pj-a.pg/a.pj).slice(0,5);
  const MOD = {parejas:tx('Parejas','Binaka'), mano:tx('Mano a mano','Buruz buru'), cuatro:tx('4 y medio',"Lau t'erdi")};
  const porMod = {}; parts.forEach(p=>{ porMod[p.modalidad]=(porMod[p.modalidad]||0)+1; });
  const porAnio = {}; parts.forEach(p=>{ const a=getYear(p); porAnio[a]=(porAnio[a]||0)+1; });
  const maxAnio = Math.max(...Object.values(porAnio));
  const dif = p => Math.abs(p.puntos1-p.puntos2);
  const recientes = [...parts].sort((a,b)=>parseDate(b.fecha)-parseDate(a.fecha));
  const ajustados = recientes.filter(p=>dif(p)===1);
  const palizas = [...parts].sort((a,b)=>dif(b)-dif(a) || parseDate(b.fecha)-parseDate(a.fecha)).slice(0,4);
  const nombres = js => js.map(x=>`<span class="clk" onclick="goToPel('${esc(x)}')">${h(x)}</span>`).join(' / ');
  const filaP = p => `<tr><td class="an-fecha">${p.fecha}</td>
    <td class="${p.ganador==='equipo1'?'an-win':''}">${h(neq(p.equipo1))}</td>
    <td class="an-num an-marcador">${p.puntos1}–${p.puntos2}</td>
    <td class="${p.ganador==='equipo2'?'an-win':''}">${h(neq(p.equipo2))}</td></tr>`;
  const vacio = cols => `<tr><td colspan="${cols}" class="an-muted">${tx('Pocos partidos','Partida gutxi')}</td></tr>`;
  return `<div class="ch-card an-fron-stats">
    <h3>${tx('Estadísticas del frontón','Frontoiaren estatistikak')}</h3>
    <div class="an-kpis">
      <div><div class="an-kpi-v">${tantos}</div><div class="an-kpi-l">${tx('Tantos por partido (los dos)','Tantoak partidako (biak)')}</div></div>
      <div><div class="an-kpi-v">${ajustados.length}</div><div class="an-kpi-l">${tx('Partidos por un tanto','Tanto bategatik')}</div></div>
      ${Object.entries(porMod).sort((a,b)=>b[1]-a[1]).map(([m,k])=>`<div><div class="an-kpi-v">${k}</div><div class="an-kpi-l">${MOD[m]||m}</div></div>`).join('')}
    </div>
    <div class="an-sub">${tx('Partidos por año','Partidak urteka')}</div>
    <div class="an-fron-anios">${Object.keys(porAnio).sort().map(a=>`<div><span class="an-fron-bar" style="height:${Math.max(6,porAnio[a]/maxAnio*60)}px"></span><b>${porAnio[a]}</b><span>${a}</span></div>`).join('')}</div>
    <div class="an-grid2">
      <div>
        <div class="an-sub">${tx(`Mejor porcentaje aquí (mín. ${MIN_PCT} partidos)`,`Ehuneko onena hemen (gutx. ${MIN_PCT} partida)`)}</div>
        <table class="comp-table"><thead><tr><th>${tx('Pelotari','Pilotaria')}</th><th class="an-num">%</th><th class="an-num">${t('abbr_v')}</th><th class="an-num">${t('abbr_pj')}</th></tr></thead>
        <tbody>${mejorPct.map(([x,s])=>`<tr><td>${nombres([x])}</td><td class="an-num"><b>${Math.round(s.pg/s.pj*100)}%</b></td><td class="an-num">${s.pg}</td><td class="an-num">${s.pj}</td></tr>`).join('') || vacio(4)}</tbody></table>
      </div>
      <div>
        <div class="an-sub">${tx(`Parejas con más victorias (mín. ${MIN_PAREJA})`,`Garaipen gehien dituzten bikoteak (gutx. ${MIN_PAREJA})`)}</div>
        <table class="comp-table"><thead><tr><th>${tx('Pareja','Bikotea')}</th><th class="an-num">${t('abbr_v')}</th><th class="an-num">${t('abbr_pj')}</th><th class="an-num">%</th></tr></thead>
        <tbody>${topParejas.map(s=>`<tr><td>${nombres(s.js)}</td><td class="an-num"><b>${s.pg}</b></td><td class="an-num">${s.pj}</td><td class="an-num">${Math.round(s.pg/s.pj*100)}%</td></tr>`).join('') || vacio(4)}</tbody></table>
      </div>
      <div>
        <div class="an-sub">${tx('Los más ajustados (por un tanto)','Estuenak (tanto batez)')}</div>
        <div class="an-table-wrap"><table class="comp-table an-partidos"><tbody>${ajustados.slice(0,5).map(filaP).join('') || vacio(4)}</tbody></table></div>
      </div>
      <div>
        <div class="an-sub">${tx('Mayores diferencias','Alde handienak')}</div>
        <div class="an-table-wrap"><table class="comp-table an-partidos"><tbody>${palizas.map(filaP).join('')}</tbody></table></div>
      </div>
    </div>
  </div>`;
}

function cerrarFronton(){
  const det = document.getElementById('frontonDetail');
  if(det){ det.classList.remove('active'); det.innerHTML=''; }
  _frontonActual = null;
  if(document.getElementById('sec-frontones').classList.contains('active')) setHash('#/frontones');
}


// ════════════════════════════════════════════════════════════
// SEGUIR PELOTARIS Y AVISOS DE LA CARTELERA
// Los pelotaris seguidos se guardan en este navegador. Cuando uno aparece en
// la cartelera se avisa con una notificación (al abrir la web y, en Android
// con la app instalada, también en segundo plano: ver sw.js).
// ════════════════════════════════════════════════════════════
const SEG_KEY = 'eskupilota-seguidos', AVI_KEY = 'eskupilota-avisados';
const leerLS = (k, def) => { try{ const v = JSON.parse(localStorage.getItem(k)); return v ?? def; }catch(e){ return def; } };
const guardarLS = (k, v) => { try{ localStorage.setItem(k, JSON.stringify(v)); }catch(e){} };
// Misma normalización que sw.js: 'P. ETXEBERRIA' = 'P.ETXEBERRIA', 'DARIO' = 'DARÍO'
const normNombre = n => (n||'').normalize('NFD').replace(/[̀-ͯ]/g,'').toLowerCase().replace(/[^a-z0-9]/g,'');
const claveAviso = (ev, p) => `${ev.fecha}|${ev.fronton||''}|${(p.eq1||[]).join('-')}|${(p.eq2||[]).join('-')}`;

function seguidos(){ return leerLS(SEG_KEY, []); }
function sigue(n){ return seguidos().includes(n); }

// Copia para el service worker, que no puede leer localStorage
function sincronizarPrefsSW(){
  try{
    caches.open('eskupilota-prefs').then(c => {
      c.put('/__prefs/seguidos', new Response(JSON.stringify(seguidos())));
      c.put('/__prefs/avisados', new Response(JSON.stringify(leerLS(AVI_KEY, []))));
    }).catch(()=>{});
  }catch(e){}
}

function pintarBotonSeguir(){
  const b = document.getElementById('btnSeguir');
  if(!b || !_perfilNombre) return;
  const on = sigue(_perfilNombre);
  b.classList.toggle('on', on);
  b.setAttribute('aria-pressed', on);
  b.querySelector('span').textContent = on ? tx('Siguiendo','Jarraitzen') : tx('Seguir','Jarraitu');
}

function toggleSeguir(nombre){
  const s = seguidos(), i = s.indexOf(nombre);
  if(i >= 0) s.splice(i, 1); else s.push(nombre);
  guardarLS(SEG_KEY, s);
  sincronizarPrefsSW();
  pintarBotonSeguir();
  if(i < 0 && 'Notification' in window && Notification.permission === 'default') pedirAvisos();
  if(typeof _CART_EVENTOS !== 'undefined' && _CART_EVENTOS.length) renderCartelera({partidos: _CART_EVENTOS});
}

function partidosDeSeguidos(eventos){
  const seg = new Map(seguidos().map(n => [normNombre(n), n]));
  if(!seg.size) return [];
  const res = [];
  (eventos||[]).forEach(ev => (ev.partidos||[]).forEach(p => {
    const suyos = [...new Set([...(p.eq1||[]), ...(p.eq2||[])].map(normNombre).filter(k => seg.has(k)).map(k => seg.get(k)))];
    if(suyos.length) res.push({ev, p, suyos, clave: claveAviso(ev, p)});
  }));
  return res;
}

async function pedirAvisos(){
  if(!('Notification' in window)) return;
  if(Notification.permission === 'default'){
    try{ await Notification.requestPermission(); }catch(e){}
  }
  if(Notification.permission === 'granted'){
    registrarAvisosSegundoPlano();
    if(typeof _CART_EVENTOS !== 'undefined' && _CART_EVENTOS.length) avisarSeguidos(_CART_EVENTOS);
  }
  if(typeof _CART_EVENTOS !== 'undefined' && _CART_EVENTOS.length) renderCartelera({partidos: _CART_EVENTOS});
}

async function registrarAvisosSegundoPlano(){
  try{
    const reg = await navigator.serviceWorker.ready;
    if(!('periodicSync' in reg)) return;
    const st = await navigator.permissions.query({name: 'periodic-background-sync'});
    if(st.state === 'granted') await reg.periodicSync.register('avisos-seguidos', {minInterval: 12*60*60*1000});
  }catch(e){}
}

async function avisarSeguidos(eventos){
  if(!('Notification' in window) || Notification.permission !== 'granted') return;
  const avisados = new Set(leerLS(AVI_KEY, []));
  const nuevos = partidosDeSeguidos(eventos).filter(x => !avisados.has(x.clave));
  if(!nuevos.length) return;
  let reg = null;
  try{ reg = navigator.serviceWorker && await navigator.serviceWorker.getRegistration(); }catch(e){}
  const mostrar = (titulo, op) => { try{ reg ? reg.showNotification(titulo, op) : new Notification(titulo, op); }catch(e){} };
  const base = {icon: 'icon-192.png', badge: 'favicon-32.png', data: {url: '/#/cartelera'}};
  if(nuevos.length > 3){
    // Muchos a la vez (p. ej. al activar los avisos): uno solo con el resumen
    mostrar(tx(`${nuevos.length} partidos de tus pelotaris en la cartelera`, `Zure pilotarien ${nuevos.length} partida kartelan`),
      {...base, tag: 'resumen-seguidos', body: [...new Set(nuevos.flatMap(x => x.suyos))].join(', ')});
  } else {
    nuevos.forEach(x => mostrar(`${x.suyos.join(', ')} · ${x.ev.fecha}${x.ev.hora ? ' ' + x.ev.hora + 'h' : ''}`,
      {...base, tag: x.clave, body: `${(x.p.eq1||[]).join(' / ')} vs ${(x.p.eq2||[]).join(' / ')} · ${x.ev.fronton||''}`}));
  }
  nuevos.forEach(x => avisados.add(x.clave));
  guardarLS(AVI_KEY, [...avisados].slice(-300));
  sincronizarPrefsSW();
}

// Recuadro al principio de la cartelera con los partidos de los seguidos
function htmlSeguidosCartelera(eventos){
  const seg = seguidos();
  if(!seg.length){
    return `<div class="cart-seg-tip">☆ ${tx('Sigue a tus pelotaris desde su ficha y verás aquí sus próximos partidos, con aviso en el móvil.',
      'Jarraitu zure pilotariei haien fitxatik eta hemen ikusiko dituzu haien hurrengo partidak, mugikorrean abisuarekin.')}</div>`;
  }
  const lista = partidosDeSeguidos(eventos);
  const permiso = 'Notification' in window ? Notification.permission : 'no';
  const avisoBtn = permiso === 'default'
    ? `<button class="btn-ghost" onclick="pedirAvisos()">🔔 ${tx('Activar avisos','Abisuak aktibatu')}</button>`
    : permiso === 'granted' ? `<span class="cart-seg-ok">🔔 ${tx('Avisos activados','Abisuak aktibatuta')}</span>`
    : permiso === 'denied' ? `<span class="an-muted">${tx('Los avisos están bloqueados en este navegador','Abisuak blokeatuta daude nabigatzaile honetan')}</span>` : '';
  return `<div class="cart-seg">
    <div class="cart-seg-head"><b>★ ${tx('Tus pelotaris','Zure pilotariak')}</b> <span class="an-muted">${seg.map(h).join(', ')}</span> ${avisoBtn}</div>
    ${lista.length ? lista.map(x => `<div class="cart-seg-fila"><span class="cart-seg-fecha">${x.ev.fecha}${x.ev.hora ? ' · ' + x.ev.hora + 'h' : ''}</span>
        <span>${h((x.p.eq1||[]).join(' / '))} <span class="an-muted">vs</span> ${h((x.p.eq2||[]).join(' / '))}</span>
        <span class="an-muted">${h(x.ev.fronton||'')}</span></div>`).join('')
      : `<div class="an-muted">${tx('No tienen partidos en la cartelera por ahora.','Oraingoz ez dute partidarik kartelan.')}</div>`}
  </div>`;
}


// ════════════════════════════════════════════════════════════
// VERSIÓN NUEVA DE LA WEB
// index.html carga app.js?v=NN. Si el index.html del servidor pide otra
// versión, esta página es vieja (caché del navegador, del service worker o
// una pestaña/app abierta desde hace días): al arrancar se recarga sola y,
// si ya se está usando, se ofrece un botón «Actualizar».
// ════════════════════════════════════════════════════════════
const VERSION_WEB = ((document.querySelector('script[src*="app.js?v="]')||{}).src||'').match(/v=(\d+)/)?.[1] || null;
let _ultimaComprobacion = 0;

async function comprobarVersion(alArrancar=false){
  if(!VERSION_WEB || Date.now() - _ultimaComprobacion < 60*1000) return;
  _ultimaComprobacion = Date.now();
  try{
    const r = await fetch('index.html?_=' + Date.now(), {cache: 'no-store'});
    if(!r.ok) return;
    const v = (await r.text()).match(/app\.js\?v=(\d+)/)?.[1];
    if(!v || v === VERSION_WEB) return;
    // Solo se recarga sola una vez por versión (si no, un proxy que sirva un
    // index.html viejo provocaría recargas sin fin)
    let ya = null; try{ ya = sessionStorage.getItem('recargado-v'); }catch(e){}
    if(alArrancar && ya !== v){ try{ sessionStorage.setItem('recargado-v', v); }catch(e){} return actualizarWeb(); }
    avisoNuevaVersion();
  }catch(e){}
}

async function actualizarWeb(){
  try{
    const reg = navigator.serviceWorker && await navigator.serviceWorker.getRegistration();
    if(reg) await reg.update().catch(()=>{});
    const ks = await caches.keys();
    await Promise.all(ks.filter(k => k !== 'eskupilota-prefs').map(k => caches.delete(k)));
  }catch(e){}
  location.reload();
}

function avisoNuevaVersion(){
  if(document.getElementById('nuevaVersion')) return;
  const d = document.createElement('div');
  d.id = 'nuevaVersion'; d.className = 'nueva-version'; d.setAttribute('role', 'status');
  d.innerHTML = `<span>${tx('Hay una versión nueva de la web','Webaren bertsio berria dago')}</span><button onclick="actualizarWeb()">${tx('Actualizar','Eguneratu')}</button>`;
  document.body.appendChild(d);
}

// Al volver a la pestaña o a la app (en el móvil quedan abiertas días)
document.addEventListener('visibilitychange', () => { if(document.visibilityState === 'visible') comprobarVersion(); });

// ════════════════════════════════════════════════════════════
// COMPARTIR POR WHATSAPP (menú del móvil)
// ════════════════════════════════════════════════════════════
function compartirWhatsApp(){
  const url = 'https://www.eskupilotastats.com/' + (LANG === 'eu' ? '?lang=eu' : '') + (location.hash || '');
  const texto = tx('Estadísticas de pelota a mano: resultados, cartelera, ranking y cara a cara 👉 ',
                   'Esku pilotako estatistikak: emaitzak, kartela, sailkapena eta aurrez aurrekoak 👉 ') + url;
  window.open('https://wa.me/?text=' + encodeURIComponent(texto), '_blank', 'noopener');
  if(typeof closeDrawer === 'function') closeDrawer();
}


// ════════════════════════════════════════════════════════════
// PERFIL: COMPARAR DOS TEMPORADAS
// ════════════════════════════════════════════════════════════
function statsTemporada(nombre, anio){
  const parts = filtByTipoYear(activeTipo, anio).filter(p=>pels(p.equipo1).includes(nombre)||pels(p.equipo2).includes(nombre));
  const lado = p => pels(p.equipo1).includes(nombre) ? 'equipo1' : 'equipo2';
  const gana = p => p.ganador === lado(p);
  const pg = parts.filter(gana).length;
  const tf = parts.reduce((s,p)=>s+(lado(p)==='equipo1'?p.puntos1:p.puntos2),0);
  const tc = parts.reduce((s,p)=>s+(lado(p)==='equipo1'?p.puntos2:p.puntos1),0);
  const ofi = parts.filter(p=>p.categoria==='campeonato'||p.categoria==='torneo');
  const comp = {}, fron = {};
  parts.forEach(p=>{
    pels(p[lado(p)]).filter(x=>x!==nombre).forEach(x=>{ comp[x]=(comp[x]||0)+1; });
    const f = fron[p.fronton] = fron[p.fronton]||{pj:0,pg:0}; f.pj++; if(gana(p)) f.pg++;
  });
  const compa = Object.entries(comp).sort((a,b)=>b[1]-a[1])[0];
  const mejorF = Object.entries(fron).filter(([,f])=>f.pj>=3).sort((a,b)=>b[1].pg/b[1].pj-a[1].pg/a[1].pj||b[1].pj-a[1].pj)[0];
  const fin = new Date(+anio, 11, 31, 23, 59), ini = new Date(+anio-1, 11, 31, 23, 59);
  const tieneElo = (ELO_HIST[nombre]||[]).some(([f])=>getYear({fecha:f})===anio);
  return {
    pj: parts.length, pg, pp: parts.length-pg, pct: parts.length ? pg/parts.length*100 : null,
    tpp: parts.length ? tf/parts.length : null, dif: tf-tc,
    elo: tieneElo ? eloEnFecha(nombre, fin) : null, deltaElo: tieneElo ? eloEnFecha(nombre, fin)-eloEnFecha(nombre, ini) : null,
    ofiV: ofi.filter(gana).length, ofiD: ofi.length-ofi.filter(gana).length,
    txapelas: palmares(nombre, parts).ganadas.length,
    compa: compa ? `${compa[0]} (${compa[1]})` : '—',
    mejorF: mejorF ? `${mejorF[0]} (${Math.round(mejorF[1].pg/mejorF[1].pj*100)}%)` : '—',
  };
}

function htmlTemporadas(nombre){
  const anios = [...new Set(PARTIDOS.filter(p=>pels(p.equipo1).includes(nombre)||pels(p.equipo2).includes(nombre)).map(getYear))].sort().reverse();
  if(anios.length < 2) return '';
  const opts = sel => anios.map(a=>`<option value="${a}" ${a===sel?'selected':''}>${a}</option>`).join('');
  return `<div class="ch-card pf-temporadas" id="pfTemporadas">
    <h3>${tx('Comparar temporadas','Denboraldiak alderatu')}</h3>
    <div class="pf-temp-sel">
      <label class="flabel" for="tmpA">${tx('Temporada','Denboraldia')} 1</label><select id="tmpA" onchange="renderTemporadas()">${opts(anios[1])}</select>
      <span class="an-muted">vs</span>
      <label class="flabel" for="tmpB">${tx('Temporada','Denboraldia')} 2</label><select id="tmpB" onchange="renderTemporadas()">${opts(anios[0])}</select>
    </div>
    <div id="pfTempTabla">${htmlTablaTemporadas(nombre, anios[1], anios[0])}</div>
  </div>`;
}

function renderTemporadas(){
  const el = document.getElementById('pfTempTabla');
  if(el) el.innerHTML = htmlTablaTemporadas(_perfilNombre, document.getElementById('tmpA').value, document.getElementById('tmpB').value);
}

function htmlTablaTemporadas(nombre, a1, a2){
  const A = statsTemporada(nombre, a1), B = statsTemporada(nombre, a2);
  const num = (v, dec=0, signo=false) => v===null ? '—' : (signo && v>0 ? '+' : '') + (dec ? v.toFixed(dec) : Math.round(v));
  // [etiqueta, valor A, valor B, clave numérica para marcar el mejor (mayor es mejor; 'menor' al revés)]
  const filas = [
    [tx('Partidos','Partidak'), A.pj, B.pj],
    [tx('Victorias','Garaipenak'), A.pg, B.pg, 'mayor'],
    [tx('Derrotas','Porrotak'), A.pp, B.pp, 'menor'],
    ['% ' + tx('victorias','garaipenak'), A.pct, B.pct, 'mayor', v=>num(v)+'%'],
    [tx('Tantos por partido','Tantoak partidako'), A.tpp, B.tpp, 'mayor', v=>num(v,1)],
    [tx('Diferencia de tantos','Tanto aldea'), A.dif, B.dif, 'mayor', v=>num(v,0,true)],
    [tx('Elo al acabar el año','Eloa urte amaieran'), A.elo, B.elo, 'mayor', v=>num(v)],
    [tx('Elo ganado en el año','Urtean irabazitako Eloa'), A.deltaElo, B.deltaElo, 'mayor', v=>num(v,0,true)],
    [tx('Oficiales (V–D)','Ofizialak (G–P)'), `${A.ofiV}–${A.ofiD}`, `${B.ofiV}–${B.ofiD}`],
    [tx('Txapelas','Txapelak'), A.txapelas, B.txapelas, 'mayor'],
    [tx('Compañero más habitual','Bikotekide ohikoena'), A.compa, B.compa],
    [tx('Mejor frontón (mín. 3)','Frontoi onena (gutx. 3)'), A.mejorF, B.mejorF],
  ];
  const celda = (v, otro, criterio, fmt) => {
    const txt = fmt ? fmt(v) : v;
    const mejor = criterio && typeof v==='number' && typeof otro==='number' && v!==otro &&
      (criterio==='mayor' ? v>otro : v<otro);
    return `<td class="an-num ${mejor?'pf-temp-mejor':''}">${h(String(txt))}</td>`;
  };
  return `<div class="an-table-wrap"><table class="comp-table pf-temp-tabla">
    <thead><tr><th></th><th class="an-num">${a1}</th><th class="an-num">${a2}</th></tr></thead>
    <tbody>${filas.map(([l,a,b,cr,fmt])=>`<tr><td>${l}</td>${celda(a,b,cr,fmt)}${celda(b,a,cr,fmt)}</tr>`).join('')}</tbody></table></div>
    ${activeTipo!=='todos' ? `<p class="an-nota">${tx('Solo la modalidad elegida arriba.','Goian aukeratutako modalitatea bakarrik.')}</p>` : ''}`;
}


// ════════════════════════════════════════════════════════════
// IMÁGENES PARA COMPARTIR: partido de la cartelera y cara a cara
// Mismo estilo que la ficha del pelotari (1080×1350, cabecera verde y pie)
// ════════════════════════════════════════════════════════════
async function lienzoCompartir(kicker, subtitulo){
  const W = 1080, H = 1350, M = 64;
  const cv = document.createElement('canvas'); cv.width = W; cv.height = H;
  const c = cv.getContext('2d');
  try{ await document.fonts.ready; }catch(e){}
  const logo = await cargarImagen('logo-header.png');
  const k = {
    cv, c, W, H, M,
    VERDE: '#007A3D', VERDE_OSC: '#005a2c', ROJO: '#C8102E', AZUL: '#2471a3', TEXTO: '#1A1A1A', GRIS: '#5e6d63', FONDO: '#f5f7f4',
    disp: (px, w=700) => `${w} ${px}px "Barlow Condensed", "Arial Narrow", sans-serif`,
    mono: px => `600 ${px}px "JetBrains Mono", monospace`,
    sans: (px, w=500) => `${w} ${px}px Inter, system-ui, sans-serif`,
  };
  k.texto = (s, x, y, font, color, align='left') => { c.font = font; c.fillStyle = color; c.textAlign = align; c.fillText(s, x, y); };
  k.caja = (x, y, w, h, r=18, color='#fff') => { c.fillStyle = color; c.beginPath(); c.roundRect(x, y, w, h, r); c.fill(); };
  // Texto que cabe en 'max' bajando la letra hasta 'min' px y, si no, cortado con '…'
  k.encajar = (s, max, px, min=30, w=700) => {
    for(; px > min; px -= 2){ c.font = k.disp(px, w); if(c.measureText(s).width <= max) return [s, k.disp(px, w)]; }
    c.font = k.disp(px, w); while(s.length > 3 && c.measureText(s).width > max) s = s.slice(0, -2) + '…';
    return [s, k.disp(px, w)];
  };
  c.fillStyle = k.FONDO; c.fillRect(0, 0, W, H);
  const g = c.createLinearGradient(0, 0, W, 250); g.addColorStop(0, k.VERDE); g.addColorStop(1, k.VERDE_OSC);
  c.fillStyle = g; c.fillRect(0, 0, W, 250);
  if(logo) c.drawImage(logo, M, 40, 220, 220 * logo.height / logo.width);
  k.texto(kicker.toUpperCase(), W-M, 88, k.mono(26), '#fff', 'right');
  if(subtitulo){ const [s2, f2] = k.encajar(subtitulo, W - 2*M, 40, 26, 600); k.texto(s2, M, 205, f2, 'rgba(255,255,255,.92)'); }
  // Pie
  c.fillStyle = k.VERDE; c.fillRect(0, H - 80, W, 80);
  k.texto('eskupilotastats.com', M, H - 30, k.sans(28, 700), '#fff');
  k.texto(`${tx('Datos hasta el','Datuak')} ${PARTIDOS.length ? PARTIDOS[0].fecha : ''}${tx('','ra arte')}`, W - M, H - 30, k.mono(20), 'rgba(255,255,255,.85)', 'right');
  return k;
}

// Nombres de un equipo en una o dos líneas grandes
function pintarEquipo(k, nombres, y, color){
  const [s, f] = k.encajar(nombres.join(' / '), k.W - 2*k.M, 92, 44, 800);
  k.texto(s, k.W/2, y, f, color, 'center');
}

function pintarForma(k, nombre, x, y, color){
  const forma = formaReciente(nombre, 5);
  const [s, f] = k.encajar(nombre, 300, 34, 24);
  k.texto(s, x, y + 38, f, color);
  forma.forEach((r, i) => {
    const xx = x + 320 + i * 48;
    k.caja(xx, y, 40, 52, 8, r === 'V' ? k.VERDE : '#e3e8e1');
    k.texto(r === 'V' ? t('abbr_v') : t('abbr_d'), xx + 20, y + 37, k.disp(30), r === 'V' ? '#fff' : k.GRIS, 'center');
  });
  if(!forma.length) k.texto('—', x + 320, y + 38, k.disp(32), k.GRIS);
}

// Partido de la cartelera: equipos, pronóstico, cara a cara y forma
async function compartirPartidoCartelera(btn, ev){
  if(ev) ev.stopPropagation();
  const card = btn.closest('.cart-partido-wrap').querySelector('[data-partido]');
  const p = JSON.parse(decodeURIComponent(card.dataset.partido));
  const eq1 = (p.eq1||[]).filter(Boolean), eq2 = (p.eq2||[]).filter(Boolean);
  const r1 = eq1.map(resolverPelotari), r2 = eq2.map(resolverPelotari);
  const conocidos = !r1.includes(null) && !r2.includes(null) && r1.length && r2.length;
  const comp = [p.categoria ? etiquetaPartido(p).lbl : '', p.fase ? textoFase(p) : ''].filter(Boolean).join(' · ');
  const k = await lienzoCompartir(tx('Próximo partido','Hurrengo partida'), `${p.fecha}${p.hora ? ' · ' + p.hora + 'h' : ''} · ${p.fronton || ''}`);
  const {c, W, M} = k;
  let y = 330;
  if(comp) k.texto(comp.toUpperCase(), W/2, y, k.mono(28), k.VERDE, 'center');
  pintarEquipo(k, eq1, y + 115, k.ROJO);
  k.texto('VS', W/2, y + 195, k.disp(54, 800), k.GRIS, 'center');
  pintarEquipo(k, eq2, y + 290, k.AZUL);
  y += 340;
  if(conocidos){
    const prob = probVictoria(r1, r2), cc = caraACara(r1, r2);
    // Pronóstico
    k.caja(M, y, W - 2*M, 150);
    k.texto(tx('PRONÓSTICO','PRONOSTIKOA'), M + 28, y + 46, k.mono(20), k.GRIS);
    if(prob !== null){
      const p1 = Math.round(prob*100), bw = W - 2*M - 56, bx = M + 28;
      k.texto(p1 + '%', bx, y + 110, k.disp(56), k.ROJO);
      k.texto((100 - p1) + '%', bx + bw, y + 110, k.disp(56), k.AZUL, 'right');
      const ix = bx + 130, iw = bw - 260;
      k.caja(ix, y + 76, iw * p1/100, 22, 11, k.ROJO);
      k.caja(ix + iw * p1/100 + 4, y + 76, iw * (100-p1)/100 - 4, 22, 11, k.AZUL);
    }
    k.texto(`${tx('CARA A CARA','AURREZ AURRE')}: ${cc.g1} – ${cc.g2}`, W - M - 28, y + 46, k.mono(20), k.GRIS, 'right');
    y += 174;
    // Forma de los cuatro (o dos)
    const filas = [...r1.map(n => [n, k.ROJO]), ...r2.map(n => [n, k.AZUL])];
    const alto = 60 + filas.length * 70;
    k.caja(M, y, W - 2*M, alto);
    k.texto(tx('FORMA · ÚLTIMOS 5','FORMA · AZKEN 5'), M + 28, y + 46, k.mono(20), k.GRIS);
    filas.forEach(([n, col], i) => pintarForma(k, n, M + 28, y + 66 + i * 70, col));
  } else {
    k.texto(tx('Pelotaris por confirmar','Pilotariak zehazteko'), W/2, y + 120, k.disp(48), k.GRIS, 'center');
  }
  entregarImagen(k.cv, `partido-${slugify([...eq1, 'vs', ...eq2].join('-'))}.jpg`,
    `${eq1.join(' / ')} vs ${eq2.join(' / ')} · EskupilotaStats`);
}

// Cara a cara (1 contra 1 o pareja contra pareja): victorias y últimos enfrentamientos
async function imagenCaraACara({kicker, filtro, nombreA, nombreB, partidos, esA, fichero}){
  const k = await lienzoCompartir(kicker, filtro);
  const {W, M} = k;
  let wA = 0, tA = 0, tB = 0;
  partidos.forEach(p => { const a = esA(p); if(p.ganador === a) wA++; tA += a==='equipo1'?p.puntos1:p.puntos2; tB += a==='equipo1'?p.puntos2:p.puntos1; });
  const n = partidos.length, wB = n - wA;
  let y = 320;
  const [sA, fA] = k.encajar(nombreA, W/2 - M - 20, 64, 32, 800);
  const [sB, fB] = k.encajar(nombreB, W/2 - M - 20, 64, 32, 800);
  k.texto(sA, M, y + 40, fA, k.ROJO);
  k.texto(sB, W - M, y + 40, fB, k.AZUL, 'right');
  k.texto(String(wA), M, y + 230, k.disp(200, 800), wA >= wB ? k.ROJO : k.GRIS);
  k.texto(String(wB), W - M, y + 230, k.disp(200, 800), wB >= wA ? k.AZUL : k.GRIS, 'right');
  k.texto(tx('VICTORIAS','GARAIPENAK'), W/2, y + 140, k.mono(22), k.GRIS, 'center');
  k.texto(`${n} ${tx('partidos','partida')}`, W/2, y + 190, k.disp(44), k.TEXTO, 'center');
  if(n){
    k.texto(`${(tA/n).toFixed(1)} ${tx('tantos/partido','tanto/partida')}`, M, y + 280, k.mono(22), k.GRIS);
    k.texto(`${(tB/n).toFixed(1)} ${tx('tantos/partido','tanto/partida')}`, W - M, y + 280, k.mono(22), k.GRIS, 'right');
  }
  y += 320;
  const ult = partidos.slice(0, 6);
  k.caja(M, y, W - 2*M, 70 + Math.max(1, ult.length) * 74);
  k.texto(tx('ÚLTIMOS ENFRENTAMIENTOS','AZKEN NORGEHIAGOKAK'), M + 28, y + 48, k.mono(20), k.GRIS);
  ult.forEach((p, i) => {
    const a = esA(p), ptA = a==='equipo1'?p.puntos1:p.puntos2, ptB = a==='equipo1'?p.puntos2:p.puntos1;
    const yy = y + 110 + i * 74;
    k.texto(p.fecha, M + 28, yy, k.mono(22), k.GRIS);
    const [fr, ff] = k.encajar(p.fronton || '', 330, 32, 22, 600);
    k.texto(fr, M + 230, yy, ff, k.TEXTO);
    k.texto(String(ptA), W - M - 150, yy, k.disp(46), ptA > ptB ? k.ROJO : k.GRIS, 'right');
    k.texto('–', W - M - 118, yy, k.disp(40), k.GRIS, 'center');
    k.texto(String(ptB), W - M - 86, yy, k.disp(46), ptB > ptA ? k.AZUL : k.GRIS, 'left');
  });
  if(!ult.length) k.texto('—', M + 28, y + 110, k.disp(40), k.GRIS);
  entregarImagen(k.cv, fichero, `${nombreA} vs ${nombreB} · EskupilotaStats`);
}

function botonImagen(onclick){
  return `<button class="btn-ghost an-share" onclick="${onclick}" aria-label="${tx('Compartir como imagen','Irudi gisa partekatu')}">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/></svg>
    <span>${tx('Compartir imagen','Irudia partekatu')}</span></button>`;
}

function compartirH2H(){
  const u = _h2hUltimo; if(!u) return;
  imagenCaraACara({kicker: tx('Cara a cara','Aurrez aurre'), filtro: u.filtro, nombreA: u.p1, nombreB: u.p2, partidos: u.enfs,
    esA: p => pels(p.equipo1).includes(u.p1) ? 'equipo1' : 'equipo2',
    fichero: `cara-a-cara-${slugify(u.p1)}-${slugify(u.p2)}.jpg`});
}

function compartirC4(){
  const u = _c4Ultimo; if(!u) return;
  const A = u.z1 ? `${u.d1} / ${u.z1}` : u.d1, B = u.z2 ? `${u.d2} / ${u.z2}` : u.d2;
  imagenCaraACara({kicker: tx('Pareja contra pareja','Bikotea bikotearen aurka'),
    filtro: u.anio !== 'todos' ? tx('Temporada ','') + u.anio + tx('',' denboraldia') : tx('Todos los partidos','Partida guztiak'),
    nombreA: A, nombreB: B, partidos: u.exactos,
    esA: p => pels(p.equipo1).includes(u.d1) ? 'equipo1' : 'equipo2',
    fichero: `parejas-${slugify(A)}-${slugify(B)}.jpg`});
}


// ════════════════════════════════════════════════════════════
// FILTROS DE LA FICHA DEL PELOTARI (detrás del botón «Filtrar»)
// Modalidad, año y fechas son los mismos de la lista de pelotaris; frontón,
// compañero y rival solo valen dentro de la ficha.
// ════════════════════════════════════════════════════════════
function toggleFiltrosPerfil(forzar){
  const p = document.getElementById('pfFiltros'), b = document.getElementById('btnFiltrarPf');
  const abrir = forzar ?? p.hidden;
  p.hidden = !abrir;
  b.setAttribute('aria-expanded', abrir);
  b.classList.toggle('on', abrir);
}

function pintarFiltrosPerfil(nombre){
  const el = document.getElementById('pfFiltros');
  if(!el) return;
  const suyos = PARTIDOS.filter(p=>pels(p.equipo1).includes(nombre)||pels(p.equipo2).includes(nombre));
  const cuenta = f => { const c={}; suyos.forEach(p=>f(p).forEach(k=>{ c[k]=(c[k]||0)+1; })); return Object.entries(c).sort((a,b)=>b[1]-a[1]||a[0].localeCompare(b[0])); };
  const mio = p => pels(p.equipo1).includes(nombre) ? pels(p.equipo1) : pels(p.equipo2);
  const suyo = p => pels(p.equipo1).includes(nombre) ? pels(p.equipo2) : pels(p.equipo1);
  const frontones = cuenta(p=>[p.fronton]), comps = cuenta(p=>mio(p).filter(x=>x!==nombre)), rivales = cuenta(p=>suyo(p));
  const opt = (lista, sel) => `<option value="">${t('sel_todos')}</option>` +
    lista.map(([k,n])=>`<option value="${h(k)}" ${k===sel?'selected':''}>${h(k)} (${n})</option>`).join('');
  const activos = [activeTipo!=='todos', activeYearPel!=='todos', !!dateRangePel.desde, !!dateRangePel.hasta,
    !!pfExtra.fronton, !!pfExtra.comp, !!pfExtra.rival].filter(Boolean).length;
  const n = document.querySelector('#btnFiltrarPf .btn-filtrar-n');
  if(n){ n.hidden = !activos; n.textContent = activos; }
  el.innerHTML = `
    <div class="pf-filtros-grid">
      <div class="fg"><label class="flabel" for="pfMod">${t('flabel_modalidad')}</label>
        <select id="pfMod" onchange="cambiarFiltroPerfil('mod',this.value)">${TIPOS.map(x=>`<option value="${x.k}" ${x.k===activeTipo?'selected':''}>${h(x.k==='todos'?t('sel_todos'):({campeonato:t('tipo_parejas'),manomanista:t('tipo_mano'),cuatro:t('tipo_cuatro')}[x.k]||x.lbl))}</option>`).join('')}</select></div>
      <div class="fg"><label class="flabel" for="pfAnio">${t('flabel_año')}</label>
        <select id="pfAnio" onchange="cambiarFiltroPerfil('anio',this.value)"><option value="todos">${t('sel_todos')}</option>${getYears().map(y=>`<option value="${y}" ${y===activeYearPel?'selected':''}>${y}</option>`).join('')}</select></div>
      <div class="fg"><label class="flabel" for="pfDesde">${t('flabel_desde')}</label>
        <input type="date" id="pfDesde" value="${dateRangePel.desde||''}" onchange="cambiarFiltroPerfil('desde',this.value)"></div>
      <div class="fg"><label class="flabel" for="pfHasta">${t('flabel_hasta')}</label>
        <input type="date" id="pfHasta" value="${dateRangePel.hasta||''}" onchange="cambiarFiltroPerfil('hasta',this.value)"></div>
      <div class="fg"><label class="flabel" for="pfFron">${t('flabel_fronton')}</label>
        <select id="pfFron" onchange="cambiarFiltroPerfil('fronton',this.value)">${opt(frontones, pfExtra.fronton)}</select></div>
      <div class="fg"><label class="flabel" for="pfComp">${tx('Compañero','Bikotekidea')}</label>
        <select id="pfComp" onchange="cambiarFiltroPerfil('comp',this.value)">${opt(comps, pfExtra.comp)}</select></div>
      <div class="fg"><label class="flabel" for="pfRival">${tx('Rival','Aurkaria')}</label>
        <select id="pfRival" onchange="cambiarFiltroPerfil('rival',this.value)">${opt(rivales, pfExtra.rival)}</select></div>
    </div>
    <div class="pf-filtros-pie">
      <span class="an-muted">${h(textoFiltroPelotaris())} · ${nPartidos(partidosPerfil(nombre).length)}</span>
      ${activos ? `<button class="btn-ghost" onclick="limpiarFiltrosPerfil()">${t('btn_limpiar')}</button>` : ''}
    </div>`;
}

// Refleja en la lista de pelotaris los filtros compartidos (sin volver a pintarla)
function sincronizarFiltrosPelotaris(){
  document.querySelectorAll('#pillsPel .pill').forEach(b=>{
    b.classList.remove('on','onF','onM','onC');
    if(b.dataset.tipo===activeTipo) b.classList.add(activeTipo==='manomanista'?'onM':activeTipo==='cuatro'?'onC':'on');
  });
  document.querySelectorAll('#yearPillsPel .ypill').forEach(b=>b.classList.toggle('on', b.dataset.year===activeYearPel));
  const d=document.getElementById('pDateDesde'), hh=document.getElementById('pDateHasta'), cl=document.getElementById('pDateClear');
  if(d) d.value = dateRangePel.desde||''; if(hh) hh.value = dateRangePel.hasta||'';
  if(cl) cl.style.display = (dateRangePel.desde||dateRangePel.hasta) ? '' : 'none';
}

function cambiarFiltroPerfil(campo, valor){
  if(campo==='mod') activeTipo = valor;
  else if(campo==='anio') activeYearPel = valor;
  else if(campo==='desde' || campo==='hasta') dateRangePel[campo] = valor;
  else pfExtra[campo] = valor;
  sincronizarFiltrosPelotaris();
  openPerfil(_perfilNombre);
}

function limpiarFiltrosPerfil(){
  activeTipo = 'todos'; activeYearPel = 'todos'; dateRangePel.desde = ''; dateRangePel.hasta = '';
  pfExtra = {fronton:'', comp:'', rival:''};
  sincronizarFiltrosPelotaris();
  openPerfil(_perfilNombre);
}


// ════════════════════════════════════════════════════════════
// DUELO INDIVIDUAL (al pulsar «Estadísticas» en un mano a mano o 4 y medio
// de la cartelera): enfrentamientos a 4½, a mano, la trayectoria de cada uno
// en la competición que se juega y, debajo, el cara a cara general
// ════════════════════════════════════════════════════════════
let _h2hContexto = null;   // {p1, p2, modalidad, competicion}

function htmlDueloIndividual(p1, p2, ctx){
  const solo = p => pels(p.equipo1).length===1 && pels(p.equipo2).length===1;
  const enfr = mod => PARTIDOS.filter(p => solo(p) && p.modalidad===mod && (
    (pels(p.equipo1)[0]===p1 && pels(p.equipo2)[0]===p2) || (pels(p.equipo1)[0]===p2 && pels(p.equipo2)[0]===p1)));
  const gana = (p, n) => pels(p[p.ganador])[0]===n;
  const bloque = (titulo, ps, cls) => {
    if(!ps.length) return '';
    const w1 = ps.filter(p=>gana(p,p1)).length, w2 = ps.length-w1;
    const pt = (p, n) => pels(p.equipo1)[0]===n ? p.puntos1 : p.puntos2;
    return `<div class="ch-card duelo-bloque ${cls}">
      <h3>${titulo} <span class="an-muted">· ${nPartidos(ps.length)}</span></h3>
      <div class="duelo-marcador">
        <div class="${w1>=w2?'an-up':''}"><span>${h(p1)}</span><b>${w1}</b></div>
        <div class="an-muted">–</div>
        <div class="${w2>=w1?'an-up':''}"><span>${h(p2)}</span><b>${w2}</b></div>
      </div>
      <div class="an-table-wrap"><table class="comp-table an-partidos"><tbody>${ps.slice(0,8).map(p=>`<tr>
        <td class="an-fecha">${p.fecha}${p.fase?`<br><span class="fase-lbl">${textoFase(p)}</span>`:''}</td>
        <td><span class="tag ${etiquetaPartido(p).cls}">${etiquetaPartido(p).lbl}</span></td>
        <td class="an-fron">${h(p.fronton)}</td>
        <td class="an-num an-marcador"><span class="${gana(p,p1)?'an-win':''}">${pt(p,p1)}</span>–<span class="${gana(p,p2)?'an-win':''}">${pt(p,p2)}</span></td></tr>`).join('')}</tbody></table></div>
    </div>`;
  };
  // Trayectoria de cada uno en la competición (todas sus ediciones)
  let compHtml = '';
  const comp = ctx.competicion && !/^festival/i.test(ctx.competicion) ? ctx.competicion : null;
  if(comp){
    const base = baseComp(comp), anio = anioComp(comp);
    const deComp = PARTIDOS.filter(p => p.competicion && baseComp(p.competicion)===base);
    const fila = n => {
      const suyos = deComp.filter(p => pels(p.equipo1).includes(n) || pels(p.equipo2).includes(n));
      const v = suyos.filter(p => pels(p[p.ganador]).includes(n)).length;
      const est = suyos.filter(p => anioComp(p.competicion)===anio);
      const ve = est.filter(p => pels(p[p.ganador]).includes(n)).length;
      const tit = suyos.filter(p => p.fase==='final' && pels(p[p.ganador]).includes(n)).map(p => anioComp(p.competicion));
      return `<tr><td><span class="clk" onclick="goToPel('${esc(n)}')">${h(n)}</span></td>
        <td class="an-num"><b class="an-up">${v}</b>–<span class="an-down">${suyos.length-v}</span></td>
        <td class="an-num">${suyos.length ? Math.round(v/suyos.length*100)+'%' : '—'}</td>
        <td class="an-num">${est.length ? `${ve}–${est.length-ve}` : '—'}</td>
        <td class="rk-titulos">${tit.length ? tit.map(a=>`<span class="rk-txapela camp">🏆 ${a}</span>`).join('') : '<span class="an-muted">—</span>'}</td></tr>`;
    };
    compHtml = `<div class="ch-card duelo-bloque">
      <h3>${h(tComp(comp.replace(/\s*\b20\d\d\b/, '')))} <span class="an-muted">· ${tx('todas las ediciones','edizio guztiak')}</span></h3>
      <div class="an-table-wrap"><table class="comp-table">
        <thead><tr><th>${tx('Pelotari','Pilotaria')}</th><th class="an-num">${t('abbr_v')}–${t('abbr_d')}</th><th class="an-num">%</th><th class="an-num">${anio||tx('Esta edición','Edizio hau')}</th><th>${tx('Txapelas','Txapelak')}</th></tr></thead>
        <tbody>${fila(p1)}${fila(p2)}</tbody></table></div>
    </div>`;
  }
  const cuatro = bloque(tx('Enfrentamientos a 4 y medio',"Lau t'erdiko norgehiagokak"), enfr('cuatro'), 'cuatro');
  const mano = bloque(tx('Enfrentamientos a mano','Buruz buruko norgehiagokak'), enfr('mano'), 'mano');
  const nada = !cuatro && !mano ? `<div class="ch-card duelo-bloque"><p class="an-muted" style="margin:0">${tx('No se han enfrentado nunca en individual (4 y medio ni mano a mano).',"Ez dira inoiz banaka aritu (lau t'erdian ez buruz buru).")}</p></div>` : '';
  return `<div class="duelo">${cuatro}${mano}${nada}${compHtml}
    <div class="duelo-general">${tx('Cara a cara general · todas las modalidades','Aurrez aurre orokorra · modalitate guztiak')}</div></div>`;
}
