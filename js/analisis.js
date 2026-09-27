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
function caraACara(eq1, eq2){
  let g1=0, g2=0;
  PARTIDOS.forEach(p=>{
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
  frontones:'frontones', ranking:'ranking', campeonatos:'campeonatos', contacto:'contacto'};
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
// RANKING ELO (pestaña del ranking)
// ════════════════════════════════════════════════════════════
function htmlRankingElo(){
  const activos = getActivePlayers();
  const lista = Object.keys(ELO)
    .filter(n=>!filterActivos || activos.has(n.toUpperCase()))
    .sort((a,b)=>ELO[b]-ELO[a]).slice(0,30);
  const filas = lista.map((n,i)=>{
    const hist = ELO_HIST[n]||[];
    const hace = hist.length>10 ? hist[hist.length-11][1] : ELO_BASE;
    const dif = Math.round(ELO[n]-hace);
    return `<tr><td class="an-num">${i+1}</td><td><span class="clk" onclick="goToPel('${esc(n)}')">${h(n)}</span> <span class="rol-badge ${getRol(n)==='zaguero'?'zag':'del'}">${getRol(n)==='zaguero'?t('rol_zag'):t('rol_del')}</span></td>
      <td class="an-num"><b>${Math.round(ELO[n])}</b></td>
      <td class="an-num ${dif>=0?'an-up':'an-down'}">${dif>0?'▲ +':dif<0?'▼ ':''}${dif}</td>
      <td>${chipsForma(formaReciente(n,5))}</td></tr>`;
  }).join('');
  return `<div class="ch-card">
    <h3>${tx('Ranking Elo','Elo sailkapena')}</h3>
    <p class="an-nota">${tx('Puntuación de fuerza calculada con todos los partidos: sube al ganar y baja al perder, más cuanto más fuerte es el rival. Todos empiezan en 1500. La columna "10 últ." es el cambio en los últimos 10 partidos.',
      'Partida guztiekin kalkulatutako indar puntuazioa: irabaztean igo eta galtzean jaisten da, arerioa zenbat eta indartsuago orduan eta gehiago. Denek 1500ean hasten dute. "Azken 10" zutabea azken 10 partidetako aldaketa da.')}</p>
    <div class="an-table-wrap"><table class="comp-table">
      <thead><tr><th>#</th><th>${tx('Pelotari','Pilotaria')}</th><th class="an-num">Elo</th><th class="an-num">${tx('10 últ.','Azken 10')}</th><th>${tx('Forma','Forma')}</th></tr></thead>
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

function buildCampeonatos(){
  const sel = document.getElementById('campSel');
  if(!sel) return;
  const comps = competicionesOficiales();
  const porAnio = {};
  comps.forEach(c=>{ const a=(c.nombre.match(/\b(20\d\d)\b/)||[])[1]||tx('Otros','Besteak'); (porAnio[a]=porAnio[a]||[]).push(c); });
  // Años de más reciente a más antiguo; las competiciones sin año, al final
  const orden = Object.keys(porAnio).sort((x,y)=>(/^\d+$/.test(y)-/^\d+$/.test(x)) || y.localeCompare(x));
  sel.innerHTML = orden.map(a=>
    `<optgroup label="${h(a)}">${porAnio[a]
      .sort((x,y)=>(y.nombre.startsWith('Campeonato')-x.nombre.startsWith('Campeonato'))||x.nombre.localeCompare(y.nombre))
      .map(c=>`<option value="${c.id}">${h(tComp(c.nombre))}</option>`).join('')}</optgroup>`).join('');
  if(!_campActual && comps.length){
    // Por defecto, el campeonato (no torneo) que se jugó más recientemente
    _campActual = (comps.find(c=>c.nombre.startsWith('Campeonato'))||comps[0]).id;
  }
  if(_campActual) sel.value = _campActual;
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

// Cuadro de eliminatorias: rondas sin grupos, coherentes (cada equipo una vez
// por ronda y no más partidos de los que caben). Cada partido se coloca a la
// altura del de la ronda siguiente al que da paso su ganador.
const RONDAS_KO = ['eliminatoria','octavos','cuartos','semifinal','final'];
const MAX_POR_RONDA = {eliminatoria:16, octavos:8, cuartos:4, semifinal:2, final:1};

function htmlCuadro(parts){
  const clave = eq => pels(eq).join(' / ');
  const ganadorDe = p => clave(p.ganador==='equipo1' ? p.equipo1 : p.equipo2);
  const rondas = RONDAS_KO
    .map(f => [f, parts.filter(p => p.fase===f && !p.grupo)])
    .filter(([f, ps]) => ps.length && ps.length <= MAX_POR_RONDA[f] &&
      new Set(ps.flatMap(p => [clave(p.equipo1), clave(p.equipo2)])).size === ps.length*2);
  if(rondas.length < 2) return '';
  for(let i = rondas.length-2; i >= 0; i--){
    const sig = rondas[i+1][1];
    const pos = p => {
      const g = ganadorDe(p);
      const j = sig.findIndex(q => clave(q.equipo1)===g || clave(q.equipo2)===g);
      return j < 0 ? 99 : j*2 + (clave(sig[j].equipo1)===g ? 0 : 1);
    };
    rondas[i][1] = [...rondas[i][1]].sort((a,b) => pos(a)-pos(b) || parseDate(a.fecha)-parseDate(b.fecha));
  }
  const deducidas = parts.some(p => p.fase_deducida && RONDAS_KO.includes(p.fase));
  const lado = (p, eq, pts) => `<div class="an-ko-eq ${p.ganador===eq?'gana':''}">
      <span>${pels(p[eq]).map(n=>`<span class="clk" onclick="goToPel('${esc(n)}')">${h(n)}</span>`).join(' / ')}</span>
      <b>${p[pts]}</b></div>`;
  const partido = p => `<div class="an-ko-m">
      ${lado(p,'equipo1','puntos1')}${lado(p,'equipo2','puntos2')}
      <div class="an-ko-info">${p.fecha} · ${h(p.fronton)}</div></div>`;
  return `<div class="ch-card">
    <h3>${tx('Eliminatorias','Kanporaketak')}</h3>
    ${deducidas ? `<p class="an-nota">${tx('ⓘ Algunas rondas están deducidas del calendario: el partido anterior de cada clasificado, cuando cuadra como eliminatoria.',
      'ⓘ Kanporaketa batzuk egutegitik ondorioztatuak dira: sailkatu bakoitzaren aurreko partida, kanporaketa gisa bat datorrenean.')}</p>` : ''}
    <div class="an-ko" style="--rondas:${rondas.length}">
      ${rondas.map(([f, ps]) => `<div class="an-ko-col">
        <div class="an-ko-tit">${t('fase_'+f)}</div>
        <div class="an-ko-lista">${ps.map(partido).join('')}</div></div>`).join('')}
    </div></div>`;
}

function setCampModo(modo){ _campModo = modo; renderCampeonato(_campActual); }

function renderCampeonato(id){
  const cont = document.getElementById('campContent');
  if(!cont) return;
  if(!document.getElementById('campSel').options.length) buildCampeonatos();
  const comp = CAT_COMPETICIONES[id] || CAT_COMPETICIONES[_campActual];
  if(!comp){ cont.innerHTML = `<div class="nodata">${tx('Sin competiciones','Txapelketarik ez')}</div>`; return; }
  _campActual = comp.id;
  document.getElementById('campSel').value = comp.id;
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
  const cc = caraACara(eq1, eq2);
  const forma = eq => chipsForma(formaReciente(eq[0],5));
  const p1 = prob==null ? null : Math.round(prob*100);
  return `<div class="an-previa" onclick="event.stopPropagation()">
    ${p1==null?'':`<div class="an-prob" title="${tx('Estimación orientativa según el Elo de cada pelotari. En partidos pasados acertó el ganador en torno al 56% de las veces: los partidos suelen estar muy igualados.','Pilotari bakoitzaren Elo-aren araberako estimazio orientagarria. Iraganeko partidetan irabazlea %56 inguru asmatu zuen: partidak oso parekatuak izan ohi dira.')}">
      <span class="an-prob-l">${tx('Pronóstico','Pronostikoa')}</span>
      <span class="an-prob-n">${p1}%</span>
      <span class="an-prob-bar" aria-hidden="true"><span style="width:${p1}%"></span></span>
      <span class="an-prob-n">${100-p1}%</span>
    </div>`}
    <div class="an-previa-row"><span class="an-prob-l">${tx('Cara a cara','Aurrez aurre')}</span>
      <span>${cc.g1+cc.g2 ? `<b>${cc.g1}</b> – <b>${cc.g2}</b>` : tx('Nunca se han enfrentado','Ez dira inoiz aurrez aurre aritu')}</span></div>
    <div class="an-previa-row"><span class="an-prob-l">${tx('Forma','Forma')} (${h(eq1[0])} · ${h(eq2[0])})</span>
      <span>${forma(eq1)} <span class="an-muted">·</span> ${forma(eq2)}</span></div>
  </div>`;
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
    </div>`;
  det.classList.add('active');
  if(scroll) det.scrollIntoView({behavior:'smooth', block:'start'});
}

function cerrarFronton(){
  const det = document.getElementById('frontonDetail');
  if(det){ det.classList.remove('active'); det.innerHTML=''; }
  _frontonActual = null;
  if(document.getElementById('sec-frontones').classList.contains('active')) setHash('#/frontones');
}
