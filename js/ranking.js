// ════════════════════════════════════════════════════════════
// RANKING OFICIAL (estilo ATP): un ranking por especialidad con su propio
// Elo, podio, movimiento del último mes, evolución del top 10 y récords.
// Las tablas antiguas siguen debajo, en «Más estadísticas».
// ════════════════════════════════════════════════════════════
const RK2_TIPOS = {
  general:   {es:'General',      eu:'Orokorra',     filtro:()=>true,                        min:10},
  mano:      {es:'Manomanista',  eu:'Buruz burukoa', filtro:p=>p.modalidad==='mano',        min:5, ajuste:true},
  cuatro:    {es:'4 y Medio',    eu:"Lau t'erdi",   filtro:p=>p.modalidad==='cuatro',       min:5, ajuste:true},
  delantero: {es:'Delanteros',   eu:'Aurrelariak',  filtro:p=>p.modalidad==='parejas',      min:10, rol:'delantero'},
  zaguero:   {es:'Zagueros',     eu:'Atzelariak',   filtro:p=>p.modalidad==='parejas',      min:10, rol:'zaguero'},
};
const RK2_COLORES = ['#1b7f4b','#2471a3','#c8102e','#d99a00','#8a5cc7','#1aa3a3','#e0672b','#6b8e23','#b0417f','#5a6b7b'];
let rk2Tipo = 'general', rk2Estado = 'activos', rk2Emp = 'todas';
let _rk2Cache = {};

// Elo, historial y resultados de una especialidad.
// - Parejas (delanteros / zagueros) y general: Elo con solo esos partidos.
// - Mano y 4 y medio (`ajuste`): Elo general + un ajuste que solo mueven los
//   partidos de la especialidad. Con un Elo propio, los de la Serie B, que
//   casi solo juegan entre ellos, inflaban su nota (Bakaikoa salía 2º en las
//   dos); el general mezcla a todos en los festivales y los pone en su sitio.
//   Probado con los partidos desde 2025: predice igual o mejor que el Elo
//   propio en mano y 4 y medio, y peor en parejas (por eso allí no se usa).
function rk2Datos(tipo){
  if(_rk2Cache[tipo] && _rk2Cache[tipo].n === PARTIDOS.length) return _rk2Cache[tipo];
  const cfg = RK2_TIPOS[tipo], filtro = cfg.filtro;
  const todos = PARTIDOS.filter(p => p.ganador).sort((a,b) => parseDate(a.fecha)-parseDate(b.fecha));
  const ps = todos.filter(filtro);
  const elo = {}, hist = {}, res = {}, racha = {}, mejorRacha = {}, fronRacha = {}, mejorFron = {};
  const gen = {}, aj = {};            // solo con `ajuste`: Elo general y ajuste de la especialidad
  const r = n => cfg.ajuste ? (gen[n] ?? ELO_BASE) + (aj[n] ?? 0) : (elo[n] ?? ELO_BASE);
  const media = (t, fn) => t.reduce((s,n)=>s+fn(n),0)/t.length;
  (cfg.ajuste ? todos : ps).forEach(p => {
    const t1 = pels(p.equipo1), t2 = pels(p.equipo2);
    if(!t1.length || !t2.length) return;
    const S = p.ganador==='equipo1' ? 1 : 0;
    const K = (p.tipo||'').startsWith('festival') ? ELO_K/2 : ELO_K;
    const f = parseDate(p.fecha), esp = !cfg.ajuste || filtro(p);
    let d = 0;
    if(esp){
      const E = 1/(1+Math.pow(10,(media(t2,r)-media(t1,r))/400));
      d = K*(S-E);
    }
    if(cfg.ajuste){
      // El general se mueve con todos los partidos; el ajuste, con los de la especialidad
      const g = n => gen[n] ?? ELO_BASE;
      const dg = K*(S - 1/(1+Math.pow(10,(media(t2,g)-media(t1,g))/400)));
      t1.forEach(n => { gen[n] = g(n) + dg; if(esp) aj[n] = (aj[n] ?? 0) + d; });
      t2.forEach(n => { gen[n] = g(n) - dg; if(esp) aj[n] = (aj[n] ?? 0) - d; });
      // Su nota cambia también con los partidos de otras modalidades (no cuentan como jugados aquí)
      [...t1, ...t2].forEach(n => { if(aj[n] !== undefined){ elo[n] = r(n); (hist[n] = hist[n] || []).push([f, elo[n], esp]); } });
      if(!esp) return;
    }
    [[t1, d, S===1], [t2, -d, S===0]].forEach(([eq, delta, gana]) => eq.forEach(n => {
      if(!cfg.ajuste){ elo[n] = r(n) + delta; (hist[n] = hist[n] || []).push([f, elo[n]]); }
      (res[n] = res[n] || []).push(gana ? 'V' : 'D');
      // Rachas de victorias (en general y en cada frontón)
      if(gana){
        const c = racha[n] = racha[n] ? {...racha[n], n:racha[n].n+1, hasta:p.fecha} : {n:1, desde:p.fecha, hasta:p.fecha};
        if(!mejorRacha[n] || c.n > mejorRacha[n].n) mejorRacha[n] = c;
      } else racha[n] = null;
      const kf = n + '|' + p.fronton;
      if(gana){
        fronRacha[kf] = (fronRacha[kf]||0) + 1;
        if(!mejorFron[n] || fronRacha[kf] > mejorFron[n].n) mejorFron[n] = {n:fronRacha[kf], fronton:p.fronton};
      } else fronRacha[kf] = 0;
    }));
  });
  return _rk2Cache[tipo] = {n:PARTIDOS.length, ps, elo, hist, res, mejorRacha, mejorFron};
}

// Elo al terminar el día `fecha` y partidos jugados hasta entonces
function rk2EloEn(D, n, fecha){
  let v = null, k = 0;
  for(const [f, e, esp] of (D.hist[n]||[])){ if(f > fecha) break; v = e; if(esp !== false) k++; }
  return {elo:v, pj:k};
}

// Quién entra en el ranking: con el mínimo de partidos, su posición (en
// delanteros / zagueros), y los filtros de estado y empresa
function rk2Candidatos(D, tipo){
  const cfg = RK2_TIPOS[tipo];
  const activos = rk2Estado==='activos' ? getActivePlayers() : null;
  return Object.keys(D.elo).filter(n => (!cfg.rol || getRol(n)===cfg.rol)
    && (!activos || activos.has(n.toUpperCase())) && deEmpresa(n, rk2Emp));
}
function rk2Ranking(D, tipo, fecha){
  const min = RK2_TIPOS[tipo].min;
  return rk2Candidatos(D, tipo).map(n => {
    const x = fecha ? rk2EloEn(D, n, fecha) : {elo:D.elo[n], pj:(D.res[n]||[]).length};
    return {n, elo:x.elo, pj:x.pj};
  }).filter(x => x.elo!=null && x.pj>=min).sort((a,b) => b.elo-a.elo);
}

function rk2Ultima(){ return PARTIDOS.reduce((mx,p)=>{ const d=parseDate(p.fecha); return d>mx?d:mx; }, new Date(0)); }
// 'MARIEZKURRENA II' → 'M', 'P.ETXEBERRIA' → 'PE' (sin los ordinales)
function rk2Iniciales(n){ return h(n.split(/[\s.]+/).filter(w=>w && !/^(I|II|III|IV|V|VI)$/i.test(w)).map(w=>w[0]).join('').slice(0,2)); }
function rk2Foto(n, cls){
  const f = getFoto(n);
  return `<span class="rk2-foto ${cls||''}">${f ? `<img src="${h(f.url)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.remove()">` : ''}<b>${rk2Iniciales(n)}</b></span>`;
}
function rk2Mov(x){
  if(x.prev==null) return `<span class="rk2-mov nuevo" title="${tx('Nuevo en el ranking','Berria sailkapenean')}">${tx('NUEVO','BERRIA')}</span>`;
  const d = x.prev - x.pos;
  return d>0 ? `<span class="rk2-mov up" title="${tx('Puestos ganados en 30 días','30 egunetan irabazitako postuak')}">▲${d}</span>`
       : d<0 ? `<span class="rk2-mov down" title="${tx('Puestos perdidos en 30 días','30 egunetan galdutako postuak')}">▼${-d}</span>`
       : `<span class="rk2-mov eq">=</span>`;
}
function rk2Forma(res){
  return `<span class="rk2-forma" aria-label="${tx('Últimos partidos','Azken partidak')}">${res.slice(-10).map(r=>`<i class="${r==='V'?'v':'d'}" title="${r==='V'?tx('Victoria','Garaipena'):tx('Derrota','Porrota')}"></i>`).join('')}</span>`;
}
function rk2Spark(D, n, hasta){
  const pts = [];
  for(let m=12; m>=0; m--){
    const f = new Date(hasta); f.setMonth(f.getMonth()-m);
    const v = rk2EloEn(D, n, f).elo; if(v!=null) pts.push(v);
  }
  if(pts.length < 2) return '<span class="rk2-spark"></span>';
  const W=96, H=26, lo=Math.min(...pts), hi=Math.max(...pts), rg=(hi-lo)||1;
  const xy = pts.map((v,i)=>`${(i/(pts.length-1)*W).toFixed(1)},${(H-2-(v-lo)/rg*(H-4)).toFixed(1)}`).join(' ');
  const sube = pts[pts.length-1] >= pts[0];
  return `<svg class="rk2-spark ${sube?'up':'down'}" viewBox="0 0 ${W} ${H}" aria-hidden="true"><polyline points="${xy}"/></svg>`;
}

// Evolución del top 10: puesto de cada uno al final de cada mes (último año)
function rk2Bump(D, tipo, top, hasta){
  const meses = [];
  for(let m=11; m>=0; m--){ const f = new Date(hasta.getFullYear(), hasta.getMonth()-m+1, 0); meses.push(f > hasta ? hasta : f); }
  const MAXP = 12, W=760, H=330, M={l:30, r:130, t:14, b:28};
  const pos = meses.map(f => { const rk = rk2Ranking(D, tipo, f); return Object.fromEntries(rk.map((x,i)=>[x.n, i+1])); });
  const x = i => M.l + i*(W-M.l-M.r)/(meses.length-1);
  const y = p => M.t + (p-1)*(H-M.t-M.b)/(MAXP-1);
  const mes = f => f.toLocaleDateString(LANG==='eu'?'eu':'es', {month:'short'});
  const lineas = top.map((n, k) => {
    const c = RK2_COLORES[k % RK2_COLORES.length];
    let d = '', puntos = '', antes = false;
    pos.forEach((pm, i) => {
      const p = pm[n];
      if(p && p <= MAXP){ d += `${antes?'L':'M'}${x(i).toFixed(1)} ${y(p).toFixed(1)}`; puntos += `<circle cx="${x(i).toFixed(1)}" cy="${y(p).toFixed(1)}" r="3.5"><title>${h(n)} · ${mes(meses[i])}: ${p}º</title></circle>`; antes = true; }
      else antes = false;
    });
    const ult = pos[pos.length-1][n];
    return `<g class="rk2-linea" style="--c:${c}"><path d="${d}"/>${puntos}${ult?`<text x="${W-M.r+8}" y="${y(ult)+4}">${ult}. ${h(n)}</text>`:''}</g>`;
  }).join('');
  return `<svg class="rk2-bump" viewBox="0 0 ${W} ${H}" role="img" aria-label="${tx('Puesto de los diez primeros cada mes del último año','Lehen hamarren postua azken urteko hilabete bakoitzean')}">
    ${Array.from({length:MAXP},(_,i)=>`<text class="rk2-eje" x="${M.l-8}" y="${y(i+1)+4}" text-anchor="end">${i+1}</text><line class="rk2-guia" x1="${M.l}" x2="${W-M.r}" y1="${y(i+1)}" y2="${y(i+1)}"/>`).join('')}
    ${meses.map((f,i)=>`<text class="rk2-eje" x="${x(i)}" y="${H-8}" text-anchor="middle">${mes(f)}</text>`).join('')}
    ${lineas}</svg>`;
}

function rk2Records(D, tipo){
  const cfg = RK2_TIPOS[tipo];
  const vale = n => !cfg.rol || getRol(n)===cfg.rol;
  const nombres = Object.keys(D.elo).filter(vale);
  const mejor = (lista, val) => lista.reduce((b, n) => (val(n)!=null && (b==null || val(n) > val(b))) ? n : b, null);
  const card = (ico, tit, valor, quien, extra) => `<div class="rk2-rec"><div class="rk2-rec-ico">${ico}</div>
    <div class="rk2-rec-t">${tit}</div><div class="rk2-rec-v">${valor}</div><div class="rk2-rec-q">${quien}</div>${extra?`<div class="rk2-rec-x">${extra}</div>`:''}</div>`;
  const link = n => `<span class="clk" onclick="goToPel('${esc(n)}')">${h(n)}</span>`;
  const out = [];
  const nr = mejor(nombres, n => D.mejorRacha[n]?.n ?? null);
  if(nr) out.push(card('🔥', tx('Racha más larga','Bolada luzeena'), `${D.mejorRacha[nr].n} ${tx('victorias','garaipen')}`, link(nr), `${D.mejorRacha[nr].desde} → ${D.mejorRacha[nr].hasta}`));
  const tit = {};
  D.ps.forEach(p => { if(p.fase==='final' && (p.categoria==='campeonato'||p.categoria==='torneo')) pels(p[p.ganador]).filter(vale).forEach(n => tit[n]=(tit[n]||0)+1); });
  const nt = mejor(Object.keys(tit), n => tit[n]);
  if(nt) out.push(card('🏆', tx('Más txapelas','Txapela gehien'), tit[nt], link(nt), tx('Desde nov. 2022','2022ko azarotik')));
  const maxElo = n => Math.max(...D.hist[n].map(x=>x[1]));
  const ne = mejor(nombres, maxElo);
  if(ne){ const [f,v] = D.hist[ne].reduce((b,x)=>x[1]>b[1]?x:b); out.push(card('📈', tx('Mejor Elo de la historia','Inoizko Elo onena'), Math.round(v), link(ne), f.toLocaleDateString(LANG==='eu'?'eu':'es'))); }
  const pal = D.ps.filter(p => [...pels(p.equipo1), ...pels(p.equipo2)].some(vale))
    .reduce((b,p) => (!b || Math.abs(p.puntos1-p.puntos2) > Math.abs(b.puntos1-b.puntos2)) ? p : b, null);
  if(pal){ const g1 = p => p.ganador==='equipo1';
    out.push(card('💥', tx('Mayor paliza','Jipoi handiena'), g1(pal) ? `${pal.puntos1}–${pal.puntos2}` : `${pal.puntos2}–${pal.puntos1}`,
      `${pels(pal[pal.ganador]).map(link).join(' / ')}`, `vs ${pels(pal[g1(pal)?'equipo2':'equipo1']).map(h).join(' / ')} · ${pal.fecha}`)); }
  const nf = mejor(nombres, n => D.mejorFron[n]?.n ?? null);
  if(nf) out.push(card('🏟️', tx('Invicto en un frontón','Pilotaleku batean garaitu gabe'), `${D.mejorFron[nf].n} ${tx('seguidas','jarraian')}`, link(nf), h(D.mejorFron[nf].fronton)));
  return out.join('');
}

function renderRanking2(){
  const el = document.getElementById('rk2'); if(!el || !PARTIDOS.length) return;
  document.querySelectorAll('#rk2Tabs .rk2-tab').forEach(b => { const on = b.dataset.tipo===rk2Tipo; b.classList.toggle('on', on); b.setAttribute('aria-selected', on); b.textContent = RK2_TIPOS[b.dataset.tipo][LANG==='eu'?'eu':'es']; });
  rk2PintarFiltros();
  const D = rk2Datos(rk2Tipo), hasta = rk2Ultima();
  const haceMes = new Date(hasta); haceMes.setDate(haceMes.getDate()-30);
  const ahora = rk2Ranking(D, rk2Tipo), antes = rk2Ranking(D, rk2Tipo, haceMes);
  const posAntes = Object.fromEntries(antes.map((x,i)=>[x.n, i+1]));
  ahora.forEach((x,i) => { x.pos = i+1; x.prev = posAntes[x.n] ?? null; x.dif = Math.round(x.elo - (rk2EloEn(D, x.n, haceMes).elo ?? x.elo)); });
  if(!ahora.length){ el.innerHTML = `<div class="nodata"><div class="ic">📭</div>${t('sin_datos')}</div>`; return; }
  const vd = n => { const r = D.res[n]; const v = r.filter(x=>x==='V').length; return `${v}–${r.length-v}`; };
  const rol = n => rk2Tipo==='general' && getRol(n)!=='otro' ? `<span class="rol-badge ${getRol(n)==='zaguero'?'zag':'del'}">${getRol(n)==='zaguero'?t('rol_zag'):t('rol_del')}</span>` : '';
  const nombre = n => `<span class="clk" onclick="goToPel('${esc(n)}')">${h(n)}</span>`;

  // Podio
  const orden = [1, 0, 2].filter(i => ahora[i]);
  const podio = `<div class="rk2-podio">${orden.map(i => { const x = ahora[i]; return `
    <div class="rk2-pod p${i+1}">
      <div class="rk2-pod-medalla">${['🥇','🥈','🥉'][i]}</div>
      ${rk2Foto(x.n, 'grande')}
      <div class="rk2-pod-nombre">${nombre(x.n)}</div>
      <div class="rk2-pod-meta">${htmlLogoEmpresa(getEmpresa(x.n))}${rol(x.n)}${rk2Mov(x)}</div>
      <div class="rk2-pod-elo">${Math.round(x.elo)}<small>Elo</small></div>
      <div class="rk2-pod-vd">${vd(x.n)} · ${rk2Forma(D.res[x.n])}</div>
    </div>`; }).join('')}</div>`;

  // Lista (desde el 4º)
  const lista = `<div class="rk2-lista">${ahora.slice(3).map(x => `
    <div class="rk2-fila">
      <div class="rk2-pos">${x.pos}</div>
      <div class="rk2-movc">${rk2Mov(x)}</div>
      <div class="rk2-quien">${rk2Foto(x.n)}<div><div class="rk2-nom">${nombre(x.n)} ${htmlLogoEmpresa(getEmpresa(x.n))}${rol(x.n)}</div><div class="rk2-vd">${vd(x.n)}</div></div></div>
      <div class="rk2-sparkc">${rk2Spark(D, x.n, hasta)}</div>
      <div class="rk2-formac">${rk2Forma(D.res[x.n])}</div>
      <div class="rk2-elo">${Math.round(x.elo)}</div>
    </div>`).join('')}</div>`;

  // En racha / en caída / racha en curso
  const mini = (tit, cls, xs, val) => `<div class="rk2-mini ${cls}"><h4>${tit}</h4>${xs.length ? xs.map(x => `<div class="rk2-mini-f">${rk2Foto(x.n,'peq')}${nombre(x.n)}<b>${val(x)}</b></div>`).join('') : `<div class="an-muted">—</div>`}</div>`;
  const suben = ahora.filter(x=>x.dif>0).sort((a,b)=>b.dif-a.dif).slice(0,3);
  const bajan = ahora.filter(x=>x.dif<0).sort((a,b)=>a.dif-b.dif).slice(0,3);
  const enCurso = ahora.map(x => { const r = D.res[x.n]; let k=0; for(let i=r.length-1;i>=0 && r[i]==='V';i--) k++; return {...x, racha:k}; })
    .filter(x=>x.racha>=2).sort((a,b)=>b.racha-a.racha).slice(0,3);
  const momento = `<div class="rk2-momento">
    ${mini('🚀 '+tx('En racha','Gorantz'), 'up', suben, x=>`+${x.dif}`)}
    ${mini('📉 '+tx('En caída','Beherantz'), 'down', bajan, x=>`${x.dif}`)}
    ${mini('🔥 '+tx('Victorias seguidas','Garaipenak jarraian'), 'racha', enCurso, x=>x.racha)}
  </div>`;

  el.innerHTML = `
    <p class="an-nota rk2-nota">${tx(`${rk2Tipo==='general' ? 'Ranking por Elo con todos los partidos' : RK2_TIPOS[rk2Tipo].ajuste ? 'Ranking por Elo: el general de cada uno más lo que rinde en esta especialidad (así los de la Serie B no se inflan ganándose entre ellos)' : 'Ranking por Elo con solo los partidos de esta especialidad'}: sube al ganar y baja al perder, más cuanto más fuerte es el rival. Mínimo ${RK2_TIPOS[rk2Tipo].min} partidos. ▲▼: puestos ganados o perdidos en los últimos 30 días.`,
      `${rk2Tipo==='general' ? 'Elo bidezko sailkapena, partida guztiekin' : RK2_TIPOS[rk2Tipo].ajuste ? 'Elo bidezko sailkapena: bakoitzaren Elo orokorra gehi espezialitate honetan ematen duena' : 'Elo bidezko sailkapena, espezialitate honetako partidekin bakarrik'}: irabaztean igo eta galtzean jaisten da, arerioa zenbat eta indartsuago orduan eta gehiago. Gutxienez ${RK2_TIPOS[rk2Tipo].min} partida. ▲▼: azken 30 egunetan irabazitako edo galdutako postuak.`)}</p>
    ${podio}
    ${momento}
    ${lista}
    <div class="ch-card rk2-bloque"><h3>${tx('Evolución del top 10','Lehen hamarren bilakaera')} <span class="an-muted">· ${tx('puesto a final de cada mes','hilabete amaierako postua')}</span></h3>
      <div class="rk2-bump-wrap">${rk2Bump(D, rk2Tipo, ahora.slice(0,10).map(x=>x.n), hasta)}</div></div>
    <div class="ch-card rk2-bloque"><h3>${tx('Récords','Errekorrak')} <span class="an-muted">· ${RK2_TIPOS[rk2Tipo][LANG==='eu'?'eu':'es']}</span></h3>
      <div class="rk2-records">${rk2Records(D, rk2Tipo)}</div></div>`;
}

function rk2PintarFiltros(){
  const el = document.getElementById('rk2Filtros'); if(!el) return;
  const pill = (k, v, txt, act) => `<button type="button" class="ypill${act===v?' on':''}" aria-pressed="${act===v}" onclick="setRk2('${k}','${v}')">${txt}</button>`;
  el.innerHTML = `<div class="year-pills" role="group" aria-label="${t('flabel_estado')}">${pill('est','activos',t('est_actuales'),rk2Estado)}${pill('est','todos',t('sel_todos'),rk2Estado)}</div>
    <div class="year-pills emp-pills" role="group" aria-label="${t('flabel_empresa')}">${pill('emp','todas',t('sel_todas'),rk2Emp)}${pill('emp','aspe',htmlLogoEmpresa('aspe'),rk2Emp)}${pill('emp','baiko',htmlLogoEmpresa('baiko'),rk2Emp)}</div>`;
}
function setRk2(k, v){
  if(k==='tipo') rk2Tipo = v; else if(k==='est') rk2Estado = v; else if(k==='emp') rk2Emp = v;
  renderRanking2();
}
