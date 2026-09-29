// ════════════════════════════════════════════════════════════
// DATOS + ESTADO
// ════════════════════════════════════════════════════════════
let PARTIDOS=[], PELOTARIS={};

// ════════════════════════════════════════════════════════════
// PESTAÑAS ACTIVOS / HISTÓRICO + RANGO DE FECHAS
// ════════════════════════════════════════════════════════════
function setActivosTab(scope, activos, btn){
  filterActivos = activos;
  // Actualizar las dos pestañas (en pelotaris y en ranking) para que estén sincronizadas
  document.querySelectorAll('.ah-tab').forEach(b=>b.classList.remove('on'));
  document.getElementById('ahtab-pel-act').classList.toggle('on', activos);
  document.getElementById('ahtab-pel-hist').classList.toggle('on', !activos);
  document.getElementById('ahtab-rk-act').classList.toggle('on', activos);
  document.getElementById('ahtab-rk-hist').classList.toggle('on', !activos);
  renderPCards();
  buildRanking();
}

function setDateRange(scope, key, value){
  const range = scope==='pel' ? dateRangePel : dateRangeRk;
  range[key] = value;
  document.getElementById(scope+'DateClear').style.display =
    (range.desde || range.hasta) ? 'inline-block' : 'none';
  if(scope==='pel') renderPCards(); else buildRanking();
}

function clearDateRange(scope){
  const range = scope==='pel' ? dateRangePel : dateRangeRk;
  range.desde = ''; range.hasta = '';
  document.getElementById(scope+'DateDesde').value = '';
  document.getElementById(scope+'DateHasta').value = '';
  document.getElementById(scope+'DateClear').style.display = 'none';
  if(scope==='pel') renderPCards(); else buildRanking();
}

function getActivePlayers(){
  // Activo = al menos un partido en los últimos 6 meses desde el partido más reciente
  if(!PARTIDOS.length) return new Set();
  const parseFecha = _parseFechaDDMMYYYY;
  const latest = PARTIDOS.reduce((mx,p)=>{ const dt=parseFecha(p.fecha); return dt>mx?dt:mx; }, new Date(0));
  const cutoff = new Date(latest); cutoff.setMonth(cutoff.getMonth()-6);
  const active = new Set();
  PARTIDOS.forEach(p=>{
    if(parseFecha(p.fecha) < cutoff) return;
    ['equipo1','equipo2'].forEach(eq=>{
      const e=p[eq]||{};
      if(e.delantero) active.add(e.delantero.toUpperCase());
      if(e.zaguero) active.add(e.zaguero.toUpperCase());
    });
  });
  return active;
}
// Rol de cada pelotari según el catálogo (lo calcula el scraper a partir de
// los puestos en los que juega por parejas)
let ROL_POR_NOMBRE = {};
let EMPRESA_POR_NOMBRE = {};   // 'baiko' | 'aspe' (data/pelotaris.json, ver tools/empresas.py)
function getEmpresa(nombre){ return EMPRESA_POR_NOMBRE[(nombre||'').toUpperCase()] || null; }
const NOMBRE_EMPRESA = {baiko:'Baiko', aspe:'Aspe'};
function getRol(nombre){
  const rol = ROL_POR_NOMBRE[(nombre||'').toUpperCase()];
  return (rol==='delantero'||rol==='zaguero') ? rol : 'otro';
}

let activeTipo='todos', sortFld='fecha', sortAsc=false;
let activeYearPel='todos', activeYearRk='todos', activeYearH2H='todos', activeYearC4='todos', activeYearFron='todos';
let h2hMod='todas', h2hSerie='todas';
let compType='h2h';
let activeTipoFron='todos';
// Filtros de fecha personalizados (formato 'YYYY-MM-DD' o '')
let dateRangePel = {desde:'', hasta:''};
let dateRangeRk  = {desde:'', hasta:''};

function _parseFechaDDMMYYYY(f){ try{const[d,m,y]=f.split('/');return new Date(+y,+m-1,+d);}catch{return new Date(0);} }
function _parseFechaISO(s){ if(!s) return null; const[y,m,d]=s.split('-'); return new Date(+y,+m-1,+d); }
// Devuelve true si la fecha del partido está dentro del rango (o si no hay rango)
function partidoEnRango(p, range){
  if(!range || (!range.desde && !range.hasta)) return true;
  const f = _parseFechaDDMMYYYY(p.fecha);
  const desde = _parseFechaISO(range.desde);
  const hasta = _parseFechaISO(range.hasta);
  if(desde && f < desde) return false;
  if(hasta){ // incluir el día 'hasta' completo
    const finDia = new Date(hasta); finDia.setHours(23,59,59,999);
    if(f > finDia) return false;
  }
  return true;
}


function normalizeComp(c){
  const s=(c||'').trim();
  // Todos los festivales bajo "Festival"
  if(/^festival/i.test(s)) return 'Festival';
  // Quita años (4 dígitos)
  return s.replace(/\s*\d{4}\s*/g,' ').replace(/\s+/g,' ').trim();
}

// Traducción de nombres de competición. Se construye por partes en vez de
// con una lista cerrada, para que cualquier competición nueva que cree el
// scraper salga traducida: "Torneo San Mateo Serie A" -> "San Mateo Txapelketa A Seriea".
function tComp(nombre){
  let s = normalizeComp(nombre);                    // sin año; festivales -> 'Festival'
  if(LANG !== 'eu'){
    return s.replace(/\bSan Fermin\b/,'San Fermín').replace(/^Desafio\b/,'Desafío').replace(/\b4 1\/2\b/,'4½');
  }
  const serie = (s.match(/\bSerie ([AB])\b/)||[])[1];
  const promo = /\bPromoción\b/.test(s);
  s = s.replace(/\s*\bSerie [AB]\b/,'').replace(/\s*\bPromoción\b/,'').trim();
  const cuatro = /\b(4 y Medio|4 1\/2|4½)\b/;
  let m, base;
  if(s === 'Festival') base = 'Jaialdia';
  else if((m = s.match(/^Festival Despedida (.+)$/))) base = `${m[1]}${/[aeiou]$/i.test(m[1])?'ren':'en'} agur jaialdia`;
  else if(/^Campeonato Parejas/.test(s)) base = 'Binakako Txapelketa';
  else if(/^Campeonato Manomanista/.test(s)) base = 'Buruz Buruko Txapelketa';
  else if(/^Campeonato/.test(s) && cuatro.test(s)) base = "Lau t'erdiko Txapelketa" + (/Eusko Label/.test(s)?' Eusko Label':'');
  else if((m = s.match(/^Torneo (.+?) (4 y Medio|4 1\/2|4½)$/))) base = `${m[1]} Lau t'erdiko Txapelketa`;
  else if((m = s.match(/^Torneo (.+?) Manomanista$/))) base = `${m[1]} Buruz Buruko Txapelketa`;
  else if((m = s.match(/^Torneo (.+)$/))) base = `${m[1]} Txapelketa`;
  else if((m = s.match(/^Desaf[ií]o Urzante ?(.*)$/))) base = `${m[1] ? m[1]+' ' : ''}Urzante Desafioa`;
  else base = s;                                    // Masters CaixaBank y nombres propios
  return base + (promo ? ' (Promozioa)' : '') + (serie ? ` ${serie} Seriea` : '');
}

// Textos de fase que vienen tal cual de la cartelera de Baiko/Aspe
// ('Semifinales (Grupo A)', '7ª Jornada', 'Festival'...)
function tFase(texto){
  if(!texto) return '';
  // Aspe da algunas fases en los dos idiomas: 'Zortzirenak // Octavos', 'Laurdenak / Cuartos'
  const bi = texto.match(/^([^\s/]+)\s*\/\/?\s*([^\s/]+)$/);
  if(bi && !/\d/.test(texto)) return LANG === 'eu' ? bi[1] : bi[2];
  if(LANG !== 'eu') return texto;
  // 'Final San Mateo Serie B' -> 'San Mateo Txapelketa B Seriea · Finala'
  const fc = texto.match(/^(Final|Finales|Semifinal|Semifinales)\s+((?:Torneo\s+)?(?:San |Masters|Aste|La Blanca|Donostia).+)$/i);
  if(fc) return `${tComp(/^(Torneo|Masters)/i.test(fc[2]) ? fc[2] : 'Torneo '+fc[2])} · ${tFase(fc[1])}`;
  if(/^(Campeonato|Torneo|Desaf[ií]o)\b/i.test(texto) && !/\b(final|jornada|grupo)/i.test(texto)) return tComp(texto);
  const reglas = [
    [/Cuartos de final/gi, 'Final-laurdenak'], [/Octavos de final/gi, 'Final-zortzirenak'],
    [/Semifinales/gi, 'Finalerdiak'], [/Semifinal/gi, 'Finalerdia'],
    [/\bFinales\b/gi, 'Finalak'], [/\bFinal\b(?![-a-z])/gi, 'Finala'],
    [/(\d+)ª Jornada/gi, '$1. jardunaldia'], [/\bJornada\b/gi, 'Jardunaldia'],
    [/\bEliminatoria\b/gi, 'Kanporaketa'], [/\bGrupo ([A-Z])\b/g, '$1 multzoa'],
    [/\bFestival\b/gi, 'Jaialdia'], [/\bAbono\b/gi, 'Abonua'], [/\bD[ií]a (\d+)\b/gi, '$1. eguna'],
    [/\bSerie ([AB])\b/g, '$1 Seriea'], [/\bTorneo San Mateo\b/g, 'San Mateo Txapelketa'],
  ];
  return reglas.reduce((t, [re, por]) => t.replace(re, por), texto);
}

const TIPOS=[
  {k:'todos',      lbl:'Todos'},
  {k:'campeonato', lbl:'Parejas'},
  {k:'manomanista',lbl:'Manomanista'},
  {k:'cuatro',     lbl:'4 y Medio'},
];
const TTAG={
  'campeonato-a':  {cls:'tA',lbl:'tag_seriea'},
  'campeonato-b':  {cls:'tB',lbl:'tag_serieb'},
  'festival':      {cls:'tF',lbl:'tag_festival'},
  'manomanista':   {cls:'tM',lbl:'tag_mano'},
  'manomanista-a': {cls:'tM',lbl:'tag_manoa'},
  'manomanista-b': {cls:'tM',lbl:'tag_manob'},
  'festival-mano': {cls:'tM',lbl:'tag_festival'},
  'cuatro-medio-a':{cls:'tC',lbl:'tag_cuatroa'},
  'cuatro-medio-b':{cls:'tC',lbl:'tag_cuatrob'},
  'festival-cuatro':{cls:'tC',lbl:'tag_festival'},
};

// Etiqueta de un partido según su categoría: campeonato ('Serie A', 'Mano B',
// '4½ A'), torneo ('Torneo A'), desafío o festival
function etiquetaPartido(p){
  const serie = (p.serie||'').toUpperCase();
  const cls = p.modalidad==='mano' ? 'tM' : p.modalidad==='cuatro' ? 'tC' : (serie==='B' ? 'tB' : 'tA');
  if(p.categoria==='torneo')   return {cls, lbl: nombreTorneo(p) || (t('tag_torneo') + (serie ? ' '+serie : ''))};
  if(p.categoria==='desafio')  return {cls:'tF', lbl: t('tag_desafio')};
  if(p.categoria==='campeonato' && p.modalidad==='parejas' && serie) return {cls, lbl: t('tag_parejas')+' '+serie};
  const tt = TTAG[p.tipo];
  return tt ? {cls: tt.cls, lbl: t(tt.lbl)} : {cls:'', lbl: p.tipo||''};
}

// 'Torneo San Mateo Serie A 2026' -> 'San Mateo A'; 'Torneo San Fermin 4 y Medio 2026' -> 'San Fermín 4½'
function nombreTorneo(p){
  const n = (p.competicion||'').replace(/\s*\b20\d\d\b/, '').replace(/^Torneo\s+/i, '')
    .replace(/\s*Serie\s+([AB])\b/i, ' $1').replace(/\s*4 y Medio/i, ' 4½').replace(/Fermin\b/, 'Fermín').trim();
  return n.length <= 22 ? n : '';
}

// 'Semifinal · Grupo A', 'Liguilla · 6ª jornada', 'Final'
function textoFase(p){
  if(!p.fase) return '';
  const partes = [t('fase_'+p.fase)];
  if(p.grupo) partes.push(t('lbl_grupo').replace('{g}', p.grupo));
  if(p.jornada && p.fase==='liga') partes.push(t('lbl_jornada').replace('{n}', p.jornada));
  return partes.join(' · ');
}

// ════════════════════════════════════════════════════════════
// I18N — Traducciones ES / EU
// ════════════════════════════════════════════════════════════
let LANG = 'es';

const I18N = {
  es: {
    // Nav
    nav_cartelera: 'Cartelera',
    nav_resultados: 'Resultados',
    nav_comparador: 'Comparador',
    nav_pelotaris: 'Pelotaris',
    nav_frontones: 'Frontones',
    nav_ranking: 'Ranking',
    nav_campeonatos: 'Campeonatos',
    nav_contacto: 'Contacto',
    // KPIs
    kpi_partidos: 'Partidos', kpi_registrados: 'registrados',
    kpi_pelotaris: 'Pelotaris', kpi_distintos: 'distintos',
    kpi_frontones: 'Frontones',
    kpi_oficiales: 'Oficiales', kpi_campeonatos: 'campeonatos y torneos',
    kpi_festivales: 'Festivales', kpi_amistosos: 'y desafíos',
    // Filtros
    flabel_modalidad: 'Modalidad', flabel_desde: 'Desde', flabel_hasta: 'Hasta',
    flabel_pelotari: 'Pelotari', flabel_competicion: 'Competición', flabel_fronton: 'Frontón',
    flabel_año: 'Año', flabel_serie: 'Serie',
    btn_limpiar: 'Limpiar',
    ph_buscar_pelotari: 'Buscar pelotari…', ph_buscar_fronton: 'Buscar frontón…',
    sel_todas: 'Todas', sel_todos: 'Todos',
    // Tipos
    tipo_todos: 'Todos', tipo_parejas: 'Parejas', tipo_mano: 'Manomanista', tipo_cuatro: '4 y Medio',
    serie_todas: 'Todas', serie_a: 'Serie A', serie_b: 'Serie B', serie_fest: 'Festivales',
    // Tabla
    th_fecha: 'Fecha', th_tipo: 'Tipo', th_fronton: 'Frontón',
    th_equipo1: 'Equipo 1', th_marcador: 'Marcador', th_equipo2: 'Equipo 2', th_comp: 'Competición',
    // Secciones
    sec_partidos: 'Partidos', sec_pelotaris: 'Pelotaris', sec_ranking: 'Ranking',
    sec_comparador: 'Comparador', sec_frontones: 'Frontones', sec_cartelera: 'Cartelera',
    sec_contacto: 'Contacto',
    // Pelotaris
    lbl_victorias: 'Victorias', lbl_partidos: 'Partidos', lbl_pct_vic: '% Vic',
    lbl_derrotas: 'Derrotas', lbl_pts_p: 'Pts/partido', lbl_diferencia: 'Diferencia',
    lbl_compañeros: 'Compañeros de pareja', lbl_rivales: 'Rivales más frecuentes',
    lbl_volver: '← Volver',
    // Comparador H2H
    comp_h2h: 'Head to Head', comp_parejas: 'Comparador Parejas',
    sel_pelotari1: '— Selecciona —', sel_pelotari2: '— Selecciona —',
    lbl_modalidad: 'Modalidad', lbl_serie_h2h: 'Serie', lbl_año_h2h: 'Año',
    h2h_sin_sel: 'Selecciona dos pelotaris distintos',
    // Comparador C4
    sel_delantero: '— Selecciona delantero —',
    sel_zaguero_any: '— Cualquier zaguero —',
    eq_colorada: 'PAREJA COLORADA', eq_azul: 'PAREJA AZUL',
    lbl_delantero: 'Delantero', lbl_zaguero: 'Zaguero',
    c4_identico: 'Partido idéntico', c4_partidos_col: 'Partidos de',
    c4_h2h_del: 'H2H Delanteros', c4_h2h_zag: 'H2H Zagueros',
    c4_sin_zag: 'Selecciona ambos zagueros para ver su H2H',
    c4_con_hist: 'Con historial juntos', c4_sin_hist: 'Sin historial juntos',
    c4_partidos_label: 'partidos',
    c4_ver_mas: 'Ver {n} más ↓',
    c4_tres_de_cuatro: 'Con 3 de los 4 pelotaris',
    c4_tres_sin_zag: 'Selecciona los dos zagueros para ver los partidos con 3 de los 4',
    c4_sin: 'Sin', c4_sin_pel: 'Sin {n}',
    // Stats
    stat_pj: 'Partidos', stat_ganados: 'Ganados', stat_pct_vic: '% Victorias', stat_over: '+36.5 tantos',
    stat_perd: 'perd.',
    // Cartelera
    cart_ver_hist: '→ Ver historial', cart_cargando: '⟳ Cargando cartelera…',
    cart_como_llegar: 'Cómo llegar',
    cart_calendario: 'Calendario', cart_calendario_t: 'Añadir al calendario', cal_pelota: 'Pelota',
    cart_estadisticas: 'Estadísticas',
    fronton_como_llegar: 'Cómo llegar',
    fmap_ver_partidos: 'Ver partidos →',
    cart_no_partidos: 'No hay próximos partidos',
    cart_recarga: '↻ Recargar',
    // Contacto
    cont_intro: '¿Tienes alguna sugerencia, has encontrado un error en los datos o quieres colaborar?<br>Escríbenos y te responderemos lo antes posible.',
    cont_nombre: 'Nombre', cont_email: 'Email', cont_asunto: 'Asunto', cont_mensaje: 'Mensaje',
    cont_ph_nombre: 'Tu nombre', cont_ph_asunto: 'Error en datos, sugerencia…', cont_ph_mensaje: 'Cuéntanos…',
    cont_btn: 'Enviar mensaje',
    cont_ok: '✓ Mensaje preparado — se abrirá tu cliente de correo.',
    cont_directo: 'O escríbenos directamente a',
    // Ranking
    rk_title: 'Ranking',
    // Nodata
    nodata_h2h: 'Sin enfrentamientos en',
    nodata_sin_comp: 'Sin compañeros', nodata_sin_riv: 'Sin rivales',
    // Tags
    tag_seriea: 'Serie A', tag_serieb: 'Serie B', tag_festival: 'Festival',
    tag_mano: 'Manomanista', tag_manoa: 'Mano A', tag_manob: 'Mano B',
    tag_cuatroa: "4½ A", tag_cuatrob: "4½ B",
    // Roles
    rol_del: 'Del', rol_zag: 'Zag',
    // Ranking tabs
    rk_tab_victorias: 'Victorias', rk_tab_tabla: 'Clasificación', rk_tab_titulos: 'Títulos',
    rk_cat_todos: 'Todos', rk_cat_oficiales: 'Oficiales', rk_cat_festivales: 'Festivales',
    rk_tab_pct: '% Victorias',
    rk_tab_roles: 'Del vs Zag',
    rk_tab_parejas: 'Parejas',
    rk_parejas_mas_v: 'Más victorias', rk_parejas_pct: 'Mejor % (mín. {n} partidos)', lbl_mejor_pareja: 'Mejor pareja', lbl_min_pj: 'mín. {n} partidos',
    rk_tab_over: '+36.5 tantos',
    rk_tab_racha: 'Racha',
    rk_min_pj: 'Mínimo {n} partidos',
    rk_solo_parejas: 'solo parejas',
    rk_delanteros: 'Delanteros',
    rk_zagueros: 'Zagueros',
    rk_mejor_racha: 'Mejor racha histórica',
    rk_racha_activa: 'Racha activa ahora',
    rk_seg: 'seg.',
    rk_actual: 'Actual',
    rk_mejor: 'Mejor',
    // Perfil
    lbl_ultimos: 'Últimos partidos',
    lbl_solo_activos: 'Solo activos',
    tab_activos: '⚡ Activos',
    tab_historico: '📜 Histórico',
    lbl_rango_fechas: 'Rango fechas',
    lbl_limpiar: '✕ Limpiar',
    lbl_individual: 'Individual',
    // H2H
    h2h_pts_partido: 'pts/partido',
    // Cartelera
    cart_eventos: 'eventos',
    cart_fuente: 'Fuente',
    cart_actualizado: 'Actualizado',
    // Meses y días
    dias: ['Dom','Lun','Mar','Mié','Jue','Vie','Sáb'],
    meses: ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'],
    // Contacto
    cont_alerta: 'Por favor rellena nombre, email y mensaje.',
    cont_asunto_def: 'Contacto EskupilotaStats',
    // Varios
    sin_partidos: 'Sin partidos',
    sin_enfrentamientos: 'Sin enfrentamientos',
    pareja_exacta: 'pareja exacta',
    lbl_fecha_sort: 'Fecha ↕',
    // Añadidas en la revisión de traducciones
    lbl_fichas: 'Fichas de todos los pelotaris',
    btn_ficha: 'Descargar ficha', btn_filtrar: 'Filtrar', derechos: 'Todos los derechos reservados', compartir_wa: 'Compartir por WhatsApp', app_instalar: 'Instalar la app', app_android: 'App para Android',
    app_ios: 'Para instalarla en el iPhone: abre la web en Safari, pulsa Compartir (el cuadrado con la flecha) y después «Añadir a pantalla de inicio».',
    aria_menu: 'Menú', aria_cerrar: 'Cerrar', aria_mapa: 'Mapa de frontones', aria_saltar: 'Saltar al contenido',
    rk_tab_elo: 'Elo',
    lbl_pelotari1: 'Pelotari 1', lbl_pelotari2: 'Pelotari 2',
    cont_ph_email: 'tu@email.com',
    lbl_pelota_mano: 'Pelota a mano',
    th_compañero: 'Compañero', th_rival: 'Rival',
    abbr_v: 'V', abbr_d: 'D', abbr_pj: 'PJ',
    sin_datos: 'Sin datos suficientes',
    n_partido: '{n} partido', n_partidos: '{n} partidos',
    pct_victorias: '{n}% victorias', pts_p: '{n} pts/p',
    comp_abbr: 'Comp.',
    cart_error: 'No se ha podido cargar la cartelera.',
    map_creditos: 'Créditos del mapa', map_acercar: 'Acercar', map_alejar: 'Alejar',
    abbr_tantos: 'Pts',
    // Categorías y fases (ver scraper/competiciones.py)
    flabel_categoria: 'Categoría', flabel_empresa: 'Empresa', flabel_fase: 'Fase',
    cat_campeonatos: 'Campeonatos', cat_torneos: 'Torneos', cat_desafios: 'Desafíos', cat_festivales: 'Festivales',
    tag_torneo: 'Torneo', tag_desafio: 'Desafío', tag_parejas: 'Parejas',
    fase_liga: 'Liguilla', fase_eliminatoria: 'Eliminatoria', fase_octavos: 'Octavos',
    fase_cuartos: 'Cuartos', fase_semifinal: 'Semifinal', fase_tercero: 'Tercer puesto', fase_final: 'Final',
    lbl_grupo: 'Grupo {g}', lbl_jornada: '{n}ª jornada',
    lbl_campeones: 'Campeones', lbl_final_deducida: 'Final deducida: es el último partido del campeonato',
  },
  eu: {
    nav_cartelera: 'Kartelera',
    nav_resultados: 'Emaitzak',
    nav_comparador: 'Konparatzailea',
    nav_pelotaris: 'Pilotariak',
    nav_frontones: 'Frontoiak',
    nav_ranking: 'Sailkapena',
    nav_campeonatos: 'Txapelketak',
    nav_contacto: 'Kontaktua',
    kpi_partidos: 'Partidak', kpi_registrados: 'erregistratuta',
    kpi_pelotaris: 'Pilotariak', kpi_distintos: 'desberdinak',
    kpi_frontones: 'Frontoiak',
    kpi_oficiales: 'Ofizialak', kpi_campeonatos: 'txapelketak eta torneoak',
    kpi_festivales: 'Jaialdiak', kpi_amistosos: 'eta desafioak',
    flabel_modalidad: 'Modalitatea', flabel_desde: 'Noiztik', flabel_hasta: 'Noiz arte',
    flabel_pelotari: 'Pilotaria', flabel_competicion: 'Lehiaketa', flabel_fronton: 'Frontoia',
    flabel_año: 'Urtea', flabel_serie: 'Seriea',
    btn_limpiar: 'Garbitu',
    ph_buscar_pelotari: 'Bilatu pilotaria…', ph_buscar_fronton: 'Bilatu frontoia…',
    sel_todas: 'Guztiak', sel_todos: 'Guztiak',
    tipo_todos: 'Guztiak', tipo_parejas: 'Binakakoa', tipo_mano: 'Buruz Burukoa', tipo_cuatro: "Lau t'erdi",
    serie_todas: 'Guztiak', serie_a: 'A Seriea', serie_b: 'B Seriea', serie_fest: 'Jaialdiak',
    th_fecha: 'Data', th_tipo: 'Mota', th_fronton: 'Frontoia',
    th_equipo1: '1. Taldea', th_marcador: 'Markagailua', th_equipo2: '2. Taldea', th_comp: 'Lehiaketa',
    sec_partidos: 'Partidak', sec_pelotaris: 'Pilotariak', sec_ranking: 'Sailkapena',
    sec_comparador: 'Konparatzailea', sec_frontones: 'Frontoiak', sec_cartelera: 'Kartelera',
    sec_contacto: 'Kontaktua',
    lbl_victorias: 'Garaipenak', lbl_partidos: 'Partidak', lbl_pct_vic: '% Garaip.',
    lbl_derrotas: 'Porrotak', lbl_pts_p: 'Tanto/partida', lbl_diferencia: 'Aldea',
    lbl_compañeros: 'Bikotekideak', lbl_rivales: 'Aurkari ohikoenak',
    lbl_volver: '← Itzuli',
    comp_h2h: 'Aurrez Aurre', comp_parejas: 'Binakako Konparatzailea',
    sel_pelotari1: '— Hautatu —', sel_pelotari2: '— Hautatu —',
    lbl_modalidad: 'Modalitatea', lbl_serie_h2h: 'Seriea', lbl_año_h2h: 'Urtea',
    h2h_sin_sel: 'Hautatu bi pilotari desberdin',
    sel_delantero: '— Hautatu aurrelaria —',
    sel_zaguero_any: '— Edozein atzelari —',
    eq_colorada: 'BIKOTE GORRIA', eq_azul: 'BIKOTE URDINA',
    lbl_delantero: 'Aurrelaria', lbl_zaguero: 'Atzelaria',
    c4_identico: 'Partida berdina', c4_partidos_col: 'Partidak',
    c4_h2h_del: 'Aurrelariak aurrez aurre', c4_h2h_zag: 'Atzelariak aurrez aurre',
    c4_sin_zag: 'Hautatu bi atzelariak aurrez aurre ikusteko',
    c4_con_hist: 'Elkarrekin jokatutakoak', c4_sin_hist: 'Historia gabe',
    c4_partidos_label: 'partida',
    c4_ver_mas: '{n} gehiago ikusi ↓',
    c4_tres_de_cuatro: 'Lau pilotarietatik hiru',
    c4_tres_sin_zag: 'Hautatu bi atzelariak lautik hiru jokatu dituzten partidak ikusteko',
    c4_sin: 'Gabe', c4_sin_pel: '{n} gabe',
    stat_pj: 'Partidak', stat_ganados: 'Irabaziak', stat_pct_vic: '% Garaipenak', stat_over: '+36.5 tanto',
    stat_perd: 'galdu.',
    cart_ver_hist: '→ Historia ikusi', cart_cargando: '⟳ Kartelera kargatzen…',
    cart_como_llegar: 'Nola iritsi',
    cart_calendario: 'Egutegia', cart_calendario_t: 'Egutegira gehitu', cal_pelota: 'Pilota',
    cart_estadisticas: 'Estatistikak',
    fronton_como_llegar: 'Nola iritsi',
    fmap_ver_partidos: 'Partidak ikusi →',
    cart_no_partidos: 'Ez dago hurrengo partidarik',
    cart_recarga: '↻ Berritu',
    cont_intro: 'Iradokizunen bat duzu, daturen bat gaizki dagoela ikusi duzu edo lagundu nahi duzu?<br>Idatzi eta ahal bezain laster erantzungo dizugu.',
    cont_nombre: 'Izena', cont_email: 'Posta elektronikoa', cont_asunto: 'Gaia', cont_mensaje: 'Mezua',
    cont_ph_nombre: 'Zure izena', cont_ph_asunto: 'Datuen errorea, iradokizuna…', cont_ph_mensaje: 'Kontaiguzu…',
    cont_btn: 'Mezua bidali',
    cont_ok: '✓ Mezua prest — posta elektronikoa irekiko da.',
    cont_directo: 'Edo idatzi zuzenean helbide honetara',
    rk_title: 'Sailkapena',
    nodata_h2h: 'Ez dute elkarren aurka jokatu:',
    nodata_sin_comp: 'Bikotekiderik gabe', nodata_sin_riv: 'Aurkaririk gabe',
    tag_seriea: 'A Seriea', tag_serieb: 'B Seriea', tag_festival: 'Jaialdia',
    tag_mano: 'Buruz Burukoa', tag_manoa: 'Buruz A', tag_manob: 'Buruz B',
    tag_cuatroa: "4½ A", tag_cuatrob: "4½ B",
    rol_del: 'Aur', rol_zag: 'Atz',
    // Añadidas en la revisión de traducciones
    rk_tab_victorias: 'Garaipenak', rk_tab_tabla: 'Sailkapena', rk_tab_titulos: 'Txapelak',
    rk_cat_todos: 'Denak', rk_cat_oficiales: 'Ofizialak', rk_cat_festivales: 'Jaialdiak',
    rk_tab_pct: '% Garaipenak',
    rk_tab_roles: 'Aurre vs Atze',
    rk_tab_parejas: 'Bikoteak',
    rk_parejas_mas_v: 'Garaipen gehien', rk_parejas_pct: 'Garaipen % onena (gutx. {n} partida)', lbl_mejor_pareja: 'Bikote onena', lbl_min_pj: 'gutx. {n} partida',
    rk_tab_over: '+36.5 tanto',
    rk_tab_racha: 'Bolada',
    rk_tab_elo: 'Elo',
    rk_min_pj: 'Gutxienez {n} partida',
    rk_solo_parejas: 'binakakoak bakarrik',
    rk_delanteros: 'Aurrelariak',
    rk_zagueros: 'Atzelariak',
    rk_mejor_racha: 'Bolada onena',
    rk_racha_activa: 'Oraingo bolada',
    rk_seg: 'jarraian',
    rk_actual: 'Orain',
    rk_mejor: 'Onena',
    lbl_ultimos: 'Azken partidak',
    lbl_solo_activos: 'Aktiboak bakarrik',
    tab_activos: '⚡ Aktiboak',
    tab_historico: '📜 Historikoa',
    lbl_rango_fechas: 'Data tartea',
    lbl_limpiar: '✕ Garbitu',
    lbl_individual: 'Banakakoa',
    h2h_pts_partido: 'tanto/partida',
    cart_eventos: 'ekitaldi',
    cart_fuente: 'Iturria',
    cart_actualizado: 'Eguneratuta',
    dias: ['Igandea','Astelehena','Asteartea','Asteazkena','Osteguna','Ostirala','Larunbata'],
    meses: ['urtarrilaren','otsailaren','martxoaren','apirilaren','maiatzaren','ekainaren','uztailaren','abuztuaren','irailaren','urriaren','azaroaren','abenduaren'],
    cont_alerta: 'Mesedez, bete izena, emaila eta mezua.',
    cont_asunto_def: 'EskupilotaStats kontaktua',
    sin_partidos: 'Partidarik ez',
    sin_enfrentamientos: 'Ez dute elkarren aurka jokatu',
    pareja_exacta: 'bikote bera',
    lbl_fecha_sort: 'Data ↕',
    lbl_fichas: 'Pilotari guztien fitxak',
    btn_ficha: 'Fitxa deskargatu', btn_filtrar: 'Iragazi', derechos: 'Eskubide guztiak erreserbatuta', compartir_wa: 'WhatsApp bidez partekatu', app_instalar: 'Aplikazioa instalatu', app_android: 'Android aplikazioa',
    app_ios: 'iPhonean instalatzeko: ireki webgunea Safarin, sakatu Partekatu (gezia duen laukia) eta gero «Gehitu hasierako pantailan».',
    aria_menu: 'Menua', aria_cerrar: 'Itxi', aria_mapa: 'Frontoien mapa', aria_saltar: 'Edukira joan',
    lbl_pelotari1: '1. pilotaria', lbl_pelotari2: '2. pilotaria',
    cont_ph_email: 'zure@helbidea.eus',
    lbl_pelota_mano: 'Esku pilota',
    th_compañero: 'Bikotekidea', th_rival: 'Aurkaria',
    abbr_v: 'G', abbr_d: 'P', abbr_pj: 'PJ',
    sin_datos: 'Ez dago datu nahikorik',
    n_partido: '{n} partida', n_partidos: '{n} partida',
    pct_victorias: '%{n} garaipen', pts_p: '{n} tanto/p',
    comp_abbr: 'Bik.',
    cart_error: 'Ezin izan da kartelera kargatu.',
    map_creditos: 'Maparen kredituak', map_acercar: 'Hurbildu', map_alejar: 'Urrundu',
    abbr_tantos: 'Tanto',
    flabel_categoria: 'Kategoria', flabel_empresa: 'Enpresa', flabel_fase: 'Fasea',
    cat_campeonatos: 'Txapelketak', cat_torneos: 'Torneoak', cat_desafios: 'Desafioak', cat_festivales: 'Jaialdiak',
    tag_torneo: 'Torneoa', tag_desafio: 'Desafioa', tag_parejas: 'Binaka',
    fase_liga: 'Liga', fase_eliminatoria: 'Kanporaketa', fase_octavos: 'Final-zortzirenak',
    fase_cuartos: 'Final-laurdenak', fase_semifinal: 'Finalerdia', fase_tercero: 'Hirugarren postua', fase_final: 'Finala',
    lbl_grupo: '{g} multzoa', lbl_jornada: '{n}. jardunaldia',
    lbl_campeones: 'Txapeldunak', lbl_final_deducida: 'Ondorioztatutako finala: txapelketako azken partida da',
  }
};

function t(key){ return (I18N[LANG]||I18N.es)[key] || I18N.es[key] || key; }

function setLang(lang){
  LANG = lang === 'eu' ? 'eu' : 'es';
  try{ localStorage.setItem('eskupilota-lang', LANG); }catch(e){}
  // Si se entró con ?lang=…, la URL sigue al idioma elegido (al recargar o compartir)
  if(idiomaDeUrl() && idiomaDeUrl() !== LANG){
    const u = new URL(location.href);
    u.searchParams.set('lang', LANG);
    history.replaceState(history.state, '', u);
  }
  document.getElementById('langEs').classList.toggle('active', LANG==='es');
  document.getElementById('langEu').classList.toggle('active', LANG==='eu');
  document.getElementById('langEs').setAttribute('aria-pressed', LANG==='es');
  document.getElementById('langEu').setAttribute('aria-pressed', LANG==='eu');
  // Los nombres de ciudad dependen del idioma: se recalculan los partidos
  if (RAW_PARTIDOS.length) PARTIDOS = RAW_PARTIDOS.map(partidoFromCatalogo);
  applyI18N();
}

function idiomaGuardado(){
  try{ return localStorage.getItem('eskupilota-lang'); }catch(e){ return null; }
}

// ?lang=eu / ?lang=es en la URL (enlaces compartidos y buscadores) manda
// sobre el idioma guardado
function idiomaDeUrl(){
  try{
    const l = new URLSearchParams(location.search).get('lang');
    return l === 'eu' || l === 'es' ? l : null;
  }catch(e){ return null; }
}

function rebuildCompFilter(){
  const fc = document.getElementById('fComp');
  if(!fc) return;
  const current = fc.value;
  // Keep first option (Todas/Guztiak)
  while(fc.options.length > 1) fc.remove(1);
  fc.options[0].textContent = t('sel_todas');
  const normMap = new Map();
  PARTIDOS.forEach(p=>{ const n=normalizeComp(p.competicion); if(n&&!normMap.has(n)) normMap.set(n,n); });
  [...normMap.keys()].sort().forEach(norm=>{
    const o=document.createElement('option');
    o.value=norm;
    o.textContent=tComp(norm);
    fc.appendChild(o);
  });
  if(current) fc.value=current;
}

function applyI18N(){
  document.documentElement.lang = LANG;
  const fichas = document.getElementById('lnkFichas');
  if(fichas) fichas.href = LANG === 'eu' ? '/eu/pelotari/' : '/pelotari/';
  // renderPCards() cierra el perfil: se recuerda para reabrirlo en el nuevo idioma
  const perfilAbierto = document.getElementById('perfilSec')?.style.display === 'block' ? _perfilNombre : null;
  // Textos fijos del HTML: data-i18n (texto), data-i18n-html, data-i18n-ph, data-i18n-aria
  document.querySelectorAll('[data-i18n]').forEach(el=>{ el.textContent = t(el.dataset.i18n); });
  document.querySelectorAll('[data-i18n-html]').forEach(el=>{ el.innerHTML = t(el.dataset.i18nHtml); });
  document.querySelectorAll('[data-i18n-ph]').forEach(el=>{ el.placeholder = t(el.dataset.i18nPh); });
  document.querySelectorAll('[data-i18n-aria]').forEach(el=>{ el.setAttribute('aria-label', t(el.dataset.i18nAria)); });
  // Botones que genera el JS
  document.querySelectorAll('.tipo-pills .pill').forEach(btn=>{
    const map = {todos:'tipo_todos',campeonato:'tipo_parejas',manomanista:'tipo_mano',cuatro:'tipo_cuatro'};
    if(map[btn.dataset.tipo]) btn.textContent = t(map[btn.dataset.tipo]);
  });
  document.querySelectorAll('.ypill[data-year="todos"]').forEach(b=>b.textContent=t('sel_todos'));
  // Zagueros del comparador: "IZTUETA (29 partidos)"
  document.querySelectorAll('#c4z1 option, #c4z2 option').forEach(o=>{
    const m = o.textContent.match(/^(.*) \((\d+) \S+\)$/);
    if(m) o.textContent = `${m[1]} (${nPartidos(+m[2])})`;
  });
  // Controles del mapa
  document.querySelector('.leaflet-control-zoom-in')?.setAttribute('title', t('map_acercar'));
  document.querySelector('.leaflet-control-zoom-out')?.setAttribute('title', t('map_alejar'));
  // Contenido que se pinta desde el JS
  buildKPIs();
  rebuildCompFilter();
  renderTabla();
  renderPCards();
  buildRanking();
  repintarVistas(perfilAbierto);
}

// Vuelve a pintar las vistas con datos (en el idioma actual) sin tocar la URL
function repintarVistas(perfilAbierto){
  const antes = _routing; _routing = true;
  try{
    renderH2H();
    if(document.getElementById('c4d1')?.value && document.getElementById('c4d2')?.value) renderC4();
    if(perfilAbierto) openPerfil(perfilAbierto);
    renderFrontones();
    if(document.getElementById('frontonDetail')?.classList.contains('active') && _frontonActual) abrirFronton(_frontonActual, false, false);
    buildCampeonatos();
    if(_campActual) renderCampeonato(_campActual);
    if(_carteleraData) renderCartelera(_carteleraData);
  } finally { _routing = antes; }
}

// "1 partido" / "5 partidos" · "1 partida" / "5 partida"
function nPartidos(n){ return t(n === 1 ? 'n_partido' : 'n_partidos').replace('{n}', n); }

// ════════════════════════════════════════════════════════════
// CARGA
// ════════════════════════════════════════════════════════════
// ════════════════════════════════════════════════════════════
// CATÁLOGOS MAESTROS (v2 — datos normalizados con IDs)
// ════════════════════════════════════════════════════════════
let CAT_PELOTARIS = {}, CAT_FRONTONES = {}, CAT_CIUDADES = {}, CAT_COMPETICIONES = {};
// Partidos tal y como vienen del JSON (con IDs), para re-resolver nombres al cambiar de idioma
let RAW_PARTIDOS = [];

async function loadData(){
  // Idioma elegido en otra visita
  const deUrl = idiomaDeUrl();
  const guardado = deUrl || idiomaGuardado();
  if(guardado === 'eu' || guardado === 'es') LANG = guardado;
  if(deUrl){ try{ localStorage.setItem('eskupilota-lang', deUrl); }catch(e){} }
  document.getElementById('langEs').classList.toggle('active', LANG==='es');
  document.getElementById('langEu').classList.toggle('active', LANG==='eu');
  document.getElementById('langEs').setAttribute('aria-pressed', LANG==='es');
  document.getElementById('langEu').setAttribute('aria-pressed', LANG==='eu');
  // Intentar cargar modelo nuevo (catálogos). Si falla, caer al antiguo.
  try{
    const [pels, ciu, fro, cmp, par] = await Promise.all([
      fetch('data/pelotaris.json').then(r=>r.json()),
      fetch('data/ciudades.json').then(r=>r.json()),
      fetch('data/frontones.json').then(r=>r.json()),
      fetch('data/competiciones.json').then(r=>r.json()),
      fetch('data/partidos.json').then(r=>r.json()),
    ]);
    // Verificar que es el formato nuevo (partidos con del_id)
    if (par.length && par[0].equipo1 && 'del_id' in par[0].equipo1) {
      CAT_PELOTARIS     = Object.fromEntries(pels.map(o=>[o.id,o]));
      ROL_POR_NOMBRE    = Object.fromEntries(pels.map(o=>[(o.nombre||'').toUpperCase(), o.rol]));
      EMPRESA_POR_NOMBRE = Object.fromEntries(pels.filter(o=>o.empresa).map(o=>[(o.nombre||'').toUpperCase(), o.empresa]));
      CAT_CIUDADES      = Object.fromEntries(ciu .map(o=>[o.id,o]));
      CAT_FRONTONES     = Object.fromEntries(fro .map(o=>[o.id,o]));
      CAT_COMPETICIONES = Object.fromEntries(cmp .map(o=>[o.id,o]));
      RAW_PARTIDOS = par;
      PARTIDOS = par.map(partidoFromCatalogo);
    } else {
      // Formato antiguo plano
      PARTIDOS = par.map(normPartido);
    }
  } catch(e) {
    console.warn('Fallback a partidos.json plano:', e);
    try{
      const r = await fetch('data/partidos.json');
      PARTIDOS = (await r.json()).map(normPartido);
    } catch { PARTIDOS = []; }
  }

  buildPelotaris();
  init();
}

// Convierte un partido del nuevo modelo (con IDs) al formato plano que
// consume todo el resto del código. Esta función es la ÚNICA capa de
// compatibilidad: así no hay que tocar las 1.500 líneas de render.
function partidoFromCatalogo(p){
  const pel = id => (CAT_PELOTARIS[id]||{}).nombre || null;
  const fro = CAT_FRONTONES[p.fronton_id] || {};
  const ciu = CAT_CIUDADES[fro.ciudad_id] || {};
  const cmp = CAT_COMPETICIONES[p.competicion_id] || {};
  // fecha en ISO → dd/mm/yyyy
  let fecha = p.fecha || '';
  if (/^\d{4}-\d{2}-\d{2}$/.test(fecha)) {
    const [y,m,d] = fecha.split('-');
    fecha = `${d}/${m}/${y}`;
  }
  return {
    fecha,
    fronton: fro.nombre || '',
    ciudad:  nombreCiudadI18N(ciu),
    provincia: '',
    tipo: p.tipo || cmp.tipo || '',
    competicion: cmp.nombre || '',
    equipo1: {delantero: pel(p.equipo1?.del_id), zaguero: pel(p.equipo1?.zag_id)},
    puntos1: p.puntos1,
    equipo2: {delantero: pel(p.equipo2?.del_id), zaguero: pel(p.equipo2?.zag_id)},
    puntos2: p.puntos2,
    ganador: p.ganador,
    // Clasificación (ver scraper/competiciones.py)
    modalidad: p.modalidad || '',
    categoria: p.categoria || cmp.categoria || ((p.tipo||'').startsWith('festival') ? 'festival' : 'campeonato'),
    serie: p.serie || null,
    fase: p.fase || null, grupo: p.grupo || null, jornada: p.jornada || null,
    fase_deducida: !!p.fase_deducida,
  };
}

// Resuelve el nombre de ciudad según el idioma actual
function nombreCiudadI18N(ciu){
  if (!ciu || !ciu.nombre) return '';
  if (LANG === 'eu' && ciu.nombre_eu) return (ciu.nombre_eu || '').toUpperCase();
  if (LANG === 'es' && ciu.nombre_es) return (ciu.nombre_es || '').toUpperCase();
  return ciu.nombre;
}

function normPartido(p){
  if(!p.tipo){
    const c=(p.competicion||'').toLowerCase();
    if(c.includes('manomanista'))p.tipo='manomanista-a';
    else if(c.includes('festival')||c.includes('amistoso'))p.tipo='festival';
    else if(c.includes('serie b'))p.tipo='campeonato-b';
    else p.tipo='campeonato-a';
  }
  return p;
}

function pels(eq){if(!eq)return[];return[eq.delantero,eq.zaguero].filter(Boolean);}
function neq(eq){if(!eq)return'—';return eq.zaguero?`${eq.delantero} / ${eq.zaguero}`:eq.delantero;}
function esc(s){return(s||'').replace(/'/g,"\\'");}

function parseDate(s){const[d,m,y]=s.split('/');return new Date(+y,+m-1,+d);}
function getYear(p){return p.fecha?p.fecha.split('/')[2]:'';}

function getYears(){
  return [...new Set(PARTIDOS.map(getYear))].filter(Boolean).sort().reverse();
}

// ════════════════════════════════════════════════════════════
// PELOTARIS
// ════════════════════════════════════════════════════════════
function buildPelotaris(){
  PELOTARIS={};
  PARTIDOS.forEach(p=>{
    const e1=pels(p.equipo1),e2=pels(p.equipo2);
    [...e1,...e2].forEach(n=>{if(!PELOTARIS[n])PELOTARIS[n]={pj:0,pg:0,pp:0,pf:0,pc:0};});
    e1.forEach(n=>{PELOTARIS[n].pj++;PELOTARIS[n].pf+=p.puntos1;PELOTARIS[n].pc+=p.puntos2;if(p.ganador==='equipo1')PELOTARIS[n].pg++;else PELOTARIS[n].pp++;});
    e2.forEach(n=>{PELOTARIS[n].pj++;PELOTARIS[n].pf+=p.puntos2;PELOTARIS[n].pc+=p.puntos1;if(p.ganador==='equipo2')PELOTARIS[n].pg++;else PELOTARIS[n].pp++;});
  });
}

function calcStats(parts){
  const s={};
  parts.forEach(p=>{
    const e1=pels(p.equipo1),e2=pels(p.equipo2);
    [...e1,...e2].forEach(n=>{if(!s[n])s[n]={pj:0,pg:0,pp:0,pf:0,pc:0};});
    e1.forEach(n=>{s[n].pj++;s[n].pf+=p.puntos1;s[n].pc+=p.puntos2;if(p.ganador==='equipo1')s[n].pg++;else s[n].pp++;});
    e2.forEach(n=>{s[n].pj++;s[n].pf+=p.puntos2;s[n].pc+=p.puntos1;if(p.ganador==='equipo2')s[n].pg++;else s[n].pp++;});
  });
  return s;
}

// ════════════════════════════════════════════════════════════
// TIPO MATCH
// ════════════════════════════════════════════════════════════
function tipoMatch(tipo,filtro){
  if(filtro==='todos') return true;
  if(filtro==='campeonato') return ['campeonato-a','campeonato-b','festival'].includes(tipo);
  if(filtro==='manomanista') return ['manomanista-a','manomanista-b','manomanista','festival-mano'].includes(tipo);
  if(filtro==='cuatro') return ['cuatro-medio-a','cuatro-medio-b','festival-cuatro'].includes(tipo);
  return tipo===filtro;
}

function filtByTipoYear(tipo,year){
  return PARTIDOS.filter(p=>tipoMatch(p.tipo,tipo)&&(year==='todos'||getYear(p)===year));
}

// ════════════════════════════════════════════════════════════
// INIT
// ════════════════════════════════════════════════════════════
function init(){
  calcElo();
  buildKPIs();
  buildAllPills();
  buildYearPills();
  buildFilters();
  renderTabla();
  buildH2HSels();
  renderPCards();
  populateC4Sels();
  renderFrontones();
  buildRanking();
  applyI18N();
  buildCampeonatos();
  initRouter();
}

function showSec(id, btn, fromHistory){
  document.querySelectorAll('.section').forEach(s=>s.classList.remove('active'));
  document.querySelectorAll('header nav button').forEach(b=>b.classList.remove('active'));
  document.getElementById('sec-'+id).classList.add('active');
  btn.classList.add('active');
  // Enlace directo (#/ranking…); si viene del botón atrás, la URL ya es la correcta.
  // Va antes de pintar la sección, que puede afinar la URL (#/campeonato/COMP026).
  if(!fromHistory) setHash('#/'+SEC_SLUG[id]);
  if(id==='cartelera') loadCartelera();
  if(id==='frontones'){ renderFrontones(); setTimeout(()=>{ initFrontonMap(); renderFrontonMarkers(); },100); }
  if(id==='campeonatos') renderCampeonato(_campActual);
  // Sincronizar el drawer
  syncDrawerActive(id);
}

// ════════════════════════════════════════════════════════════
// DRAWER MÓVIL
// ════════════════════════════════════════════════════════════
function toggleDrawer(){
  const abierto = document.getElementById('drawer').classList.toggle('open');
  document.getElementById('drawerBackdrop').classList.toggle('open', abierto);
  const btn = document.getElementById('hamburgerBtn');
  btn.classList.toggle('open', abierto);
  btn.setAttribute('aria-expanded', abierto);
  if(abierto) document.querySelector('.drawer-nav button')?.focus();
}
function closeDrawer(){
  const estabaAbierto = document.getElementById('drawer').classList.contains('open');
  document.getElementById('drawer').classList.remove('open');
  document.getElementById('drawerBackdrop').classList.remove('open');
  const btn = document.getElementById('hamburgerBtn');
  btn.classList.remove('open');
  btn.setAttribute('aria-expanded', 'false');
  if(estabaAbierto && document.getElementById('drawer').contains(document.activeElement)) btn.focus();
}

// ── Accesibilidad ──
// Los elementos con onclick que no son botones ni enlaces (tarjetas, nombres,
// filas…) se pueden enfocar con Tab y activar con Enter o Espacio.
const A11Y_NO = 'button,a,input,select,textarea,label,option,.drawer-backdrop,.an-previa';
function a11yClicables(raiz){
  (raiz || document).querySelectorAll('[onclick]:not([data-a11y])').forEach(el => {
    el.dataset.a11y = '1';
    if(el.matches(A11Y_NO)) return;
    if(!el.hasAttribute('role')) el.setAttribute('role', 'button');
    if(!el.hasAttribute('tabindex')) el.tabIndex = 0;
  });
}
document.addEventListener('keydown', e => {
  const el = e.target;
  if((e.key === 'Enter' || e.key === ' ') && el.getAttribute && el.getAttribute('role') === 'button' && el.dataset.a11y){
    e.preventDefault();
    el.click();
  }
  if(e.key === 'Escape' && document.getElementById('drawer')?.classList.contains('open')) closeDrawer();
});
let _a11yPendiente = false;
new MutationObserver(() => {
  if(_a11yPendiente) return;
  _a11yPendiente = true;
  requestAnimationFrame(() => { _a11yPendiente = false; a11yClicables(); });
}).observe(document.documentElement, {childList:true, subtree:true});
document.addEventListener('DOMContentLoaded', () => a11yClicables());
function showSecFromDrawer(id, idx){
  // Activar la sección y el botón del nav principal correspondiente
  const headerBtn = document.querySelectorAll('header nav button')[idx];
  if (headerBtn) showSec(id, headerBtn);
  closeDrawer();
}
function syncDrawerActive(id){
  const map = {cartelera:0, partidos:1, comparador:2, pelotaris:3, frontones:4, ranking:5, campeonatos:6, contacto:7};
  const idx = map[id];
  if (idx === undefined) return;
  document.querySelectorAll('.drawer-nav button').forEach((b,i) => {
    b.classList.toggle('active', i === idx);
  });
  // Sección actual para lectores de pantalla
  document.querySelectorAll('header nav button, .drawer-nav button').forEach(b => {
    if(b.classList.contains('active')) b.setAttribute('aria-current', 'page'); else b.removeAttribute('aria-current');
  });
}

// ════════════════════════════════════════════════════════════
// KPIs
// ════════════════════════════════════════════════════════════
function buildKPIs(){
  const kpis=[
    {l:t('kpi_partidos'),  v:PARTIDOS.length,                                s:t('kpi_registrados')},
    {l:t('kpi_pelotaris'), v:Object.keys(PELOTARIS).length,                  s:t('kpi_distintos'),  onclick:"goToNav('pelotaris')"},
    {l:t('kpi_frontones'), v:new Set(PARTIDOS.map(p=>p.fronton)).size,       s:t('kpi_distintos'),  onclick:"goToNav('frontones')"},
    {l:t('kpi_oficiales'), v:PARTIDOS.filter(p=>p.categoria==='campeonato'||p.categoria==='torneo').length, s:t('kpi_campeonatos')},
    {l:t('kpi_festivales'),v:PARTIDOS.filter(p=>p.categoria==='festival'||p.categoria==='desafio').length, s:t('kpi_amistosos')},
  ];
  document.getElementById('kpiRow').innerHTML=kpis.map(k=>`
    <div class="kpi ${k.onclick?'clickable':''}" ${k.onclick?`onclick="${k.onclick}"`:''}>
      <div class="kpi-label">${k.l}</div>
      <div class="kpi-value">${k.v}</div>
      <div class="kpi-sub">${k.s}</div>
    </div>`).join('');
}

// ════════════════════════════════════════════════════════════
// PILLS
// ════════════════════════════════════════════════════════════
function buildAllPills(){
  ['pillsPartidos','pillsPel','pillsRk','pillsFron'].forEach(id=>{
    const el=document.getElementById(id);if(!el)return;
    el.innerHTML=TIPOS.map(t=>`
      <button class="pill ${t.k==='todos'?'on':''}" data-tipo="${t.k}" onclick="setTipo('${t.k}','${id}')">${t.lbl}</button>`
    ).join('');
  });
}

function setTipo(k,id){
  document.querySelectorAll(`#${id} .pill`).forEach(b=>b.classList.remove('on','onF','onM','onC'));
  document.querySelector(`#${id} .pill[data-tipo="${k}"]`)?.classList.add(
    k==='manomanista'?'onM':k==='cuatro'?'onC':'on'
  );
  if(id==='pillsPartidos'){activeTipo=k;renderTabla();}
  else if(id==='pillsPel'){activeTipo=k;renderPCards();}
  else if(id==='pillsRk'){activeTipo=k;buildRanking();}
  else if(id==='pillsFron'){activeTipoFron=k;renderFrontones();}
}

// ════════════════════════════════════════════════════════════
// YEAR PILLS
// ════════════════════════════════════════════════════════════
function buildYearPills(){
  const years=getYears();
  const configs=[
    {id:'yearPillsPel',  activeVar:'activeYearPel',  cb:()=>renderPCards()},
    {id:'yearPillsRk',   activeVar:'activeYearRk',   cb:()=>buildRanking()},
    {id:'yearPillsH2H',  activeVar:'activeYearH2H',  cb:()=>renderH2H()},
    {id:'yearPillsC4',   activeVar:'activeYearC4',   cb:()=>renderC4()},
    {id:'yearPillsFron', activeVar:'activeYearFron',  cb:()=>renderFrontones()},
  ];
  configs.forEach(({id,activeVar,cb})=>{
    const el=document.getElementById(id);if(!el)return;
    el.innerHTML=[
      `<button class="ypill on" data-year="todos" onclick="setYear('${id}','${activeVar}','todos')">Todos</button>`,
      ...years.map(y=>`<button class="ypill" data-year="${y}" onclick="setYear('${id}','${activeVar}','${y}')">${y}</button>`)
    ].join('');
  });
}

function setYear(pillsId,varName,year){
  if(varName==='activeYearPel') activeYearPel=year;
  else if(varName==='activeYearRk') activeYearRk=year;
  else if(varName==='activeYearH2H') activeYearH2H=year;
  else if(varName==='activeYearC4') activeYearC4=year;
  else if(varName==='activeYearFron') activeYearFron=year;
  document.querySelectorAll(`#${pillsId} .ypill`).forEach(b=>b.classList.remove('on'));
  document.querySelector(`#${pillsId} .ypill[data-year="${year}"]`)?.classList.add('on');
  if(varName==='activeYearPel') renderPCards();
  else if(varName==='activeYearRk') buildRanking();
  else if(varName==='activeYearH2H') renderH2H();
  else if(varName==='activeYearC4') renderC4();
  else if(varName==='activeYearFron') renderFrontones();
}

// ════════════════════════════════════════════════════════════
// FILTROS PARTIDOS
// ════════════════════════════════════════════════════════════
function buildFilters(){
  const fc=document.getElementById('fComp');
  // Agrupar competiciones por nombre normalizado (sin año)
  const normMap=new Map(); // normalizado -> primer valor original representativo
  PARTIDOS.forEach(p=>{
    const norm=normalizeComp(p.competicion);
    if(norm&&!normMap.has(norm)) normMap.set(norm, norm);
  });
  [...normMap.keys()].sort().forEach(norm=>{
    const o=document.createElement('option');o.value=norm;o.textContent=tComp(norm);fc.appendChild(o);
  });
  const ff=document.getElementById('fFron');
  [...new Set(PARTIDOS.map(p=>p.fronton))].sort().forEach(f=>{
    const o=document.createElement('option');o.value=f;o.textContent=f;ff.appendChild(o);
  });
}

function resetFiltros(){
  document.getElementById('fDesde').value='';
  document.getElementById('fHasta').value='';
  document.getElementById('sInput').value='';
  document.getElementById('fComp').value='';
  document.getElementById('fFron').value='';
  if(document.getElementById('fCat')) document.getElementById('fCat').value='';
  if(document.getElementById('fFase')) document.getElementById('fFase').value='';
  activeTipo='todos';
  document.querySelectorAll('#pillsPartidos .pill').forEach(b=>b.classList.remove('on','onM','onC'));
  document.querySelector('#pillsPartidos .pill[data-tipo="todos"]')?.classList.add('on');
  renderTabla();
}

// ════════════════════════════════════════════════════════════
// TABLA PARTIDOS
// ════════════════════════════════════════════════════════════
function sortT(f){if(sortFld===f)sortAsc=!sortAsc;else{sortFld=f;sortAsc=false;}renderTabla();}

function renderTabla(){
  const q=(document.getElementById('sInput').value||'').toLowerCase();
  const comp=document.getElementById('fComp').value;
  const fron=document.getElementById('fFron').value;
  const desde=document.getElementById('fDesde').value;
  const hasta=document.getElementById('fHasta').value;
  const cat=document.getElementById('fCat')?.value||'';
  const fase=document.getElementById('fFase')?.value||'';

  let data=PARTIDOS.filter(p=>{
    if(!tipoMatch(p.tipo,activeTipo)) return false;
    if(cat&&p.categoria!==cat) return false;
    if(fase&&p.fase!==fase) return false;
    if(comp&&normalizeComp(p.competicion)!==comp) return false;
    if(fron&&p.fronton!==fron) return false;
    if(q&&!(neq(p.equipo1)+' '+neq(p.equipo2)).toLowerCase().includes(q)) return false;
    if(desde||hasta){
      const d=parseDate(p.fecha);
      if(desde&&d<new Date(desde)) return false;
      if(hasta&&d>new Date(hasta)) return false;
    }
    return true;
  });

  data.sort((a,b)=>{
    let av=a[sortFld],bv=b[sortFld];
    if(sortFld==='fecha'){av=parseDate(av);bv=parseDate(bv);}
    return sortAsc?(av>bv?1:-1):(av<bv?1:-1);
  });

  document.getElementById('tBody').innerHTML=data.map(p=>{
    const w1=p.ganador==='equipo1',w2=p.ganador==='equipo2';
    const ti=etiquetaPartido(p);
    return`<tr>
      <td style="font-family:var(--mono);font-size:.66rem;white-space:nowrap">${p.fecha}</td>
      <td><span class="tag ${ti.cls}">${ti.lbl}</span></td>
      <td style="font-size:.74rem">${p.fronton}<br><span style="font-size:.6rem;color:var(--muted)">${p.ciudad||''}</span></td>
      <td style="font-weight:${w1?800:400};color:var(--red)">${neq(p.equipo1)}</td>
      <td style="text-align:center;white-space:nowrap"><span class="sc ${w1?'w':'l'}">${p.puntos1}</span><span style="color:var(--muted);margin:0 .28rem">—</span><span class="sc ${w2?'w':'l'}">${p.puntos2}</span></td>
      <td style="font-weight:${w2?800:400};color:var(--blue)">${neq(p.equipo2)}</td>
      <td style="font-size:.7rem;color:var(--muted)">${tComp(p.competicion)}${p.fase?`<br><span class="fase-lbl">${textoFase(p)}</span>`:''}</td>
    </tr>`;
  }).join('');
  document.getElementById('tCount').textContent=`${data.length} / ${PARTIDOS.length} ${t('kpi_partidos').toLowerCase()}`;
  renderMobileCards(data);
}

let _mCardBlockId = 0;
function renderMobileCardsHTML(data, opts){
  opts = opts || {};
  const paginated = !!opts.paginated;
  const initial = opts.initial || 5;
  const step = opts.step || 5;
  const renderRow = (p)=>{
    const w1=p.ganador==='equipo1', w2=p.ganador==='equipo2';
    const ti=etiquetaPartido(p);
    const tiCls=ti.cls;
    const rowCls=p.tipo.includes('festival')?'festival-row':p.tipo.includes('manomanista')?'mano-row':p.tipo.includes('cuatro')?'cuatro-row':'';
    const n1=neq(p.equipo1), n2=neq(p.equipo2);
    return `<div class="m-card-row ${rowCls}">
      <div class="m-card-header" onclick="toggleMCard(this)">
        <div>
          <div class="m-card-fecha">${p.fecha} · ${p.fronton}</div>
          ${p.categoria && p.categoria!=='festival' ? `<div class="m-card-oficial"><span class="tag ${tiCls}">${ti.lbl}</span>${p.fase?`<span class="fase-lbl">${textoFase(p)}</span>`:''}</div>` : ''}
          <div style="display:flex;gap:.4rem;align-items:center;margin-top:.2rem;">
            <span style="font-size:.78rem;font-weight:${w1?800:400};color:var(--red)">${n1}</span>
            <span style="color:var(--muted);font-size:.65rem">vs</span>
            <span style="font-size:.78rem;font-weight:${w2?800:400};color:var(--blue)">${n2}</span>
          </div>
        </div>
        <div style="display:flex;align-items:center;gap:.5rem;flex-shrink:0;">
          <div class="m-card-marcador">
            <span class="m-card-score ${w1?'w':'l'}">${p.puntos1}</span>
            <span class="m-card-sep">—</span>
            <span class="m-card-score ${w2?'w':'l'}">${p.puntos2}</span>
          </div>
          <span class="m-card-arrow">›</span>
        </div>
      </div>
      <div class="m-card-body">
        <div class="m-card-teams">
          <div class="m-card-team"><span style="font-weight:${w1?800:400};color:var(--red)">${n1}</span><span style="font-family:var(--display);font-size:1.1rem;margin-left:auto;color:var(--red);font-weight:${w1?800:400}">${p.puntos1}</span></div>
          <div class="m-card-team"><span style="font-weight:${w2?800:400};color:var(--blue)">${n2}</span><span style="font-family:var(--display);font-size:1.1rem;margin-left:auto;color:var(--blue);font-weight:${w2?800:400}">${p.puntos2}</span></div>
        </div>
        <div class="m-card-meta">
          <span class="tag ${tiCls}">${ti.lbl}</span>
          <span class="m-card-fronton">📍 ${p.fronton}${p.ciudad?' · '+p.ciudad:''}</span>
          <span class="m-card-comp">${tComp(p.competicion)}${p.fase?' · '+textoFase(p):''}</span>
        </div>
      </div>
    </div>`;
  };
  // Sin paginación: comportamiento clásico (toda la lista)
  if(!paginated || data.length <= initial){
    return data.map(renderRow).join('');
  }
  // Con paginación: visibles + ocultos + botón "Ver más"
  const id = 'mcb'+(++_mCardBlockId);
  const visibles = data.slice(0, initial).map(renderRow).join('');
  const ocultos = data.slice(initial).map(renderRow).join('');
  const restantes = data.length - initial;
  return `<div class="m-card-list" data-step="${step}">
    ${visibles}
    <div class="m-card-hidden" id="${id}-extra" style="display:none">${ocultos}</div>
    <div class="m-card-vermas" id="${id}-btnwrap">
      <button class="btn-ghost m-card-btn" onclick="mCardsVerMas('${id}')">
        ${t('c4_ver_mas').replace('{n}', Math.min(step, restantes))}
      </button>
    </div>
  </div>`;
}

function mCardsVerMas(id){
  const extra = document.getElementById(id+'-extra');
  const btnWrap = document.getElementById(id+'-btnwrap');
  if(!extra || !btnWrap) return;
  const list = btnWrap.parentElement;
  const step = parseInt(list.dataset.step || '5', 10);
  const children = Array.from(extra.children);
  const aMover = children.slice(0, step);
  aMover.forEach(c => list.insertBefore(c, extra));
  const restantes = extra.children.length;
  if(restantes <= 0){
    btnWrap.remove();
  } else {
    const btn = btnWrap.querySelector('button');
    btn.textContent = t('c4_ver_mas').replace('{n}', Math.min(step, restantes));
  }
}

function renderMobileCards(data){
  const el = document.getElementById('mCards');
  if(!el) return;
  el.innerHTML = renderMobileCardsHTML(data);
}

function toggleMCard(header){
  const body = header.nextElementSibling;
  const arrow = header.querySelector('.m-card-arrow');
  body.classList.toggle('open');
  arrow.classList.toggle('open');
}

// ════════════════════════════════════════════════════════════
// PELOTARIS
// ════════════════════════════════════════════════════════════
function renderPCards(){
  const q=(document.getElementById('pSearch').value||'').toLowerCase();
  closePerfil();
  const parts=partidosFiltroPelotaris();
  const st=calcStats(parts);
  const activePlayers = filterActivos ? getActivePlayers() : null;
  let ns=Object.keys(st).filter(n=>{
    if(q&&!n.toLowerCase().includes(q)) return false;
    if(activePlayers&&!activePlayers.has(n.toUpperCase())) return false;
    return true;
  });
  ns.sort((a,b)=>st[b].pg-st[a].pg);

  function mkCard(n){
    const s=st[n];const pct=s.pj>0?Math.round(s.pg/s.pj*100):0;
    return`<div class="pcard" onclick="openPerfil('${esc(n)}')">
      <div class="pname">${n}</div>
      <div class="pstats">
        <div class="ps"><span class="val" style="color:var(--green)">${s.pg}</span><span class="lbl">${t('lbl_victorias')}</span></div>
        <div class="ps"><span class="val">${s.pj}</span><span class="lbl">${t('lbl_partidos')}</span></div>
        <div class="ps"><span class="val">${pct}%</span><span class="lbl">${t('lbl_pct_vic')}</span></div>
      </div>
      <div class="pbar-bg"><div class="pbar-fill" style="width:${pct}%"></div></div>
    </div>`;
  }

  const dels  = ns.filter(n=>getRol(n)==='delantero');
  const zags  = ns.filter(n=>getRol(n)==='zaguero');
  const otros = ns.filter(n=>getRol(n)==='otro');

  let html = '';
  if(dels.length){
    html += `<div class="pel-group-header del">${t('rk_delanteros')} <span class="pel-group-count">${dels.length}</span></div>`;
    html += `<div class="cards-grid">${dels.map(mkCard).join('')}</div>`;
  }
  if(zags.length){
    html += `<div class="pel-group-header zag">${t('rk_zagueros')} <span class="pel-group-count">${zags.length}</span></div>`;
    html += `<div class="cards-grid">${zags.map(mkCard).join('')}</div>`;
  }
  // no mostrar grupo 'otros'
  document.getElementById('pCards').innerHTML = html;
}

function partidosFiltroPelotaris(){
  return filtByTipoYear(activeTipo,activeYearPel).filter(p=>partidoEnRango(p, dateRangePel));
}

// Filtros que solo tiene la ficha de un pelotari (además de modalidad, año y fechas)
let pfExtra = {fronton:'', comp:'', rival:''};

// Partidos del pelotari con todos los filtros de su ficha
function partidosPerfil(nombre){
  return partidosFiltroPelotaris().filter(p=>{
    const e1=pels(p.equipo1), e2=pels(p.equipo2);
    const mio = e1.includes(nombre) ? e1 : e2.includes(nombre) ? e2 : null;
    if(!mio) return false;
    const suyo = mio===e1 ? e2 : e1;
    if(pfExtra.fronton && p.fronton!==pfExtra.fronton) return false;
    if(pfExtra.comp && !mio.includes(pfExtra.comp)) return false;
    if(pfExtra.rival && !suyo.includes(pfExtra.rival)) return false;
    return true;
  });
}

function openPerfil(nombre){
  if(nombre!==_perfilNombre) pfExtra = {fronton:'', comp:'', rival:''};
  document.getElementById('pCards').style.display='none';
  document.getElementById('pSearch').style.display='none';
  // En la ficha los filtros van detrás del botón «Filtrar», junto al nombre
  document.querySelectorAll('#sec-pelotaris > .ah-tabs, #sec-pelotaris > .filter-panel').forEach(e=>e.hidden=true);
  const ps=document.getElementById('perfilSec');
  ps.style.display='block';
  document.getElementById('perfilTitle').textContent=nombre;

  // Filtros de la ficha: modalidad, año, fechas, frontón, compañero y rival
  const parts=partidosPerfil(nombre);
  const st=calcStats(parts);
  const s=st[nombre]||{pj:0,pg:0,pp:0,pf:0,pc:0};
  const pct=s.pj>0?Math.round(s.pg/s.pj*100):0;
  const pmf=s.pj>0?(s.pf/s.pj).toFixed(1):'—';
  const diff=s.pf-s.pc;

  const comp={};
  parts.forEach(p=>{
    const e1=pels(p.equipo1),e2=pels(p.equipo2);
    const enE1=e1.includes(nombre),enE2=e2.includes(nombre);
    if(!enE1&&!enE2)return;
    const mis=enE1?e1:e2,gano=(enE1&&p.ganador==='equipo1')||(enE2&&p.ganador==='equipo2');
    mis.filter(n=>n!==nombre).forEach(c=>{if(!comp[c])comp[c]={pj:0,pg:0};comp[c].pj++;if(gano)comp[c].pg++;});
  });

  const riv={};
  parts.forEach(p=>{
    const e1=pels(p.equipo1),e2=pels(p.equipo2);
    const enE1=e1.includes(nombre),enE2=e2.includes(nombre);
    if(!enE1&&!enE2)return;
    const ellos=enE1?e2:e1,gano=(enE1&&p.ganador==='equipo1')||(enE2&&p.ganador==='equipo2');
    ellos.forEach(r=>{if(!riv[r])riv[r]={pj:0,pg:0};riv[r].pj++;if(gano)riv[r].pg++;});
  });

  // Mejor pareja: más % de victorias con al menos 5 partidos juntos
  const MIN_PAREJA = 5;
  const mejorPareja = Object.entries(comp).filter(([,s])=>s.pj>=MIN_PAREJA)
    .sort((a,b)=>b[1].pg/b[1].pj-a[1].pg/a[1].pj||b[1].pj-a[1].pj)[0];
  const mejorParejaHtml = mejorPareja ? `<div class="pf-mejor">⭐ ${t('lbl_mejor_pareja')}:
    <span class="clk" onclick="openPerfil('${esc(mejorPareja[0])}')">${mejorPareja[0]}</span>
    · ${mejorPareja[1].pg}${t('abbr_v')}–${mejorPareja[1].pj-mejorPareja[1].pg}${t('abbr_d')} · ${Math.round(mejorPareja[1].pg/mejorPareja[1].pj*100)}%
    <span class="an-muted">(${t('lbl_min_pj').replace('{n}',MIN_PAREJA)})</span></div>` : '';
  const compRows=Object.entries(comp).sort((a,b)=>b[1].pj-a[1].pj).map(([n,s])=>`
    <tr><td class="clk" onclick="openPerfil('${esc(n)}')">${mejorPareja&&mejorPareja[0]===n?'⭐ ':''}${n}</td>
    <td style="font-family:var(--mono);text-align:center;color:var(--green)">${s.pg}</td>
    <td style="font-family:var(--mono);text-align:center;color:var(--red)">${s.pj-s.pg}</td>
    <td style="font-family:var(--mono);text-align:center">${s.pj}</td>
    <td style="font-family:var(--mono);text-align:center">${Math.round(s.pg/s.pj*100)}%</td></tr>`
  ).join('')||`<tr><td colspan="5" class="nodata">${t('nodata_sin_comp')}</td></tr>`;

  const rivRows=Object.entries(riv).sort((a,b)=>b[1].pj-a[1].pj).slice(0,10).map(([n,s])=>`
    <tr><td class="clk" onclick="openPerfil('${esc(n)}')">${n}</td>
    <td style="font-family:var(--mono);text-align:center;color:var(--green)">${s.pg}</td>
    <td style="font-family:var(--mono);text-align:center;color:var(--red)">${s.pj-s.pg}</td>
    <td style="font-family:var(--mono);text-align:center">${s.pj}</td></tr>`
  ).join('')||`<tr><td colspan="4" class="nodata">${t('nodata_sin_riv')}</td></tr>`;

  document.getElementById('pfWrap').innerHTML=`
    <div>
      <div class="pf-header"><div class="pf-nombre">${nombre}</div><div style="font-family:var(--mono);font-size:.62rem;opacity:.8;margin-top:.25rem">${getEmpresa(nombre)?`<span class="pf-empresa ${getEmpresa(nombre)}">${NOMBRE_EMPRESA[getEmpresa(nombre)]}</span> · `:''}${t('lbl_pelota_mano')}</div></div>
      <div class="pf-sgrid">
        <div class="pf-s"><div class="v g">${s.pg}</div><div class="l">${t('lbl_victorias')}</div></div>
        <div class="pf-s"><div class="v r">${s.pp}</div><div class="l">${t('lbl_derrotas')}</div></div>
        <div class="pf-s"><div class="v">${s.pj}</div><div class="l">${t('lbl_partidos')}</div></div>
        <div class="pf-s"><div class="v ${pct>=50?'g':'r'}">${pct}%</div><div class="l">${t('lbl_pct_vic')}</div></div>
        <div class="pf-s"><div class="v">${pmf}</div><div class="l">${t('lbl_pts_p')}</div></div>
        <div class="pf-s"><div class="v ${diff>=0?'g':'r'}">${diff>0?'+':''}${diff}</div><div class="l">${t('lbl_diferencia')}</div></div>
      </div>
    </div>
    <div class="pf-main">
      <div class="ch-card gr">
        <h3>${t('lbl_compañeros')}</h3>
        ${mejorParejaHtml}
        <table class="comp-table"><thead><tr><th>${t('th_compañero')}</th><th>${t('abbr_v')}</th><th>${t('abbr_d')}</th><th>${t('abbr_pj')}</th><th>%</th></tr></thead><tbody>${compRows}</tbody></table>
      </div>
      <div class="ch-card">
        <h3>${t('lbl_rivales')}</h3>
        <table class="comp-table"><thead><tr><th>${t('th_rival')}</th><th>${t('abbr_v')}</th><th>${t('abbr_d')}</th><th>${t('abbr_pj')}</th></tr></thead><tbody>${rivRows}</tbody></table>
      </div>
      <div class="ch-card pf-ultimos" id="pfUltimos">
        <h3 id="pfUltimosTitle">${t('lbl_ultimos')}</h3>
        <div id="pfPartidosList"></div>
        <div class="pf-load-more" id="pfLoadMore" style="display:none">
          <button class="btn-ghost" onclick="loadMorePerfilPartidos()">${t('c4_ver_mas').replace('{n}',10)}</button>
        </div>
      </div>
    </div>`;

  // Init recent matches
  _perfilNombre = nombre;
  _perfilPage = 1;
  pintarFiltrosPerfil(nombre);
  renderPerfilPartidos();
  document.querySelector('#pfWrap .pf-main').insertAdjacentHTML('afterbegin', htmlPalmares(nombre, parts) + htmlEvolucionPerfil(nombre) + htmlTemporadas(nombre));
  pintarBotonSeguir();
  setHash('#/pelotari/'+slugify(nombre));
}


function getPerfilPartidos(){
  return partidosPerfil(_perfilNombre);   // ya ordenados del más reciente al más antiguo
}

function renderPerfilPartidos(){
  _perfilPage = 1;
  const all = getPerfilPartidos();
  const nombre = _perfilNombre;
  const show = all.slice(0, 5); // first 5
  const list = document.getElementById('pfPartidosList');
  const more = document.getElementById('pfLoadMore');
  if(!list) return;
  list.innerHTML = show.map(p=>renderPerfilPartidoRow(p, nombre)).join('');
  if(more) more.style.display = all.length > 5 ? 'block' : 'none';
}

function loadMorePerfilPartidos(){
  _perfilPage++;
  const all = getPerfilPartidos();
  const nombre = _perfilNombre;
  const start = 5 + (_perfilPage-2)*PERFIL_PAGE_SIZE;
  const end = start + PERFIL_PAGE_SIZE;
  const list = document.getElementById('pfPartidosList');
  const more = document.getElementById('pfLoadMore');
  if(!list) return;
  list.innerHTML += all.slice(start, end).map(p=>renderPerfilPartidoRow(p, nombre)).join('');
  if(more) more.style.display = end >= all.length ? 'none' : 'block';
}

function renderPerfilPartidoRow(p, nombre){
  const e1=pels(p.equipo1), e2=pels(p.equipo2);
  const enE1 = e1.includes(nombre);
  const gano = (enE1&&p.ganador==='equipo1')||(!enE1&&p.ganador==='equipo2');
  const n1 = neq(p.equipo1), n2 = neq(p.equipo2);
  const w1 = p.ganador==='equipo1', w2 = p.ganador==='equipo2';
  const ti = etiquetaPartido(p); const tiTag = `<span class="tag ${ti.cls}">${ti.lbl}</span>`;
  return `<div class="m-card-row ${p.tipo?.includes('festival')?'festival-row':p.tipo?.includes('manomanista')?'mano-row':p.tipo?.includes('cuatro')?'cuatro-row':''}">
    <div class="m-card-header" onclick="toggleMCard(this)">
      <div>
        <div class="m-card-fecha">${p.fecha} · ${p.fronton}</div>
        <div style="display:flex;gap:.4rem;align-items:center;margin-top:.2rem;">
          <span style="font-size:.78rem;font-weight:${w1?800:400};color:var(--red)">${n1}</span>
          <span style="color:var(--muted);font-size:.65rem">vs</span>
          <span style="font-size:.78rem;font-weight:${w2?800:400};color:var(--blue)">${n2}</span>
        </div>
      </div>
      <div style="display:flex;align-items:center;gap:.5rem;flex-shrink:0;">
        <div class="m-card-marcador">
          <span class="m-card-score ${w1?'w':'l'}">${p.puntos1}</span>
          <span class="m-card-sep">—</span>
          <span class="m-card-score ${w2?'w':'l'}">${p.puntos2}</span>
        </div>
        <span class="m-card-arrow">›</span>
      </div>
    </div>
    <div class="m-card-body">
      <div class="m-card-teams">
        <div class="m-card-team"><span style="font-weight:${w1?800:400};color:var(--red)">${n1}</span><span style="font-family:var(--display);font-size:1.1rem;margin-left:auto;color:var(--red);font-weight:${w1?800:400}">${p.puntos1}</span></div>
        <div class="m-card-team"><span style="font-weight:${w2?800:400};color:var(--blue)">${n2}</span><span style="font-family:var(--display);font-size:1.1rem;margin-left:auto;color:var(--blue);font-weight:${w2?800:400}">${p.puntos2}</span></div>
      </div>
      <div class="m-card-meta">
        ${tiTag}
        <span class="m-card-fronton">📍 ${p.fronton}${p.ciudad?' · '+p.ciudad:''}</span>
        <span class="m-card-comp">${tComp(p.competicion)}${p.fase?' · '+textoFase(p):''}</span>
      </div>
    </div>
  </div>`;
}

function closePerfil(){
  document.getElementById('perfilSec').style.display='none';
  document.querySelectorAll('#sec-pelotaris > .ah-tabs, #sec-pelotaris > .filter-panel').forEach(e=>e.hidden=false);
  const pf=document.getElementById('pfFiltros'); if(pf) pf.hidden=true;
  document.getElementById('pCards').style.display='grid';
  document.getElementById('pSearch').style.display='block';
  if(document.getElementById('sec-pelotaris').classList.contains('active')) setHash('#/pelotaris');
}

function goToPel(n){goToNav('pelotaris');setTimeout(()=>openPerfil(n),80);}

// ════════════════════════════════════════════════════════════
// RANKING
// ════════════════════════════════════════════════════════════
let activeRkTab = 'tabla';
let activeCatRk = 'todos';   // todos | oficial (campeonatos y torneos) | festival (festivales y desafíos)

let activeEmpRk = 'todas';   // todas | baiko | aspe
function setEmpRk(emp){
  activeEmpRk = emp;
  document.querySelectorAll('#empPillsRk .ypill').forEach(b=>{ b.classList.toggle('on', b.dataset.emp===emp); b.setAttribute('aria-pressed', b.dataset.emp===emp); });
  buildRanking();
}

function catRkMatch(p){
  if(activeCatRk==='oficial') return p.categoria==='campeonato' || p.categoria==='torneo';
  if(activeCatRk==='festival') return p.categoria==='festival' || p.categoria==='desafio';
  return true;
}

function setCatRk(cat){
  activeCatRk = cat;
  document.querySelectorAll('#catPillsRk .ypill').forEach(b=>{
    b.classList.toggle('on', b.dataset.cat===cat);
    b.setAttribute('aria-pressed', b.dataset.cat===cat);
  });
  buildRanking();
}
let _perfilNombre = '';
let _perfilPage = 1;
const PERFIL_PAGE_SIZE = 10;
let filterActivos = true;

function setRkTab(tab, btn){
  activeRkTab = tab;
  document.querySelectorAll('.rk-tab').forEach(b=>b.classList.remove('on'));
  btn.classList.add('on');
  buildRanking();
}

function buildRanking(){
  if(activeRkTab==='elo'){ document.getElementById('rkContent').innerHTML = htmlRankingElo(); return; }
  let parts = filtByTipoYear(activeTipo, activeYearRk);
  // Filtrado por rango de fechas
  parts = parts.filter(p=>partidoEnRango(p, dateRangeRk) && catRkMatch(p));
  const el = document.getElementById('rkContent');

  // Helper: render a single ranking card
  function mkRkCard(titulo, lista, statFn, extraCols, cls='', nombreFn=null, desde=0){
    const mx = lista[0] ? statFn(lista[0][1]).val : 1;
    return `<div class="rk-card ${cls}"><h3>${titulo}</h3>
    ${lista.map(([n,s],i)=>{
      const main = statFn(s);
      return `<div class="rk-row">
        <div class="rk-pos ${i+desde===0?'p1':i+desde===1?'p2':i+desde===2?'p3':''}">${i+desde+1}</div>
        ${nombreFn ? `<div class="rk-name">${nombreFn(n,s)}</div>` : `<div class="rk-name clk" onclick="goToPel('${esc(n)}')">${n}</div>`}
        ${extraCols(s)}
        <div class="rk-stat pg" style="color:var(--green);font-weight:700">${main.lbl}</div>
      </div>
      <div class="wbar-bg"><div class="wbar-fill" style="width:${mx>0?(main.val/mx)*100:0}%"></div></div>`;
    }).join('')}
    </div>`;
  }

  function mkGrid(cards){
    return `<div class="rk-grid">${cards.join('')}</div>`;
  }

  let _activePlayers = filterActivos ? getActivePlayers() : null;
  // Empresa: se deja solo a los pelotaris de Baiko o de Aspe
  if(activeEmpRk!=='todas'){
    const base = _activePlayers || new Set(Object.keys(PELOTARIS).map(n=>n.toUpperCase()));
    _activePlayers = new Set([...base].filter(n=>EMPRESA_POR_NOMBRE[n]===activeEmpRk));
  }
  function filterActive(entries){ return _activePlayers ? entries.filter(([n])=>_activePlayers.has(n.toUpperCase())) : entries; }

  // ── TAB: Clasificación (tabla ordenable) y Títulos ──
  if(activeRkTab === 'tabla'){ el.innerHTML = htmlRankingTabla(parts, _activePlayers); return; }
  if(activeRkTab === 'titulos'){ el.innerHTML = htmlRankingTitulos(parts, _activePlayers); return; }

  // ── TAB: Delanteros vs Zagueros ──
  else if(activeRkTab === 'roles'){
    const parsParejas = parts.filter(p=>['campeonato-a','campeonato-b','festival'].includes(p.tipo));
    const st = calcStats(parsParejas);
    const dels = filterActive(Object.entries(st).filter(([n])=>getRol(n)==='delantero').sort((a,b)=>b[1].pg-a[1].pg)).slice(0,15);
    const zags = filterActive(Object.entries(st).filter(([n])=>getRol(n)==='zaguero').sort((a,b)=>b[1].pg-a[1].pg)).slice(0,15);
    const sf = s=>({val:s.pg, lbl:s.pg+t('abbr_v')});
    const ec = s=>`<div class="rk-stat" style="color:var(--red)">${s.pp}${t('abbr_d')}</div><div class="rk-stat">${s.pj}${t('abbr_pj')}</div><div class="rk-stat">${Math.round(s.pg/s.pj*100)}%</div>`;
    el.innerHTML = mkGrid([
      mkRkCard(t('rk_delanteros'), dels, sf, ec),
      mkRkCard(t('rk_zagueros'), zags, sf, ec, 'azul')
    ]);
  }

  // ── TAB: Parejas ──
  else if(activeRkTab === 'parejas'){
    const parsParejas = parts.filter(p=>['campeonato-a','campeonato-b','festival'].includes(p.tipo));
    const pStats = {};
    parsParejas.forEach(p=>{
      [p.equipo1, p.equipo2].forEach((eq,idx)=>{
        const d=eq.delantero, z=eq.zaguero;
        if(!d||!z) return;
        const key = d+' / '+z;
        if(!pStats[key]) pStats[key]={pj:0,pg:0,pp:0,pf:0,pc:0,d,z};
        pStats[key].pj++;
        const ganaron = (idx===0&&p.ganador==='equipo1')||(idx===1&&p.ganador==='equipo2');
        if(ganaron) pStats[key].pg++; else pStats[key].pp++;
        pStats[key].pf += idx===0?p.puntos1:p.puntos2;
        pStats[key].pc += idx===0?p.puntos2:p.puntos1;
      });
    });
    // Con "solo activos", las dos de la pareja tienen que estar activas
    const activas = Object.entries(pStats).filter(([,s])=>!_activePlayers||(_activePlayers.has(s.d.toUpperCase())&&_activePlayers.has(s.z.toUpperCase())));
    const MIN_PCT = 10;
    const masV = activas.filter(([,s])=>s.pj>=3).sort((a,b)=>b[1].pg-a[1].pg||b[1].pg/b[1].pj-a[1].pg/a[1].pj).slice(0,15);
    const mejorPct = activas.filter(([,s])=>s.pj>=MIN_PCT).sort((a,b)=>b[1].pg/b[1].pj-a[1].pg/a[1].pj||b[1].pj-a[1].pj).slice(0,15);
    const nombres = (n,s)=>[s.d,s.z].map(x=>`<span class="clk" onclick="goToPel('${esc(x)}')">${x}</span>`).join(' / ');
    const ec = s=>`<div class="rk-stat" style="color:var(--red)">${s.pp}${t('abbr_d')}</div><div class="rk-stat">${s.pj}${t('abbr_pj')}</div>`;
    el.innerHTML = masV.length ? mkGrid([
      mkRkCard(t('rk_parejas_mas_v'), masV, s=>({val:s.pg, lbl:s.pg+t('abbr_v')}),
        s=>ec(s)+`<div class="rk-stat">${Math.round(s.pg/s.pj*100)}%</div>`, '', nombres),
      mkRkCard(t('rk_parejas_pct').replace('{n}',MIN_PCT), mejorPct, s=>({val:s.pg/s.pj*100, lbl:Math.round(s.pg/s.pj*100)+'%'}),
        s=>`<div class="rk-stat">${s.pg}${t('abbr_v')}</div>`+ec(s), 'azul', nombres)
    ]) : `<div class="nodata"><div class="ic">📭</div>${t('sin_datos')}</div>`;
  }

  // ── TAB: +36.5 tantos ──
  else if(activeRkTab === 'over365'){
    const parsParejas = parts.filter(p=>['campeonato-a','campeonato-b','festival'].includes(p.tipo));
    const over = {};
    parsParejas.forEach(p=>{
      const total = p.puntos1 + p.puntos2;
      [p.equipo1, p.equipo2].forEach(eq=>{
        [eq.delantero, eq.zaguero].filter(Boolean).forEach(n=>{
          if(!over[n]) over[n]={pj:0,over:0};
          over[n].pj++;
          if(total>36.5) over[n].over++;
        });
      });
    });
    const MIN_PJ = 10;
    const sorted = filterActive(Object.entries(over)
      .filter(([,s])=>s.pj>=MIN_PJ)
      .sort((a,b)=>(b[1].over/b[1].pj)-(a[1].over/a[1].pj)))
      .slice(0,20);
    const h = Math.ceil(sorted.length/2);
    const sf = s=>({val:s.over/s.pj*100, lbl:Math.round(s.over/s.pj*100)+'%'});
    const ec = s=>`<div class="rk-stat">${nPartidos(s.over)}</div><div class="rk-stat">${s.pj}${t('abbr_pj')}</div>`;
    el.innerHTML = `<div class="rk-min-label">${t('rk_min_pj').replace('{n}',MIN_PJ)} · ${t('rk_solo_parejas')}</div>` + mkGrid([
      mkRkCard(`1º — ${h}º`, sorted.slice(0,h), sf, ec),
      mkRkCard(`${h+1}º — ${sorted.length}º`, sorted.slice(h), sf, ec, '', null, h)
    ]);
  }

  // ── TAB: Racha ──
  else if(activeRkTab === 'racha'){
    // Rachas con los partidos filtrados (modalidad, año, fechas y categoría), del más antiguo al más reciente
    const parseFecha = _parseFechaDDMMYYYY;
    const allParts = [...parts].sort((a,b)=>parseFecha(a.fecha)-parseFecha(b.fecha));
    const rachas = {};
    allParts.forEach(p=>{
      const e1=pels(p.equipo1), e2=pels(p.equipo2);
      [...e1,...e2].forEach(n=>{
        if(!rachas[n]) rachas[n]={actual:0,max:0};
        const gano=(e1.includes(n)&&p.ganador==='equipo1')||(e2.includes(n)&&p.ganador==='equipo2');
        if(gano){ rachas[n].actual++; rachas[n].max=Math.max(rachas[n].max,rachas[n].actual); }
        else rachas[n].actual=0;
      });
    });
    const byMax = filterActive(Object.entries(rachas).filter(([,s])=>s.max>=3).sort((a,b)=>b[1].max-a[1].max)).slice(0,20);
    const byActual = filterActive(Object.entries(rachas).filter(([,s])=>s.actual>=2).sort((a,b)=>b[1].actual-a[1].actual)).slice(0,10);
    const sfMax = s=>({val:s.max, lbl:s.max+' '+t('rk_seg')});
    const sfAct = s=>({val:s.actual, lbl:s.actual+' '+t('rk_seg')});
    const ecMax = s=>`<div class="rk-stat">${t('rk_actual')}: ${s.actual}</div>`;
    const ecAct = s=>`<div class="rk-stat">${t('rk_mejor')}: ${s.max}</div>`;
    el.innerHTML = mkGrid([
      mkRkCard(t('rk_mejor_racha'), byMax.slice(0,10), sfMax, ecMax),
      byActual.length ? mkRkCard(t('rk_racha_activa'), byActual, sfAct, ecAct, 'azul') : ''
    ].filter(Boolean));
  }
}

// ════════════════════════════════════════════════════════════
// COMPARADOR — selector de tipo
// ════════════════════════════════════════════════════════════
function setCompType(type,btn){
  compType=type;
  document.querySelectorAll('.ctype-btn').forEach(b=>b.classList.remove('on'));
  btn.classList.add('on');
  document.querySelectorAll('.comp-panel').forEach(p=>p.classList.remove('active'));
  document.getElementById('panel-'+type).classList.add('active');
}

// ════════════════════════════════════════════════════════════
// H2H — Head to Head (1 vs 1)
// ════════════════════════════════════════════════════════════
function buildH2HSels(){
  const ns=Object.keys(PELOTARIS).sort();
  ['h2hP1','h2hP2'].forEach(id=>{
    const s=document.getElementById(id);
    if(!s) return;
    while(s.options.length>1) s.remove(1);
    ns.forEach(n=>{const o=document.createElement('option');o.value=n;o.textContent=n;s.appendChild(o);});
  });
}

function setH2HMod(mod){
  h2hMod=mod;
  document.querySelectorAll('.comp-mod-btn').forEach(b=>b.classList.remove('on','on-mano','on-cuatro'));
  const cls=mod==='manomanista'?'on-mano':mod==='cuatro'?'on-cuatro':'on';
  document.querySelector(`.comp-mod-btn[data-mod="${mod}"]`)?.classList.add(cls);
  // Hide serie filter when 'todas' selected
  const serieRow = document.querySelector('#sec-comparador .comp-mod-row:nth-child(2)');
  if(serieRow) serieRow.style.opacity = mod==='todas'?'0.35':'1';
  renderH2H();
}

function setH2HSerie(serie){
  h2hSerie=serie;
  document.querySelectorAll('.comp-sub-pill').forEach(b=>b.classList.remove('on'));
  document.querySelector(`.comp-sub-pill[data-serie="${serie}"]`)?.classList.add('on');
  renderH2H();
}

function h2hTipos(){
  if(h2hMod==='todas') return null;
  const MAP={
    parejas:{todas:['campeonato-a','campeonato-b','festival'],a:['campeonato-a'],b:['campeonato-b'],festival:['festival']},
    manomanista:{todas:['manomanista-a','manomanista-b','manomanista','festival-mano'],a:['manomanista-a'],b:['manomanista-b'],festival:['festival-mano','manomanista']},
    cuatro:{todas:['cuatro-medio-a','cuatro-medio-b','festival-cuatro'],a:['cuatro-medio-a'],b:['cuatro-medio-b'],festival:['festival-cuatro']},
  };
  return MAP[h2hMod]?.[h2hSerie]||null;
}

let _h2hUltimo = null, _c4Ultimo = null;   // lo último mostrado, para la imagen de compartir
function renderH2H(){
  const p1=document.getElementById('h2hP1').value;
  const p2=document.getElementById('h2hP2').value;
  const el=document.getElementById('h2hRes');
  if(!p1||!p2||p1===p2){
    el.innerHTML=`<div class="nodata"><div class="ic">🏸</div>${t('h2h_sin_sel')}</div>`;
    return;
  }

  const tipos=h2hTipos();
  const enfs=PARTIDOS.filter(p=>{
    if(activeYearH2H!=='todos'&&getYear(p)!==activeYearH2H) return false;
    const e1=pels(p.equipo1),e2=pels(p.equipo2);
    const enfrentan=(e1.includes(p1)&&e2.includes(p2))||(e1.includes(p2)&&e2.includes(p1));
    if(!enfrentan) return false;
    if(tipos) return tipos.includes(p.tipo);
    return true;
  });

  const modLbl={todas:t('tipo_todos'),parejas:t('tipo_parejas'),manomanista:t('tipo_mano'),cuatro:t('tipo_cuatro')}[h2hMod]||t('tipo_todos');
  const serieLbl={todas:t('serie_todas'),a:t('serie_a'),b:t('serie_b'),festival:t('serie_fest')}[h2hSerie];

  if(!enfs.length){
    el.innerHTML=`<div class="nodata"><div class="ic">📭</div>${t('nodata_h2h')} <strong>${modLbl} — ${serieLbl}</strong></div>`;
    return;
  }

  let w1=0,w2=0,pf1=0,pf2=0;
  enfs.forEach(p=>{
    const e1=pels(p.equipo1),p1enE1=e1.includes(p1);
    const a=p1enE1?p.puntos1:p.puntos2,b=p1enE1?p.puntos2:p.puntos1;
    pf1+=a;pf2+=b;
    if((p1enE1&&p.ganador==='equipo1')||(!p1enE1&&p.ganador==='equipo2'))w1++;else w2++;
  });

  const esMano=h2hMod==='manomanista' && h2hSerie!=='todas';
  const filas=enfs.map(p=>{
    const e1=pels(p.equipo1),p1enE1=e1.includes(p1);
    const a=p1enE1?p.puntos1:p.puntos2,b=p1enE1?p.puntos2:p.puntos1;
    const gP1=(p1enE1&&p.ganador==='equipo1')||(!p1enE1&&p.ganador==='equipo2');
    const cP1=(p1enE1?pels(p.equipo1):pels(p.equipo2)).filter(n=>n!==p1).join('/')||'—';
    const cP2=(p1enE1?pels(p.equipo2):pels(p.equipo1)).filter(n=>n!==p2).join('/')||'—';
    const ti=etiquetaPartido(p);
    return`<tr>
      <td style="font-family:var(--mono);font-size:.66rem">${p.fecha}</td>
      <td><span class="tag ${ti.cls}">${ti.lbl}</span></td>
      <td>${p.fronton}</td>
      ${esMano?'':`<td style="font-size:.7rem;color:var(--muted)">${cP1}</td>`}
      <td style="color:var(--green);font-weight:${gP1?800:400};font-family:var(--mono)">${a}</td>
      <td style="color:var(--muted);text-align:center">—</td>
      <td style="color:var(--green-dark);font-weight:${!gP1?800:400};font-family:var(--mono)">${b}</td>
      ${esMano?'':`<td style="font-size:.7rem;color:var(--muted)">${cP2}</td>`}
    </tr>`;
  }).join('');

  const filtroImg = [h2hMod!=='todas'?modLbl:'', h2hSerie!=='todas'?serieLbl:'', activeYearH2H!=='todos'?activeYearH2H:''].filter(Boolean).join(' · ');
  _h2hUltimo = {p1, p2, enfs, filtro: filtroImg || tx('Todos los partidos','Partida guztiak')};
  el.innerHTML=`
    <div class="an-head" style="margin-bottom:.8rem"><div style="font-family:var(--mono);font-size:.62rem;color:var(--muted);">${modLbl} · ${serieLbl}${activeYearH2H!=='todos'?' · '+activeYearH2H:''} · ${nPartidos(enfs.length)}</div>${botonImagen('compartirH2H()')}</div>
    <div class="comp-bar">
      <div><div class="comp-nm" style="color:var(--green)">${p1}</div><div class="comp-wins izq">${w1}</div><div class="comp-sb">${(pf1/enfs.length).toFixed(1)} ${t('h2h_pts_partido')}</div></div>
      <div><div class="comp-pj">${enfs.length} ${t('abbr_pj')}</div></div>
      <div><div class="comp-nm" style="color:var(--green-dark)">${p2}</div><div class="comp-wins der" style="color:var(--green-dark)">${w2}</div><div class="comp-sb">${(pf2/enfs.length).toFixed(1)} ${t('h2h_pts_partido')}</div></div>
    </div>
    <div class="twrap"><table><thead><tr>
      <th>${t('th_fecha')}</th><th>${t('th_tipo')}</th><th>${t('th_fronton')}</th>
      ${esMano?'':`<th>${t('comp_abbr')} ${p1}</th>`}
      <th>${p1}</th><th></th><th>${p2}</th>
      ${esMano?'':`<th>${t('comp_abbr')} ${p2}</th>`}
    </tr></thead><tbody>${filas}</tbody></table></div>
    <div class="m-card">${renderMobileCardsHTML(enfs, {paginated:true, initial:5, step:5})}</div>`;
}

// Alias por compatibilidad
function renderH2HUnificado(){ renderH2H(); }

// ════════════════════════════════════════════════════════════
// COMPARADOR 4
// ════════════════════════════════════════════════════════════
function populateC4Sels(){
  // Comparador obsoleto
  if(!document.getElementById('c4d1')) return;
  // Solo delanteros conocidos que además tienen partidos en el JSON
  const delants=[...new Set(PARTIDOS.flatMap(p=>[p.equipo1.delantero,p.equipo2.delantero]))]
    .filter(Boolean)
    .filter(n=>getRol(n)==='delantero')
    .sort();
  ['c4d1','c4d2'].forEach(id=>{
    const s=document.getElementById(id);
    delants.forEach(n=>{const o=document.createElement('option');o.value=n;o.textContent=n;s.appendChild(o);});
  });
  // Zagueros para los selects de zaguero (habilitados desde el inicio)
  const zagueros=[...new Set(PARTIDOS.flatMap(p=>[p.equipo1.zaguero,p.equipo2.zaguero]))]
    .filter(Boolean)
    .filter(n=>getRol(n)==='zaguero')
    .sort();
  ['c4z1','c4z2'].forEach(id=>{
    const s=document.getElementById(id);
    s.disabled=false;
    s.innerHTML=`<option value="" data-i18n="sel_zaguero_any">${t('sel_zaguero_any')}</option>`;
    zagueros.forEach(n=>{const o=document.createElement('option');o.value=n;o.textContent=n;s.appendChild(o);});
  });
}

function c4DelanteroChange(num){
  if(!document.getElementById('c4d'+num)) return;
  const del=document.getElementById('c4d'+num).value;
  const zagSel=document.getElementById('c4z'+num);
  // Contar partidos jugados con este delantero para mostrar frecuencia
  const comps=new Map();
  const partsYear=activeYearC4==='todos'?PARTIDOS:PARTIDOS.filter(p=>getYear(p)===activeYearC4);
  if(del){
    partsYear.forEach(p=>{
      const e1=p.equipo1,e2=p.equipo2;
      let z=null;
      if(e1.delantero===del&&e1.zaguero)z=e1.zaguero;
      else if(e2.delantero===del&&e2.zaguero)z=e2.zaguero;
      if(z)comps.set(z,(comps.get(z)||0)+1);
    });
  }
  // Siempre mostrar todos los zagueros conocidos, con frecuencia si aplica
  zagSel.disabled=false;
  zagSel.innerHTML=`<option value="" data-i18n="sel_zaguero_any">${t('sel_zaguero_any')}</option>`;
  const todosZag=[...new Set(PARTIDOS.flatMap(p=>[p.equipo1.zaguero,p.equipo2.zaguero]))]
    .filter(Boolean).filter(n=>getRol(n)==='zaguero').sort();
  // Primero los que han jugado con este delantero (por frecuencia), luego el resto
  const conHistorial=[...comps.entries()].sort((a,b)=>b[1]-a[1]).map(([z])=>z);
  const sinHistorial=todosZag.filter(z=>!comps.has(z));
  if(conHistorial.length){
    const gr1=document.createElement('optgroup');gr1.label=t('c4_con_hist');
    conHistorial.forEach(z=>{
      const o=document.createElement('option');o.value=z;
      o.textContent=`${z} (${nPartidos(comps.get(z))})`;gr1.appendChild(o);
    });
    zagSel.appendChild(gr1);
  }
  if(sinHistorial.length){
    const gr2=document.createElement('optgroup');gr2.label=t('c4_sin_hist');
    sinHistorial.forEach(z=>{
      const o=document.createElement('option');o.value=z;o.textContent=z;gr2.appendChild(o);
    });
    zagSel.appendChild(gr2);
  }
  renderC4();
}

function c4Partidos(d1,z1,d2,z2,partsBase){
  return partsBase.filter(p=>{
    const e1=pels(p.equipo1),e2=pels(p.equipo2);
    const eq1col=e1.includes(d1)&&(!z1||e1.includes(z1));
    const eq2col=e2.includes(d1)&&(!z1||e2.includes(z1));
    const eq1az=e1.includes(d2)&&(!z2||e1.includes(z2));
    const eq2az=e2.includes(d2)&&(!z2||e2.includes(z2));
    return(eq1col&&eq2az)||(eq2col&&eq1az);
  });
}

function c4FilaPartido(p, pivotCol, pivotAz){
  const e1=pels(p.equipo1);
  const colEnE1=e1.includes(pivotCol);
  const ptCol=colEnE1?p.puntos1:p.puntos2;
  const ptAz=colEnE1?p.puntos2:p.puntos1;
  const compCol=(colEnE1?pels(p.equipo1):pels(p.equipo2)).filter(n=>n!==pivotCol).join('/')||'—';
  const compAz=(colEnE1?pels(p.equipo2):pels(p.equipo1)).filter(n=>n!==pivotAz).join('/')||'—';
  const ganCol=(colEnE1&&p.ganador==='equipo1')||(!colEnE1&&p.ganador==='equipo2');
  const ti=etiquetaPartido(p);
  return`<tr>
    <td style="font-family:var(--mono);font-size:.66rem;white-space:nowrap">${p.fecha}</td>
    <td><span class="tag ${ti.cls}">${ti.lbl}</span></td>
    <td style="font-size:.74rem">${p.fronton}</td>
    <td style="font-size:.7rem;color:var(--red)">${compCol}</td>
    <td style="font-family:var(--display);font-size:1.3rem;color:${ganCol?'var(--red)':'var(--muted)'}">${ptCol}</td>
    <td style="color:var(--muted);text-align:center;font-family:var(--mono)">—</td>
    <td style="font-family:var(--display);font-size:1.3rem;color:${!ganCol?'var(--blue)':'var(--muted)'}">${ptAz}</td>
    <td style="font-size:.7rem;color:var(--blue)">${compAz}</td>
  </tr>`;
}

let _c4BlockId = 0;
function c4TablaPartidos(partidos, pivotCol, pivotAz, colLabel, azLabel){
  if(!partidos.length) return`<div class="nodata" style="padding:1.5rem"><div class="ic">📭</div>${t('sin_enfrentamientos')}</div>`;
  const MAX = 3;
  const id = 'c4b' + (++_c4BlockId);
  const thead = `<thead><tr>
    <th>${t('th_fecha')}</th><th>${t('th_tipo')}</th><th>${t('th_fronton')}</th>
    <th style="color:var(--red)">${t('comp_abbr')}</th>
    <th style="color:var(--red)">${colLabel}</th><th></th>
    <th style="color:var(--blue)">${azLabel}</th>
    <th style="color:var(--blue)">${t('comp_abbr')}</th>
  </tr></thead>`;
  const filas = partidos.map(p => c4FilaPartido(p, pivotCol, pivotAz));
  const visibles = filas.slice(0, MAX).join('');
  const ocultos = filas.slice(MAX).join('');
  const btnVerMas = ocultos ? `<tr id="${id}-btn"><td colspan="8" style="text-align:center;padding:.5rem">
    <button class="btn-ghost" onclick="c4VerMas('${id}')" style="font-size:.6rem;padding:.3rem .9rem">
      ${t('c4_ver_mas').replace('{n}', partidos.length - MAX)}
    </button></td></tr>` : '';
  const filasOcultas = ocultos ? `<tbody id="${id}-extra" style="display:none">${ocultos}</tbody>` : '';
  return`<div class="twrap"><table>${thead}
    <tbody>${visibles}</tbody>
    ${filasOcultas}
    <tbody>${btnVerMas}</tbody>
  </table></div><div class="m-card">${renderMobileCardsHTML(partidos, {paginated:true, initial:5, step:5})}</div>`;
}


function c4TablaSoloPareja(partidos, pivotD, pivotZ, parejaLabel, color){
  if(!partidos.length) return`<div class="nodata" style="padding:1.5rem"><div class="ic">📭</div>${t('sin_partidos')}</div>`;
  const MAX=3; const id='c4b'+(++_c4BlockId);
  const colorMain = color==='col'?'var(--red)':'var(--blue)';
  const thead=`<thead><tr>
    <th>${t('th_fecha')}</th><th>${t('th_tipo')}</th><th>${t('th_fronton')}</th>
    <th style="color:${colorMain}">${parejaLabel}</th>
    <th style="color:${colorMain}">${t('abbr_tantos')}</th><th></th>
    <th style="color:var(--muted)">${t('th_rival')}</th>
    <th style="color:var(--muted)">${t('abbr_tantos')}</th>
  </tr></thead>`;
  const filas = partidos.map(p=>{
    const e1=pels(p.equipo1), e2=pels(p.equipo2);
    const pInE1 = e1.includes(pivotD)&&(!pivotZ||e1.includes(pivotZ));
    const pSide = pInE1?e1:e2;
    const rSide = pInE1?e2:e1;
    const ptP = pInE1?p.puntos1:p.puntos2;
    const ptR = pInE1?p.puntos2:p.puntos1;
    const gano = (pInE1&&p.ganador==='equipo1')||(!pInE1&&p.ganador==='equipo2');
    const compP = pSide.filter(n=>n!==pivotD).join('/')||'—';
    const compR = rSide.join('/')||'—';
    const ti=etiquetaPartido(p);const tiTag=`<span class="tag ${ti.cls}">${ti.lbl}</span>`;
    return`<tr>
      <td style="font-family:var(--mono);font-size:.66rem;white-space:nowrap">${p.fecha}</td>
      <td>${tiTag}</td>
      <td style="font-size:.74rem">${p.fronton}</td>
      <td style="font-size:.7rem;color:${colorMain}">${compP}</td>
      <td style="font-family:var(--display);font-size:1.3rem;color:${colorMain};font-weight:${gano?800:400}">${ptP}</td>
      <td style="color:var(--muted);text-align:center;font-family:var(--mono)">—</td>
      <td style="font-size:.7rem;color:var(--muted);font-weight:${!gano?700:400}">${compR}</td>
      <td style="font-family:var(--display);font-size:1.3rem;color:var(--muted);font-weight:${!gano?800:400}">${ptR}</td>
    </tr>`;
  });
  const visibles=filas.slice(0,MAX).join('');
  const ocultos=filas.slice(MAX).join('');
  const btnVerMas=ocultos?`<tr id="${id}-btn"><td colspan="8" style="text-align:center;padding:.5rem">
    <button class="btn-ghost" onclick="c4VerMas('${id}')" style="font-size:.6rem;padding:.3rem .9rem">
      ${t('c4_ver_mas').replace('{n}',partidos.length-MAX)}
    </button></td></tr>`:'';
  const filasOcultas=ocultos?`<tbody id="${id}-extra" style="display:none">${ocultos}</tbody>`:'';
  return`<div class="twrap"><table>${thead}<tbody>${visibles}</tbody>${filasOcultas}<tbody>${btnVerMas}</tbody></table></div><div class="m-card">${renderMobileCardsHTML(partidos, {paginated:true, initial:5, step:5})}</div>`;
}

// Partidos con 3 de los 4 pelotaris elegidos: una de las dos parejas juega
// junta tal cual y enfrente está el delantero o el zaguero de la otra (no los
// dos). No cuentan los partidos con los tres repartidos de otra manera
// (p. ej. un pelotari de cada pareja jugando juntos).
function c4TresDeCuatro(cuatro, partsBase){
  const [d1,z1,d2,z2]=cuatro;
  const juntos=(e,a,b)=>e.includes(a)&&e.includes(b);
  const uno=(e,a,b)=>e.includes(a)!==e.includes(b);
  const vale=(eA,eB)=>(juntos(eA,d1,z1)&&uno(eB,d2,z2)&&!eA.includes(d2)&&!eA.includes(z2)) ||
                      (juntos(eA,d2,z2)&&uno(eB,d1,z1)&&!eA.includes(d1)&&!eA.includes(z1));
  return partsBase.filter(p=>{
    const e1=pels(p.equipo1), e2=pels(p.equipo2);
    return vale(e1,e2)||vale(e2,e1);
  });
}

function c4TablaTresDeCuatro(partidos, d1, z1, d2, z2){
  if(!partidos.length) return`<div class="nodata" style="padding:1.5rem"><div class="ic">📭</div>${t('sin_partidos')}</div>`;
  const MAX=3; const id='c4b'+(++_c4BlockId);
  const cuatro=[d1,z1,d2,z2];
  // Los cuatro elegidos en su color; quien entra por el que falta, en gris
  const nombre=n=>{
    const c=(n===d1||n===z1)?'var(--red)':(n===d2||n===z2)?'var(--blue)':null;
    return c?`<span style="color:${c};font-weight:600">${n}</span>`:`<span style="color:var(--muted);font-style:italic">${n}</span>`;
  };
  const thead=`<thead><tr>
    <th>${t('th_fecha')}</th><th>${t('th_tipo')}</th><th>${t('th_fronton')}</th>
    <th style="color:var(--red)">${t('eq_colorada')}</th><th>${t('abbr_tantos')}</th><th></th>
    <th>${t('abbr_tantos')}</th><th style="color:var(--blue)">${t('eq_azul')}</th>
    <th>${t('c4_sin')}</th>
  </tr></thead>`;
  // A la izquierda (en rojo) el lado con más pelotaris de la pareja colorada
  const rojos=e=>pels(e).filter(n=>n===d1||n===z1).length;
  const orientados=partidos.map(p=>{
    const izq1=rojos(p.equipo1)>rojos(p.equipo2)||(rojos(p.equipo1)===rojos(p.equipo2)&&!pels(p.equipo2).includes(d1));
    return izq1?p:{...p, equipo1:p.equipo2, equipo2:p.equipo1, puntos1:p.puntos2, puntos2:p.puntos1,
      ganador:p.ganador==='equipo1'?'equipo2':'equipo1'};
  });
  const filas=orientados.map(p=>{
    const e1=pels(p.equipo1), e2=pels(p.equipo2);
    const [eI,eD,ptI,ptD]=[e1,e2,p.puntos1,p.puntos2];
    const ganaI=p.ganador==='equipo1';
    const falta=cuatro.find(n=>!e1.includes(n)&&!e2.includes(n));
    const ti=etiquetaPartido(p);
    return`<tr>
      <td style="font-family:var(--mono);font-size:.66rem;white-space:nowrap">${p.fecha}</td>
      <td><span class="tag ${ti.cls}">${ti.lbl}</span></td>
      <td style="font-size:.74rem">${p.fronton}</td>
      <td style="font-size:.7rem">${eI.map(nombre).join(' / ')}</td>
      <td style="font-family:var(--display);font-size:1.3rem;color:${ganaI?'var(--red)':'var(--muted)'}">${ptI}</td>
      <td style="color:var(--muted);text-align:center;font-family:var(--mono)">—</td>
      <td style="font-family:var(--display);font-size:1.3rem;color:${!ganaI?'var(--blue)':'var(--muted)'}">${ptD}</td>
      <td style="font-size:.7rem">${eD.map(nombre).join(' / ')}</td>
      <td style="font-size:.66rem;color:var(--muted);white-space:nowrap">${falta}</td>
    </tr>`;
  });
  const visibles=filas.slice(0,MAX).join('');
  const ocultos=filas.slice(MAX).join('');
  const btnVerMas=ocultos?`<tr id="${id}-btn"><td colspan="9" style="text-align:center;padding:.5rem">
    <button class="btn-ghost" onclick="c4VerMas('${id}')" style="font-size:.6rem;padding:.3rem .9rem">
      ${t('c4_ver_mas').replace('{n}',partidos.length-MAX)}
    </button></td></tr>`:'';
  const filasOcultas=ocultos?`<tbody id="${id}-extra" style="display:none">${ocultos}</tbody>`:'';
  // Cuántos partidos sin cada uno de los cuatro
  const sinCada=cuatro.map(n=>[n,partidos.filter(p=>!pels(p.equipo1).includes(n)&&!pels(p.equipo2).includes(n)).length])
    .filter(([,k])=>k).map(([n,k])=>`<span class="c4-sin-chip">${t('c4_sin_pel').replace('{n}',n)}: ${k}</span>`).join('');
  return`<div class="c4-sin-chips">${sinCada}</div>
    <div class="twrap"><table>${thead}<tbody>${visibles}</tbody>${filasOcultas}<tbody>${btnVerMas}</tbody></table></div>
    <div class="m-card">${renderMobileCardsHTML(orientados, {paginated:true, initial:5, step:5})}</div>`;
}

function c4VerMas(id){
  const extra = document.getElementById(id+'-extra');
  const btn = document.getElementById(id+'-btn');
  if(extra) extra.style.display='';
  if(btn) btn.style.display='none';
}

function c4Bar(partidos, pivot_col, pivot_az, label, soloColor){
  // soloColor: 'col' | 'az' | undefined (enfrentamiento entre los dos)
  let wCol=0,wAz=0,pfCol=0,pfAz=0,over365=0;
  partidos.forEach(p=>{
    const colEnE1=pels(p.equipo1).includes(pivot_col);
    const ptC=colEnE1?p.puntos1:p.puntos2;
    const ptA=colEnE1?p.puntos2:p.puntos1;
    pfCol+=ptC; pfAz+=ptA;
    if(ptC+ptA>36.5) over365++;
    if((colEnE1&&p.ganador==='equipo1')||(!colEnE1&&p.ganador==='equipo2'))wCol++;else wAz++;
  });
  const n=partidos.length; if(!n) return'';
  const pctCol=Math.round(wCol/n*100);
  const pctOver=Math.round(over365/n*100);

  if(soloColor){
    // Panel de una sola pareja - siempre usar wCol (pivot_col gana)
    const w=wCol;
    const l=n-w;
    const pct=Math.round(w/n*100);
    const color=soloColor==='col'?'var(--red)':'var(--blue)';
    return`<div class="c4-marcador" style="grid-template-columns:repeat(4,1fr)">
      <div style="text-align:center">
        <div style="font-family:var(--mono);font-size:.52rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin-bottom:.25rem">${t('stat_pj')}</div>
        <div style="font-family:var(--display);font-size:2rem;line-height:1;color:${color}">${n}</div>
      </div>
      <div style="text-align:center">
        <div style="font-family:var(--mono);font-size:.52rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin-bottom:.25rem">${t('stat_ganados')}</div>
        <div style="font-family:var(--display);font-size:2rem;line-height:1;color:var(--green)">${w}</div>
        <div style="font-family:var(--mono);font-size:.52rem;color:var(--muted)">${l} ${t('stat_perd')}</div>
      </div>
      <div style="text-align:center">
        <div style="font-family:var(--mono);font-size:.52rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin-bottom:.25rem">${t('stat_pct_vic')}</div>
        <div style="font-family:var(--display);font-size:2rem;line-height:1;color:${pct>=50?'var(--green)':'var(--red)'}">${pct}%</div>
      </div>
      <div style="text-align:center">
        <div style="font-family:var(--mono);font-size:.52rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin-bottom:.25rem">${t('stat_over')}</div>
        <div style="font-family:var(--display);font-size:2rem;line-height:1;color:var(--text)">${pctOver}%</div>
        <div style="font-family:var(--mono);font-size:.52rem;color:var(--muted)">${over365} ${t('stat_pj').toLowerCase()}</div>
      </div>
    </div>`;
  }

  // Panel de enfrentamiento (dos parejas)
  return`<div class="c4-marcador">
    <div>
      <div class="c4-mnombre" style="color:var(--red)">${pivot_col}</div>
      <div class="c4-mwins col">${wCol}</div>
      <div class="c4-mpj">${t('pct_victorias').replace('{n}',pctCol)}</div>
      <div class="c4-mpj">${t('pts_p').replace('{n}',(pfCol/n).toFixed(1))}</div>
    </div>
    <div style="text-align:center">
      <div style="font-family:var(--display);font-size:1.1rem;color:var(--muted)">${n} PJ</div>
      <div style="font-family:var(--mono);font-size:.55rem;color:var(--muted);margin-top:.3rem">${t('stat_over')}: ${pctOver}%</div>
      <div style="font-family:var(--mono);font-size:.52rem;color:var(--muted);margin-top:.1rem">${label}</div>
    </div>
    <div>
      <div class="c4-mnombre" style="color:var(--blue)">${pivot_az}</div>
      <div class="c4-mwins az">${wAz}</div>
      <div class="c4-mpj">${t('pct_victorias').replace('{n}',100-pctCol)}</div>
      <div class="c4-mpj">${t('pts_p').replace('{n}',(pfAz/n).toFixed(1))}</div>
    </div>
  </div>`;
}

function renderC4(){
  // Comparador de parejas obsoleto: unificado en renderH2HUnificado()
  if(!document.getElementById('c4d1')) return;
  const d1=document.getElementById('c4d1').value||null;
  const z1=document.getElementById('c4z1').value||null;
  const d2=document.getElementById('c4d2').value||null;
  const z2=document.getElementById('c4z2').value||null;
  const el=document.getElementById('c4Res');
  if(!d1||!d2){el.innerHTML='';return;}

  const partsBase=activeYearC4==='todos'?PARTIDOS:PARTIDOS.filter(p=>getYear(p)===activeYearC4);
  const exactos=c4Partidos(d1,z1,d2,z2,partsBase);
  const delants=partsBase.filter(p=>{const e1=pels(p.equipo1),e2=pels(p.equipo2);return(e1.includes(d1)&&e2.includes(d2))||(e1.includes(d2)&&e2.includes(d1));});
  const zagueros=z1&&z2?partsBase.filter(p=>{const e1=pels(p.equipo1),e2=pels(p.equipo2);return(e1.includes(z1)&&e2.includes(z2))||(e1.includes(z2)&&e2.includes(z1));}):[];
  const colLabel=z1?`${d1} / ${z1}`:d1;
  const azLabel=z2?`${d2} / ${z2}`:d2;

  // Partidos de cada pareja por separado (todos los que incluyen esa pareja)
  const soloCol = partsBase.filter(p=>{
    const e1=pels(p.equipo1),e2=pels(p.equipo2);
    return (e1.includes(d1)&&(!z1||e1.includes(z1))) || (e2.includes(d1)&&(!z1||e2.includes(z1)));
  });
  const soloAz = partsBase.filter(p=>{
    const e1=pels(p.equipo1),e2=pels(p.equipo2);
    return (e1.includes(d2)&&(!z2||e1.includes(z2))) || (e2.includes(d2)&&(!z2||e2.includes(z2)));
  });

  // H2H delanteros (solo delanteros, cualquier zaguero)
  const h2hDel = partsBase.filter(p=>{
    const e1=pels(p.equipo1),e2=pels(p.equipo2);
    return (e1.includes(d1)&&e2.includes(d2))||(e1.includes(d2)&&e2.includes(d1));
  });
  // H2H zagueros
  const h2hZag = z1&&z2 ? partsBase.filter(p=>{
    const e1=pels(p.equipo1),e2=pels(p.equipo2);
    return (e1.includes(z1)&&e2.includes(z2))||(e1.includes(z2)&&e2.includes(z1));
  }) : [];

  // Partidos con 3 de los 4 (el cuarto sustituido por otro pelotari)
  const tresDeCuatro = z1&&z2 ? c4TresDeCuatro([d1,z1,d2,z2], partsBase) : [];

  _c4BlockId = 0; // reset IDs
  _c4Ultimo = {d1, z1, d2, z2, exactos, anio: activeYearC4};

  const mkBloque = (emoji,titulo,n,bar,tabla) => `
    <div class="c4-bloque">
      <div class="c4-bloque-title">${emoji} ${titulo} <span style="font-family:var(--mono);font-size:.65rem;color:var(--muted);font-weight:400">${nPartidos(n)}</span></div>
      ${bar}${tabla}
    </div>`;

  el.innerHTML=
    mkBloque('⚡',t('c4_identico'),exactos.length,
      exactos.length?`<div style="display:flex;justify-content:flex-end;margin-bottom:.5rem">${botonImagen('compartirC4()')}</div>`+c4Bar(exactos,d1,d2,t('pareja_exacta')):'',
      c4TablaPartidos(exactos,d1,d2,colLabel,azLabel))
    +mkBloque('🔴',`${t('c4_partidos_col')} ${colLabel}`,soloCol.length,
      soloCol.length?c4Bar(soloCol,d1,d1,t('eq_colorada'),'col'):'',
      c4TablaSoloPareja(soloCol,d1,z1,colLabel,'col'))
    +mkBloque('🔵',`${t('c4_partidos_col')} ${azLabel}`,soloAz.length,
      soloAz.length?c4Bar(soloAz,d2,d2,t('eq_azul'),'az'):'',
      c4TablaSoloPareja(soloAz,d2,z2,azLabel,'az'))
    +mkBloque('⚔️',`${t('c4_h2h_del')} — ${d1} vs ${d2}`,h2hDel.length,
      h2hDel.length?c4Bar(h2hDel,d1,d2,t('c4_h2h_del')):'',
      c4TablaPartidos(h2hDel,d1,d2,d1,d2))
    +(z1&&z2
      ? mkBloque('🔄',`${t('c4_h2h_zag')} — ${z1} vs ${z2}`,h2hZag.length,
          h2hZag.length?c4Bar(h2hZag,z1,z2,t('c4_h2h_zag')):'',
          c4TablaPartidos(h2hZag,z1,z2,z1,z2))
      : `<div class="c4-bloque"><div class="c4-bloque-title">🔄 ${t('c4_h2h_zag')}</div><div class="nodata" style="padding:1.5rem"><div class="ic">🔵</div>${t('c4_sin_zag')}</div></div>`
    )
    +(z1&&z2
      ? mkBloque('3️⃣',t('c4_tres_de_cuatro'),tresDeCuatro.length,'',
          c4TablaTresDeCuatro(tresDeCuatro,d1,z1,d2,z2))
      : `<div class="c4-bloque"><div class="c4-bloque-title">3️⃣ ${t('c4_tres_de_cuatro')}</div><div class="nodata" style="padding:1.5rem"><div class="ic">🔵</div>${t('c4_tres_sin_zag')}</div></div>`
    );
}

// ════════════════════════════════════════════════════════════
// FRONTONES
// ════════════════════════════════════════════════════════════
function renderFrontones(){
  const q=(document.getElementById('frontonSearch')?.value||'').toLowerCase();
  const parts=PARTIDOS.filter(p=>
    tipoMatch(p.tipo,activeTipoFron)&&
    (activeYearFron==='todos'||getYear(p)===activeYearFron)
  );

  const stats={};
  parts.forEach(p=>{
    const f=p.fronton;
    if(!stats[f])stats[f]={partidos:0,ciudad:p.ciudad||''};
    stats[f].partidos++;
    if(p.ciudad&&!stats[f].ciudad)stats[f].ciudad=p.ciudad;
  });

  const lista=Object.entries(stats)
    .filter(([f,s])=>!q||f.toLowerCase().includes(q)||s.ciudad.toLowerCase().includes(q))
    .sort((a,b)=>b[1].partidos-a[1].partidos);

  document.getElementById('frontonesCount').textContent=lista.length;
  renderFrontonMarkers();
  // Construir índice por nombre del catálogo de frontones
  const FRO_BY_NAME = {};
  Object.values(CAT_FRONTONES).forEach(fr=>{
    if(fr && fr.nombre) FRO_BY_NAME[fr.nombre.toUpperCase().trim()] = fr;
  });
  document.getElementById('frontonGrid').innerHTML=lista.map(([f,s])=>{
    const info = FRO_BY_NAME[f.toUpperCase().trim()];
    const mapsUrl = info?.google_maps_link
      || `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent('Fronton '+f)}`;
    return `
    <div class="fronton-card">
      <div class="fronton-card-top" onclick="abrirFronton('${esc(f)}')">
        <div>
          <div class="fname">${f}</div>
          ${s.ciudad?`<div class="fcity">${s.ciudad}</div>`:''}
        </div>
        <div class="fcount">${s.partidos}</div>
      </div>
      <div class="fronton-card-actions">
        <a href="${mapsUrl}" target="_blank" rel="noopener noreferrer"
           class="fronton-btn"
           onclick="event.stopPropagation()"
           title="${t('fronton_como_llegar')}">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/><circle cx="12" cy="10" r="3"/></svg>
          <span>${t('fronton_como_llegar')}</span>
        </a>
      </div>
    </div>`;
  }).join('');
}

function filtrarPorFronton(fronton){
  goToNav('partidos');
  setTimeout(()=>{
    document.getElementById('fFron').value=fronton;
    renderTabla();
  },80);
}

function goToNav(secId){
  const btn=document.querySelector(`nav button[onclick*="${secId}"]`);
  if(btn)btn.click();
}


// ════════════════════════════════════════════════════════════
// CARTELERA
// ════════════════════════════════════════════════════════════
let cartelaraLoaded = false;
let _carteleraData = null;

async function loadCartelera(){
  if(cartelaraLoaded) return;
  const el = document.getElementById('carteleraContainer');
  el.innerHTML = `<div class="cart-loading">${t('cart_cargando')}</div>`;
  try{
    const r = await fetch('data/cartelera.json?_='+Date.now());
    if(!r.ok) throw new Error('HTTP '+r.status);
    const data = await r.json();
    _carteleraData = data;
    renderCartelera(data);
    cartelaraLoaded = true;
    avisarSeguidos(data.partidos || []);
  } catch(e){
    el.innerHTML = `<div class="cart-error">
      <div class="cart-error-title">${t('cart_error')}</div>
      <br><button class="btn" onclick="cartelaraLoaded=false;loadCartelera()">${t('cart_recarga')}</button>
    </div>`;
  }
}

function renderCartelera(data){
  const el = document.getElementById('carteleraContainer');
  const eventos = data.partidos || [];
  document.getElementById('carteleraBadge').textContent = eventos.length + ' ' + t('cart_eventos');
  if(!eventos.length){
    el.innerHTML=`<div class="nodata"><div class="ic">📅</div>${t('cart_no_partidos')}</div>`;
    return;
  }
  const DIAS=t('dias');
  const MESES=t('meses');
  function fmtFecha(f){
    try{ const[d,m,y]=f.split('/'); const dt=new Date(+y,+m-1,+d);
      // eu: "Igandea, 2026ko irailaren 27a"
      if(LANG==='eu') return `${DIAS[dt.getDay()]}, ${y}ko ${MESES[+m-1]} ${+d}a`;
      return `${DIAS[dt.getDay()]} ${+d} ${MESES[+m-1]} ${y}`; }
    catch{ return f; }
  }
  function tipoBadgeClass(t){
    if((t||'').includes('manomanista')) return 'mano';
    if((t||'').includes('cuatro')) return 'cuatro';
    if((t||'').includes('-b')) return 'b';
    if((t||'').includes('festival')) return 'fest';
    return 'a';
  }
  function tipoLabel(tp){
    const m={'campeonato-a':t('tag_seriea'),'campeonato-b':t('tag_serieb'),'festival':t('tag_festival'),
      'manomanista-a':t('tag_manoa'),'manomanista-b':t('tag_manob'),'cuatro-medio-a':t('tag_cuatroa'),'cuatro-medio-b':t('tag_cuatrob')};
    return m[tp]||tp||'';
  }
  _CART_EVENTOS = eventos;
  const byDate={};
  eventos.forEach(e=>{ if(!byDate[e.fecha])byDate[e.fecha]=[]; byDate[e.fecha].push(e); });
  const html = Object.entries(byDate).map(([fecha,evs])=>`
    <div class="cart-day-label">${fmtFecha(fecha)}</div>
    ${evs.map(ev=>{
      const parts = ev.partidos||[];
      const partList = parts.length ? parts : [{eq1:[],eq2:[],tipo:ev.tipo||'campeonato-a',serie:'a',raw:(ev.cartel||[]).join(' ')}];
      const tipoEv = ev.tipo || (partList[0]?.tipo||'campeonato-a');
      const faseBadge = ev.fase ? `<span class="cart-badge fase">${tFase(ev.fase)}</span>` : '';
      const compBadge = ev.competicion ? `<span class="cart-badge fase">${tFase(ev.competicion)}</span>` : '';
      const tvBadge = '';
      // URL de Google Maps a partir del frontón del evento (una vez por evento)
      let mapsUrl = '';
      if(ev.fronton){
        const FRO_BY_NAME = window._FRO_BY_NAME || (window._FRO_BY_NAME = (() => {
          const idx = {};
          Object.values(CAT_FRONTONES).forEach(fr=>{
            if(fr && fr.nombre) idx[fr.nombre.toUpperCase().trim()] = fr;
          });
          return idx;
        })());
        const fronInfo = FRO_BY_NAME[ev.fronton.toUpperCase().trim()];
        mapsUrl = fronInfo?.google_maps_link
          || `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent('Fronton '+ev.fronton)}`;
      }
      const llegarBtn = mapsUrl
        ? `<a href="${mapsUrl}" target="_blank" rel="noopener noreferrer" class="cart-evento-llegar" title="${t('cart_como_llegar')}">
             <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/><circle cx="12" cy="10" r="3"/></svg>
             <span>${t('cart_como_llegar')}</span>
           </a>`
        : '';
      const calBtn = ev.fecha && ev.hora
        ? `<button type="button" class="cart-evento-llegar" onclick="descargarICS(${eventos.indexOf(ev)})" title="${t('cart_calendario_t')}" aria-label="${t('cart_calendario_t')}">
             <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="3" y="4" width="18" height="18" rx="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/><line x1="12" y1="14" x2="12" y2="18"/><line x1="10" y1="16" x2="14" y2="16"/></svg>
             <span>${t('cart_calendario')}</span>
           </button>`
        : '';
      const partidosHtml = partList.map((p,idx)=>{
        const e1 = (p.eq1||[]).filter(Boolean).join(' / ')||'—';
        const e2 = (p.eq2||[]).filter(Boolean).join(' / ')||'—';
        const serieBadge = p.serie && p.serie!=='a' ? `<span class="cart-partido-serie">${t(p.serie==='b'?'tag_serieb':'tag_seriea')}</span>` : '';
        const pTipo = p.categoria ? etiquetaPartido(p).lbl : tipoLabel(p.tipo||tipoEv||'');
        const tipoBadge = `<span class="cart-badge ${p.categoria==='torneo'?'torneo':p.categoria==='desafio'?'fest':tipoBadgeClass(p.tipo||tipoEv)}">${pTipo}</span>`
          + (p.fase && tFase(ev.fase||'').toLowerCase().indexOf(t('fase_'+p.fase).toLowerCase())<0 ? `<span class="cart-badge fase">${textoFase(p)}</span>` : '');
        const encoded = encodeURIComponent(JSON.stringify({...p, fecha:ev.fecha, hora:ev.hora, fronton:ev.fronton}));
        const segSet = new Set(seguidos().map(normNombre));
        const esSeguido = [...(p.eq1||[]),...(p.eq2||[])].some(n=>segSet.has(normNombre(n)));
        return `<div class="cart-partido-wrap">
          <div class="cart-partido-card" onclick="carteleraGoStats(this)" data-partido="${encoded}" aria-label="${h(t('cart_estadisticas')+': '+e1+' vs '+e2)}">
            <div>
              <div style="display:flex;gap:.35rem;margin-bottom:.3rem;">${tipoBadge}${serieBadge}${esSeguido?`<span class="cart-seguido" title="${h(tx('Juega uno de tus pelotaris','Zure pilotarietako batek jokatzen du'))}">★</span>`:''}</div>
              <div class="cart-partido-equipos">
                <span class="cart-partido-eq" style="color:var(--red)">${e1}</span>
                <span class="cart-partido-vs">vs</span>
                <span class="cart-partido-eq" style="color:var(--blue)">${e2}</span>
              </div>
              ${htmlPrevia(p)}
            </div>
            <div class="cart-partido-arrow">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg>
              <span>${t('cart_estadisticas')} →</span>
            </div>
          </div>
          ${(p.eq1||[]).filter(Boolean).length && (p.eq2||[]).filter(Boolean).length
            ? `<button type="button" class="cart-img-btn" onclick="compartirPartidoCartelera(this,event)" aria-label="${h(tx('Compartir el partido como imagen','Partida irudi gisa partekatu'))}">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/></svg>
                ${tx('Compartir imagen','Irudia partekatu')}</button>` : ''}
        </div>`;
      }).join('');
      return `<div class="cart-evento">
        <div class="cart-evento-header">
          <div class="cart-evento-meta">
            <div class="cart-evento-comp">${faseBadge||compBadge}</div>
            <div class="cart-evento-lugar"><strong>${ev.hora||'—'}h</strong> · ${ev.fronton||'—'}</div>
            <div class="cart-evento-ciudad">${ev.ciudad&&ev.ciudad!==ev.fronton?ev.ciudad:''}</div>
          </div>
          <div class="cart-evento-btns">${calBtn}${llegarBtn}</div>
        </div>
        <div class="cart-partidos-list">${partidosHtml}</div>
        
      </div>`;
    }).join('')}`
  ).join('');
  el.innerHTML = htmlSeguidosCartelera(eventos) + html;
}

// ── Añadir al calendario (.ics) ──
let _CART_EVENTOS = [];

// Hora de Madrid, para que el partido salga a su hora en cualquier calendario
const ICS_TZ = ['BEGIN:VTIMEZONE','TZID:Europe/Madrid',
  'BEGIN:DAYLIGHT','TZOFFSETFROM:+0100','TZOFFSETTO:+0200','TZNAME:CEST','DTSTART:19700329T020000','RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU','END:DAYLIGHT',
  'BEGIN:STANDARD','TZOFFSETFROM:+0200','TZOFFSETTO:+0100','TZNAME:CET','DTSTART:19701025T030000','RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU','END:STANDARD',
  'END:VTIMEZONE'];

function icsTexto(s){ return String(s||'').replace(/\\/g,'\\\\').replace(/;/g,'\\;').replace(/,/g,'\\,').replace(/\r?\n/g,'\\n'); }

// Líneas de 75 octetos como máximo (RFC 5545)
function icsPlegar(linea){
  const bytes = new TextEncoder().encode(linea);
  if(bytes.length <= 75) return linea;
  const out = []; let actual = '', n = 0;
  for(const ch of linea){
    const b = new TextEncoder().encode(ch).length;
    if(n + b > (out.length ? 74 : 75)){ out.push(actual); actual = ''; n = 0; }
    actual += ch; n += b;
  }
  out.push(actual);
  return out.join('\r\n ');
}

function eventoICS(ev){
  const [d,m,y] = ev.fecha.split('/').map(Number);
  const [hh,mm] = ev.hora.split(':').map(Number);
  const dos = n => String(n).padStart(2,'0');
  const fin = new Date(y, m-1, d, hh+3, mm);  // unas 3 horas de festival
  const inicio = `${y}${dos(m)}${dos(d)}T${dos(hh)}${dos(mm)}00`;
  const final = `${fin.getFullYear()}${dos(fin.getMonth()+1)}${dos(fin.getDate())}T${dos(fin.getHours())}${dos(fin.getMinutes())}00`;
  const eq = e => (e||[]).filter(Boolean).map(n=>n.toUpperCase()).join('-') || '?';
  const partidos = (ev.partidos||[]).map(p=>`${eq(p.eq1)} vs ${eq(p.eq2)}`);
  const comp = ev.fase || ev.competicion || '';
  const titulo = partidos.length === 1
    ? `${t('cal_pelota')}: ${partidos[0]}`
    : `${t('cal_pelota')} · ${ev.fronton||''}${comp ? ' · '+tFase(comp) : ''}`;
  const fr = Object.values(CAT_FRONTONES).find(f => f && f.nombre && ev.fronton && f.nombre.toUpperCase() === ev.fronton.toUpperCase());
  const lugar = [ev.fronton, fr?.direccion, ev.ciudad && ev.ciudad !== ev.fronton ? ev.ciudad : ''].filter(Boolean).join(', ');
  const desc = [comp ? tFase(comp) : '', ...partidos, '', 'https://www.eskupilotastats.com/#/cartelera'].join('\n');
  const ahora = new Date().toISOString().replace(/[-:]/g,'').replace(/\.\d+Z$/,'Z');
  const uid = `${inicio}-${(ev.fronton||'').toLowerCase().replace(/[^a-z0-9]+/g,'-')}@eskupilotastats.com`;
  return ['BEGIN:VCALENDAR','VERSION:2.0','PRODID:-//EskupilotaStats//Cartelera//ES','CALSCALE:GREGORIAN','METHOD:PUBLISH',
    ...ICS_TZ,
    'BEGIN:VEVENT', `UID:${uid}`, `DTSTAMP:${ahora}`,
    `DTSTART;TZID=Europe/Madrid:${inicio}`, `DTEND;TZID=Europe/Madrid:${final}`,
    `SUMMARY:${icsTexto(titulo)}`, `LOCATION:${icsTexto(lugar)}`, `DESCRIPTION:${icsTexto(desc)}`,
    ...(fr && fr.lat != null ? [`GEO:${fr.lat};${fr.lon}`] : []),
    'URL:https://www.eskupilotastats.com/#/cartelera',
    'END:VEVENT','END:VCALENDAR'].map(icsPlegar).join('\r\n') + '\r\n';
}

function descargarICS(i){
  const ev = _CART_EVENTOS[i];
  if(!ev) return;
  const blob = new Blob([eventoICS(ev)], {type:'text/calendar;charset=utf-8'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  const [d,m,y] = ev.fecha.split('/');
  a.download = `pelota-${y}-${m}-${d}-${(ev.fronton||'').toLowerCase().replace(/[^a-z0-9]+/g,'-')}.ics`;
  document.body.appendChild(a); a.click(); a.remove();
  setTimeout(() => URL.revokeObjectURL(a.href), 1000);
}

// Pulsar un partido de la cartelera lleva a sus estadísticas (comparador)
function carteleraGoStats(el){
  const card = el.closest('.cart-partido-card');
  if(!card) return;
  try{ goToComparadorFromPartido(JSON.parse(decodeURIComponent(card.dataset.partido))); }
  catch(e){ console.error(e); }
}

function goToComparadorFromPartido(p){
  const tipo = p.tipo||'campeonato-a';
  const isMano = tipo.includes('manomanista');
  if(p.pendiente){ goToNav('comparador'); return; }

  function selOpt(selId, nombre){
    if(!nombre || nombre==='?') return;
    const sel=document.getElementById(selId); if(!sel) return;
    const norm=nombre.toUpperCase().trim();
    let found=[...sel.options].find(o=>o.value.toUpperCase()===norm);
    if(!found) found=[...sel.options].find(o=>o.value.toUpperCase().startsWith(norm.split(' ')[0]));
    if(found){ sel.value=found.value; sel.dispatchEvent(new Event('change')); }
  }

  if(isMano){
    // 1 vs 1 → ir al comparador H2H
    const p1=(p.eq1||[])[0], p2=(p.eq2||[])[0];
    if(!p1||!p2||p1==='?'||p2==='?'){ goToNav('comparador'); return; }
    goToNav('comparador');
    setTimeout(()=>{
      document.querySelector('.ctype-btn[onclick*="h2h"]')?.click();
      setH2HMod('manomanista');
      // Determinar serie por el tipo
      let serie = 'todas';
      if(tipo.endsWith('-a')) serie='a';
      else if(tipo.endsWith('-b')) serie='b';
      else if(tipo.startsWith('festival')) serie='festival';
      setH2HSerie(serie);
      selOpt('h2hP1',p1); selOpt('h2hP2',p2);
      renderH2H();
    },100);
  } else {
    // Pareja / 4½ → ir al comparador de parejas
    const d1=(p.eq1||[])[0], z1=(p.eq1||[])[1]||null;
    const d2=(p.eq2||[])[0], z2=(p.eq2||[])[1]||null;
    if(!d1||!d2||d1==='?'||d2==='?'){ goToNav('comparador'); return; }
    goToNav('comparador');
    setTimeout(()=>{
      document.querySelector('.ctype-btn[onclick*="parejas"]')?.click();
      selOpt('c4d1',d1); c4DelanteroChange(1);
      selOpt('c4d2',d2); c4DelanteroChange(2);
      setTimeout(()=>{
        if(z1 && z1!=='?') selOpt('c4z1',z1);
        if(z2 && z2!=='?') selOpt('c4z2',z2);
        renderC4();
      },250);
    },100);
  }
}

// ════════════════════════════════════════════════════════════
// CONTACTO
// ════════════════════════════════════════════════════════════
function enviarContacto(){
  const nombre  = document.getElementById('cNombre').value.trim();
  const email   = document.getElementById('cEmail').value.trim();
  const asunto  = document.getElementById('cAsunto').value.trim() || t('cont_asunto_def');
  const mensaje = document.getElementById('cMensaje').value.trim();
  if(!nombre||!email||!mensaje){
    alert(t('cont_alerta'));
    return;
  }
  const body = encodeURIComponent(`Nombre: ${nombre}\nEmail: ${email}\n\n${mensaje}`);
  const subj = encodeURIComponent(asunto);
  window.location.href = `mailto:euskopilotastats@gmail.com?subject=${subj}&body=${body}`;
  document.getElementById('contactOk').style.display='block';
}


// ════════════════════════════════════════════════════════════
// MAPA FRONTONES
// ════════════════════════════════════════════════════════════

let _frontonMap = null;
let _leaflet = null;

// Leaflet (vendor/leaflet) solo se descarga la primera vez que se abre el
// mapa: el resto de la web arranca sin esperar a sus 160 KB.
function cargarLeaflet(){
  if(window.L) return Promise.resolve();
  if(_leaflet) return _leaflet;
  _leaflet = new Promise((ok, ko) => {
    const css = document.createElement('link');
    css.rel = 'stylesheet';
    css.href = 'vendor/leaflet/leaflet.css';
    document.head.appendChild(css);
    const css2 = document.createElement('link');
    css2.rel = 'stylesheet';
    css2.href = 'vendor/leaflet/MarkerCluster.css';
    document.head.appendChild(css2);
    const script = (src, sig) => {
      const js = document.createElement('script');
      js.src = src;
      js.onload = sig;
      js.onerror = () => { _leaflet = null; ko(new Error(src)); };
      document.head.appendChild(js);
    };
    // Leaflet y después la agrupación de marcadores (necesita Leaflet)
    script('vendor/leaflet/leaflet.js', () => script('vendor/leaflet/leaflet.markercluster.js', ok));
  });
  return _leaflet;
}

function initFrontonMap(){
  if(!document.getElementById('frontonMap')) return;
  if(_frontonMap){ _frontonMap.invalidateSize(); return; }
  cargarLeaflet().then(crearFrontonMap).catch(() => {});
}

let _frontonCapa = null, _frontonEncuadre = '';

function crearFrontonMap(){
  if(_frontonMap) return;

  _frontonMap = L.map('frontonMap', {zoomControl:false, attributionControl:false}).setView([43.0, -2.0], 8);
  L.control.zoom({zoomInTitle: t('map_acercar'), zoomOutTitle: t('map_alejar')}).addTo(_frontonMap);
  // Mapa base: OpenStreetMap estándar (no pide clave; CARTO empezó a exigirla y
  // salía «API KEY REQUIRED»). En modo oscuro se oscurece con un filtro (CSS).
  L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
  }).addTo(_frontonMap);
  // Atribución (obligatoria por la licencia de OpenStreetMap) plegada en un ⓘ
  const Atribucion = L.Control.extend({
    options: {position: 'bottomright'},
    onAdd(){
      const div = L.DomUtil.create('div', 'fmap-atrib');
      div.innerHTML = `<button type="button" class="fmap-atrib-btn" aria-label="${t('map_creditos')}" aria-expanded="false">i</button>
        <span class="fmap-atrib-txt">© <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener">OpenStreetMap</a> · <a href="https://leafletjs.com" target="_blank" rel="noopener">Leaflet</a></span>`;
      const btn = div.querySelector('button');
      btn.onclick = () => btn.setAttribute('aria-expanded', div.classList.toggle('abierto'));
      L.DomEvent.disableClickPropagation(div);
      return div;
    }
  });
  new Atribucion().addTo(_frontonMap);

  // Frontones cercanos agrupados en un círculo con el número; se separan al acercar
  _frontonCapa = L.markerClusterGroup({
    showCoverageOnHover: false,
    maxClusterRadius: 45,
    spiderfyOnMaxZoom: true,
    zoomToBoundsOnClick: false,
    disableClusteringAtZoom: 15,
    iconCreateFunction: grupo => {
      const n = grupo.getChildCount();
      const tam = n < 10 ? 34 : n < 40 ? 42 : 50;
      return L.divIcon({html: `<span>${n}</span>`, className: 'fmap-cluster', iconSize: [tam, tam]});
    },
  }).addTo(_frontonMap);
  // Al pulsar un grupo: si sus frontones están muy juntos (el mismo pueblo)
  // o ya estamos cerca, se abren en abanico; si no, se acerca hasta ellos
  _frontonCapa.on('clusterclick', e => {
    const b = e.layer.getBounds();
    const zoomNuevo = Math.min(13, _frontonMap.getBoundsZoom(b, false, L.point(80, 80)));
    if(b.getNorthEast().distanceTo(b.getSouthWest()) < 2000 || zoomNuevo <= _frontonMap.getZoom()) e.layer.spiderfy();
    else _frontonMap.fitBounds(b, {padding: [40, 40], maxZoom: 13});
  });

  renderFrontonMarkers(true);
}

function renderFrontonMarkers(encuadrar){
  if(!_frontonMap || !_frontonCapa) return;
  _frontonCapa.clearLayers();

  const parts = PARTIDOS.filter(p =>
    tipoMatch(p.tipo, activeTipoFron) &&
    (activeYearFron==='todos' || getYear(p)===activeYearFron)
  );

  const stats = {};
  parts.forEach(p => {
    const f = p.fronton;
    if(!stats[f]) stats[f] = {count:0, ciudad: p.ciudad||''};
    stats[f].count++;
  });

  // Índice del catálogo de frontones por nombre (coordenadas y enlace de Google Maps)
  const FRO_BY_NAME = {};
  Object.values(CAT_FRONTONES).forEach(fr=>{
    if(fr && fr.nombre) FRO_BY_NAME[fr.nombre.toUpperCase().trim()] = fr;
  });

  const marcadores = [];
  Object.entries(stats).forEach(([f, s]) => {
    const info = FRO_BY_NAME[f.toUpperCase().trim()];
    if(!info || info.lat==null || info.lon==null) return;
    // Tamaño según los partidos jugados en él
    const tam = Math.round(12 + Math.min(14, Math.sqrt(s.count) * 1.6));
    const icon = L.divIcon({className: 'fmap-pin' + (s.count >= 20 ? ' grande' : ''), iconSize: [tam, tam]});
    // title: nombre accesible del marcador (lectores de pantalla y teclado)
    const marker = L.marker([info.lat, info.lon], {icon, title: `${f} · ${nPartidos(s.count)}`, alt: f});
    marker.bindTooltip(`<b>${f}</b> · ${nPartidos(s.count)}`, {direction: 'top', offset: [0, -tam/2], className: 'fmap-tip'});
    const mapsUrl = info?.google_maps_link
      || `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent('Fronton '+f)}`;
    marker.bindPopup(`
      <div class="fmap-name">${f}</div>
      <div class="fmap-count">${s.ciudad} · ${nPartidos(s.count)}</div>
      <div class="fmap-actions">
        <button class="fmap-btn" onclick="filtrarPorFronton('${esc(f)}');_frontonMap.closePopup()">${t('fmap_ver_partidos')}</button>
        <a href="${mapsUrl}" target="_blank" rel="noopener noreferrer" class="fmap-btn-ghost">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/><circle cx="12" cy="10" r="3"/></svg>
          <span>${t('fronton_como_llegar')}</span>
        </a>
      </div>
    `);
    marcadores.push(marker);
  });
  _frontonCapa.addLayers(marcadores);
  // Encuadra los frontones al abrir el mapa y cuando el filtro cambia cuáles se ven
  const clave = Object.keys(stats).sort().join('|');
  if(marcadores.length && (encuadrar === true || clave !== _frontonEncuadre)){
    // La zona de la gran mayoría: sin el 5 % más alejado por cada lado
    // (unos pocos frontones lejanos dejarían el mapa demasiado alejado)
    const lats = marcadores.map(m => m.getLatLng().lat).sort((a,b) => a-b);
    const lons = marcadores.map(m => m.getLatLng().lng).sort((a,b) => a-b);
    const q = (v, f) => v[Math.min(v.length-1, Math.max(0, Math.round(f*(v.length-1))))];
    const recorte = marcadores.length >= 20 ? 0.05 : 0;
    _frontonMap.fitBounds([[q(lats, recorte), q(lons, recorte)], [q(lats, 1-recorte), q(lons, 1-recorte)]],
      {padding: [24, 24], maxZoom: 11});
  }
  _frontonEncuadre = clave;
}


// ════════════════════════════════════════════════════════════
// INSTALAR LA APP (discreto: final del menú y pie de página)
// ════════════════════════════════════════════════════════════
// Dirección de la APK de Android (p. ej. la de una release de GitHub).
// Vacía: el enlace «App para Android» no se muestra.
const APK_URL = '';
let _promptInstalar = null;

// ¿Se está viendo dentro de la app instalada (PWA o APK)? Entonces no se
// ofrece instalarla. La APK (Trusted Web Activity) abre la web con el
// referrer android-app://; se recuerda para el resto de la visita.
function enLaApp(){
  try{
    if(document.referrer.startsWith('android-app://')) sessionStorage.setItem('eskupilota-app', '1');
    if(sessionStorage.getItem('eskupilota-app')) return true;
  }catch(e){}
  return (window.matchMedia && matchMedia('(display-mode: standalone)').matches) || navigator.standalone === true;
}
const esIOS = () => /iphone|ipad|ipod/i.test(navigator.userAgent) || (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);
const esAndroid = () => /android/i.test(navigator.userAgent);

function mostrarInstalar(){
  const app = enLaApp();
  document.querySelectorAll('.app-instalar').forEach(b => b.hidden = app || !(_promptInstalar || esIOS()));
  document.querySelectorAll('.app-apk').forEach(a => {
    a.hidden = app || !APK_URL || !esAndroid();
    if(APK_URL) a.href = APK_URL;
  });
}

async function instalarApp(){
  if(_promptInstalar){
    _promptInstalar.prompt();
    try{ await _promptInstalar.userChoice; }catch(e){}
    _promptInstalar = null;
    mostrarInstalar();
  } else if(esIOS()){
    alert(t('app_ios'));
  }
}

// Chrome/Edge avisan cuando la web se puede instalar
window.addEventListener('beforeinstallprompt', e => { e.preventDefault(); _promptInstalar = e; mostrarInstalar(); });
window.addEventListener('appinstalled', () => { _promptInstalar = null; mostrarInstalar(); });
document.addEventListener('DOMContentLoaded', mostrarInstalar);

