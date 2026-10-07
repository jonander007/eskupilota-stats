const CACHE = 'eskupilota-v48';
const PREFS = 'eskupilota-prefs';   // pelotaris seguidos y avisos ya dados (los escribe la web)

// Todo lo necesario para que la web arranque sin conexión
const PRECACHE = [
  '/',
  '/index.html',
  '/css/app.css',
  '/js/app.js',
  '/js/analisis.js',
  '/js/porra.js',
  '/data/partidos.json',
  '/data/pelotaris.json',
  '/data/frontones.json',
  '/data/ciudades.json',
  '/data/competiciones.json',
  '/data/cartelera.json',
  '/favicon.ico',
  '/favicon-32.png',
  '/icon-192.png',
  '/icon-512.png',
  '/logo-header.png',
];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(PRECACHE)));
  self.skipWaiting();
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE && k !== PREFS).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

// Red primero y copia en caché; si no hay red, lo último guardado.
// La cartelera se pide con ?_=timestamp, así que se busca ignorando la query.
// cache: 'no-cache' obliga a revalidar con el servidor: si no, el navegador
// puede dar un app.js o app.css de hace unos minutos junto a un index.html nuevo
function networkFirst(request, fallbackUrl) {
  return fetch(request, { cache: 'no-cache' })
    .then(res => {
      if (res && res.ok) {
        const clone = res.clone();
        caches.open(CACHE).then(c => c.put(request, clone));
      }
      return res;
    })
    .catch(() =>
      caches.match(request, { ignoreSearch: true })
        .then(cached => cached || (fallbackUrl && caches.match(fallbackUrl)))
    );
}

self.addEventListener('fetch', e => {
  if (e.request.method !== 'GET') return;
  const url = new URL(e.request.url);
  // La porra (Supabase) siempre en directo: nunca de la caché
  if (url.hostname.endsWith('.supabase.co')) return;

  // Datos, HTML, CSS y JS propios: red primero, para que las actualizaciones lleguen rápido
  if (url.origin === self.location.origin &&
      (url.pathname.startsWith('/data/') || url.pathname.startsWith('/css/') ||
       url.pathname.startsWith('/js/') || url.pathname.endsWith('.html') ||
       url.pathname === '/')) {
    e.respondWith(networkFirst(e.request));
    return;
  }
  if (e.request.mode === 'navigate') {
    e.respondWith(networkFirst(e.request, '/index.html'));
    return;
  }

  // Resto (iconos, fuentes, Leaflet, etc.): caché primero, red como respaldo
  e.respondWith(
    caches.match(e.request).then(cached => {
      if (cached) return cached;
      return fetch(e.request).then(res => {
        if (res && res.status === 200) {
          const clone = res.clone();
          caches.open(CACHE).then(c => c.put(e.request, clone));
        }
        return res;
      });
    })
  );
});


// ── Avisos de pelotaris seguidos ──
// En Android, con la app instalada, el navegador despierta al service worker
// de vez en cuando (periodicsync) y se mira la cartelera sin abrir la web.
// Misma normalización y misma clave que js/analisis.js.
const normNombre = n => (n||'').normalize('NFD').replace(/[̀-ͯ]/g,'').toLowerCase().replace(/[^a-z0-9]/g,'');
const claveAviso = (ev, p) => `${ev.fecha}|${ev.fronton||''}|${(p.eq1||[]).join('-')}|${(p.eq2||[]).join('-')}`;

async function avisosEnSegundoPlano(){
  const prefs = await caches.open(PREFS);
  const leer = async k => { const r = await prefs.match('/__prefs/' + k); return r ? r.json() : []; };
  const seg = new Map((await leer('seguidos')).map(n => [normNombre(n), n]));
  if(!seg.size) return;
  const avisados = new Set(await leer('avisados'));
  const res = await fetch('/data/cartelera.json', {cache: 'no-cache'});
  if(!res.ok) return;
  const data = await res.json();
  for(const ev of data.partidos || []){
    for(const p of ev.partidos || []){
      const clave = claveAviso(ev, p);
      const suyos = [...new Set([...(p.eq1||[]), ...(p.eq2||[])].map(normNombre).filter(k => seg.has(k)).map(k => seg.get(k)))];
      if(!suyos.length || avisados.has(clave)) continue;
      await self.registration.showNotification(`${suyos.join(', ')} · ${ev.fecha}${ev.hora ? ' ' + ev.hora + 'h' : ''}`, {
        body: `${(p.eq1||[]).join(' / ')} vs ${(p.eq2||[]).join(' / ')} · ${ev.fronton||''}`,
        tag: clave, icon: '/icon-192.png', badge: '/favicon-32.png', data: {url: '/#/cartelera'},
      });
      avisados.add(clave);
    }
  }
  await prefs.put('/__prefs/avisados', new Response(JSON.stringify([...avisados].slice(-300))));
}

self.addEventListener('periodicsync', e => {
  if(e.tag === 'avisos-seguidos') e.waitUntil(avisosEnSegundoPlano());
});

// Al pulsar un aviso: abrir la cartelera (o traer al frente la pestaña abierta)
self.addEventListener('notificationclick', e => {
  e.notification.close();
  const url = (e.notification.data && e.notification.data.url) || '/';
  e.waitUntil(clients.matchAll({type: 'window', includeUncontrolled: true}).then(cs => {
    for(const c of cs){
      if('focus' in c){ if('navigate' in c) c.navigate(url).catch(() => {}); return c.focus(); }
    }
    return clients.openWindow(url);
  }));
});
