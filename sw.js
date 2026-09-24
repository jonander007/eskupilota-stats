const CACHE = 'eskupilota-v6';

// Todo lo necesario para que la web arranque sin conexión
const PRECACHE = [
  '/',
  '/index.html',
  '/css/app.css',
  '/js/app.js',
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
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

// Red primero y copia en caché; si no hay red, lo último guardado.
// La cartelera se pide con ?_=timestamp, así que se busca ignorando la query.
function networkFirst(request, fallbackUrl) {
  return fetch(request)
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
