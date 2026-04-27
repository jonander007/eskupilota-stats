const CACHE = 'eskupilota-v6';

self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE).then(c => c.addAll([
      '/',
      '/index.html',
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
      '/apple-touch-icon.png',
      '/logo-header.png',
      '/manifest.json',
    ]))
  );
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

self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);

  // Datos JSON (catálogos y partidos): network first (intentar actualizar siempre)
  if (url.pathname.startsWith('/data/') && url.pathname.endsWith('.json')) {
    e.respondWith(
      fetch(e.request)
        .then(res => {
          const clone = res.clone();
          caches.open(CACHE).then(c => c.put(e.request, clone));
          return res;
        })
        .catch(() => caches.match(e.request))
    );
    return;
  }

  // Resto: cache first, network fallback
  e.respondWith(
    caches.match(e.request).then(cached => {
      if (cached) return cached;
      return fetch(e.request).then(res => {
        if(res && res.status === 200) {
          const clone = res.clone();
          caches.open(CACHE).then(c => c.put(e.request, clone));
        }
        return res;
      });
    })
  );
});
