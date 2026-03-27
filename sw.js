const CACHE = 'eskupilota-v1';
const PRECACHE = [
  '/',
  '/index.html',
  '/data/partidos.json',
  '/favicon.ico',
  '/icon-192.png',
  '/icon-512.png',
];

// Instalar y cachear recursos estáticos
self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE).then(c => c.addAll(PRECACHE))
  );
  self.skipWaiting();
});

// Activar y limpiar caches antiguas
self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

// Estrategia: Network first, cache fallback
// Para partidos.json y cartelera.json siempre intenta red primero
self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);

  // Datos JSON: network first (siempre intentar actualizar)
  if (url.pathname.includes('/data/')) {
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
        const clone = res.clone();
        caches.open(CACHE).then(c => c.put(e.request, clone));
        return res;
      });
    })
  );
});
