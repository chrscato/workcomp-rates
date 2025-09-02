// Service Worker for preloading and caching insights data
const CACHE_NAME = 'workcomp-insights-v1';
const STATIC_CACHE = 'workcomp-static-v1';

// Files to cache immediately
const STATIC_FILES = [
    '/static/css/shared_filters.css',
    'https://cdn.plot.ly/plotly-2.24.1.min.js'
];

// Install event - cache static files
self.addEventListener('install', event => {
    event.waitUntil(
        caches.open(STATIC_CACHE)
            .then(cache => {
                console.log('Caching static files');
                return cache.addAll(STATIC_FILES);
            })
    );
});

// Fetch event - serve from cache when possible
self.addEventListener('fetch', event => {
    const url = new URL(event.request.url);
    
    // Handle insights page requests
    if (url.pathname.includes('/commercial/insights/') && url.pathname.endsWith('/')) {
        event.respondWith(
            caches.open(CACHE_NAME)
                .then(cache => {
                    return cache.match(event.request)
                        .then(response => {
                            if (response) {
                                console.log('Serving insights page from cache');
                                return response;
                            }
                            
                            // If not in cache, fetch and cache it
                            return fetch(event.request)
                                .then(fetchResponse => {
                                    if (fetchResponse.status === 200) {
                                        cache.put(event.request, fetchResponse.clone());
                                    }
                                    return fetchResponse;
                                });
                        });
                })
        );
        return;
    }
    
    // Handle API requests for preloading
    if (url.pathname.includes('/api/')) {
        event.respondWith(
            caches.open(CACHE_NAME)
                .then(cache => {
                    return cache.match(event.request)
                        .then(response => {
                            if (response) {
                                console.log('Serving API response from cache');
                                return response;
                            }
                            
                            // If not in cache, fetch and cache it
                            return fetch(event.request)
                                .then(fetchResponse => {
                                    if (fetchResponse.status === 200) {
                                        cache.put(event.request, fetchResponse.clone());
                                    }
                                    return fetchResponse;
                                });
                        });
                })
        );
        return;
    }
    
    // Handle static file requests
    if (STATIC_FILES.some(file => url.href.includes(file))) {
        event.respondWith(
            caches.open(STATIC_CACHE)
                .then(cache => {
                    return cache.match(event.request)
                        .then(response => {
                            if (response) {
                                return response;
                            }
                            return fetch(event.request);
                        });
                })
        );
        return;
    }
    
    // For all other requests, use network first
    event.respondWith(fetch(event.request));
});

// Background sync for preloading
self.addEventListener('sync', event => {
    if (event.tag === 'preload-insights') {
        event.waitUntil(preloadInsightsData());
    }
});

async function preloadInsightsData() {
    try {
        // Get all cached insights pages
        const cache = await caches.open(CACHE_NAME);
        const requests = await cache.keys();
        
        // Preload insights pages in background
        for (const request of requests) {
            if (request.url.includes('/commercial/insights/') && request.url.endsWith('/')) {
                await fetch(request.url, { method: 'GET' });
            }
        }
        
        console.log('Background preloading completed');
    } catch (error) {
        console.error('Background preloading failed:', error);
    }
}

// Clean up old caches
self.addEventListener('activate', event => {
    event.waitUntil(
        caches.keys().then(cacheNames => {
            return Promise.all(
                cacheNames.map(cacheName => {
                    if (cacheName !== CACHE_NAME && cacheName !== STATIC_CACHE) {
                        console.log('Deleting old cache:', cacheName);
                        return caches.delete(cacheName);
                    }
                })
            );
        })
    );
});
