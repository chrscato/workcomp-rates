const loadingMessages = [
    "Accessing MRF negotiated rates database...",
    "Comparing against Workers' Comp fee schedules...",
    "Calculating Medicare benchmark differentials...",
    "Identifying steerage opportunities...",
    "Building transparency intelligence report..."
];

function showBusinessLoading() {
    let messageIndex = 0;
    const loadingEl = document.getElementById('loading-message');
    
    if (!loadingEl) return;
    
    const interval = setInterval(() => {
        if (messageIndex < loadingMessages.length) {
            loadingEl.textContent = loadingMessages[messageIndex];
            messageIndex++;
        } else {
            clearInterval(interval);
        }
    }, 1500);
}

// Auto-show loading on page load if loading element exists
document.addEventListener('DOMContentLoaded', function() {
    const loadingElement = document.getElementById('loading-message');
    if (loadingElement) {
        showBusinessLoading();
    }
});
