// Toast Notification System

/**
 * Show a toast notification
 * @param {string} message - The message to display
 * @param {string} type - Type of toast: 'success', 'error', 'warning', 'info'
 * @param {number} duration - Duration in milliseconds (default: 3000)
 */
function showToast(message, type = 'info', duration = 3000) {
    const container = document.getElementById('toast-container');
    if (!container) return;

    const toast = document.createElement('div');
    toast.className = `toast ${type}`;

    // Choose icon based on type
    const icons = {
        success: '✓',
        error: '✕',
        warning: '⚠',
        info: 'ℹ'
    };

    toast.innerHTML = `
        <span class="toast-icon">${icons[type] || icons.info}</span>
        <span class="toast-message">${message}</span>
        <button class="toast-close" onclick="this.parentElement.remove()">×</button>
    `;

    container.appendChild(toast);

    // Auto-remove after duration
    setTimeout(() => {
        if (toast.parentElement) {
            toast.style.opacity = '0';
            toast.style.transform = 'translateY(-20px)';
            setTimeout(() => toast.remove(), 300);
        }
    }, duration);
}

// Theme Toggle Function
function toggleTheme() {
    const html = document.documentElement;
    const currentTheme = html.getAttribute('data-theme');
    const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
    const themeIcon = document.getElementById('theme-toggle');

    html.setAttribute('data-theme', newTheme);
    localStorage.setItem('theme', newTheme);

    // Update icon
    themeIcon.textContent = newTheme === 'dark' ? '☀️' : '🌙';

    // Show toast
    showToast(
        newTheme === 'dark' ? 'מצב לילה הופעל' : 'מצב יום הופעל',
        'success',
        2000
    );
}

// Load saved theme on page load
(function() {
    const savedTheme = localStorage.getItem('theme') || 'light';
    const html = document.documentElement;
    const themeIcon = document.getElementById('theme-toggle');

    html.setAttribute('data-theme', savedTheme);
    if (themeIcon) {
        themeIcon.textContent = savedTheme === 'dark' ? '☀️' : '🌙';
    }
})();
