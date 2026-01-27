// Yad2 Monitor Dashboard JavaScript

const API_BASE = '/api';
let allApartments = [];
let searchTimeout = null;
let currentSearchQuery = '';

// XSS Protection: Escape HTML special characters
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

async function fetchData(endpoint) {
    try {
        const response = await fetch(`${API_BASE}${endpoint}`);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        return await response.json();
    } catch (error) {
        console.error('Error fetching data:', error);
        showToast('שגיאה בטעינת נתונים מהשרת', 'error', 3000);
        return null;
    }
}

async function loadStats() {
    const data = await fetchData('/stats');
    if (data) {
        document.getElementById('total-apartments').textContent = data.total_listings || 0;
        document.getElementById('avg-price').textContent =
            data.avg_price ? `₪${data.avg_price.toLocaleString()}` : '-';
        document.getElementById('new-today').textContent = data.new_this_week || 0;
        document.getElementById('price-changes').textContent = data.price_changes_this_week || 0;
    }
}

async function loadApartments() {
    const data = await fetchData('/apartments');
    if (data && data.apartments) {
        allApartments = data.apartments;
        populateAutocomplete();
        filterApartments();
    }
}

function renderApartments(apartments) {
    const list = document.getElementById('apartment-list');
    if (!apartments.length) {
        list.innerHTML = '<li class="empty-state">אין דירות להצגה</li>';
        return;
    }
    list.innerHTML = apartments.map(apt => `
        <li class="apartment-item">
            <div>
                <div class="apartment-title">${escapeHtml(apt.title) || 'ללא כותרת'}</div>
                <div class="apartment-location">${escapeHtml(apt.street_address || apt.location)}</div>
            </div>
            <div>
                <span class="apartment-price">₪${(apt.price || 0).toLocaleString()}</span>
                <a href="${escapeHtml(apt.link)}" target="_blank" class="btn">צפייה</a>
                <button class="btn btn-fav" onclick="toggleFavorite('${escapeHtml(apt.id)}')">⭐</button>
            </div>
        </li>
    `).join('');
}

function filterApartments() {
    const minPrice = parseInt(document.getElementById('min-price').value) || 0;
    const maxPrice = parseInt(document.getElementById('max-price').value) || Infinity;
    const minRooms = parseFloat(document.getElementById('min-rooms').value) || 0;
    const maxRooms = parseFloat(document.getElementById('max-rooms').value) || Infinity;
    const minSqm = parseInt(document.getElementById('min-sqm').value) || 0;
    const maxSqm = parseInt(document.getElementById('max-sqm').value) || Infinity;
    const cityFilter = document.getElementById('city-filter').value.trim().toLowerCase();
    const neighborhoodFilter = document.getElementById('neighborhood-filter').value.trim().toLowerCase();
    const sortBy = document.getElementById('sort-by').value;

    let filtered = allApartments.filter(apt => {
        // Price filter
        if (apt.price < minPrice || apt.price > maxPrice) return false;

        // Rooms filter
        const rooms = apt.rooms || 0;
        if (rooms < minRooms || rooms > maxRooms) return false;

        // Square meters filter
        const sqm = apt.sqm || 0;
        if (sqm > 0 && (sqm < minSqm || sqm > maxSqm)) return false;

        // City filter
        if (cityFilter && (!apt.city || !apt.city.toLowerCase().includes(cityFilter))) {
            return false;
        }

        // Neighborhood filter
        if (neighborhoodFilter && (!apt.neighborhood || !apt.neighborhood.toLowerCase().includes(neighborhoodFilter))) {
            return false;
        }

        // Search query filter
        if (currentSearchQuery) {
            const query = currentSearchQuery.toLowerCase();
            const searchableText = [
                apt.title,
                apt.street_address,
                apt.location,
                apt.city,
                apt.neighborhood
            ].filter(Boolean).join(' ').toLowerCase();

            if (!searchableText.includes(query)) return false;
        }

        return true;
    });

    // Apply sorting
    if (sortBy === 'price-asc') {
        filtered.sort((a, b) => (a.price || 0) - (b.price || 0));
    } else if (sortBy === 'price-desc') {
        filtered.sort((a, b) => (b.price || 0) - (a.price || 0));
    } else if (sortBy === 'rooms-asc') {
        filtered.sort((a, b) => (a.rooms || 0) - (b.rooms || 0));
    } else if (sortBy === 'rooms-desc') {
        filtered.sort((a, b) => (b.rooms || 0) - (a.rooms || 0));
    } else if (sortBy === 'sqm-desc') {
        filtered.sort((a, b) => (b.sqm || 0) - (a.sqm || 0));
    } else {
        // Default: sort by date (newest first)
        filtered.sort((a, b) => {
            const dateA = new Date(a.first_seen || 0);
            const dateB = new Date(b.first_seen || 0);
            return dateB - dateA;
        });
    }

    renderApartments(filtered);
    updateResultsCount(filtered.length);
}

async function loadFavorites() {
    const data = await fetchData('/favorites');
    const list = document.getElementById('favorites-list');
    if (data && data.favorites && data.favorites.length) {
        list.innerHTML = data.favorites.map(apt => `
            <li class="apartment-item">
                <div>
                    <div class="apartment-title">${escapeHtml(apt.title) || 'ללא כותרת'}</div>
                    <div class="apartment-location">${escapeHtml(apt.street_address)}</div>
                </div>
                <div>
                    <span class="apartment-price">₪${(apt.price || 0).toLocaleString()}</span>
                    <a href="${escapeHtml(apt.link)}" target="_blank" class="btn">צפייה</a>
                </div>
            </li>
        `).join('');
    } else {
        list.innerHTML = '<li class="empty-state">אין מועדפים</li>';
    }
}

async function loadPriceDrops() {
    const data = await fetchData('/price-drops');
    const list = document.getElementById('price-drops-list');
    if (data && data.drops && data.drops.length) {
        list.innerHTML = data.drops.map(item => `
            <li class="apartment-item">
                <div>
                    <div class="apartment-title">${escapeHtml(item.title) || 'ללא כותרת'}</div>
                    <div class="price-change-down">
                        ₪${item.old_price.toLocaleString()} → ₪${item.new_price.toLocaleString()}
                        (${item.drop_pct}%-)
                    </div>
                </div>
                <div>
                    <a href="${escapeHtml(item.link)}" target="_blank" class="btn">צפייה</a>
                </div>
            </li>
        `).join('');
    } else {
        list.innerHTML = '<li class="empty-state">אין ירידות מחיר אחרונות</li>';
    }
}

async function loadAnalytics() {
    const data = await fetchData('/analytics');
    const content = document.getElementById('analytics-content');
    if (data) {
        let html = '<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px;">';

        if (data.overall) {
            html += `
                <div class="card" style="margin: 0;">
                    <h3>סיכום כללי</h3>
                    <p>סה"כ דירות: ${data.overall.total_listings}</p>
                    <p>מחיר ממוצע: ₪${data.overall.avg_price?.toLocaleString() || '-'}</p>
                    <p>מ"ר ממוצע: ${data.overall.avg_sqm || '-'}</p>
                </div>
            `;
        }

        if (data.price_distribution) {
            html += `
                <div class="card" style="margin: 0;">
                    <h3>התפלגות מחירים</h3>
                    ${data.price_distribution.map(d =>
                        `<p>${d.range}: ${d.count} דירות</p>`
                    ).join('')}
                </div>
            `;
        }

        if (data.top_neighborhoods) {
            html += `
                <div class="card" style="margin: 0;">
                    <h3>שכונות מובילות</h3>
                    ${data.top_neighborhoods.slice(0, 5).map(n =>
                        `<p>${n.name}: ${n.count} דירות</p>`
                    ).join('')}
                </div>
            `;
        }

        html += '</div>';
        content.innerHTML = html;

        // Render Chart.js visualizations
        if (typeof renderAllCharts === 'function') {
            renderAllCharts(data);
        }
    }
}

async function toggleFavorite(aptId) {
    try {
        const response = await fetch(`${API_BASE}/favorites/${aptId}`, { method: 'POST' });
        if (response.ok) {
            showToast('נוסף למועדפים!', 'success', 2000);
            loadFavorites();
        } else {
            showToast('שגיאה בהוספה למועדפים', 'error');
        }
    } catch (error) {
        console.error('Error toggling favorite:', error);
        showToast('שגיאה בתקשורת עם השרת', 'error');
    }
}

function showTab(tabName) {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('[id$="-tab"]').forEach(t => t.classList.add('hidden'));

    event.target.classList.add('active');
    document.getElementById(`${tabName}-tab`).classList.remove('hidden');

    if (tabName === 'favorites') loadFavorites();
    else if (tabName === 'price-drops') loadPriceDrops();
    else if (tabName === 'analytics') loadAnalytics();
}

// Search with debouncing
function handleSearch() {
    const searchInput = document.getElementById('search-input');
    const clearBtn = document.getElementById('clear-search');

    currentSearchQuery = searchInput.value.trim();

    // Show/hide clear button
    clearBtn.style.display = currentSearchQuery ? 'flex' : 'none';

    // Debounce search
    clearTimeout(searchTimeout);
    searchTimeout = setTimeout(() => {
        filterApartments();
    }, 300);
}

function clearSearch() {
    document.getElementById('search-input').value = '';
    currentSearchQuery = '';
    document.getElementById('clear-search').style.display = 'none';
    filterApartments();
}

// Update results count
function updateResultsCount(count) {
    const resultsCount = document.getElementById('results-count');
    if (resultsCount) {
        resultsCount.textContent = `${count} דירות`;
    }
}

// Populate autocomplete lists for cities and neighborhoods
function populateAutocomplete() {
    if (!allApartments.length) return;

    const cities = new Set();
    const neighborhoods = new Set();

    allApartments.forEach(apt => {
        if (apt.city) cities.add(apt.city);
        if (apt.neighborhood) neighborhoods.add(apt.neighborhood);
    });

    // Populate cities datalist
    const citiesList = document.getElementById('cities-list');
    citiesList.innerHTML = Array.from(cities).sort()
        .map(city => `<option value="${escapeHtml(city)}">`)
        .join('');

    // Populate neighborhoods datalist
    const neighborhoodsList = document.getElementById('neighborhoods-list');
    neighborhoodsList.innerHTML = Array.from(neighborhoods).sort()
        .map(neighborhood => `<option value="${escapeHtml(neighborhood)}">`)
        .join('');
}

// Save filters to localStorage
function saveFilters() {
    const filters = {
        minPrice: document.getElementById('min-price').value,
        maxPrice: document.getElementById('max-price').value,
        minRooms: document.getElementById('min-rooms').value,
        maxRooms: document.getElementById('max-rooms').value,
        minSqm: document.getElementById('min-sqm').value,
        maxSqm: document.getElementById('max-sqm').value,
        city: document.getElementById('city-filter').value,
        neighborhood: document.getElementById('neighborhood-filter').value,
        sortBy: document.getElementById('sort-by').value
    };

    localStorage.setItem('yad2_filters', JSON.stringify(filters));
    showToast('הפילטר נשמר בהצלחה!', 'success', 2000);
}

// Load filters from localStorage
function loadFilters() {
    const saved = localStorage.getItem('yad2_filters');
    if (!saved) {
        showToast('לא נמצא פילטר שמור', 'warning', 2000);
        return;
    }

    try {
        const filters = JSON.parse(saved);
        document.getElementById('min-price').value = filters.minPrice || '';
        document.getElementById('max-price').value = filters.maxPrice || '';
        document.getElementById('min-rooms').value = filters.minRooms || '';
        document.getElementById('max-rooms').value = filters.maxRooms || '';
        document.getElementById('min-sqm').value = filters.minSqm || '';
        document.getElementById('max-sqm').value = filters.maxSqm || '';
        document.getElementById('city-filter').value = filters.city || '';
        document.getElementById('neighborhood-filter').value = filters.neighborhood || '';
        document.getElementById('sort-by').value = filters.sortBy || 'date';

        filterApartments();
        showToast('הפילטר נטען בהצלחה!', 'success', 2000);
    } catch (e) {
        console.error('Error loading filters:', e);
        showToast('שגיאה בטעינת הפילטר', 'error');
    }
}

// Clear all filters
function clearFilters() {
    document.getElementById('min-price').value = '';
    document.getElementById('max-price').value = '';
    document.getElementById('min-rooms').value = '';
    document.getElementById('max-rooms').value = '';
    document.getElementById('min-sqm').value = '';
    document.getElementById('max-sqm').value = '';
    document.getElementById('city-filter').value = '';
    document.getElementById('neighborhood-filter').value = '';
    document.getElementById('sort-by').value = 'date';
    document.getElementById('search-input').value = '';
    currentSearchQuery = '';
    document.getElementById('clear-search').style.display = 'none';

    filterApartments();
    showToast('הפילטרים נוקו', 'info', 2000);
}

// Initial load
loadStats();
loadApartments();

// Try to load saved filters on startup
setTimeout(() => {
    const saved = localStorage.getItem('yad2_filters');
    if (saved) {
        loadFilters();
    }
}, 500);

// Refresh every 5 minutes
setInterval(() => {
    loadStats();
    loadApartments();
}, 300000);
