// Chart.js Visualizations for Yad2 Monitor Dashboard

let priceDistributionChart = null;
let neighborhoodsChart = null;
let marketTrendsChart = null;

// Chart.js default configuration for RTL
Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
Chart.defaults.plugins.legend.rtl = true;

/**
 * Create or update price distribution histogram
 */
function renderPriceDistributionChart(data) {
    const ctx = document.getElementById('price-distribution-chart');
    if (!ctx || !data || !data.price_distribution) return;

    const labels = data.price_distribution.map(d => d.range);
    const values = data.price_distribution.map(d => d.count);

    if (priceDistributionChart) {
        priceDistributionChart.destroy();
    }

    priceDistributionChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'מספר דירות',
                data: values,
                backgroundColor: 'rgba(102, 126, 234, 0.6)',
                borderColor: 'rgba(102, 126, 234, 1)',
                borderWidth: 2,
                borderRadius: 8
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            aspectRatio: 2,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    rtl: true,
                    textDirection: 'rtl',
                    callbacks: {
                        label: function(context) {
                            return `${context.parsed.y} דירות`;
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        precision: 0
                    },
                    title: {
                        display: true,
                        text: 'מספר דירות'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'טווח מחירים (₪)'
                    }
                }
            }
        }
    });
}

/**
 * Create or update top neighborhoods bar chart
 */
function renderNeighborhoodsChart(data) {
    const ctx = document.getElementById('neighborhoods-chart');
    if (!ctx || !data || !data.top_neighborhoods) return;

    const neighborhoods = data.top_neighborhoods.slice(0, 8); // Top 8
    const labels = neighborhoods.map(n => n.name);
    const values = neighborhoods.map(n => n.count);

    if (neighborhoodsChart) {
        neighborhoodsChart.destroy();
    }

    neighborhoodsChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'מספר דירות',
                data: values,
                backgroundColor: 'rgba(118, 75, 162, 0.6)',
                borderColor: 'rgba(118, 75, 162, 1)',
                borderWidth: 2,
                borderRadius: 8
            }]
        },
        options: {
            indexAxis: 'y', // Horizontal bars for better label readability
            responsive: true,
            maintainAspectRatio: true,
            aspectRatio: 1.5,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    rtl: true,
                    textDirection: 'rtl',
                    callbacks: {
                        label: function(context) {
                            return `${context.parsed.x} דירות`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    beginAtZero: true,
                    ticks: {
                        precision: 0
                    },
                    title: {
                        display: true,
                        text: 'מספר דירות'
                    }
                }
            }
        }
    });
}

/**
 * Create or update market trends line chart
 * Shows new apartments and price changes over the last 7 days
 */
async function renderMarketTrendsChart() {
    const ctx = document.getElementById('market-trends-chart');
    if (!ctx) return;

    // Fetch 7-day trend data from the API
    try {
        const response = await fetch('/api/trends?type=daily&days=7');
        const data = await response.json();

        if (!data || !data.daily_stats) {
            console.log('No trend data available');
            return;
        }

        const labels = data.daily_stats.map(d => {
            const date = new Date(d.date);
            return date.toLocaleDateString('he-IL', { month: 'short', day: 'numeric' });
        });
        const newApartments = data.daily_stats.map(d => d.new_count || 0);
        const priceChanges = data.daily_stats.map(d => d.price_changes || 0);

        if (marketTrendsChart) {
            marketTrendsChart.destroy();
        }

        marketTrendsChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'דירות חדשות',
                        data: newApartments,
                        borderColor: 'rgba(40, 167, 69, 1)',
                        backgroundColor: 'rgba(40, 167, 69, 0.1)',
                        borderWidth: 3,
                        tension: 0.3,
                        fill: true
                    },
                    {
                        label: 'שינויי מחיר',
                        data: priceChanges,
                        borderColor: 'rgba(255, 193, 7, 1)',
                        backgroundColor: 'rgba(255, 193, 7, 0.1)',
                        borderWidth: 3,
                        tension: 0.3,
                        fill: true
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                aspectRatio: 2,
                plugins: {
                    legend: {
                        display: true,
                        position: 'top',
                        rtl: true
                    },
                    tooltip: {
                        rtl: true,
                        textDirection: 'rtl',
                        mode: 'index',
                        intersect: false
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            precision: 0
                        },
                        title: {
                            display: true,
                            text: 'מספר'
                        }
                    },
                    x: {
                        title: {
                            display: true,
                            text: 'תאריך'
                        }
                    }
                }
            }
        });
    } catch (error) {
        console.error('Error loading market trends:', error);
        // Fallback: render empty chart with message
        if (ctx) {
            ctx.parentElement.innerHTML = '<p style="text-align: center; color: #666;">אין מספיק נתונים להצגת מגמות</p>';
        }
    }
}

/**
 * Render all charts with analytics data
 */
function renderAllCharts(analyticsData) {
    if (!analyticsData) return;

    renderPriceDistributionChart(analyticsData);
    renderNeighborhoodsChart(analyticsData);
    renderMarketTrendsChart();
}

/**
 * Destroy all charts (cleanup)
 */
function destroyAllCharts() {
    if (priceDistributionChart) {
        priceDistributionChart.destroy();
        priceDistributionChart = null;
    }
    if (neighborhoodsChart) {
        neighborhoodsChart.destroy();
        neighborhoodsChart = null;
    }
    if (marketTrendsChart) {
        marketTrendsChart.destroy();
        marketTrendsChart = null;
    }
}
