// PowerBI-Style Client-Side Data Manipulation
// This approach loads data once and handles all filtering/analysis on the client
class ClientSideDatasetManager {
    constructor() {
        this.dataset = [];
        this.filteredData = [];
        this.currentFilters = {};
        this.isLoading = false;
        this.debounceTimeout = null;
        this.init();
    }
    
    init() {
        this.bindEvents();
        this.loadDataset();
    }
    
    bindEvents() {
        // Filter form submission
        const filterForm = document.getElementById('additionalFiltersForm');
        if (filterForm) {
            filterForm.addEventListener('submit', (e) => {
                e.preventDefault();
                this.applyFilters();
            });
        }
        
        // Clear filter buttons
        const clearAllBtn = document.getElementById('clearAllFilters');
        if (clearAllBtn) {
            clearAllBtn.addEventListener('click', (e) => {
                e.preventDefault();
                this.clearAllFilters();
            });
        }
        
        // Real-time filter inputs with debouncing
        const filterInputs = document.querySelectorAll('#additionalFiltersForm input[name]');
        filterInputs.forEach(input => {
            input.addEventListener('input', () => {
                // Debounce the filtering for better performance
                clearTimeout(this.debounceTimeout);
                this.debounceTimeout = setTimeout(() => {
                    this.applyFilters();
                }, 300);
            });
        });
        
        // Clear filter buttons
        document.querySelectorAll('[data-clear-filter]').forEach(button => {
            button.addEventListener('click', (e) => {
                const filterName = e.target.getAttribute('data-clear-filter');
                this.clearFilter(filterName);
            });
        });
        
        // Handle modal events for proper focus management
        const modal = document.getElementById('addFiltersModal');
        if (modal) {
            modal.addEventListener('shown.bs.modal', () => {
                // Focus on first input when modal opens
                const firstInput = modal.querySelector('input[type="text"]');
                if (firstInput) {
                    firstInput.focus();
                }
            });
            
            modal.addEventListener('hidden.bs.modal', () => {
                // Ensure focus is removed when modal is hidden
                const focusedElement = document.activeElement;
                if (focusedElement && modal.contains(focusedElement)) {
                    focusedElement.blur();
                }
            });
        }
    }
    
    async loadDataset() {
        if (this.isLoading) return;
        
        this.isLoading = true;
        this.showLoadingState('Loading dataset...');
        
        // Set up timeout for large datasets (60 seconds)
        const timeoutId = setTimeout(() => {
            this.showTimeoutWarning();
        }, 60000);
        
        try {
            // Pass current URL parameters to the data endpoint
            const currentParams = new URLSearchParams(window.location.search);
            const dataUrl = `${window.location.pathname}data/?${currentParams.toString()}`;
            
            const response = await fetch(dataUrl);
            const result = await response.json();
            
            // Clear timeout if request completes successfully
            clearTimeout(timeoutId);
            
            if (result.has_data) {
                this.dataset = result.data;
                this.filteredData = [...this.dataset];
                
                // Update filter options
                this.updateFilterOptions(result.filter_options);
                
                // Debug: Show sample data
                console.log('Sample data (first 3 records):', this.dataset.slice(0, 3));
                console.log('Available columns:', Object.keys(this.dataset[0] || {}));
                
                // Debug: Show unique values for key columns
                const keyColumns = ['code', 'county_name', 'primary_taxonomy_desc', 'stat_area_name'];
                keyColumns.forEach(col => {
                    const uniqueValues = [...new Set(this.dataset.map(r => r[col]).filter(v => v && v !== "__NULL__" && v !== "NaN"))].slice(0, 10);
                    console.log(`Unique values for ${col}:`, uniqueValues);
                });
                
                // Update filter options
                this.updateFilterOptions(result.filter_options);
                
                // Hide loading state and show analysis section
                this.hideAnalysisLoading();
                
                // Generate initial analysis
                this.updateAnalysis();
                
                this.showNotification(`Dataset loaded: ${result.metadata.total_rows.toLocaleString()} records`, 'success');
            } else {
                this.showNotification(result.error || 'Failed to load dataset', 'error');
            }
        } catch (error) {
            console.error('Error loading dataset:', error);
            clearTimeout(timeoutId);
            this.showNotification('Error loading dataset. Please try again.', 'error');
        } finally {
            this.isLoading = false;
            this.hideLoadingState();
        }
    }
    
    updateFilterOptions(filterOptions) {
        Object.entries(filterOptions).forEach(([column, options]) => {
            const input = document.getElementById(column);
            if (input) {
                // Handle both array and string formats
                let optionsArray;
                if (Array.isArray(options)) {
                    optionsArray = options;
                } else if (typeof options === 'string') {
                    try {
                        optionsArray = JSON.parse(options);
                    } catch (e) {
                        console.warn(`Failed to parse options for ${column}:`, e);
                        optionsArray = [];
                    }
                } else {
                    optionsArray = [];
                }
                
                // Update autocomplete options
                input.setAttribute('data-options', JSON.stringify(optionsArray));
                
                // Create datalist for autocomplete
                let datalist = input.nextElementSibling;
                if (!datalist || datalist.tagName !== 'DATALIST') {
                    datalist = document.createElement('datalist');
                    datalist.id = `${column}_options`;
                    input.setAttribute('list', datalist.id);
                    input.parentNode.appendChild(datalist);
                }
                
                // Update datalist options
                datalist.innerHTML = '';
                optionsArray.slice(0, 50).forEach(option => {
                    const optionElement = document.createElement('option');
                    optionElement.value = option;
                    datalist.appendChild(optionElement);
                });
            }
        });
    }
    
    applyFilters() {
        console.log('applyFilters called');
        const form = document.getElementById('additionalFiltersForm');
        const formData = new FormData(form);
        
        // Get current filter values
        this.currentFilters = {};
        for (const [key, value] of formData.entries()) {
            if (value.trim()) {
                if (this.currentFilters[key]) {
                    if (!Array.isArray(this.currentFilters[key])) {
                        this.currentFilters[key] = [this.currentFilters[key]];
                    }
                    this.currentFilters[key].push(value);
                } else {
                    this.currentFilters[key] = value;
                }
            }
        }
        
        console.log('Current filters:', this.currentFilters);
        
        // Apply filters to dataset
        this.filteredData = this.dataset.filter(record => {
            return Object.entries(this.currentFilters).every(([column, filterValue]) => {
                const recordValue = record[column];
                
                // Skip if filter value is empty
                if (!filterValue || filterValue.toString().trim() === '') {
                    return true;
                }
                
                // Handle special cases for data quality issues
                if (recordValue === null || recordValue === undefined || 
                    recordValue === "__NULL__" || recordValue === "NaN" || 
                    recordValue === "" || recordValue === "null") {
                    return false;
                }
                
                if (Array.isArray(filterValue)) {
                    return filterValue.some(fv => {
                        const recordStr = recordValue.toString().toLowerCase().trim();
                        const filterStr = fv.toString().toLowerCase().trim();
                        return recordStr.includes(filterStr) || recordStr === filterStr;
                    });
                } else {
                    // Case-insensitive partial matching - more flexible
                    const recordStr = recordValue.toString().toLowerCase().trim();
                    const filterStr = filterValue.toString().toLowerCase().trim();
                    
                    // Try exact match first, then partial match
                    return recordStr === filterStr || recordStr.includes(filterStr);
                }
            });
        });
        
        // Debug: Log filtering results
        console.log(`Filtered ${this.filteredData.length} of ${this.dataset.length} records`);
        
        // Update analysis with filtered data
        this.updateAnalysis();
        
        // Close modal if open (with proper focus management)
        this.closeModal();
        
        this.showNotification(`Applied filters: ${this.filteredData.length.toLocaleString()} of ${this.dataset.length.toLocaleString()} records`, 'info');
    }
    
    updateAnalysis() {
        console.log('updateAnalysis called');
        
        // Update summary statistics
        this.updateSummaryStats();
        
        // Update key metrics
        this.updateKeyMetrics();
        
        // Update financial metrics
        this.updateFinancialMetrics();
        
        // Update geographic metrics
        this.updateGeographicMetrics();
        
        // Update sample data
        this.updateSampleData();
        
        // Update header metrics
        this.updateHeaderMetrics();
        
        // Skip updating applied filters display - keep original server-rendered design
        // this.updateAppliedFiltersDisplay();
        
        console.log('updateAnalysis completed');
    }
    
    updateSummaryStats() {
        const summaryDiv = document.querySelector('#summary .card-body');
        if (summaryDiv) {
            const originalCount = this.dataset.length;
            const filteredCount = this.filteredData.length;
            const filteredOut = originalCount - filteredCount;
            const retentionRate = originalCount > 0 ? ((filteredCount / originalCount) * 100).toFixed(1) : 0;
            
            summaryDiv.innerHTML = `
                <div class="row">
                    <div class="col-md-3">
                        <div class="card text-center">
                            <div class="card-body">
                                <h5 class="card-title text-primary">${filteredCount.toLocaleString()}</h5>
                                <p class="card-text">Filtered Rows</p>
                            </div>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="card text-center">
                            <div class="card-body">
                                <h5 class="card-title text-info">${originalCount.toLocaleString()}</h5>
                                <p class="card-text">Original Rows</p>
                            </div>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="card text-center">
                            <div class="card-body">
                                <h5 class="card-title text-warning">${filteredOut.toLocaleString()}</h5>
                                <p class="card-text">Rows Filtered Out</p>
                            </div>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="card text-center">
                            <div class="card-body">
                                <h5 class="card-title text-success">${retentionRate}%</h5>
                                <p class="card-text">Data Retained</p>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        }
    }
    
    updateKeyMetrics() {
        console.log('updateKeyMetrics called with filteredData length:', this.filteredData.length);
        
        // Find the key metrics section - it could be in different places
        let keyMetricsDiv = document.querySelector('#key-metrics .card-body');
        
        // If not found, try to find the key-metrics-container from the template
        if (!keyMetricsDiv) {
            const keyMetricsContainer = document.querySelector('.key-metrics-container');
            if (keyMetricsContainer) {
                keyMetricsDiv = keyMetricsContainer.parentElement; // The card-body containing the container
            }
        }
        
        // If still not found, try to find any element with "Key Metrics" in the header
        if (!keyMetricsDiv) {
            const headers = document.querySelectorAll('.card-header');
            for (const header of headers) {
                if (header.textContent.includes('Key Metrics')) {
                    const card = header.closest('.card');
                    if (card) {
                        keyMetricsDiv = card.querySelector('.card-body');
                        break;
                    }
                }
            }
        }
        
        if (!keyMetricsDiv) {
            console.log('Key metrics section not found - available headers:', 
                Array.from(document.querySelectorAll('.card-header')).map(h => h.textContent));
            return;
        }
        
        console.log('Found key metrics div:', keyMetricsDiv);
        
        if (!this.filteredData.length) {
            keyMetricsDiv.innerHTML = '<div class="alert alert-warning">No data available for analysis</div>';
            return;
        }
        
        // Comprehensive key metrics based on filtered data
        const keyColumns = [
            'proc_class', 'proc_group', 'code', 'county_name', 'stat_area_name', 
            'primary_taxonomy_desc', 'tin_value', 'enumeration_type', 'organization_name'
        ];
        
        let html = '<div class="row">';
        
        keyColumns.forEach(column => {
            // Check if column exists in the data
            if (!this.filteredData[0] || !(column in this.filteredData[0])) {
                return;
            }
            
            // Get unique values and counts from filtered data
            const valueCounts = {};
            const valueRates = {};
            const valueMedicareProf = {};
            const valueMedicareASC = {};
            const valueMedicareOPPS = {};
            
            this.filteredData.forEach(record => {
                const value = record[column];
                if (value && value !== null && value !== undefined && value !== '') {
                    if (!valueCounts[value]) {
                        valueCounts[value] = 0;
                        valueRates[value] = [];
                        valueMedicareProf[value] = [];
                        valueMedicareASC[value] = [];
                        valueMedicareOPPS[value] = [];
                    }
                    valueCounts[value]++;
                    
                    // Collect financial metrics
                    if (record.negotiated_rate) {
                        valueRates[value].push(record.negotiated_rate);
                    }
                    if (record.medicare_professional_rate) {
                        valueMedicareProf[value].push(record.medicare_professional_rate);
                    }
                    if (record.medicare_asc_stateavg) {
                        valueMedicareASC[value].push(record.medicare_asc_stateavg);
                    }
                    if (record.medicare_opps_stateavg) {
                        valueMedicareOPPS[value].push(record.medicare_opps_stateavg);
                    }
                }
            });
            
            // Sort by count and take top 10
            const topValues = Object.entries(valueCounts)
                .sort(([,a], [,b]) => b - a)
                .slice(0, 10);
            
            if (topValues.length === 0) return;
            
            html += `
                <div class="col-md-6 mb-4">
                    <div class="card h-100">
                        <div class="card-header bg-primary text-white">
                            <h6 class="card-title mb-0">
                                <i class="fas fa-chart-bar me-2"></i>${this.formatColumnName(column)}
                                <span class="badge bg-light text-dark ms-2">${Object.keys(valueCounts).length} unique values</span>
                            </h6>
                        </div>
                        <div class="card-body">
                            <div class="table-responsive">
                                <table class="table table-sm">
                                    <thead>
                                        <tr>
                                            <th>Value</th>
                                            <th>Count</th>
                                            <th>%</th>
                                            <th>Avg Rate</th>
                                            <th>vs Medicare</th>
                                        </tr>
                                    </thead>
                                    <tbody>
            `;
            
            topValues.forEach(([value, count]) => {
                const percentage = ((count / this.filteredData.length) * 100).toFixed(1);
                const rates = valueRates[value];
                const medicareProfRates = valueMedicareProf[value];
                
                const avgRate = rates.length > 0 ? rates.reduce((a, b) => a + b, 0) / rates.length : null;
                const avgMedicareProf = medicareProfRates.length > 0 ? 
                    medicareProfRates.reduce((a, b) => a + b, 0) / medicareProfRates.length : null;
                
                const medicarePct = (avgRate && avgMedicareProf) ? 
                    ((avgRate / avgMedicareProf) * 100).toFixed(1) + '%' : '-';
                
                html += `
                    <tr>
                        <td><strong>${value}</strong></td>
                        <td>${count.toLocaleString()}</td>
                        <td>${percentage}%</td>
                        <td>${this.formatCurrency(avgRate)}</td>
                        <td>${medicarePct}</td>
                    </tr>
                `;
            });
            
            html += `
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        });
        
        html += '</div>';
        keyMetricsDiv.innerHTML = html;
    }
    
    updateFinancialMetrics() {
        // Create or update financial metrics section
        let financialDiv = document.querySelector('#financial-metrics .card-body');
        if (!financialDiv) {
            // Create the financial metrics section if it doesn't exist
            const keyMetricsSection = document.querySelector('#key-metrics');
            if (keyMetricsSection) {
                const newSection = document.createElement('div');
                newSection.id = 'financial-metrics';
                newSection.className = 'row mb-4';
                newSection.innerHTML = `
                    <div class="col-12">
                        <div class="card">
                            <div class="card-header">
                                <h5 class="card-title mb-0">Financial Metrics</h5>
                            </div>
                            <div class="card-body"></div>
                        </div>
                    </div>
                `;
                keyMetricsSection.parentNode.insertBefore(newSection, keyMetricsSection.nextSibling);
                financialDiv = newSection.querySelector('.card-body');
            }
        }
        
        if (!financialDiv || !this.filteredData.length) return;
        
        // Calculate financial metrics from filtered data
        const rates = this.filteredData.map(r => r.negotiated_rate).filter(r => r && !isNaN(r));
        const medicareProfRates = this.filteredData.map(r => r.medicare_professional_rate).filter(r => r && !isNaN(r));
        const medicareASCRates = this.filteredData.map(r => r.medicare_asc_stateavg).filter(r => r && !isNaN(r));
        const medicareOPPSRates = this.filteredData.map(r => r.medicare_opps_stateavg).filter(r => r && !isNaN(r));
        
        if (rates.length === 0) {
            financialDiv.innerHTML = '<div class="alert alert-info">No financial data available in filtered dataset</div>';
            return;
        }
        
        // Calculate statistics
        const avgRate = rates.reduce((a, b) => a + b, 0) / rates.length;
        const medianRate = rates.sort((a, b) => a - b)[Math.floor(rates.length / 2)];
        const minRate = Math.min(...rates);
        const maxRate = Math.max(...rates);
        const stdDev = Math.sqrt(rates.reduce((sq, n) => sq + Math.pow(n - avgRate, 2), 0) / rates.length);
        
        // Medicare comparisons
        const avgMedicareProf = medicareProfRates.length > 0 ? 
            medicareProfRates.reduce((a, b) => a + b, 0) / medicareProfRates.length : null;
        const avgMedicareASC = medicareASCRates.length > 0 ? 
            medicareASCRates.reduce((a, b) => a + b, 0) / medicareASCRates.length : null;
        const avgMedicareOPPS = medicareOPPSRates.length > 0 ? 
            medicareOPPSRates.reduce((a, b) => a + b, 0) / medicareOPPSRates.length : null;
        
        let html = `
            <div class="row">
                <div class="col-md-6">
                    <div class="card bg-light">
                        <div class="card-header bg-success text-white">
                            <h6 class="mb-0">Negotiated Rates</h6>
                        </div>
                        <div class="card-body">
                            <div class="row text-center">
                                <div class="col-6 mb-2">
                                    <strong>Average</strong><br>
                                    <span class="h5 text-success">${this.formatCurrency(avgRate)}</span>
                                </div>
                                <div class="col-6 mb-2">
                                    <strong>Median</strong><br>
                                    <span class="h5 text-success">${this.formatCurrency(medianRate)}</span>
                                </div>
                                <div class="col-6 mb-2">
                                    <strong>Min</strong><br>
                                    <span class="text-info">${this.formatCurrency(minRate)}</span>
                                </div>
                                <div class="col-6 mb-2">
                                    <strong>Max</strong><br>
                                    <span class="text-info">${this.formatCurrency(maxRate)}</span>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="col-md-6">
                    <div class="card bg-light">
                        <div class="card-header bg-warning text-dark">
                            <h6 class="mb-0">vs Medicare Benchmarks</h6>
                        </div>
                        <div class="card-body">
        `;
        
        if (avgMedicareProf) {
            const profPct = ((avgRate / avgMedicareProf) * 100).toFixed(1);
            html += `
                <div class="mb-2">
                    <strong>vs Medicare Professional:</strong><br>
                    <span class="h6 ${profPct > 100 ? 'text-danger' : 'text-success'}">${profPct}%</span>
                </div>
            `;
        }
        
        if (avgMedicareASC) {
            const ascPct = ((avgRate / avgMedicareASC) * 100).toFixed(1);
            html += `
                <div class="mb-2">
                    <strong>vs Medicare ASC:</strong><br>
                    <span class="h6 ${ascPct > 100 ? 'text-danger' : 'text-success'}">${ascPct}%</span>
                </div>
            `;
        }
        
        if (avgMedicareOPPS) {
            const oppsPct = ((avgRate / avgMedicareOPPS) * 100).toFixed(1);
            html += `
                <div class="mb-2">
                    <strong>vs Medicare OPPS:</strong><br>
                    <span class="h6 ${oppsPct > 100 ? 'text-danger' : 'text-success'}">${oppsPct}%</span>
                </div>
            `;
        }
        
        html += `
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        financialDiv.innerHTML = html;
    }
    
    updateGeographicMetrics() {
        // Create or update geographic metrics section
        let geoDiv = document.querySelector('#geographic-metrics .card-body');
        if (!geoDiv) {
            const financialSection = document.querySelector('#financial-metrics');
            if (financialSection) {
                const newSection = document.createElement('div');
                newSection.id = 'geographic-metrics';
                newSection.className = 'row mb-4';
                newSection.innerHTML = `
                    <div class="col-12">
                        <div class="card">
                            <div class="card-header">
                                <h5 class="card-title mb-0">Geographic Distribution</h5>
                            </div>
                            <div class="card-body"></div>
                        </div>
                    </div>
                `;
                financialSection.parentNode.insertBefore(newSection, financialSection.nextSibling);
                geoDiv = newSection.querySelector('.card-body');
            }
        }
        
        if (!geoDiv || !this.filteredData.length) return;
        
        // Calculate geographic metrics from filtered data
        const stateCounts = {};
        const countyCounts = {};
        const statAreaCounts = {};
        
        this.filteredData.forEach(record => {
            // State distribution
            if (record.state) {
                stateCounts[record.state] = (stateCounts[record.state] || 0) + 1;
            }
            
            // County distribution
            if (record.county_name) {
                countyCounts[record.county_name] = (countyCounts[record.county_name] || 0) + 1;
            }
            
            // Statistical area distribution
            if (record.stat_area_name) {
                statAreaCounts[record.stat_area_name] = (statAreaCounts[record.stat_area_name] || 0) + 1;
            }
        });
        
        const topStates = Object.entries(stateCounts).sort(([,a], [,b]) => b - a).slice(0, 5);
        const topCounties = Object.entries(countyCounts).sort(([,a], [,b]) => b - a).slice(0, 5);
        const topStatAreas = Object.entries(statAreaCounts).sort(([,a], [,b]) => b - a).slice(0, 5);
        
        let html = `
            <div class="row">
                <div class="col-md-4">
                    <h6>Top States</h6>
                    <div class="table-responsive">
                        <table class="table table-sm">
                            <thead>
                                <tr><th>State</th><th>Count</th><th>%</th></tr>
                            </thead>
                            <tbody>
        `;
        
        topStates.forEach(([state, count]) => {
            const percentage = ((count / this.filteredData.length) * 100).toFixed(1);
            html += `<tr><td>${state}</td><td>${count.toLocaleString()}</td><td>${percentage}%</td></tr>`;
        });
        
        html += `
                            </tbody>
                        </table>
                    </div>
                </div>
                <div class="col-md-4">
                    <h6>Top Counties</h6>
                    <div class="table-responsive">
                        <table class="table table-sm">
                            <thead>
                                <tr><th>County</th><th>Count</th><th>%</th></tr>
                            </thead>
                            <tbody>
        `;
        
        topCounties.forEach(([county, count]) => {
            const percentage = ((count / this.filteredData.length) * 100).toFixed(1);
            html += `<tr><td>${county}</td><td>${count.toLocaleString()}</td><td>${percentage}%</td></tr>`;
        });
        
        html += `
                            </tbody>
                        </table>
                    </div>
                </div>
                <div class="col-md-4">
                    <h6>Top Statistical Areas</h6>
                    <div class="table-responsive">
                        <table class="table table-sm">
                            <thead>
                                <tr><th>Area</th><th>Count</th><th>%</th></tr>
                            </thead>
                            <tbody>
        `;
        
        topStatAreas.forEach(([area, count]) => {
            const percentage = ((count / this.filteredData.length) * 100).toFixed(1);
            html += `<tr><td>${area}</td><td>${count.toLocaleString()}</td><td>${percentage}%</td></tr>`;
        });
        
        html += `
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        `;
        
        geoDiv.innerHTML = html;
    }
    
    updateSampleData() {
        const previewContentDiv = document.querySelector('#preview-content');
        const previewLoadingDiv = document.querySelector('#preview-loading');
        
        if (!previewContentDiv || !previewLoadingDiv) return;
        
        // Use virtual scrolling for large datasets
        const maxDisplayRows = 50;
        const displayData = this.filteredData.slice(0, maxDisplayRows);
        
        let html = `<h6>Sample Data (Showing ${displayData.length} of ${this.filteredData.length.toLocaleString()} rows)</h6>`;
        html += '<div class="table-responsive"><table class="table table-sm table-striped">';
        
        if (displayData.length > 0) {
            const columns = Object.keys(displayData[0]);
            
            // Header
            html += '<thead><tr>';
            columns.forEach(col => {
                html += `<th>${this.formatColumnName(col)}</th>`;
            });
            html += '</tr></thead><tbody>';
            
            // Data rows
            displayData.forEach(record => {
                html += '<tr>';
                columns.forEach(col => {
                    const value = record[col];
                    html += `<td>${value !== null && value !== undefined ? value : ''}</td>`;
                });
                html += '</tr>';
            });
            html += '</tbody>';
        } else {
            html += '<tbody><tr><td colspan="100%" class="text-center text-muted">No data available</td></tr></tbody>';
        }
        
        html += '</table></div>';
        
        // Add export button for full dataset
        if (this.filteredData.length > maxDisplayRows) {
            html += `
                <div class="mt-3 text-center">
                    <button class="btn btn-sm btn-outline-primary" onclick="exportFilteredData()">
                        <i class="fas fa-download"></i> Export Full Dataset (${this.filteredData.length.toLocaleString()} rows)
                    </button>
                </div>
            `;
        }
        
        // Hide loading spinner and show content
        previewLoadingDiv.style.display = 'none';
        previewContentDiv.innerHTML = html;
        previewContentDiv.style.display = 'block';
    }
    
    updateHeaderMetrics() {
        // Update total records in header
        const totalRecordsElement = document.getElementById('totalRecords');
        if (totalRecordsElement) {
            totalRecordsElement.textContent = this.filteredData.length.toLocaleString();
        }
        
        // Update any other header metrics that might exist
        const headerMetrics = document.querySelectorAll('[data-metric]');
        headerMetrics.forEach(element => {
            const metricType = element.getAttribute('data-metric');
            switch (metricType) {
                case 'total-records':
                    element.textContent = this.filteredData.length.toLocaleString();
                    break;
                case 'filtered-records':
                    element.textContent = this.filteredData.length.toLocaleString();
                    break;
                case 'original-records':
                    element.textContent = this.dataset.length.toLocaleString();
                    break;
            }
        });
    }
    
    clearAllFilters() {
        if (confirm('Are you sure you want to clear all additional filters?')) {
            const filterInputs = document.querySelectorAll('#additionalFiltersForm input[type="text"]');
            filterInputs.forEach(input => {
                input.value = '';
            });
            
            this.currentFilters = {};
            this.filteredData = [...this.dataset];
            this.updateAnalysis();
            
            this.showNotification('All additional filters cleared', 'info');
        }
    }
    
    clearFilter(filterName) {
        const input = document.getElementById(filterName);
        if (input) {
            input.value = '';
            this.applyFilters();
            this.showNotification(`Filter ${filterName} cleared`, 'info');
        }
    }
    
    updateAppliedFiltersDisplay() {
        // Find the applied filters card by looking for the header with "Applied Filters" text
        const cardHeaders = document.querySelectorAll('.card-header');
        let filtersCard = null;
        
        for (const header of cardHeaders) {
            if (header.textContent.includes('Applied Filters')) {
                filtersCard = header.closest('.card');
                break;
            }
        }
        
        if (!filtersCard) return;
        
        const filtersCardBody = filtersCard.querySelector('.card-body');
        if (!filtersCardBody) return;
        
        // Get current filter values from the form
        const form = document.getElementById('additionalFiltersForm');
        if (!form) return;
        
        const activeFilters = [];
        
        // Process each filter input
        const filterInputs = form.querySelectorAll('input[name]');
        filterInputs.forEach(input => {
            if (input.value && input.value.trim()) {
                const filterName = this.formatColumnName(input.name);
                const filterValue = input.value.trim();
                activeFilters.push({
                    name: filterName,
                    value: filterValue,
                    key: input.name
                });
            }
        });
        
        // Update the display
        if (activeFilters.length > 0) {
            let html = '<div class="row">';
            activeFilters.forEach(filter => {
                html += `
                    <div class="col-md-4 mb-2">
                        <div class="d-flex align-items-center">
                            <strong class="me-2">${filter.name}:</strong>
                            <span class="badge bg-primary me-2">${filter.value}</span>
                            <button type="button" class="btn btn-sm btn-outline-danger" 
                                    onclick="window.datasetManager.clearFilter('${filter.key}')" 
                                    title="Clear ${filter.name} filter">
                                <i class="fas fa-times"></i>
                            </button>
                        </div>
                    </div>
                `;
            });
            html += '</div>';
            filtersCardBody.innerHTML = html;
        } else {
            filtersCardBody.innerHTML = '<p class="text-muted mb-0">No additional filters applied. Data shows all records matching your initial selection.</p>';
        }
    }
    
    exportFilteredData() {
        if (!this.filteredData.length) {
            this.showNotification('No data to export', 'warning');
            return;
        }
        
        // Convert to CSV
        const columns = Object.keys(this.filteredData[0]);
        const csvContent = [
            columns.join(','),
            ...this.filteredData.map(row => 
                columns.map(col => {
                    const value = row[col];
                    return typeof value === 'string' && value.includes(',') ? `"${value}"` : value;
                }).join(',')
            )
        ].join('\n');
        
        // Download file
        const blob = new Blob([csvContent], { type: 'text/csv' });
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `filtered_data_${new Date().toISOString().split('T')[0]}.csv`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
        
        this.showNotification(`Exported ${this.filteredData.length.toLocaleString()} records`, 'success');
    }
    
    formatColumnName(columnName) {
        return columnName.split('_').map(word => 
            word.charAt(0).toUpperCase() + word.slice(1)
        ).join(' ');
    }
    
    formatCurrency(value) {
        if (value === null || value === undefined || isNaN(value)) {
            return 'N/A';
        }
        return `$${Number(value).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    }
    
    showLoadingState(message = 'Loading...') {
        const loadingDiv = document.getElementById('filterLoading');
        if (loadingDiv) {
            loadingDiv.innerHTML = `
                <div class="spinner-border text-primary" role="status">
                    <span class="visually-hidden">Loading...</span>
                </div>
                <p class="mt-2">${message}</p>
            `;
            loadingDiv.style.display = 'block';
        }
    }
    
    hideLoadingState() {
        const loadingDiv = document.getElementById('filterLoading');
        if (loadingDiv) {
            loadingDiv.style.display = 'none';
        }
    }
    
    hideAnalysisLoading() {
        const analysisLoadingDiv = document.getElementById('analysis-loading');
        const analysisSectionDiv = document.getElementById('analysis-section');
        const datasetSummaryDiv = document.getElementById('dataset-summary');
        const payerBreakdownDiv = document.getElementById('payer-breakdown');
        const keyMetricsAnalysisDiv = document.getElementById('key-metrics-analysis');
        const payerComparisonAnalysisDiv = document.getElementById('payer-comparison-analysis');
        const basicStatsAnalysisDiv = document.getElementById('basic-stats-analysis');
        
        if (analysisLoadingDiv) {
            analysisLoadingDiv.style.display = 'none';
        }
        
        if (analysisSectionDiv) {
            analysisSectionDiv.style.display = 'block';
        }
        
        if (datasetSummaryDiv) {
            datasetSummaryDiv.style.display = 'block';
        }
        
        if (payerBreakdownDiv) {
            payerBreakdownDiv.style.display = 'block';
        }
        
        if (keyMetricsAnalysisDiv) {
            keyMetricsAnalysisDiv.style.display = 'block';
        }
        
        if (payerComparisonAnalysisDiv) {
            payerComparisonAnalysisDiv.style.display = 'block';
        }
        
        if (basicStatsAnalysisDiv) {
            basicStatsAnalysisDiv.style.display = 'block';
        }
    }
    
    closeModal() {
        const modal = bootstrap.Modal.getInstance(document.getElementById('addFiltersModal'));
        if (modal) {
            // Remove focus from any focused element inside the modal before closing
            const focusedElement = document.activeElement;
            if (focusedElement && document.getElementById('addFiltersModal').contains(focusedElement)) {
                focusedElement.blur();
            }
            modal.hide();
        }
    }
    
    showNotification(message, type = 'info') {
        // Create notification element
        const notification = document.createElement('div');
        notification.className = `alert alert-${type} alert-dismissible fade show position-fixed`;
        notification.style.top = '20px';
        notification.style.right = '20px';
        notification.style.zIndex = '9999';
        notification.innerHTML = `
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        `;
        
        document.body.appendChild(notification);
        
        // Auto-remove after 5 seconds
        setTimeout(() => {
            if (notification.parentNode) {
                notification.remove();
            }
        }, 5000);
    }
    
    showTimeoutWarning() {
        // Hide the loading state
        this.hideLoadingState();
        
        // Hide analysis loading and show analysis section (even if empty)
        this.hideAnalysisLoading();
        
        // Create timeout warning modal
        const modalHtml = `
            <div class="modal fade" id="timeoutModal" tabindex="-1" aria-labelledby="timeoutModalLabel" aria-hidden="true">
                <div class="modal-dialog modal-dialog-centered">
                    <div class="modal-content">
                        <div class="modal-header bg-warning text-dark">
                            <h5 class="modal-title" id="timeoutModalLabel">
                                <i class="bi bi-exclamation-triangle me-2"></i>
                                Loading Timeout
                            </h5>
                        </div>
                        <div class="modal-body">
                            <div class="alert alert-warning">
                                <h6 class="alert-heading">Dataset Loading Taking Too Long</h6>
                                <p class="mb-0">Your dataset is taking longer than 60 seconds to load. This usually happens with very large datasets that may impact performance.</p>
                            </div>
                            <p>Please consider:</p>
                            <ul>
                                <li>Adding more specific filters to reduce the dataset size</li>
                                <li>Selecting a smaller time period</li>
                                <li>Choosing a specific procedure set or taxonomy</li>
                            </ul>
                            <p class="mb-0">Would you like to go back to the selection page to refine your filters?</p>
                        </div>
                        <div class="modal-footer">
                            <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">
                                Stay Here
                            </button>
                            <a href="/dashboard/" class="btn btn-primary">
                                <i class="bi bi-arrow-left me-2"></i>Back to Selection
                            </a>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        // Remove any existing timeout modal
        const existingModal = document.getElementById('timeoutModal');
        if (existingModal) {
            existingModal.remove();
        }
        
        // Add the modal to the page
        document.body.insertAdjacentHTML('beforeend', modalHtml);
        
        // Show the modal
        const modal = new bootstrap.Modal(document.getElementById('timeoutModal'));
        modal.show();
        
        // Clean up when modal is hidden
        document.getElementById('timeoutModal').addEventListener('hidden.bs.modal', function() {
            this.remove();
        });
        
        // Reset loading state
        this.isLoading = false;
    }
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.datasetManager = new ClientSideDatasetManager();
});

// Make export function globally available
window.exportFilteredData = () => {
    if (window.datasetManager) {
        window.datasetManager.exportFilteredData();
    }
};
