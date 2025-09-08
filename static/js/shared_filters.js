// Shared filter functionality for all analysis views
document.addEventListener('DOMContentLoaded', function() {
    // Check if we're on a page with filters before initializing
    const filterForm = document.getElementById('filterForm');
    const filterSelects = document.querySelectorAll('.filter-select');
    
    // Only initialize if we have the required filter elements
    if (!filterForm || filterSelects.length === 0) {
        console.log('No filter elements found on this page, skipping filter initialization');
        return;
    }
    
    // Check if jQuery and Select2 are available
    if (typeof $ === 'undefined' || !$.fn.select2) {
        console.log('jQuery or Select2 not available, skipping Select2 initialization');
        return;
    }
    
    // Initialize Select2 for all filter dropdowns with multi-select support
    $('.select2-dropdown').select2({
        placeholder: 'Select one or more options...',
        allowClear: true,
        width: '100%',
        minimumInputLength: 0,
        closeOnSelect: false,
        templateResult: function(data) {
            if (data.loading) return data.text;
            if (!data.id) return data.text;
            return $('<span>' + data.text + '</span>');
        },
        templateSelection: function(data) {
            if (!data.id) return data.text;
            return data.text;
        }
    }).on('select2:select select2:unselect', function() {
        if (autoSubmit) {
            submitForm();
        } else {
            updateFilterPreview(); // Show preview, don't submit
        }
    });
    
    // Debounced form submission to prevent rapid reloads
    let submitTimeout;
    function submitForm() {
        // Clear any existing timeout
        if (submitTimeout) {
            clearTimeout(submitTimeout);
        }
        
        // Show loading indicator
        showLoadingIndicator();
        
        // Debounce the actual submission
        submitTimeout = setTimeout(() => {
            const formData = new FormData(filterForm);
            const params = new URLSearchParams(formData);
            window.location.href = `${window.location.pathname}?${params.toString()}`;
        }, 500); // 500ms delay
    }
    
    // Function to show loading indicator
    function showLoadingIndicator() {
        // Create or update loading indicator
        let loadingIndicator = document.getElementById('filterLoadingIndicator');
        if (!loadingIndicator) {
            loadingIndicator = document.createElement('div');
            loadingIndicator.id = 'filterLoadingIndicator';
            loadingIndicator.className = 'alert alert-info d-flex align-items-center';
            loadingIndicator.innerHTML = `
                <div class="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></div>
                <span>Updating filters...</span>
            `;
            
            // Insert after the filter form
            const filterCard = document.querySelector('.card.shadow-sm');
            if (filterCard) {
                filterCard.parentNode.insertBefore(loadingIndicator, filterCard.nextSibling);
            }
        }
        
        loadingIndicator.style.display = 'block';
    }

    // Check if auto-submit is enabled (default behavior)
    const autoSubmit = filterForm.getAttribute('data-auto-submit') !== 'false';
    
    // Function to update filter preview and enable apply button
    function updateFilterPreview() {
        updateActiveFiltersDisplay();
        
        // Enable apply button if in manual mode
        if (!autoSubmit) {
            const applyButton = filterForm.querySelector('button[type="submit"]');
            if (applyButton) {
                applyButton.disabled = false;
                applyButton.classList.remove('btn-secondary');
                applyButton.classList.add('btn-primary');
            }
        }
    }

    // Add change event listeners to all filters (only for non-Select2 elements)
    filterSelects.forEach(select => {
        // Skip Select2 elements as they have their own event handlers
        if (!$(select).hasClass('select2-hidden-accessible') && !$(select).hasClass('select2-dropdown')) {
            select.addEventListener('change', () => {
                if (autoSubmit) {
                    submitForm();  // Submit form with debouncing
                } else {
                    updateFilterPreview(); // Show preview, don't submit
                }
            });
        }
    });


    // Handle form submission manually for manual apply mode
    if (!autoSubmit) {
        // Prevent all form submissions by default
        filterForm.addEventListener('submit', function(e) {
            e.preventDefault();
            // Only submit if the apply button was clicked
            if (e.submitter && e.submitter.type === 'submit') {
                submitForm();
            }
        });
        
        // Add click handler to apply button
        const applyButton = filterForm.querySelector('button[type="submit"]');
        if (applyButton) {
            applyButton.addEventListener('click', function(e) {
                e.preventDefault();
                submitForm();
            });
        }
    }

    // Initialize active filters display
    updateActiveFiltersDisplay();
    
    // Only collapse filters on first load (no active filters)
    const urlParams = new URLSearchParams(window.location.search);
    const hasActiveFilters = Array.from(urlParams.values()).some(value => value && value !== '');
    
    const filterCardBody = document.getElementById('filterCardBody');
    const filterToggleIcon = document.getElementById('filterToggleIcon');
    
    if (filterCardBody && filterToggleIcon) {
        if (!hasActiveFilters) {
            // First load - keep collapsed
            filterCardBody.style.display = 'none';
            filterToggleIcon.className = 'fas fa-chevron-right';
        } else {
            // User has applied filters - expand and show
            filterCardBody.style.display = 'block';
            filterToggleIcon.className = 'fas fa-chevron-down';
        }
    }
});

// Global functions for collapsible filters and active filters
function toggleFilters() {
    const filterCardBody = document.getElementById('filterCardBody');
    const filterToggleIcon = document.getElementById('filterToggleIcon');
    
    // Check if required elements exist
    if (!filterCardBody || !filterToggleIcon) {
        console.log('Filter toggle elements not found, cannot toggle filters');
        return;
    }
    
    if (filterCardBody.style.display === 'none') {
        filterCardBody.style.display = 'block';
        filterToggleIcon.className = 'fas fa-chevron-down';
    } else {
        filterCardBody.style.display = 'none';
        filterToggleIcon.className = 'fas fa-chevron-right';
    }
}

function updateActiveFiltersDisplay() {
    const activeFiltersSummary = document.getElementById('activeFiltersSummary');
    const activeFiltersText = document.getElementById('activeFiltersText');
    const filterSelects = document.querySelectorAll('.filter-select');
    
    // Check if required elements exist
    if (!activeFiltersSummary || !activeFiltersText) {
        console.log('Active filters display elements not found, skipping update');
        return;
    }
    
    const activeFilters = [];
    
    filterSelects.forEach(select => {
        if (select.value && select.value !== '') {
            const filterName = select.getAttribute('data-filter') || select.name;
            const filterValue = select.value;
            
            // Handle both single values and arrays
            if (Array.isArray(filterValue) && filterValue.length > 0) {
                activeFilters.push(`${filterName}: ${filterValue.join(', ')}`);
            } else if (typeof filterValue === 'string' && filterValue.trim() !== '') {
                activeFilters.push(`${filterName}: ${filterValue}`);
            }
        }
    });
    
    if (activeFilters.length > 0) {
        activeFiltersText.textContent = activeFilters.join(', ');
        activeFiltersSummary.style.display = 'block';
    } else {
        activeFiltersText.textContent = 'No filters applied';
        activeFiltersSummary.style.display = 'none';
    }
 }

function clearAllFilters() {
    const filterSelects = document.querySelectorAll('.filter-select');
    const filterForm = document.getElementById('filterForm');
    
    // Check if required elements exist
    if (!filterForm) {
        console.log('Filter form not found, cannot clear filters');
        return;
    }
    
    filterSelects.forEach(select => {
        select.value = '';
        if (typeof $ !== 'undefined' && $(select).hasClass('select2-hidden-accessible')) {
            $(select).val('').trigger('change');
        }
    });
    
    // Submit the form to clear filters
    const formData = new FormData(filterForm);
    const params = new URLSearchParams(formData);
    window.location.href = `${window.location.pathname}?${params.toString()}`;
}
