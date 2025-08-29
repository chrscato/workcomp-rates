// Shared filter functionality for all analysis views
document.addEventListener('DOMContentLoaded', function() {
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
    });
    
    // Filter Logic
    const filterForm = document.getElementById('filterForm');
    const filterSelects = document.querySelectorAll('.filter-select');
    
    // Function to submit form
    function submitForm() {
        const formData = new FormData(filterForm);
        const params = new URLSearchParams(formData);
        window.location.href = `${window.location.pathname}?${params.toString()}`;
    }

    // Add change event listeners to all filters
    filterSelects.forEach(select => {
        select.addEventListener('change', () => {
            submitForm();  // Submit form immediately on any filter change
        });
    });

    // Handle Select2 change events
    $('.select2-dropdown').on('select2:select select2:unselect', function() {
        submitForm();
    });

    // Initialize active filters display
    updateActiveFiltersDisplay();
    
    // Only collapse filters on first load (no active filters)
    const urlParams = new URLSearchParams(window.location.search);
    const hasActiveFilters = Array.from(urlParams.values()).some(value => value && value !== '');
    
    if (!hasActiveFilters) {
        // First load - keep collapsed
        document.getElementById('filterCardBody').style.display = 'none';
        document.getElementById('filterToggleIcon').className = 'fas fa-chevron-right';
    } else {
        // User has applied filters - expand and show
        document.getElementById('filterCardBody').style.display = 'block';
        document.getElementById('filterToggleIcon').className = 'fas fa-chevron-down';
    }
});

// Global functions for collapsible filters and active filters
function toggleFilters() {
    const filterCardBody = document.getElementById('filterCardBody');
    const filterToggleIcon = document.getElementById('filterToggleIcon');
    
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
    
    filterSelects.forEach(select => {
        select.value = '';
        if ($(select).hasClass('select2-hidden-accessible')) {
            $(select).val('').trigger('change');
        }
    });
    
    // Submit the form to clear filters
    const filterForm = document.getElementById('filterForm');
    const formData = new FormData(filterForm);
    const params = new URLSearchParams(formData);
    window.location.href = `${window.location.pathname}?${params.toString()}`;
}
