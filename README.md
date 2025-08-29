# WorkComp Rates - Commercial Rate Insights Platform

A comprehensive platform for analyzing commercial workers' compensation rates across different states, with interactive visualizations and data filtering capabilities.

## Features

### Interactive State Map
- Click on any state to view available data
- Visual indicators for data availability status
- Seamless navigation between states

### Data Overview Page (NEW!)
- **Performance Improvement**: Shows dataset statistics before loading detailed insights
- **Prefilters**: Set key filters (Payer, Organization, Procedure Set) to reduce data size
- **Data Comprehension**: Understand the scope and coverage of available data
- **Smart Navigation**: Prefilters are carried forward to the insights page

### Detailed Insights Dashboard
- Interactive charts and visualizations
- Comprehensive filtering options
- Rate comparisons and analysis
- Sample data previews

### User Activity Tracking
- Monitor user interactions and data access patterns
- Admin dashboard for usage analytics

## User Workflow

### Optimized Data Exploration Path:
1. **Map View** → Select a state
2. **Overview Page** → Review dataset statistics and set prefilters
3. **Insights Page** → View detailed analysis with improved performance

### Benefits of the New Overview Page:
- **Faster Loading**: Prefilters reduce the dataset size before processing
- **Better Understanding**: See data comprehensiveness upfront
- **Performance Optimization**: Avoid loading unnecessary data
- **User Control**: Choose what data to analyze before detailed processing

## Technical Architecture

- **Backend**: Django with DuckDB for efficient parquet file processing
- **Frontend**: Bootstrap 5 with interactive JavaScript components
- **Data Storage**: Parquet files for optimal performance
- **Caching**: Smart data loading with prefilter optimization

## Performance Improvements

The overview page addresses performance issues by:
1. **Data Prefiltering**: Users can select specific payers, organizations, or procedure sets
2. **Reduced Data Load**: Only relevant data is processed in the insights page
3. **Smart Caching**: Efficient parquet file handling with DuckDB
4. **User Guidance**: Clear understanding of data scope before detailed analysis

## Installation & Setup

```bash
# Clone the repository
git clone <repository-url>
cd workcomp-rates

# Install dependencies
pip install -r requirements.txt

# Run migrations
python manage.py migrate

# Start development server
python manage.py runserver
```

## Usage

1. Navigate to the interactive map
2. Click on a state to view the overview page
3. Review dataset statistics and set prefilters as needed
4. Proceed to detailed insights with improved performance
5. Use the comprehensive filtering and visualization tools

## Contributing

Please read our contributing guidelines and ensure all code follows the project's coding standards.

## License

[Your License Here] 