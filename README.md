# WorkComp Rates

A professional workers' compensation rate analysis and insights platform built with Django.

## Features

- **Commercial Rate Insights**: Interactive analysis of commercial rate data with advanced filtering
- **Side-by-Side Comparison**: Compare rates across multiple organizations and payers
- **User Authentication**: Secure user registration and login system
- **Modern UI**: Clean, responsive interface built with Bootstrap 5
- **Data Visualization**: Interactive charts and statistics
- **Production Ready**: Configured for deployment with proper security settings

## Technology Stack

- **Backend**: Django 5.2+
- **Database**: SQLite (development) / PostgreSQL (production)
- **Frontend**: Bootstrap 5, jQuery, Select2, Plotly
- **Data Processing**: Pandas, PyArrow, DuckDB
- **Package Management**: uv
- **Deployment**: Gunicorn, WhiteNoise

## Quick Start

### Prerequisites

- Python 3.12+
- uv (Python package manager)

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd workcomp-rates
   ```

2. **Install dependencies**
   ```bash
   uv sync
   ```

3. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

4. **Run migrations**
   ```bash
   uv run python manage.py migrate
   ```

5. **Create a superuser**
   ```bash
   uv run python manage.py createsuperuser
   ```

6. **Run the development server**
   ```bash
   uv run python manage.py runserver
   ```

7. **Visit the application**
   - Open http://localhost:8000
   - Register a new account or login with your superuser credentials

## Project Structure

```
workcomp-rates/
├── workcomp_rates/          # Django project settings
├── core/                    # Main application
│   ├── utils/              # Data processing utilities
│   ├── templates/          # Core templates
│   └── views.py           # Core views
├── accounts/               # User authentication
│   ├── templates/         # Auth templates
│   ├── models.py         # User profile model
│   └── views.py          # Auth views
├── templates/             # Base templates
├── static/               # Static files (CSS, JS)
├── manage.py             # Django management script
├── pyproject.toml        # Project dependencies
└── README.md            # This file
```

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
SECRET_KEY=your-secret-key-here
DEBUG=False
DATABASE_URL=sqlite:///db.sqlite3
ALLOWED_HOSTS=workcomp-rates.com,www.workcomp-rates.com
```

### Production Deployment

1. **Set up your server**
   - Install Python 3.12+
   - Install uv: `pip install uv`

2. **Deploy the application**
   ```bash
   git clone <repository-url>
   cd workcomp-rates
   uv sync
   uv run python manage.py migrate
   uv run python manage.py collectstatic
   ```

3. **Configure your web server**
   - Set up Nginx/Apache to proxy to Gunicorn
   - Configure SSL certificates
   - Set up environment variables

4. **Run with Gunicorn**
   ```bash
   uv run gunicorn workcomp_rates.wsgi:application --bind 0.0.0.0:8000
   ```

## Data Management

### Adding Commercial Rate Data

1. Place your commercial rate data in `core/data/commercial_rates.parquet`
2. The application will automatically detect and load the data
3. Ensure the parquet file has the required columns:
   - `billing_code`
   - `code_desc`
   - `org_name`
   - `payer`
   - `rate`
   - `billing_class`
   - `procedure_set`
   - `procedure_class`
   - `procedure_group`
   - `cbsa`
   - `primary_taxonomy_desc`
   - `GA_PROF_MAR`
   - `medicare_prof`
   - `GA_OP_MAR`
   - `medicare_op`

## Development

### Running Tests
```bash
uv run pytest
```

### Code Formatting
```bash
uv run black .
```

### Linting
```bash
uv run flake8
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support and questions, please contact:
- Email: christopher@clarity-dx.com
- Website: https://workcomp-rates.com

## Changelog

### v0.1.0 (2025-08-27)
- Initial release
- Commercial rate insights functionality
- User authentication system
- Side-by-side comparison tool
- Modern Bootstrap 5 UI
- Production-ready configuration 