# WorkComp Rates Deployment Guide

## �� Quick Deployment

### Prerequisites
- Ubuntu 20.04+ server
- Domain name pointing to server IP
- SSH access to server

### Server Setup (One-time)

1. **Connect to server**
   ```bash
   ssh root@134.209.13.85
   ```

2. **Run server setup**
   ```bash
   chmod +x setup-server.sh
   ./setup-server.sh
   ```

3. **Clone repository**
   ```bash
   cd /var/www/workcomp-rates
   git clone <your-github-repo-url> .
   chown -R www-data:www-data /var/www/workcomp-rates
   ```

4. **Set up environment**
   ```bash
   cd /var/www/workcomp-rates
   cp .env.example .env
   nano .env  # Edit with production settings
   ```

5. **Install dependencies**
   ```bash
   uv venv
   source .venv/bin/activate
   uv sync
   ```

6. **Set up database**
   ```bash
   uv run python manage.py migrate
   uv run python manage.py createsuperuser
   ```

7. **Collect static files**
   ```bash
   uv run python manage.py collectstatic --noinput
   ```

8. **Configure services**
   ```bash
   # Copy service files
   cp workcomp-rates.service /etc/systemd/system/
   cp nginx-workcomp-rates.conf /etc/nginx/sites-available/workcomp-rates
   
   # Enable nginx site
   ln -s /etc/nginx/sites-available/workcomp-rates /etc/nginx/sites-enabled/
   rm /etc/nginx/sites-enabled/default
   
   # Test and start services
   nginx -t
   systemctl enable workcomp-rates
   systemctl start workcomp-rates
   systemctl reload nginx
   ```

9. **Set up SSL**
   ```bash
   certbot --nginx -d workcomp-rates.com -d www.workcomp-rates.com
   ```

### Regular Deployments

1. **Update application**
   ```bash
   chmod +x deploy.sh
   ./deploy.sh
   ```

2. **Manual deployment**
   ```bash
   cd /var/www/workcomp-rates
   git pull origin main
   uv sync
   uv run python manage.py migrate
   uv run python manage.py collectstatic --noinput
   sudo systemctl restart workcomp-rates
   ```

## 🔧 Maintenance

### Useful Commands

```bash
# Check service status
systemctl status workcomp-rates
systemctl status nginx

# View logs
tail -f /var/log/workcomp-rates/django.log
tail -f /var/log/workcomp-rates/gunicorn_error.log
tail -f /var/log/nginx/error.log

# Restart services
sudo systemctl restart workcomp-rates
sudo systemctl reload nginx

# Backup database
sudo -u www-data python manage.py dumpdata > backup_$(date +%Y%m%d).json
```

### Troubleshooting

1. **Application not starting**
   - Check logs: `journalctl -u workcomp-rates -f`
   - Verify permissions: `ls -la /var/www/workcomp-rates`
   - Check environment: `cat /var/www/workcomp-rates/.env`

2. **Nginx issues**
   - Test configuration: `nginx -t`
   - Check logs: `tail -f /var/log/nginx/error.log`
   - Verify SSL certificates: `certbot certificates`

3. **Static files not loading**
   - Check static files: `ls -la /var/www/workcomp-rates/staticfiles`
   - Recollect: `uv run python manage.py collectstatic --noinput`

## 🔒 Security

- Keep system updated: `apt update && apt upgrade`
- Monitor logs regularly
- Use strong passwords
- Keep SSL certificates renewed
- Regular backups

## 📞 Support

For deployment issues, check:
1. Service logs
2. Nginx configuration
3. File permissions
4. Environment variables