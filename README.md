# Veriflow — Data Pipeline

A self-hosted CSV validation, cleaning, and transformation tool.

---

## Stack

- **Frontend** — single-page HTML, no build step
- **Backend** — Node.js + Express
- **Process manager** — PM2
- **Reverse proxy** — Nginx + Let's Encrypt

---

## Deploy

### 1. Server requirements

- Ubuntu 22.04 (or any Debian-based Linux)
- Node.js 18+
- A domain pointed at the server

### 2. Install Node.js

```bash
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs
```

### 3. Upload and install

```bash
# Upload project files to /var/www/veriflow
cd /var/www/veriflow
npm install --production
cp .env.example .env
nano .env   # set CORS_ORIGINS to your domain
```

### 4. Start with PM2

```bash
sudo npm install -g pm2
pm2 start ecosystem.config.js --env production
pm2 save && pm2 startup
```

### 5. Nginx + HTTPS

```bash
sudo apt install -y nginx certbot python3-certbot-nginx
sudo cp nginx.conf /etc/nginx/sites-available/your-domain.com
sudo ln -s /etc/nginx/sites-available/your-domain.com /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
sudo certbot --nginx -d your-domain.com
```

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `3001` | Server port |
| `MAX_FILE_MB` | `100` | Maximum upload size |
| `TOKEN_TTL_MS` | `1800000` | Download link expiry (ms) |
| `CORS_ORIGINS` | `*` | Allowed origin domain(s) |
| `RATE_LIMIT_RPM` | `10` | Pipeline requests per IP per minute |

---

## API

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/health` | Server status |
| `POST` | `/api/schema` | Instant column profiling (first 500 rows) |
| `POST` | `/api/process` | Full pipeline — validate, clean, transform |
| `GET` | `/api/download/:token` | Stream transformed CSV (token valid 30 min) |
