// PM2 Ecosystem — Veriflow
// Usage:
//   npm install -g pm2
//   pm2 start ecosystem.config.js --env production
//   pm2 save && pm2 startup

module.exports = {
  apps: [
    {
      name:     'veriflow',
      script:   'server.js',
      instances: 'max',
      exec_mode: 'cluster',
      watch:     false,
      max_memory_restart: '512M',
      error_file: './logs/error.log',
      out_file:   './logs/out.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss',

      env_production: {
        NODE_ENV:       'production',
        PORT:           3001,
        MAX_FILE_MB:    100,
        TOKEN_TTL_MS:   1800000,
        CORS_ORIGINS:   'https://veriflow.f4w4z.dev',
        RATE_LIMIT_RPM: 10,
      },
    },
  ],
};
