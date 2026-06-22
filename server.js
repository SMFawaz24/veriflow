/**
 * Veriflow — Data Pipeline Backend  v2.1
 * ──────────────────────────────────────────
 * Endpoints
 *   POST /api/process          upload + config → results + downloadToken
 *   POST /api/schema           upload only → column schema inference
 *   GET  /api/download/:token  stream transformed CSV
 *   GET  /api/health           uptime / stats
 *
 * Setup
 *   cp .env.example .env   (edit values)
 *   npm install
 *   node server.js          (dev)
 *   pm2 start ecosystem.config.js --env production  (prod)
 */

'use strict';

require('dotenv').config();

const express      = require('express');
const multer       = require('multer');
const cors         = require('cors');
const path         = require('path');
const crypto       = require('crypto');
const { parse }    = require('csv-parse/sync');
const { stringify } = require('csv-stringify/sync');

// ── CONFIG ───────────────────────────────────────────────
const PORT       = parseInt(process.env.PORT)          || 3001;
const MAX_MB     = parseInt(process.env.MAX_FILE_MB)   || 100;
const TOKEN_TTL  = parseInt(process.env.TOKEN_TTL_MS)  || 30 * 60 * 1000;
const RPM        = parseInt(process.env.RATE_LIMIT_RPM)|| 10;
const ORIGINS    = (process.env.CORS_ORIGINS || '*').split(',').map(s => s.trim());

const app = express();

// ── CORS ─────────────────────────────────────────────────
app.use(cors({
  origin: ORIGINS.includes('*') ? '*' : (origin, cb) => {
    if (!origin || ORIGINS.includes(origin)) cb(null, true);
    else cb(new Error('Not allowed by CORS'));
  },
  methods: ['GET', 'POST', 'OPTIONS'],
}));

app.use(express.json({ limit: '1mb' }));
app.use(express.static(path.join(__dirname)));

// ── RATE LIMITER (in-memory, per IP) ────────────────────
const ratemap = new Map();
setInterval(() => ratemap.clear(), 60_000);

function rateLimit(req, res, next) {
  const ip  = req.ip || req.socket.remoteAddress;
  const hit = (ratemap.get(ip) || 0) + 1;
  ratemap.set(ip, hit);
  if (hit > RPM) return res.status(429).json({ error: `Rate limit: ${RPM} requests/min` });
  next();
}

// ── MULTER ────────────────────────────────────────────────
const upload = multer({
  storage: multer.memoryStorage(),
  limits:  { fileSize: MAX_MB * 1024 * 1024 },
  fileFilter(req, file, cb) {
    if (/\.(csv|tsv|txt)$/i.test(file.originalname)) cb(null, true);
    else cb(new Error('Only .csv / .tsv / .txt files accepted'));
  },
});

// ── DOWNLOAD TOKEN STORE ─────────────────────────────────
const tokenStore = new Map();
setInterval(() => {
  const now = Date.now();
  for (const [k, v] of tokenStore) if (v.expires < now) tokenStore.delete(k);
}, 10 * 60_000);

function storeDownload(csv, filename) {
  const token = crypto.randomBytes(18).toString('hex');
  tokenStore.set(token, { csv, filename, expires: Date.now() + TOKEN_TTL });
  return token;
}

// ── PROCESS STATS ─────────────────────────────────────────
const serverStats = { requests: 0, processed: 0, errors: 0, startedAt: Date.now() };

// ── MATH UTILS ────────────────────────────────────────────
const sum   = a => a.reduce((s, v) => s + v, 0);
const mean  = a => sum(a) / a.length;
const median = a => {
  const s = [...a].sort((x, y) => x - y), m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m-1] + s[m]) / 2;
};
const stdev = a => { const m = mean(a); return Math.sqrt(sum(a.map(v => (v-m)**2)) / a.length); };
const variance = a => { const m = mean(a); return sum(a.map(v => (v-m)**2)) / a.length; };
const skewness = a => {
  const m = mean(a), s = stdev(a);
  if (s === 0) return 0;
  return sum(a.map(v => ((v-m)/s)**3)) / a.length;
};
const mode = a => {
  const f = {}; a.forEach(v => { f[v] = (f[v]||0)+1; });
  return Object.entries(f).sort((x,y) => y[1]-x[1])[0]?.[0] ?? '';
};
const percentile = (a, p) => {
  const s = [...a].sort((x,y)=>x-y);
  const i = (p/100) * (s.length-1);
  const lo = Math.floor(i), hi = Math.ceil(i);
  return s[lo] + (s[hi]-s[lo]) * (i-lo);
};

// ── SCHEMA INFERENCE ──────────────────────────────────────
function inferSchema(headers, data) {
  return headers.map(h => {
    const raw  = data.map(r => r[h]);
    const vals = raw.filter(v => v !== '' && v != null);
    const nulls = raw.length - vals.length;

    // Type detection
    const allNum  = vals.every(v => !isNaN(Number(v)));
    const allBool = vals.every(v => /^(true|false|0|1|yes|no)$/i.test(v));
    const allDate = !allNum && vals.every(v => !isNaN(Date.parse(v)));
    const emailRe = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    const allEmail = vals.every(v => emailRe.test(v));

    let type = 'text';
    if (allNum)  type = 'numeric';
    if (allBool) type = 'boolean';
    if (allDate) type = 'datetime';
    if (allEmail)type = 'email';

    const unique = new Set(vals).size;
    const cardinality = vals.length > 0 ? (unique / vals.length) : 0;

    const schema = { column: h, type, nulls, nullPct: vals.length ? +(nulls/raw.length*100).toFixed(1) : 100, unique, cardinality: +cardinality.toFixed(3) };

    if (type === 'numeric') {
      const nums = vals.map(Number);
      schema.min  = Math.min(...nums);
      schema.max  = Math.max(...nums);
      schema.mean = +mean(nums).toFixed(4);
      schema.std  = +stdev(nums).toFixed(4);
      schema.p25  = +percentile(nums, 25).toFixed(4);
      schema.p75  = +percentile(nums, 75).toFixed(4);
    } else {
      schema.topValues = Object.entries(
        vals.reduce((acc, v) => { acc[v] = (acc[v]||0)+1; return acc; }, {})
      ).sort((a,b)=>b[1]-a[1]).slice(0,5).map(([v,c])=>({ value:v, count:c }));
    }

    return schema;
  });
}

// ── PIPELINE ENGINE ────────────────────────────────────────
function detectDelimiter(text) {
  const s = text.slice(0, 4096);
  const opts = { ',': (s.match(/,/g)||[]).length, '\t': (s.match(/\t/g)||[]).length, ';': (s.match(/;/g)||[]).length, '|': (s.match(/\|/g)||[]).length };
  return Object.entries(opts).sort((a,b)=>b[1]-a[1])[0][0];
}

function isNumericCol(col, data) {
  const v = data.map(r=>r[col]).filter(x=>x!==''&&x!=null);
  return v.length > 0 && v.every(x=>!isNaN(Number(x)));
}
function numericVals(col, data) {
  return data.map(r=>r[col]).filter(x=>x!==''&&x!=null&&!isNaN(Number(x))).map(Number);
}

function runPipeline(rawText, config) {
  const log      = [];
  const issues   = [];
  const statsOut = {};
  const ts       = () => new Date().toLocaleTimeString('en-US',{hour12:false});
  const addLog   = (type, msg) => log.push({ type, msg, ts: ts() });

  // Parse
  const delim = detectDelimiter(rawText);
  let records;
  try {
    records = parse(rawText, {
      delimiter: delim, columns: true, skip_empty_lines: true,
      trim: false, relax_quotes: true, relax_column_count: true,
    });
  } catch (err) { throw new Error('CSV parse failed: ' + err.message); }
  if (!records.length) throw new Error('File contains no data rows');

  let headers = Object.keys(records[0]);
  let data    = records;
  addLog('info', `Parsed ${data.length} rows × ${headers.length} columns (delimiter: ${JSON.stringify(delim)})`);

  const schema = inferSchema(headers, data);
  addLog('info', `Schema inferred: ${schema.filter(s=>s.type==='numeric').length} numeric, ${schema.filter(s=>s.type==='text').length} text, ${schema.filter(s=>s.type==='datetime').length} datetime`);

  // ── VALIDATE ──
  addLog('info', 'Validation started');

  if (config.vNull) {
    schema.forEach(s => {
      if (s.nulls) { issues.push({lvl:'y', msg:`Column <strong>${s.column}</strong>: ${s.nulls} null/empty (${s.nullPct}%)`}); addLog('warn',`NULL "${s.column}": ${s.nulls}`); }
    });
  }
  if (config.vDup) {
    const seen = new Set(); let dups = 0;
    data.forEach(r=>{ const k=JSON.stringify(Object.values(r)); if(seen.has(k))dups++; else seen.add(k); });
    if (dups) { issues.push({lvl:'y',msg:`Found <strong>${dups}</strong> duplicate row${dups>1?'s':''}`}); addLog('warn',`DUPS: ${dups}`); }
    else addLog('ok','No duplicate rows');
  }
  if (config.vType) {
    schema.forEach(s => {
      if (s.type === 'text' && s.cardinality < 0.01 && s.unique > 1)
        addLog('info', `Low-cardinality text "${s.column}": ${s.unique} unique values — consider encoding`);
    });
  }
  if (config.vEmail) {
    const re = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    headers.filter(h=>/email|mail/i.test(h)).forEach(h=>{
      const bad = data.filter(r=>r[h]&&!re.test(r[h])).length;
      if (bad) { issues.push({lvl:'r',msg:`Column <strong>${h}</strong>: ${bad} invalid email format${bad>1?'s':''}`}); addLog('warn',`EMAIL "${h}": ${bad} invalid`); }
    });
  }
  if (config.vPhone) {
    const re = /^[\+]?[\d\s\-\(\)]{7,15}$/;
    headers.filter(h=>/phone|mobile|tel/i.test(h)).forEach(h=>{
      const bad = data.filter(r=>r[h]&&!re.test(r[h])).length;
      if (bad) { issues.push({lvl:'y',msg:`Column <strong>${h}</strong>: ${bad} unusual phone format${bad>1?'s':''}`}); }
    });
  }
  if (config.vRange) {
    headers.forEach(h => {
      if (!isNumericCol(h,data)) return;
      const v = numericVals(h,data); if (v.length<4) return;
      const q1=percentile(v,25), q3=percentile(v,75), iqr=q3-q1;
      const out = v.filter(x=>x<q1-1.5*iqr||x>q3+1.5*iqr).length;
      if (out) { issues.push({lvl:'y',msg:`Column <strong>${h}</strong>: ${out} IQR outlier${out>1?'s':''}`}); addLog('warn',`OUTLIER "${h}": ${out}`); }
    });
  }
  if (!issues.length) { issues.push({lvl:'g',msg:'All validation checks passed — no issues found.'}); addLog('ok','All checks passed'); }

  // ── CLEAN ──
  addLog('info', 'Cleaning started');
  if (config.cTrim) { data=data.map(r=>{const o={};Object.entries(r).forEach(([k,v])=>{o[k]=typeof v==='string'?v.trim():v});return o}); addLog('ok','Trimmed whitespace'); }
  if (config.cDropNull) { const b=data.length; data=data.filter(r=>Object.values(r).some(v=>v!==''&&v!=null)); addLog('ok',`Dropped ${b-data.length} fully null rows`); }
  if (config.cDedupe)   { const seen=new Set();const b=data.length; data=data.filter(r=>{const k=JSON.stringify(Object.values(r));if(seen.has(k))return false;seen.add(k);return true}); addLog('ok',`Removed ${b-data.length} duplicates`); }
  if (config.cFillMean) {
    headers.forEach(h=>{
      if(!isNumericCol(h,data))return;
      const v=numericVals(h,data);if(!v.length)return;
      const m=mean(v);let n=0;
      data=data.map(r=>{if(r[h]===''||r[h]==null||isNaN(Number(r[h]))){n++;return{...r,[h]:m.toFixed(4)}}return r});
      if(n)addLog('ok',`Fill mean "${h}": ${n} cells → ${m.toFixed(3)}`);
    });
  }
  if (config.cFillMode) {
    headers.forEach(h=>{
      if(isNumericCol(h,data))return;
      const v=data.map(r=>r[h]).filter(x=>x!==''&&x!=null);if(!v.length)return;
      const m=mode(v);let n=0;
      data=data.map(r=>{if(r[h]===''||r[h]==null){n++;return{...r,[h]:m}}return r});
      if(n)addLog('ok',`Fill mode "${h}": ${n} cells → "${m}"`);
    });
  }
  if (config.cNormHdr) {
    const newH=headers.map(h=>h.trim().replace(/\s+/g,'_').replace(/[^a-zA-Z0-9_]/g,'').toLowerCase());
    data=data.map(r=>{const o={};headers.forEach((h,i)=>{o[newH[i]]=r[h]});return o});
    headers=newH; addLog('ok','Normalized headers');
  }
  if (config.cLower)   { headers.forEach(h=>{if(isNumericCol(h,data))return;data=data.map(r=>({...r,[h]:typeof r[h]==='string'?r[h].toLowerCase():r[h]}))}); addLog('ok','Lowercased text'); }
  if (config.cSpecial) { headers.forEach(h=>{if(isNumericCol(h,data))return;data=data.map(r=>({...r,[h]:typeof r[h]==='string'?r[h].replace(/[^\w\s@.\-]/g,''):r[h]}))}); addLog('ok','Stripped special chars'); }

  // ── STATISTICS ──
  addLog('info','Computing statistics');
  headers.forEach(h=>{
    if(!isNumericCol(h,data))return;
    const v=numericVals(h,data);if(!v.length)return;
    const s={};
    if(config.sMean)   s.mean      = +mean(v).toFixed(4);
    if(config.sMedian) s.median    = +median(v).toFixed(4);
    if(config.sStd)    s.std       = +stdev(v).toFixed(4);
    if(config.sVar)    s.variance  = +variance(v).toFixed(4);
    if(config.sMinMax){s.min=Math.min(...v);s.max=Math.max(...v);}
    if(config.sSkew)   s.skewness  = +skewness(v).toFixed(4);
    if(config.sCount)  s.nulls     = data.filter(r=>r[h]===''||r[h]==null||isNaN(Number(r[h]))).length;
    s.p25 = +percentile(v,25).toFixed(4);
    s.p75 = +percentile(v,75).toFixed(4);
    statsOut[h]=s;
  });

  let corrMatrix = null;
  if (config.sCorr) {
    const numH=Object.keys(statsOut);
    if(numH.length>=2){
      const colData={};numH.forEach(h=>{colData[h]=numericVals(h,data)});
      const minL=Math.min(...numH.map(h=>colData[h].length));
      corrMatrix={
        columns:numH,
        matrix:numH.map(h1=>numH.map(h2=>{
          const a=colData[h1].slice(0,minL),b=colData[h2].slice(0,minL);
          const ma=mean(a),mb=mean(b);
          const num=a.reduce((s,v,i)=>s+(v-ma)*(b[i]-mb),0);
          const den=Math.sqrt(a.reduce((s,v)=>s+(v-ma)**2,0)*b.reduce((s,v)=>s+(v-mb)**2,0));
          return den===0?0:+(num/den).toFixed(4);
        })),
      };
    }
  }
  addLog('ok',`Stats for ${Object.keys(statsOut).length} numeric columns`);

  // ── TRANSFORM ──
  addLog('info','Transformations started');
  const numCols  = Array.isArray(config.numCols) ? config.numCols : [];
  const catCols  = Array.isArray(config.catCols) ? config.catCols : [];
  const binCount = Math.max(2,Math.min(50,config.binCount||5));

  if(config.tZscore)  numCols.forEach(h=>{if(!isNumericCol(h,data))return;const v=numericVals(h,data),m=mean(v),s=stdev(v);if(s===0)return;data=data.map(r=>({...r,[h+'_zscore']:+((Number(r[h])-m)/s).toFixed(6)}));addLog('ok',`Z-score "${h}"`)});
  if(config.tMinMax)  numCols.forEach(h=>{if(!isNumericCol(h,data))return;const v=numericVals(h,data),mn=Math.min(...v),mx=Math.max(...v),rng=mx-mn;if(rng===0)return;data=data.map(r=>({...r,[h+'_scaled']:+((Number(r[h])-mn)/rng).toFixed(6)}));addLog('ok',`Min-Max "${h}"`)});
  if(config.tLog)     numCols.forEach(h=>{if(!isNumericCol(h,data))return;data=data.map(r=>{const v=Number(r[h]);return{...r,[h+'_log']:v>0?+Math.log(v).toFixed(6):null}});addLog('ok',`Log "${h}"`)});
  if(config.tSqrt)    numCols.forEach(h=>{if(!isNumericCol(h,data))return;data=data.map(r=>{const v=Number(r[h]);return{...r,[h+'_sqrt']:v>=0?+Math.sqrt(v).toFixed(6):null}});addLog('ok',`Sqrt "${h}"`)});
  if(config.tBin) {
    numCols.forEach(h=>{
      if(!isNumericCol(h,data))return;
      const v=numericVals(h,data),mn=Math.min(...v),mx=Math.max(...v),step=(mx-mn)/binCount;
      data=data.map(r=>({...r,[h+'_bin']:`bin_${Math.min(Math.floor((Number(r[h])-mn)/step),binCount-1)+1}`}));
      addLog('ok',`Bin "${h}" → ${binCount} bins`);
    });
  }
  if(config.tLabel) catCols.forEach(h=>{const u=[...new Set(data.map(r=>r[h]))].sort();const map={};u.forEach((v,i)=>{map[v]=i});data=data.map(r=>({...r,[h+'_label']:map[r[h]]??''}));addLog('ok',`Label encode "${h}" (${u.length} classes)`)});
  if(config.tFreq)  catCols.forEach(h=>{const f={};data.forEach(r=>{f[r[h]]=(f[r[h]]||0)+1});data=data.map(r=>({...r,[h+'_freq']:+(f[r[h]]/data.length).toFixed(4)}));addLog('ok',`Freq encode "${h}"`)});
  if(config.tOneHot) {
    catCols.forEach(h=>{
      const u=[...new Set(data.map(r=>r[h]))].sort();
      if(u.length>100){addLog('warn',`Skip one-hot "${h}": ${u.length} unique > 100`);return}
      data=data.map(r=>{const o={...r};u.forEach(v=>{o[`${h}_${v}`]=r[h]===v?1:0});return o});
      addLog('ok',`One-hot "${h}" → ${u.length} columns`);
    });
  }

  addLog('info','Pipeline complete');
  const finalHeaders = data.length ? Object.keys(data[0]) : headers;
  const csvOut = stringify(data, { header:true, columns:finalHeaders });

  return { originalRows: records.length, transformedRows: data.length, columns: finalHeaders, preview: data.slice(0,50), validation: issues, statistics: statsOut, corrMatrix, schema, log, csvOut };
}

// ── ROUTES ────────────────────────────────────────────────

app.get('/api/health', (req, res) => {
  res.json({ status:'ok', uptime:process.uptime(), uptimeHuman: fmtUptime(process.uptime()), ...serverStats, tokensCached: tokenStore.size, ts: Date.now() });
});

// Schema-only endpoint (fast — no full processing)
app.post('/api/schema', rateLimit, upload.single('file'), (req, res) => {
  serverStats.requests++;
  try {
    if (!req.file) return res.status(400).json({ error:'No file uploaded' });
    const text    = req.file.buffer.toString('utf-8');
    const delim   = detectDelimiter(text);
    const records = parse(text, { delimiter:delim, columns:true, skip_empty_lines:true, trim:true, relax_quotes:true, relax_column_count:true, to:500 });
    if (!records.length) return res.status(422).json({ error:'No data rows found' });
    const headers = Object.keys(records[0]);
    const schema  = inferSchema(headers, records);
    res.json({ ok:true, rows: records.length, columns: headers.length, delimiter: delim, schema });
  } catch (err) {
    serverStats.errors++;
    res.status(422).json({ error: err.message });
  }
});

app.post('/api/process', rateLimit, upload.single('file'), (req, res) => {
  serverStats.requests++;
  try {
    if (!req.file) return res.status(400).json({ error:'No file uploaded' });
    let config = {};
    try { config = JSON.parse(req.body.config || '{}'); } catch(_) {}

    const rawText = req.file.buffer.toString('utf-8');
    const result  = runPipeline(rawText, config);
    serverStats.processed++;

    const filename = (req.file.originalname.replace(/\.[^.]+$/,'')||'data') + '_transformed.csv';
    const token    = storeDownload(result.csvOut, filename);

    res.json({ ok:true, originalRows:result.originalRows, transformedRows:result.transformedRows, columns:result.columns, preview:result.preview, validation:result.validation, statistics:result.statistics, corrMatrix:result.corrMatrix, schema:result.schema, log:result.log, downloadToken:token });
  } catch (err) {
    serverStats.errors++;
    console.error('[/api/process]', err.message);
    res.status(422).json({ error: err.message });
  }
});

app.get('/api/download/:token', (req, res) => {
  const entry = tokenStore.get(req.params.token);
  if (!entry)               return res.status(404).json({ error:'Token not found or expired' });
  if (entry.expires < Date.now()) { tokenStore.delete(req.params.token); return res.status(410).json({ error:'Token expired' }); }
  res.setHeader('Content-Type','text/csv');
  res.setHeader('Content-Disposition',`attachment; filename="${entry.filename}"`);
  res.send(entry.csv);
});

// SPA fallback
app.get('*', (req, res) => res.sendFile(path.join(__dirname,'index.html')));

// Multer / general error handler
app.use((err, req, res, next) => {
  if (err.code === 'LIMIT_FILE_SIZE') return res.status(413).json({ error:`File too large (max ${MAX_MB} MB)` });
  res.status(400).json({ error: err.message });
});

// ── HELPERS ───────────────────────────────────────────────
function fmtUptime(s) {
  const h = Math.floor(s/3600), m = Math.floor((s%3600)/60), sec = Math.floor(s%60);
  return `${h}h ${m}m ${sec}s`;
}

// ── START ─────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`\n  Veriflow v2.1\n  → http://localhost:${PORT}\n  → Max upload : ${MAX_MB} MB\n  → Token TTL  : ${TOKEN_TTL/60000} min\n  → Rate limit : ${RPM} req/min\n  → CORS       : ${ORIGINS.join(', ')}\n`);
});
