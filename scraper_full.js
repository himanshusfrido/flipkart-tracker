/**
 * Flipkart Product Tracker - Full Scraper
 * Scrapes product data for all FSNs across 4 pincodes, writes to Google Sheets & CSV backup
 */

const puppeteer = require('puppeteer-core');
const fs = require('fs');
const path = require('path');
const https = require('https');
const crypto = require('crypto');
const delay = ms => new Promise(r => setTimeout(r, ms));

// ─── CONFIG ─────────────────────────────────────────────────────────────────
const SPREADSHEET_ID = '1QKqHqi8iB_pDsYHxuKtCLzfiQfXa--aXHb_NNcqSK0A';
const PINCODES = ['110001', '560001', '400098', '411045'];
const CITY_NAMES = ['Delhi', 'Bangalore', 'Mumbai', 'Pune'];
const CHROME_PATH = 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe';
const FSN_FILE = path.join(__dirname, 'FSN_LIST.txt');
const BATCH_SIZE = 10;
const HEADLESS = true;
const LOG_DIR = path.join(__dirname, 'logs');
const CSV_DIR = path.join(__dirname, 'csv_backups');

// ─── LOGGING ────────────────────────────────────────────────────────────────
if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });
if (!fs.existsSync(CSV_DIR)) fs.mkdirSync(CSV_DIR, { recursive: true });

const now = new Date();
const dateStr = now.toISOString().replace(/[:.]/g, '-').substring(0, 19);
const logFile = path.join(LOG_DIR, `scrape_${dateStr}.log`);
const logStream = fs.createWriteStream(logFile, { flags: 'a' });

function log(msg) {
  const ts = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
  const line = `[${ts}] ${msg}`;
  console.log(line);
  logStream.write(line + '\n');
}

function logError(msg, err) {
  const ts = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
  const line = `[${ts}] ERROR: ${msg} ${err ? '- ' + (err.message || err) : ''}`;
  console.error(line);
  logStream.write(line + '\n');
}

// ─── GOOGLE SHEETS AUTH (Service Account JWT) ──────────────────────────────
let cachedToken = null;
let tokenExpiry = 0;

function base64url(data) {
  return Buffer.from(data).toString('base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

function readServiceAccount() {
  const keyPath = path.join(__dirname, 'service-account-key.json');
  if (!fs.existsSync(keyPath)) {
    throw new Error(`Service account key not found at ${keyPath}. See Step 1 in the setup plan.`);
  }
  return JSON.parse(fs.readFileSync(keyPath, 'utf8'));
}

function createSignedJWT(email, privateKey) {
  const now = Math.floor(Date.now() / 1000);
  const header = base64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const payload = base64url(JSON.stringify({
    iss: email,
    scope: 'https://www.googleapis.com/auth/spreadsheets',
    aud: 'https://oauth2.googleapis.com/token',
    iat: now,
    exp: now + 3600
  }));
  const signInput = header + '.' + payload;
  const signature = crypto.createSign('RSA-SHA256').update(signInput).sign(privateKey, 'base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
  return signInput + '.' + signature;
}

function exchangeJWTForToken(jwt) {
  return new Promise((resolve, reject) => {
    const postData = new URLSearchParams({
      grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
      assertion: jwt
    }).toString();

    const req = https.request({
      hostname: 'oauth2.googleapis.com',
      path: '/token',
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(postData)
      }
    }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try {
          const p = JSON.parse(data);
          if (p.access_token) {
            cachedToken = p.access_token;
            tokenExpiry = Date.now() + ((p.expires_in || 3600) * 1000 - 120000);
            resolve(p.access_token);
          } else {
            reject(new Error('JWT token exchange failed: ' + data.substring(0, 300)));
          }
        } catch (e) {
          reject(new Error('Token parse error: ' + e.message));
        }
      });
    });
    req.on('error', reject);
    req.write(postData);
    req.end();
  });
}

async function getAccessToken() {
  if (cachedToken && Date.now() < tokenExpiry) {
    return cachedToken;
  }
  const sa = readServiceAccount();
  const jwt = createSignedJWT(sa.client_email, sa.private_key);
  return exchangeJWTForToken(jwt);
}

// ─── SHEETS API ─────────────────────────────────────────────────────────────
function sheetsRequest(method, apiPath, body, token) {
  return new Promise((resolve, reject) => {
    const bodyStr = body ? JSON.stringify(body) : null;
    const headers = {
      'Authorization': 'Bearer ' + token,
      'Content-Type': 'application/json'
    };
    if (bodyStr) headers['Content-Length'] = Buffer.byteLength(bodyStr);

    const req = https.request({
      hostname: 'sheets.googleapis.com',
      path: apiPath,
      method,
      headers
    }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        if (res.statusCode >= 400) {
          reject(new Error(`Sheets API ${res.statusCode}: ${data.substring(0, 300)}`));
        } else {
          try {
            resolve(JSON.parse(data || '{}'));
          } catch (e) {
            resolve({});
          }
        }
      });
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('Request timeout')); });
    if (bodyStr) req.write(bodyStr);
    req.end();
  });
}

async function writeBatchToSheet(rows, startRow, token) {
  const endRow = startRow + rows.length - 1;
  const range = `Tracker!A${startRow}:R${endRow}`;
  const encodedRange = encodeURIComponent(range);
  const apiPath = `/v4/spreadsheets/${SPREADSHEET_ID}/values/${encodedRange}?valueInputOption=RAW`;

  try {
    await sheetsRequest('PUT', apiPath, { range, values: rows }, token);
    log(`  >> Sheet: wrote rows ${startRow}-${endRow}`);
    return true;
  } catch (e) {
    logError(`Sheet write failed for rows ${startRow}-${endRow}`, e);
    // Try refreshing token and retry once
    try {
      const newToken = await refreshAccessToken(readClaspTokens());
      await sheetsRequest('PUT', apiPath, { range, values: rows }, newToken);
      log(`  >> Sheet: wrote rows ${startRow}-${endRow} (after token refresh)`);
      return true;
    } catch (e2) {
      logError('Sheet retry also failed', e2);
      return false;
    }
  }
}

async function setupSheetHeaders(token) {
  const headers = [
    'FSN', 'Product Name', 'MRP', 'Selling Price',
    'Delhi Stock', 'Delhi Delivery', 'Delhi Del. Cost',
    'Bangalore Stock', 'Bangalore Delivery', 'Bangalore Del. Cost',
    'Mumbai Stock', 'Mumbai Delivery', 'Mumbai Del. Cost',
    'Pune Stock', 'Pune Delivery', 'Pune Del. Cost',
    'Last Checked'
  ];
  const range = 'Tracker!A1:Q1';
  const encodedRange = encodeURIComponent(range);
  try {
    await sheetsRequest('PUT',
      `/v4/spreadsheets/${SPREADSHEET_ID}/values/${encodedRange}?valueInputOption=RAW`,
      { range, values: [headers] }, token
    );
    log('Sheet headers written');
  } catch (e) {
    logError('Could not write sheet headers', e);
  }
}

// ─── FLIPKART SCRAPER (tested & confirmed working) ─────────────────────────
async function scrapeProduct(page, fsn, pincode) {
  try {
    const url = `https://www.flipkart.com/product/p/itm?pid=${fsn}`;
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 45000 });
    await delay(2500);

    // Close login popup
    try {
      const btns = await page.$$('button');
      for (const b of btns) {
        const t = await page.evaluate(el => el.textContent, b);
        if (t.includes('\u2715')) { await b.click(); break; }
      }
    } catch (e) {}
    await delay(500);

    // Click "Select delivery location" to open pincode popup
    try {
      const links = await page.$$('a, div, span');
      for (const el of links) {
        const text = await page.evaluate(e => e.textContent.trim(), el);
        if (text === 'Select delivery location' || text === 'Location not setSelect delivery location') {
          await el.click();
          await delay(2000);
          break;
        }
      }

      // Find pincode input in popup
      const allInputs = await page.$$('input');
      for (const inp of allInputs) {
        const ph = await page.evaluate(e => e.placeholder, inp);
        if (ph && (ph.toLowerCase().includes('pincode') || ph.toLowerCase().includes('pin code') || ph.includes('Enter'))) {
          await inp.click({ clickCount: 3 });
          await inp.type(pincode, { delay: 30 });
          await delay(500);
          const btns2 = await page.$$('button, span');
          for (const b of btns2) {
            const t = await page.evaluate(e => e.textContent.trim(), b);
            if (t === 'Apply' || t === 'Check' || t === 'Submit') { await b.click(); break; }
          }
          await delay(3000);
          break;
        }
      }
    } catch (e) {
      log(`  Pincode entry issue for ${fsn}/${pincode}: ${e.message}`);
    }

    // Extract data
    const data = await page.evaluate(() => {
      const bt = document.body.innerText;

      // Product name
      let name = 'N/A';
      const lines = bt.split('\n').map(l => l.trim()).filter(l => l.length > 10 && l.length < 300);
      for (let i = 0; i < lines.length; i++) {
        if (lines[i].includes('Visit store') && i + 1 < lines.length) {
          name = lines[i + 1];
          break;
        }
      }
      // Fallback: title-based selectors
      if (name === 'N/A') {
        const titleEl = document.querySelector('[class*="VU-ZEz"]') || document.querySelector('[class*="G6XhRU"]');
        if (titleEl) name = titleEl.textContent.trim();
      }

      // Price
      let sp = 'N/A', mrp = 'N/A';
      const dm = bt.match(/(\d+)%\n([\d,]+)\n\u20B9([\d,]+)/);
      if (dm) {
        mrp = '\u20B9' + dm[2];
        sp = '\u20B9' + dm[3];
      } else {
        const dm2 = bt.match(/(\d+)%\s*[\n\r]*([\d,]+)\s*[\n\r]*\u20B9([\d,]+)/);
        if (dm2) {
          mrp = '\u20B9' + dm2[2];
          sp = '\u20B9' + dm2[3];
        } else {
          const pm = bt.match(/\u20B9([\d,]+)/);
          if (pm) sp = '\u20B9' + pm[1];
        }
      }

      // Availability
      const oos = bt.includes('Currently unavailable') || bt.includes('Sold Out') || bt.includes('Out of stock');

      // Delivery info
      let dd = 'N/A', dc = 'N/A';
      const dlm = bt.match(/Delivery\s*by\s+(\d+\s+\w+(?:,\s*\w+)?)/i) ||
                  bt.match(/Delivered\s*by\s+(\d+\s+\w+(?:,\s*\w+)?)/i) ||
                  bt.match(/Get it by\s+(\d+\s+\w+)/i);
      if (dlm) dd = dlm[1].trim();

      if (bt.includes('not serviceable') || bt.includes('Cannot be delivered')) {
        dd = 'Not Serviceable';
      }
      if (bt.includes('FREE Delivery') || bt.includes('Free Delivery') || bt.includes('Free delivery')) {
        dc = 'FREE';
      } else {
        const c = bt.match(/\u20B9(\d+)\s*delivery/i);
        if (c) dc = '\u20B9' + c[1];
      }

      return { name: name.substring(0, 150), mrp, sp, available: !oos, dd, dc };
    });

    return data;
  } catch (e) {
    logError(`Scrape error ${fsn}/${pincode}`, e);
    return { name: 'Error', mrp: 'N/A', sp: 'N/A', available: false, dd: 'Error', dc: 'N/A' };
  }
}

// ─── CSV HELPERS ────────────────────────────────────────────────────────────
function escapeCSV(val) {
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

// ─── MAIN ───────────────────────────────────────────────────────────────────
async function main() {
  const startTime = Date.now();
  log('========================================');
  log('  Flipkart Product Tracker - Full Run');
  log('========================================');
  log(`Start time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`);

  // Check for dry-run mode (--dry-run or --test flags)
  const isDryRun = process.argv.includes('--dry-run') || process.argv.includes('--test');
  const maxFSNs = isDryRun ? 1 : Infinity;
  if (isDryRun) log('*** DRY RUN MODE - processing only 1 FSN ***');

  // Load FSNs
  if (!fs.existsSync(FSN_FILE)) {
    logError(`FSN file not found: ${FSN_FILE}`);
    process.exit(1);
  }
  let fsns = fs.readFileSync(FSN_FILE, 'utf8').trim().split(/\r?\n/).map(f => f.trim()).filter(Boolean);
  log(`Total FSNs loaded: ${fsns.length}`);
  if (maxFSNs < fsns.length) fsns = fsns.slice(0, maxFSNs);
  log(`FSNs to process: ${fsns.length}`);
  log(`Pincodes: ${CITY_NAMES.map((c, i) => c + ' (' + PINCODES[i] + ')').join(', ')}`);

  // Get Google Sheets token
  let token = null;
  let sheetsEnabled = false;
  try {
    token = await getAccessToken();
    log('Google Sheets: authenticated successfully');
    sheetsEnabled = true;
  } catch (e) {
    logError('Google Sheets auth failed - will save CSV only', e);
  }

  // Write sheet headers if authenticated
  if (sheetsEnabled) {
    await setupSheetHeaders(token);
  }

  // Prepare CSV file
  const csvFile = path.join(CSV_DIR, `flipkart_${dateStr}.csv`);
  const csvHeaders = [
    'FSN', 'Product Name', 'MRP', 'Selling Price',
    ...CITY_NAMES.flatMap(c => [`${c} Stock`, `${c} Delivery`, `${c} Del. Cost`]),
    'Last Checked'
  ];
  fs.writeFileSync(csvFile, csvHeaders.map(escapeCSV).join(',') + '\n', 'utf8');
  log(`CSV backup: ${csvFile}`);

  // Launch browser
  log('Launching Chrome (headless)...');
  let browser;
  try {
    browser = await puppeteer.launch({
      executablePath: CHROME_PATH,
      headless: HEADLESS ? 'new' : false,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
        '--window-size=1366,768',
        '--disable-extensions',
        '--disable-background-timer-throttling',
        '--disable-backgrounding-occluded-windows',
        '--disable-renderer-backgrounding'
      ]
    });
    log('Chrome launched successfully');
  } catch (e) {
    logError('Failed to launch Chrome', e);
    process.exit(1);
  }

  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
  await page.setViewport({ width: 1366, height: 768 });
  // Block images and fonts to speed up loading
  await page.setRequestInterception(true);
  page.on('request', (req) => {
    const rt = req.resourceType();
    if (rt === 'image' || rt === 'font' || rt === 'media') {
      req.abort();
    } else {
      req.continue();
    }
  });

  // Processing loop
  let batchRows = [];
  let totalProcessed = 0;
  let totalErrors = 0;
  let sheetRowCursor = 2; // row 1 = header, data starts at row 2

  for (let i = 0; i < fsns.length; i++) {
    const fsn = fsns[i];
    log(`[${i + 1}/${fsns.length}] Processing FSN: ${fsn}`);

    const row = [fsn];
    let productName = 'N/A';

    for (let p = 0; p < PINCODES.length; p++) {
      let retries = 2;
      let data = null;
      while (retries > 0) {
        try {
          data = await scrapeProduct(page, fsn, PINCODES[p]);
          break;
        } catch (e) {
          retries--;
          logError(`  Retry remaining: ${retries} for ${fsn}/${PINCODES[p]}`, e);
          if (retries > 0) await delay(3000);
        }
      }
      if (!data) {
        data = { name: 'Error', mrp: 'N/A', sp: 'N/A', available: false, dd: 'Error', dc: 'N/A' };
        totalErrors++;
      }

      // Use product info from first pincode
      if (p === 0) {
        productName = data.name;
        row.push(productName, data.mrp, data.sp);
      }

      row.push(
        data.available ? 'In Stock' : 'Out of Stock',
        data.dd,
        data.dc
      );

      log(`  ${CITY_NAMES[p]} (${PINCODES[p]}): ${data.available ? 'In Stock' : 'OOS'} | Del: ${data.dd} | Cost: ${data.dc}`);
    }

    // Add timestamp
    const checkedAt = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
    row.push(checkedAt);

    // Append to CSV immediately
    fs.appendFileSync(csvFile, row.map(escapeCSV).join(',') + '\n', 'utf8');

    batchRows.push(row);
    totalProcessed++;

    // Write to Google Sheet in batches of BATCH_SIZE or at the end
    if (sheetsEnabled && (batchRows.length >= BATCH_SIZE || i === fsns.length - 1)) {
      try {
        // Refresh token if needed
        token = await getAccessToken();
        const success = await writeBatchToSheet(batchRows, sheetRowCursor, token);
        if (success) {
          sheetRowCursor += batchRows.length;
        } else {
          log('  >> Sheet write failed, data saved in CSV');
        }
      } catch (e) {
        logError('Batch sheet write error', e);
      }
      batchRows = [];
    }

    // Small delay between FSNs to be polite
    if (i < fsns.length - 1) {
      await delay(1000);
    }
  }

  // Cleanup
  await browser.close();
  log('Chrome closed');

  // Summary
  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  log('========================================');
  log('  Run Complete');
  log('========================================');
  log(`Total FSNs processed: ${totalProcessed}/${fsns.length}`);
  log(`Errors: ${totalErrors}`);
  log(`Time elapsed: ${elapsed} minutes`);
  log(`CSV backup: ${csvFile}`);
  log(`Log file: ${logFile}`);
  if (sheetsEnabled) {
    log(`Google Sheet: https://docs.google.com/spreadsheets/d/${SPREADSHEET_ID}`);
  }
  log('========================================');

  logStream.end();
}

main().catch(e => {
  logError('FATAL ERROR', e);
  logStream.end();
  process.exit(1);
});
