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
const FSN_CAT_FILE = path.join(__dirname, 'FSN_CATEGORIES.csv');
const BATCH_SIZE = 10;
const MAX_RETRIES = 3;
const RETRY_DELAYS = [5000, 15000, 30000];
const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
];
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
  const range = `Tracker!A${startRow}:M${endRow}`;
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
      const newToken = await getAccessToken();
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
    'Delhi Stock', 'Delhi Delivery',
    'Bangalore Stock', 'Bangalore Delivery',
    'Mumbai Stock', 'Mumbai Delivery',
    'Pune Stock', 'Pune Delivery',
    'Last Checked'
  ];
  const range = `Tracker!A1:M1`;
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

// ─── CATEGORY HELPERS ────────────────────────────────────────────────────────

function prettifyCategory(cat) {
  return cat.split('_').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
}

function loadFSNCategories() {
  if (!fs.existsSync(FSN_CAT_FILE)) return null;
  const lines = fs.readFileSync(FSN_CAT_FILE, 'utf8').trim().split('\n');
  const map = {};
  const catFSNs = {};
  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i].split(',');
    if (parts.length >= 2) {
      const fsn = parts[0].trim();
      const tabName = prettifyCategory(parts[1].trim());
      map[fsn] = tabName;
      if (!catFSNs[tabName]) catFSNs[tabName] = [];
      catFSNs[tabName].push(fsn);
    }
  }
  return { map, catFSNs };
}

async function ensureCategoryTabs(catNames, token) {
  const meta = await sheetsRequest('GET',
    `/v4/spreadsheets/${SPREADSHEET_ID}?fields=sheets.properties.title`,
    null, token
  );
  const existing = new Set((meta.sheets || []).map(s => s.properties.title));
  const toCreate = catNames.filter(n => !existing.has(n));
  if (toCreate.length > 0) {
    const requests = toCreate.map(title => ({
      addSheet: { properties: { title } }
    }));
    await sheetsRequest('POST',
      `/v4/spreadsheets/${SPREADSHEET_ID}:batchUpdate`,
      { requests }, token
    );
    log(`Created ${toCreate.length} category tabs: ${toCreate.join(', ')}`);
  }

  const headers = [
    'FSN', 'Product Name', 'MRP', 'Selling Price',
    'Delhi Stock', 'Delhi Delivery', 'Bangalore Stock', 'Bangalore Delivery',
    'Mumbai Stock', 'Mumbai Delivery', 'Pune Stock', 'Pune Delivery', 'Last Checked'
  ];
  const headerData = catNames.map(tab => ({
    range: `'${tab}'!A1:M1`, values: [headers]
  }));
  headerData.push({ range: 'Tracker!A1:M1', values: [headers] });
  await sheetsRequest('POST',
    `/v4/spreadsheets/${SPREADSHEET_ID}/values:batchUpdate`,
    { valueInputOption: 'RAW', data: headerData }, token
  );
  log('Headers written to all tabs');
}

async function addRedFormatting(token) {
  const meta = await sheetsRequest('GET',
    `/v4/spreadsheets/${SPREADSHEET_ID}?fields=sheets.properties`,
    null, token
  );
  const sheets = (meta.sheets || []).map(s => s.properties);
  const stockCols = [4, 6, 8, 10];
  const requests = [];
  for (const sheet of sheets) {
    for (const col of stockCols) {
      requests.push({
        addConditionalFormatRule: {
          rule: {
            ranges: [{
              sheetId: sheet.sheetId, startRowIndex: 1,
              startColumnIndex: col, endColumnIndex: col + 1
            }],
            booleanRule: {
              condition: { type: 'TEXT_EQ', values: [{ userEnteredValue: 'Out of Stock' }] },
              format: {
                backgroundColor: { red: 1, green: 0.8, blue: 0.8 },
                textFormat: { foregroundColor: { red: 0.8, green: 0, blue: 0 }, bold: true }
              }
            }
          },
          index: 0
        }
      });
    }
  }
  if (requests.length > 0) {
    await sheetsRequest('POST',
      `/v4/spreadsheets/${SPREADSHEET_ID}:batchUpdate`,
      { requests }, token
    );
    log(`Red formatting applied to ${sheets.length} tabs`);
  }
}

// ─── FLIPKART SCRAPER ────────────────────────────────────────────────────────

// Load a product page once, extract product info (name, MRP, SP) from JSON-LD
async function loadProductPage(page, fsn) {
  try {
    await page.goto(`https://www.flipkart.com/product/p/itm?pid=${fsn}`, { waitUntil: 'networkidle2', timeout: 45000 });
    await delay(1500);

    // Close login popup
    try {
      const btns = await page.$$('button');
      for (const b of btns) {
        const t = await page.evaluate(el => el.textContent, b);
        if (t.includes('\u2715')) { await b.click(); break; }
      }
    } catch (e) {}
    await delay(300);

    // Extract product info from JSON-LD structured data (most reliable)
    const info = await page.evaluate(() => {
      let name = 'N/A', mrp = 'N/A', sp = 'N/A', available = true;

      // Try JSON-LD first
      const scripts = document.querySelectorAll('script[type="application/ld+json"]');
      for (const s of scripts) {
        try {
          const data = JSON.parse(s.textContent);
          const product = Array.isArray(data) ? data[0] : data;
          if (product['@type'] === 'Product' && product.name) {
            name = product.name;
            if (product.offers) {
              const offer = product.offers;
              if (offer.price) sp = '\u20B9' + Number(offer.price).toLocaleString('en-IN');
              if (offer.availability && offer.availability.includes('OutOfStock')) available = false;
            }
            break;
          }
        } catch (e) {}
      }

      // Get MRP from body text (JSON-LD only has selling price)
      // Skip AD blocks — find the price section after "Selected Color" or brand name
      const bt = document.body.innerText;
      const colorIdx = bt.indexOf('Selected Color');
      const priceText = colorIdx > -1 ? bt.substring(colorIdx) : bt;
      const dm = priceText.match(/(\d+)%\s+([\d,]+)\s+\u20B9([\d,]+)/);
      if (dm) {
        mrp = '\u20B9' + dm[2];
        if (sp === 'N/A') sp = '\u20B9' + dm[3];
      }

      // Fallback: name from body text
      if (name === 'N/A') {
        const lines = bt.split('\n').map(l => l.trim()).filter(l => l.length > 10 && l.length < 300);
        for (let i = 0; i < lines.length; i++) {
          if (lines[i].includes('Visit store') && i + 1 < lines.length) { name = lines[i + 1]; break; }
        }
      }

      // Availability from body text
      if (bt.includes('Currently unavailable') || bt.includes('Sold Out') || bt.includes('Out of stock')) {
        available = false;
      }

      return { name: name.substring(0, 150), mrp, sp, available };
    });

    return info;
  } catch (e) {
    logError(`Page load error ${fsn}`, e);
    return { name: 'Error', mrp: 'N/A', sp: 'N/A', available: false };
  }
}

// Enter/change pincode and extract delivery info (without reloading the page)
async function checkPincode(page, pincode) {
  try {
    // Click delivery location link (single evaluate — no round-trips)
    await page.evaluate(() => {
      const els = document.querySelectorAll('a, div, span');
      for (const el of els) {
        const t = el.textContent.trim();
        if (t === 'Select delivery location' || t === 'Change' || t === 'Enter pincode') {
          el.click();
          break;
        }
      }
    });

    // Wait for pincode input to appear
    let inputFound = false;
    try {
      await page.waitForFunction(() => {
        const inputs = document.querySelectorAll('input');
        for (const inp of inputs) {
          const ph = (inp.placeholder || '').toLowerCase();
          if (ph.includes('pincode') || ph.includes('pin code') || ph.includes('enter')) return true;
        }
        return false;
      }, { timeout: 5000 });
      inputFound = true;
    } catch (e) {}

    if (inputFound) {
      // Fill pincode using evaluate (React-compatible native setter)
      await page.evaluate((pin) => {
        const inputs = document.querySelectorAll('input');
        for (const inp of inputs) {
          const ph = (inp.placeholder || '').toLowerCase();
          if (ph.includes('pincode') || ph.includes('pin code') || ph.includes('enter')) {
            const nativeSet = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
            nativeSet.call(inp, pin);
            inp.dispatchEvent(new Event('input', { bubbles: true }));
            inp.dispatchEvent(new Event('change', { bubbles: true }));
            break;
          }
        }
      }, pincode);
      await delay(500);

      // Click Apply/Check/Submit button
      await page.evaluate(() => {
        const btns = document.querySelectorAll('button, span');
        for (const b of btns) {
          const t = b.textContent.trim();
          if (t === 'Apply' || t === 'Check' || t === 'Submit') { b.click(); break; }
        }
      });
      await delay(2500);
    }

    // Extract delivery info — scoped to delivery section
    const delivery = await page.evaluate(() => {
      const bt = document.body.innerText;
      if (bt.includes('Currently unavailable') || bt.includes('Sold Out') || bt.includes('Out of stock')) {
        return { dd: 'N/A', available: false };
      }
      if (bt.includes('not serviceable') || bt.includes('Cannot be delivered') || bt.includes('delivery not available')) {
        return { dd: 'Not Serviceable', available: false };
      }

      let dd = 'N/A';
      const delivIdx = bt.indexOf('Delivery details');
      const scopedText = delivIdx > -1 ? bt.substring(delivIdx, delivIdx + 500) : bt;

      const patterns = [
        /Delivery\s+by\s+(\d+\s+\w+,?\s*\w*)/i,
        /Delivery\s*\n\s*by\s+(\d+\s+\w+,?\s*\w*)/i,
        /Delivered\s+by\s+(\d+\s+\w+,?\s*\w*)/i,
        /Get it by\s+(\d+\s+\w+,?\s*\w*)/i,
        /Delivery in\s+(\d+\s+\w+)/i
      ];
      for (const p of patterns) {
        const m = scopedText.match(p);
        if (m) { dd = m[1].trim().replace(/,?\s*(Mon|Tue|Wed|Thu|Fri|Sat|Sun)$/i, ''); break; }
      }
      if (dd === 'N/A') {
        for (const p of patterns) {
          const m = bt.match(p);
          if (m) { dd = m[1].trim().replace(/,?\s*(Mon|Tue|Wed|Thu|Fri|Sat|Sun)$/i, ''); break; }
        }
      }
      return { dd, available: true };
    });

    return delivery;
  } catch (e) {
    return { dd: 'Error', available: false };
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

  // Load category mapping
  const catData = loadFSNCategories();
  const useCategories = !!catData;
  if (useCategories) {
    const catNames = Object.keys(catData.catFSNs);
    log(`Categories loaded: ${catNames.length} (${Object.keys(catData.map).length} FSN mappings)`);
  } else {
    log('No FSN_CATEGORIES.csv found — writing to Tracker tab only');
  }

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

  // Setup category tabs + red formatting, or fallback to Tracker headers
  if (sheetsEnabled && useCategories) {
    try {
      await ensureCategoryTabs(Object.keys(catData.catFSNs), token);
      await addRedFormatting(token);
    } catch (e) {
      logError('Tab setup error', e);
    }
  } else if (sheetsEnabled) {
    await setupSheetHeaders(token);
  }

  // Pre-assign row numbers for each FSN (avoids race conditions with concurrent workers)
  const fsnRowMap = {};
  const catCounters = {};
  if (useCategories) {
    for (const tab of Object.keys(catData.catFSNs)) catCounters[tab] = 2;
  }
  for (let i = 0; i < fsns.length; i++) {
    const fsn = fsns[i];
    const catTab = useCategories ? catData.map[fsn] : null;
    fsnRowMap[fsn] = {
      trackerRow: 2 + i,
      catTab,
      catRow: (catTab && catCounters[catTab] !== undefined) ? catCounters[catTab]++ : null
    };
  }

  // Prepare CSV file
  const csvFile = path.join(CSV_DIR, `flipkart_${dateStr}.csv`);
  const csvHeaders = [
    'FSN', 'Product Name', 'MRP', 'Selling Price',
    ...CITY_NAMES.flatMap(c => [`${c} Stock`, `${c} Delivery`]),
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
  await page.setViewport({ width: 1366, height: 768 });
  await page.setRequestInterception(true);
  page.on('request', (req) => {
    const rt = req.resourceType();
    if (rt === 'image' || rt === 'font' || rt === 'media') req.abort();
    else req.continue();
  });

  let pendingWrites = [];
  let totalProcessed = 0;
  let totalErrors = 0;

  for (let i = 0; i < fsns.length; i++) {
    const fsn = fsns[i];
    log(`[${i + 1}/${fsns.length}] Processing FSN: ${fsn}`);

    // Rotate User-Agent
    await page.setUserAgent(USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)]);

    // Load page with exponential backoff retry
    let info = null;
    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      info = await loadProductPage(page, fsn);
      if (info.name !== 'Error') break;
      if (attempt < MAX_RETRIES) {
        const retryDelay = RETRY_DELAYS[attempt];
        log(`  Retry ${attempt + 1}/${MAX_RETRIES} after ${retryDelay / 1000}s...`);
        await page.goto('about:blank').catch(() => {});
        await delay(retryDelay);
      }
    }

    const row = [fsn, info.name, info.mrp, info.sp];

    // Check each pincode
    for (let p = 0; p < PINCODES.length; p++) {
      let delivery;
      try {
        delivery = await checkPincode(page, PINCODES[p]);
      } catch (e) {
        logError(`  Pincode error ${fsn}/${PINCODES[p]}`, e);
        delivery = { dd: 'Error', available: false };
        totalErrors++;
      }
      const inStock = info.available && delivery.available;
      row.push(inStock ? 'In Stock' : 'Out of Stock', delivery.dd);
      log(`  ${CITY_NAMES[p]}: ${inStock ? 'In Stock' : 'OOS'} | Del: ${delivery.dd}`);
    }

    row.push(new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }));
    totalProcessed++;

    // Append to CSV
    fs.appendFileSync(csvFile, row.map(escapeCSV).join(',') + '\n', 'utf8');

    // Queue sheet writes using pre-assigned rows
    if (sheetsEnabled) {
      const rowInfo = fsnRowMap[fsn];
      pendingWrites.push({
        range: `Tracker!A${rowInfo.trackerRow}:M${rowInfo.trackerRow}`,
        values: [row]
      });
      if (rowInfo.catTab && rowInfo.catRow) {
        pendingWrites.push({
          range: `'${rowInfo.catTab}'!A${rowInfo.catRow}:M${rowInfo.catRow}`,
          values: [row]
        });
      }

      if (pendingWrites.length >= BATCH_SIZE * 2 || i === fsns.length - 1) {
        try {
          token = await getAccessToken();
          await sheetsRequest('POST',
            `/v4/spreadsheets/${SPREADSHEET_ID}/values:batchUpdate`,
            { valueInputOption: 'RAW', data: pendingWrites }, token
          );
          log(`  >> Flushed ${pendingWrites.length} ranges to sheet`);
        } catch (e) {
          logError('Sheet write error', e);
        }
        pendingWrites = [];
      }
    }

    // Delay between FSNs: 3s + random 0-2s jitter
    if (i < fsns.length - 1) {
      const jitter = Math.floor(Math.random() * 2000);
      await delay(3000 + jitter);
    }
  }

  await browser.close();
  log('Chrome closed');

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
