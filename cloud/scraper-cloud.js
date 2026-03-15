/**
 * Flipkart Product Tracker - Cloud Version (GitHub Actions)
 *
 * Adapted from local scraper.js to run on GitHub Actions Ubuntu runners.
 * Key changes:
 *   - Uses full 'puppeteer' (not puppeteer-core) which bundles Chromium
 *   - Auth tokens come from GitHub Secrets (environment variables)
 *   - Supports chunk-based execution for parallel matrix jobs
 *   - Robust error handling with retry logic for cloud environment
 *
 * Usage:
 *   node scraper-cloud.js                  # Run all FSNs
 *   node scraper-cloud.js --chunk 0 --total-chunks 4  # Run chunk 0 of 4
 */

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

const fs = require('fs');
const https = require('https');
const path = require('path');
const crypto = require('crypto');
const delay = ms => new Promise(r => setTimeout(r, ms));

// ─── CONFIG ─────────────────────────────────────────────────
const SPREADSHEET_ID = '1QKqHqi8iB_pDsYHxuKtCLzfiQfXa--aXHb_NNcqSK0A';
const PINCODES = ['110001', '560001', '400098', '411045'];
const CITY_NAMES = ['Delhi', 'Bangalore', 'Mumbai', 'Pune'];
const FSN_FILE = path.join(__dirname, '..', 'FSN_LIST.txt');
const FSN_CAT_FILE = path.join(__dirname, '..', 'FSN_CATEGORIES.csv');
const BATCH_SIZE = 10;
const PAGE_TIMEOUT = 30000;
const MAX_RETRIES = 3;
const RETRY_DELAYS = [5000, 15000, 30000];
const USER_AGENTS = [
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
];

// ─── PARSE CLI ARGS ─────────────────────────────────────────
function parseArgs() {
  const args = process.argv.slice(2);
  const opts = { chunk: null, totalChunks: null };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--chunk') opts.chunk = parseInt(args[i + 1]);
    if (args[i] === '--total-chunks') opts.totalChunks = parseInt(args[i + 1]);
  }
  return opts;
}

// ─── GOOGLE SHEETS AUTH (Service Account JWT) ───────────────
function base64url(data) {
  return Buffer.from(data).toString('base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

function getServiceAccountConfig() {
  // Priority 1: GitHub Secret as env var (JSON string)
  if (process.env.GOOGLE_SERVICE_ACCOUNT_KEY) {
    try {
      return JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_KEY);
    } catch (e) {
      console.error('Failed to parse GOOGLE_SERVICE_ACCOUNT_KEY env var:', e.message);
    }
  }

  // Priority 2: Local key file (for testing)
  const keyPath = path.join(__dirname, '..', 'service-account-key.json');
  if (fs.existsSync(keyPath)) {
    return JSON.parse(fs.readFileSync(keyPath, 'utf8'));
  }

  console.warn('WARNING: No service account credentials found. Sheet updates will be skipped.');
  return null;
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

let cachedAccessToken = null;
let tokenExpiry = 0;

async function getAccessToken(saConfig) {
  if (!saConfig) return null;
  if (cachedAccessToken && Date.now() < tokenExpiry) return cachedAccessToken;

  return new Promise((resolve, reject) => {
    const jwt = createSignedJWT(saConfig.client_email, saConfig.private_key);
    const postData = new URLSearchParams({
      grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
      assertion: jwt
    }).toString();

    const req = https.request({
      hostname: 'oauth2.googleapis.com', path: '/token', method: 'POST',
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
            cachedAccessToken = p.access_token;
            tokenExpiry = Date.now() + ((p.expires_in || 3600) * 1000 - 120000);
            resolve(p.access_token);
          } else {
            console.error('JWT token exchange response:', data.substring(0, 300));
            reject(new Error('JWT token exchange failed'));
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

function sheetsAPI(method, apiPath, body, token) {
  return new Promise((resolve, reject) => {
    const bodyStr = body ? JSON.stringify(body) : null;
    const headers = {
      'Authorization': 'Bearer ' + token,
      'Content-Type': 'application/json'
    };
    if (bodyStr) headers['Content-Length'] = Buffer.byteLength(bodyStr);

    const req = https.request({
      hostname: 'sheets.googleapis.com', path: apiPath, method, headers
    }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        if (res.statusCode >= 400) {
          console.error(`Sheets API ${res.statusCode}:`, data.substring(0, 300));
          reject(new Error('Sheets API error ' + res.statusCode));
        } else {
          resolve(JSON.parse(data || '{}'));
        }
      });
    });
    req.on('error', reject);
    if (bodyStr) req.write(bodyStr);
    req.end();
  });
}

async function writeToSheet(tabName, rows, startRow, token) {
  const endRow = startRow + rows.length - 1;
  const range = `'${tabName}'!A${startRow}:M${endRow}`;
  await sheetsAPI('PUT',
    `/v4/spreadsheets/${SPREADSHEET_ID}/values/${encodeURIComponent(range)}?valueInputOption=RAW`,
    { range, values: rows },
    token
  );
  return endRow;
}

// ─── CATEGORY HELPERS ──────────────────────────────────────

function prettifyCategory(cat) {
  return cat.split('_').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
}

function loadFSNCategories() {
  if (!fs.existsSync(FSN_CAT_FILE)) return null;
  const lines = fs.readFileSync(FSN_CAT_FILE, 'utf8').trim().split('\n');
  const map = {}; // fsn -> category tab name
  const catFSNs = {}; // tab name -> [fsn, ...]
  for (let i = 1; i < lines.length; i++) { // skip header
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
  // Get existing sheet tabs
  const meta = await sheetsAPI('GET',
    `/v4/spreadsheets/${SPREADSHEET_ID}?fields=sheets.properties.title`,
    null, token
  );
  const existing = new Set((meta.sheets || []).map(s => s.properties.title));

  // Create missing tabs
  const toCreate = catNames.filter(n => !existing.has(n));
  if (toCreate.length > 0) {
    const requests = toCreate.map(title => ({
      addSheet: { properties: { title } }
    }));
    await sheetsAPI('POST',
      `/v4/spreadsheets/${SPREADSHEET_ID}:batchUpdate`,
      { requests }, token
    );
    console.log(`Created ${toCreate.length} category tabs: ${toCreate.join(', ')}`);
  }

  // Write headers to all category tabs
  const headers = [
    'FSN', 'Product Name', 'MRP', 'Selling Price',
    'Delhi Stock', 'Delhi Delivery',
    'Bangalore Stock', 'Bangalore Delivery',
    'Mumbai Stock', 'Mumbai Delivery',
    'Pune Stock', 'Pune Delivery',
    'Last Checked'
  ];
  const headerData = catNames.map(tab => ({
    range: `'${tab}'!A1:M1`,
    values: [headers]
  }));
  // Also write to Tracker tab
  headerData.push({ range: 'Tracker!A1:M1', values: [headers] });

  await sheetsAPI('POST',
    `/v4/spreadsheets/${SPREADSHEET_ID}/values:batchUpdate`,
    { valueInputOption: 'RAW', data: headerData }, token
  );
  console.log('Headers written to all tabs');
}

async function addRedFormatting(token) {
  // Get all sheet IDs
  const meta = await sheetsAPI('GET',
    `/v4/spreadsheets/${SPREADSHEET_ID}?fields=sheets.properties`,
    null, token
  );
  const sheets = (meta.sheets || []).map(s => s.properties);

  // Stock columns: E(4), G(6), I(8), K(10) — 0-indexed
  const stockCols = [4, 6, 8, 10];
  const requests = [];

  for (const sheet of sheets) {
    for (const col of stockCols) {
      requests.push({
        addConditionalFormatRule: {
          rule: {
            ranges: [{
              sheetId: sheet.sheetId,
              startRowIndex: 1,
              startColumnIndex: col,
              endColumnIndex: col + 1
            }],
            booleanRule: {
              condition: {
                type: 'TEXT_EQ',
                values: [{ userEnteredValue: 'Out of Stock' }]
              },
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
    await sheetsAPI('POST',
      `/v4/spreadsheets/${SPREADSHEET_ID}:batchUpdate`,
      { requests }, token
    );
    console.log(`Red formatting applied to ${sheets.length} tabs`);
  }
}

// ─── FLIPKART SCRAPER ──────────────────────────────────────

// Load a product page once, extract product info (name, MRP, SP) from JSON-LD
async function loadProductPage(page, fsn) {
  try {
    await page.goto(`https://www.flipkart.com/product/p/itm?pid=${fsn}`, { waitUntil: 'domcontentloaded', timeout: PAGE_TIMEOUT });
    // Wait for JSON-LD or product content to be available
    try {
      await page.waitForFunction(() => {
        return document.querySelector('script[type="application/ld+json"]') ||
               document.body.innerText.length > 500;
      }, { timeout: 10000 });
    } catch (e) {}
    await delay(1500);

    // Close login popup
    try {
      const btns = await page.$$('button');
      for (const b of btns) {
        const t = await page.evaluate(el => el.textContent, b);
        if (t.includes('\u2715') || t.includes('\u00D7')) { await b.click(); break; }
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
      // Skip AD blocks — find the price section after "Selected Color"
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
    console.error(`  Page load error ${fsn}: ${e.message}`);
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
    } catch (e) {
      // Input didn't appear — try direct entry anyway
    }

    if (inputFound) {
      // Fill pincode using evaluate (fast, no round-trips per element)
      await page.evaluate((pin) => {
        const inputs = document.querySelectorAll('input');
        for (const inp of inputs) {
          const ph = (inp.placeholder || '').toLowerCase();
          if (ph.includes('pincode') || ph.includes('pin code') || ph.includes('enter')) {
            // Clear and set value using native input setter (React-compatible)
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

    // Extract delivery info — scoped to delivery section, not full page
    const delivery = await page.evaluate(() => {
      const bt = document.body.innerText;
      if (bt.includes('Currently unavailable') || bt.includes('Sold Out') || bt.includes('Out of stock')) {
        return { dd: 'N/A', available: false };
      }
      if (bt.includes('not serviceable') || bt.includes('Cannot be delivered') || bt.includes('delivery not available')) {
        return { dd: 'Not Serviceable', available: false };
      }

      // Scope to delivery section — find "Delivery" near pincode area
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
      // Fallback: try full body if scoped didn't find anything
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

// ─── PARALLEL CATEGORY WORKER ────────────────────────────────

// How many categories to scrape in parallel per chunk
const MAX_PARALLEL_CATEGORIES = 4;

async function createWorkerPage(browser) {
  const page = await browser.newPage();
  await page.setViewport({ width: 1366, height: 768 });
  await page.setUserAgent(USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)]);
  await page.setExtraHTTPHeaders({
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'Upgrade-Insecure-Requests': '1'
  });
  await page.setRequestInterception(true);
  page.on('request', (req) => {
    const type = req.resourceType();
    if (['image', 'font', 'media'].includes(type)) req.abort();
    else req.continue();
  });
  return page;
}

// Process all FSNs for one category on a dedicated page
async function processCategoryWorker(page, catName, catFSNs, fsnRowMap, context) {
  const { startTime, saConfig } = context;
  let catErrors = 0;
  let catProcessed = 0;
  let consecutiveErrors = 0;
  let pendingWrites = [];

  console.log(`[${catName}] Starting — ${catFSNs.length} FSNs`);

  for (let i = 0; i < catFSNs.length; i++) {
    const fsn = catFSNs[i];
    const elapsed = ((Date.now() - startTime) / 60000).toFixed(1);
    console.log(`[${catName}] [${i + 1}/${catFSNs.length}] ${fsn} (${elapsed}m)`);

    // Rotate User-Agent per FSN
    await page.setUserAgent(USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)]);

    // Load page with exponential backoff retry
    let info = null;
    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      info = await loadProductPage(page, fsn);
      if (info.name !== 'Error') break;
      if (attempt < MAX_RETRIES) {
        const retryDelay = RETRY_DELAYS[attempt];
        console.log(`[${catName}]   Retry ${attempt + 1}/${MAX_RETRIES} after ${retryDelay / 1000}s...`);
        await page.goto('about:blank').catch(() => {});
        await delay(retryDelay);
      }
    }

    const row = [fsn, info.name, info.mrp, info.sp];

    if (info.name === 'Error') {
      catErrors++;
      consecutiveErrors++;
      if (consecutiveErrors >= 5) {
        console.log(`[${catName}]   >> ${consecutiveErrors} consecutive errors — pausing 60s`);
        await delay(60000);
      } else if (consecutiveErrors >= 3) {
        console.log(`[${catName}]   >> ${consecutiveErrors} consecutive errors — pausing 30s`);
        await delay(30000);
      }
    } else {
      consecutiveErrors = 0;
    }

    // Check each pincode WITHOUT reloading
    for (let p = 0; p < PINCODES.length; p++) {
      let delivery;
      try {
        delivery = await checkPincode(page, PINCODES[p]);
      } catch (e) {
        delivery = { dd: 'Error', available: false };
      }
      const inStock = info.available && delivery.available;
      row.push(inStock ? 'In Stock' : 'Out of Stock', delivery.dd);
    }

    row.push(new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }));
    catProcessed++;

    // Queue writes for both Tracker and category tab
    const rowInfo = fsnRowMap[fsn];
    if (rowInfo) {
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
    }

    // Flush every BATCH_SIZE FSNs or at end of category
    if (pendingWrites.length >= BATCH_SIZE * 2 || i === catFSNs.length - 1) {
      let token = context.getToken();
      if (token && pendingWrites.length > 0) {
        try {
          await sheetsAPI('POST',
            `/v4/spreadsheets/${SPREADSHEET_ID}/values:batchUpdate`,
            { valueInputOption: 'RAW', data: pendingWrites }, token
          );
          console.log(`[${catName}]   >> Flushed ${pendingWrites.length} ranges`);
        } catch (e) {
          console.error(`[${catName}]   >> Sheet write error: ${e.message}`);
          try {
            token = await getAccessToken(saConfig);
            context.setToken(token);
            await sheetsAPI('POST',
              `/v4/spreadsheets/${SPREADSHEET_ID}/values:batchUpdate`,
              { valueInputOption: 'RAW', data: pendingWrites }, token
            );
          } catch (e2) {
            console.error(`[${catName}]   >> Retry failed: ${e2.message}`);
          }
        }
        pendingWrites = [];
      }
    }

    // Delay between FSNs: 2s + random 0-2s jitter
    if (i < catFSNs.length - 1) {
      const jitter = Math.floor(Math.random() * 2000);
      await delay(2000 + jitter);
    }
  }

  console.log(`[${catName}] DONE — ${catProcessed} processed, ${catErrors} errors`);
  await page.close();
  return { catName, processed: catProcessed, errors: catErrors };
}

// ─── MAIN ───────────────────────────────────────────────────
async function main() {
  const startTime = Date.now();
  const opts = parseArgs();

  console.log('=== Flipkart Product Tracker (Cloud) ===');
  console.log(`Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`);
  console.log(`Environment: ${process.env.GITHUB_ACTIONS ? 'GitHub Actions' : 'Local'}`);
  console.log(`Parallel categories per chunk: ${MAX_PARALLEL_CATEGORIES}`);

  // Load ALL FSNs (needed for chunk offset calculation)
  const allFSNs = fs.readFileSync(FSN_FILE, 'utf8').trim().split('\n').map(f => f.trim()).filter(Boolean);
  console.log(`Total FSNs in file: ${allFSNs.length}`);

  // Load category mapping
  const catData = loadFSNCategories();
  const useCategories = !!catData;
  if (useCategories) {
    const catNames = Object.keys(catData.catFSNs);
    console.log(`Categories loaded: ${catNames.length} (${Object.keys(catData.map).length} FSN mappings)`);
  } else {
    console.log('No FSN_CATEGORIES.csv found — writing to Tracker tab only');
  }

  // Determine which FSNs this chunk processes
  let fsns = allFSNs;
  let chunkStart = 0;
  if (opts.chunk !== null && opts.totalChunks !== null) {
    const chunkSize = Math.ceil(allFSNs.length / opts.totalChunks);
    chunkStart = opts.chunk * chunkSize;
    const end = Math.min(chunkStart + chunkSize, allFSNs.length);
    fsns = allFSNs.slice(chunkStart, end);
    console.log(`Chunk ${opts.chunk + 1}/${opts.totalChunks}: FSNs ${chunkStart + 1}-${end} (${fsns.length} items)`);
  }

  console.log(`FSNs to process: ${fsns.length}`);
  console.log(`Pincodes: ${CITY_NAMES.join(', ')}`);

  // Get Sheets API token (service account)
  const saConfig = getServiceAccountConfig();
  let token = null;
  try {
    token = await getAccessToken(saConfig);
    if (token) console.log('Google Sheets: authenticated');
  } catch (e) {
    console.error('Failed to get Sheets token:', e.message);
    console.log('Continuing without sheet updates...');
  }

  // Setup category tabs + red formatting (chunk 0 only to avoid race conditions)
  if (token && useCategories && (opts.chunk === null || opts.chunk === 0)) {
    const catNames = Object.keys(catData.catFSNs);
    try {
      await ensureCategoryTabs(catNames, token);
      await addRedFormatting(token);
    } catch (e) {
      console.error('Tab setup error:', e.message);
    }
  }

  // If chunk > 0, wait for chunk 0 to create tabs
  if (token && useCategories && opts.chunk !== null && opts.chunk > 0) {
    console.log('Waiting 30s for chunk 0 to create tabs...');
    await delay(30000);
  }

  // Pre-assign row numbers for each FSN (avoids race conditions)
  const fsnRowMap = {};
  const catCounters = {};
  if (useCategories) {
    for (const tab of Object.keys(catData.catFSNs)) catCounters[tab] = 2;
    // Offset for FSNs in earlier chunks
    for (let j = 0; j < chunkStart; j++) {
      const tab = catData.map[allFSNs[j]];
      if (tab && catCounters[tab] !== undefined) catCounters[tab]++;
    }
  }
  for (let i = 0; i < fsns.length; i++) {
    const fsn = fsns[i];
    const catTab = useCategories ? catData.map[fsn] : null;
    fsnRowMap[fsn] = {
      trackerRow: 2 + chunkStart + i,
      catTab,
      catRow: (catTab && catCounters[catTab] !== undefined) ? catCounters[catTab]++ : null
    };
  }

  // Group this chunk's FSNs by category
  const chunkCatFSNs = {}; // catName -> [fsn, ...]
  for (const fsn of fsns) {
    const catTab = useCategories ? catData.map[fsn] : 'Tracker';
    if (!chunkCatFSNs[catTab]) chunkCatFSNs[catTab] = [];
    chunkCatFSNs[catTab].push(fsn);
  }

  const categoryNames = Object.keys(chunkCatFSNs);
  console.log(`\nCategories in this chunk: ${categoryNames.length}`);
  for (const cat of categoryNames) {
    console.log(`  ${cat}: ${chunkCatFSNs[cat].length} FSNs`);
  }

  // Launch browser
  console.log('\nLaunching Chromium...');
  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--disable-extensions',
      '--disable-background-networking',
      '--disable-default-apps',
      '--disable-sync',
      '--disable-translate',
      '--metrics-recording-only',
      '--no-first-run',
      '--safebrowsing-disable-auto-update',
      '--disable-blink-features=AutomationControlled'
    ]
  });

  // Shared token context (thread-safe since JS is single-threaded)
  const tokenContext = {
    startTime,
    saConfig,
    getToken: () => token,
    setToken: (t) => { token = t; }
  };

  // Run categories in parallel with concurrency limit
  let totalProcessed = 0;
  let totalErrors = 0;

  // Sort categories largest-first for better load balancing
  categoryNames.sort((a, b) => chunkCatFSNs[b].length - chunkCatFSNs[a].length);

  // Worker pool: process categories with MAX_PARALLEL_CATEGORIES concurrent pages
  const queue = [...categoryNames];
  const activeWorkers = [];

  function startNextWorker() {
    if (queue.length === 0) return null;
    const catName = queue.shift();
    const catFSNs = chunkCatFSNs[catName];

    const workerPromise = (async () => {
      const page = await createWorkerPage(browser);
      // Stagger start by a small random delay to avoid burst requests
      await delay(Math.floor(Math.random() * 3000));
      return processCategoryWorker(page, catName, catFSNs, fsnRowMap, tokenContext);
    })();

    return workerPromise;
  }

  // Fill initial worker slots
  for (let i = 0; i < Math.min(MAX_PARALLEL_CATEGORIES, categoryNames.length); i++) {
    activeWorkers.push(startNextWorker());
  }

  // As workers complete, start new ones until queue is empty
  while (activeWorkers.length > 0) {
    const result = await Promise.race(activeWorkers.map((p, idx) => p.then(r => ({ r, idx }))));
    const { r, idx } = result;

    totalProcessed += r.processed;
    totalErrors += r.errors;
    console.log(`\n>> Category "${r.catName}" complete: ${r.processed} OK, ${r.errors} errors`);

    // Remove completed worker
    activeWorkers.splice(idx, 1);

    // Start next category if any remain
    const next = startNextWorker();
    if (next) activeWorkers.push(next);
  }

  await browser.close();

  const totalTime = ((Date.now() - startTime) / 60000).toFixed(1);
  console.log(`\n=== COMPLETE ===`);
  console.log(`Processed: ${totalProcessed}/${fsns.length} FSNs`);
  console.log(`Errors: ${totalErrors}`);
  console.log(`Time: ${totalTime} minutes`);
  console.log(`Sheet: https://docs.google.com/spreadsheets/d/${SPREADSHEET_ID}`);

  if (totalErrors > fsns.length * 0.5) {
    console.error('WARNING: More than 50% errors. Flipkart may be blocking this IP.');
    process.exit(1);
  }
}

main().catch(e => {
  console.error('Fatal:', e);
  process.exit(1);
});
