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

const puppeteer = require('puppeteer');
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
const BATCH_SIZE = 10; // Write to Google Sheet every N FSNs
const PAGE_TIMEOUT = 45000;

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

async function writeToSheet(rows, startRow, token) {
  const endRow = startRow + rows.length - 1;
  const range = `Tracker!A${startRow}:M${endRow}`;
  await sheetsAPI('PUT',
    `/v4/spreadsheets/${SPREADSHEET_ID}/values/${encodeURIComponent(range)}?valueInputOption=RAW`,
    { range, values: rows },
    token
  );
  return endRow;
}

// ─── FLIPKART SCRAPER ──────────────────────────────────────

// Load a product page once, extract product info (name, MRP, SP) from JSON-LD
async function loadProductPage(page, fsn) {
  try {
    await page.goto(`https://www.flipkart.com/product/p/itm?pid=${fsn}`, { waitUntil: 'networkidle2', timeout: PAGE_TIMEOUT });
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
      const dm = priceText.match(/(\d+)%\n([\d,]+)\n\u20B9([\d,]+)/);
      if (dm) {
        mrp = '\u20B9' + dm[2];
        if (sp === 'N/A') sp = '\u20B9' + dm[3];
      } else {
        const dm2 = priceText.match(/(\d+)%\s*[\n\r]*([\d,]+)\s*[\n\r]*\u20B9([\d,]+)/);
        if (dm2) {
          mrp = '\u20B9' + dm2[2];
          if (sp === 'N/A') sp = '\u20B9' + dm2[3];
        }
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
    // Click delivery location link
    const links = await page.$$('a, div, span');
    for (const el of links) {
      const text = await page.evaluate(e => e.textContent.trim(), el);
      if (text === 'Select delivery location' || text.includes('Select delivery location')
          || text === 'Change' || text.includes('Enter pincode')) {
        await el.click();
        await delay(1000);
        break;
      }
    }

    // Find and fill pincode input
    const allInputs = await page.$$('input');
    for (const inp of allInputs) {
      const ph = await page.evaluate(e => e.placeholder, inp);
      if (ph && (ph.toLowerCase().includes('pincode') || ph.toLowerCase().includes('pin code') || ph.includes('Enter'))) {
        await inp.click({ clickCount: 3 });
        await inp.type(pincode, { delay: 20 });
        await delay(300);
        const btns = await page.$$('button, span');
        for (const b of btns) {
          const t = await page.evaluate(e => e.textContent.trim(), b);
          if (t === 'Apply' || t === 'Check' || t === 'Submit') { await b.click(); break; }
        }
        await delay(2000);
        break;
      }
    }

    // Extract delivery info
    const delivery = await page.evaluate(() => {
      const bt = document.body.innerText;
      if (bt.includes('not serviceable') || bt.includes('Cannot be delivered') || bt.includes('delivery not available')) {
        return { dd: 'Not Serviceable', available: false };
      }
      let dd = 'N/A';
      const patterns = [
        /Delivery\s*\n\s*by\s+(\d+\s+\w+,?\s*\w*)/i,
        /Delivery\s+by\s+(\d+\s+\w+,?\s*\w*)/i,
        /Delivered\s+by\s+(\d+\s+\w+,?\s*\w*)/i,
        /Get it by\s+(\d+\s+\w+,?\s*\w*)/i,
        /Delivery in\s+(\d+\s+\w+)/i
      ];
      for (const p of patterns) {
        const m = bt.match(p);
        if (m) { dd = m[1].trim().replace(/,?\s*(Mon|Tue|Wed|Thu|Fri|Sat|Sun)$/i, ''); break; }
      }
      const oos = bt.includes('Currently unavailable') || bt.includes('Sold Out') || bt.includes('Out of stock');
      return { dd, available: !oos };
    });

    return delivery;
  } catch (e) {
    return { dd: 'Error', available: false };
  }
}

// ─── MAIN ───────────────────────────────────────────────────
async function main() {
  const startTime = Date.now();
  const opts = parseArgs();

  console.log('=== Flipkart Product Tracker (Cloud) ===');
  console.log(`Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`);
  console.log(`Environment: ${process.env.GITHUB_ACTIONS ? 'GitHub Actions' : 'Local'}`);

  // Load FSNs
  let fsns = fs.readFileSync(FSN_FILE, 'utf8').trim().split('\n').map(f => f.trim()).filter(Boolean);
  console.log(`Total FSNs in file: ${fsns.length}`);

  // If running as a chunk (parallel matrix job), slice the FSN list
  if (opts.chunk !== null && opts.totalChunks !== null) {
    const chunkSize = Math.ceil(fsns.length / opts.totalChunks);
    const start = opts.chunk * chunkSize;
    const end = Math.min(start + chunkSize, fsns.length);
    fsns = fsns.slice(start, end);
    console.log(`Chunk ${opts.chunk + 1}/${opts.totalChunks}: FSNs ${start + 1}-${end} (${fsns.length} items)`);
  }

  console.log(`FSNs to process: ${fsns.length}`);
  console.log(`Estimated time: ~${Math.ceil(fsns.length * 0.2)} minutes`);
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

  // Launch browser (full puppeteer includes Chromium)
  console.log('Launching Chromium...');
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
      '--single-process'
    ]
  });

  const page = await browser.newPage();
  await page.setUserAgent(
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
  );
  await page.setViewport({ width: 1366, height: 768 });

  // Block unnecessary resources to speed up loading
  await page.setRequestInterception(true);
  page.on('request', (req) => {
    const type = req.resourceType();
    if (['image', 'font', 'media'].includes(type)) {
      req.abort();
    } else {
      req.continue();
    }
  });

  // Calculate starting row in Google Sheet based on chunk offset
  let sheetRowOffset = 2; // Row 1 = header, data starts at row 2
  if (opts.chunk !== null && opts.totalChunks !== null) {
    const chunkSize = Math.ceil(
      fs.readFileSync(FSN_FILE, 'utf8').trim().split('\n').filter(Boolean).length / opts.totalChunks
    );
    sheetRowOffset = 2 + (opts.chunk * chunkSize);
  }

  // Write sheet headers
  if (token) {
    const headers = [
      'FSN', 'Product Name', 'MRP', 'Selling Price',
      'Delhi Stock', 'Delhi Delivery',
      'Bangalore Stock', 'Bangalore Delivery',
      'Mumbai Stock', 'Mumbai Delivery',
      'Pune Stock', 'Pune Delivery',
      'Last Checked'
    ];
    try {
      const hRange = 'Tracker!A1:M1';
      await sheetsAPI('PUT',
        `/v4/spreadsheets/${SPREADSHEET_ID}/values/${encodeURIComponent(hRange)}?valueInputOption=RAW`,
        { range: hRange, values: [headers] }, token
      );
      console.log('Sheet headers written');
    } catch (e) {
      console.error('Could not write headers:', e.message);
    }
  }

  const batchBuffer = [];
  let processed = 0;
  let errors = 0;
  let consecutiveErrors = 0;

  for (let i = 0; i < fsns.length; i++) {
    const fsn = fsns[i];
    const elapsed = ((Date.now() - startTime) / 60000).toFixed(1);
    console.log(`[${i + 1}/${fsns.length}] ${fsn} (${elapsed}m elapsed)`);

    // Load page ONCE per FSN — extract product info from JSON-LD
    let info = await loadProductPage(page, fsn);

    // Retry once on error with a longer delay
    if (info.name === 'Error') {
      console.log(`  Retrying ${fsn} after 5s...`);
      await delay(5000);
      info = await loadProductPage(page, fsn);
    }

    const row = [fsn, info.name, info.mrp, info.sp];

    if (info.name === 'Error') {
      errors++;
      consecutiveErrors++;
      // Backoff: if many consecutive errors, Flipkart is likely blocking this IP
      if (consecutiveErrors >= 5) {
        console.log(`  >> ${consecutiveErrors} consecutive errors — pausing 60s to avoid IP block`);
        await delay(60000);
      } else if (consecutiveErrors >= 3) {
        console.log(`  >> ${consecutiveErrors} consecutive errors — pausing 30s`);
        await delay(30000);
      }
    } else {
      consecutiveErrors = 0;
    }

    // Check each pincode WITHOUT reloading the page
    for (let p = 0; p < PINCODES.length; p++) {
      let delivery;
      try {
        delivery = await checkPincode(page, PINCODES[p]);
      } catch (e) {
        delivery = { dd: 'Error', available: false };
        errors++;
      }

      const inStock = info.available && delivery.available;
      row.push(inStock ? 'In Stock' : 'Out of Stock', delivery.dd);

      console.log(`  ${CITY_NAMES[p]} (${PINCODES[p]}): ${inStock ? 'In Stock' : 'OOS'} | Del: ${delivery.dd}`);
    }

    // Add timestamp
    row.push(new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }));
    batchBuffer.push(row);
    processed++;

    // Write to sheet in batches
    if (token && (batchBuffer.length >= BATCH_SIZE || i === fsns.length - 1)) {
      const startRow = sheetRowOffset + (i - batchBuffer.length + 1);
      try {
        await writeToSheet(batchBuffer, startRow, token);
        console.log(`  >> Written ${batchBuffer.length} rows (${startRow}-${startRow + batchBuffer.length - 1}) to Google Sheet`);
      } catch (e) {
        console.error(`  >> Sheet write error: ${e.message}`);
        // Refresh token and retry
        try {
          token = await getAccessToken(saConfig);
          await writeToSheet(batchBuffer, startRow, token);
          console.log(`  >> Retry succeeded`);
        } catch (e2) {
          console.error(`  >> Retry also failed: ${e2.message}`);
        }
      }
      batchBuffer.length = 0;
    }

    // Delay between FSNs (1.5s to be less aggressive)
    if (i < fsns.length - 1) await delay(1500);
  }

  await browser.close();

  const totalTime = ((Date.now() - startTime) / 60000).toFixed(1);
  console.log(`\n=== COMPLETE ===`);
  console.log(`Processed: ${processed}/${fsns.length} FSNs`);
  console.log(`Errors: ${errors}`);
  console.log(`Time: ${totalTime} minutes`);
  console.log(`Sheet: https://docs.google.com/spreadsheets/d/${SPREADSHEET_ID}`);

  if (errors > fsns.length * 0.5) {
    console.error('WARNING: More than 50% errors. Flipkart may be blocking this IP.');
    process.exit(1);
  }
}

main().catch(e => {
  console.error('Fatal:', e);
  process.exit(1);
});
