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
const MAX_RETRIES = 2; // Retry failed scrapes
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
  const range = `Tracker!A${startRow}:Q${endRow}`;
  await sheetsAPI('PUT',
    `/v4/spreadsheets/${SPREADSHEET_ID}/values/${encodeURIComponent(range)}?valueInputOption=RAW`,
    { range, values: rows },
    token
  );
  return endRow;
}

// ─── FLIPKART SCRAPER ──────────────────────────────────────
async function scrapeProduct(page, fsn, pincode) {
  try {
    const url = `https://www.flipkart.com/product/p/itm?pid=${fsn}`;
    await page.goto(url, { waitUntil: 'networkidle2', timeout: PAGE_TIMEOUT });
    await delay(2500);

    // Close login popup if present
    try {
      const closeBtns = await page.$$('button');
      for (const btn of closeBtns) {
        const text = await page.evaluate(el => el.textContent, btn);
        if (text.includes('\u2715') || text.includes('\u00D7')) {
          await btn.click();
          await delay(500);
          break;
        }
      }
    } catch (e) { /* popup may not appear */ }

    // Enter pincode
    try {
      // First try direct pincode input on the page
      let pincodeInput = await page.$('input[id="pincodeInputId"]') ||
                         await page.$('input[placeholder*="pincode" i]') ||
                         await page.$('input[placeholder*="Pincode" i]');

      // If not found, try clicking "Select delivery location" to open popup
      if (!pincodeInput) {
        const elements = await page.$$('a, div, span');
        for (const el of elements) {
          const text = await page.evaluate(e => e.textContent.trim(), el);
          if (text === 'Select delivery location' || text.includes('Select delivery location')) {
            await el.click();
            await delay(2000);
            break;
          }
        }
        // Search for input again after popup
        const allInputs = await page.$$('input');
        for (const inp of allInputs) {
          const ph = await page.evaluate(e => (e.placeholder || '').toLowerCase(), inp);
          if (ph.includes('pincode') || ph.includes('pin code') || ph.includes('enter')) {
            pincodeInput = inp;
            break;
          }
        }
      }

      if (pincodeInput) {
        await pincodeInput.click({ clickCount: 3 });
        await delay(200);
        await pincodeInput.type(pincode, { delay: 30 });
        await delay(300);

        // Find and click Check/Apply button
        const btns = await page.$$('span, button');
        for (const btn of btns) {
          const text = await page.evaluate(el => el.textContent.trim(), btn);
          if (text === 'Check' || text === 'Apply' || text === 'Change' || text === 'Submit') {
            await btn.click();
            break;
          }
        }
        await delay(2500);
      }
    } catch (e) {
      console.log(`  Pincode entry failed for ${fsn}: ${e.message}`);
    }

    // Extract all data from page
    const data = await page.evaluate(() => {
      const bodyText = document.body.innerText;

      // Product name
      let name = document.title
        .replace(/\s*-\s*Buy.*$/i, '')
        .replace(/\s*\|.*$/, '')
        .replace(/Online at Best.*$/i, '')
        .replace(/Itm Store Online.*$/i, '')
        .trim();
      if (!name || name.length < 3) name = 'N/A';

      // Prices
      let sellingPrice = 'N/A';
      let mrp = 'N/A';
      const priceMatches = bodyText.match(/\u20B9[\d,]+/g);
      if (priceMatches && priceMatches.length >= 1) {
        const discountMatch = bodyText.match(/(\d+)%\s*[\n\r]*\u20B9?([\d,]+)\s*[\n\r]*\u20B9([\d,]+)/);
        if (discountMatch) {
          mrp = '\u20B9' + discountMatch[2];
          sellingPrice = '\u20B9' + discountMatch[3];
        } else {
          sellingPrice = priceMatches[0];
          if (priceMatches.length > 1) mrp = priceMatches[0];
        }
      }

      // Availability
      const outOfStock = bodyText.includes('Currently unavailable') ||
                         bodyText.includes('Sold Out') ||
                         bodyText.includes('Out of stock') ||
                         bodyText.includes('coming soon');

      // Delivery
      let deliveryDate = 'N/A';
      let deliveryCharge = 'N/A';
      let notServiceable = false;

      if (bodyText.includes('not serviceable') || bodyText.includes('Cannot be delivered') ||
          bodyText.includes('not delivered') || bodyText.includes('delivery not available')) {
        notServiceable = true;
        deliveryDate = 'Not Serviceable';
      }

      const delMatch = bodyText.match(/Delivery by[,\s]*([A-Za-z]+[\s,]*(?:[A-Za-z]+\s+)?\d+[^|\n]*)/i) ||
                       bodyText.match(/Delivered by[,\s]*([A-Za-z]+[\s,]*(?:[A-Za-z]+\s+)?\d+[^|\n]*)/i) ||
                       bodyText.match(/Get it by[,\s]*([A-Za-z]+[\s,]*(?:[A-Za-z]+\s+)?\d+[^|\n]*)/i);
      if (delMatch) deliveryDate = delMatch[1].trim().substring(0, 50);

      if (bodyText.includes('FREE Delivery') || bodyText.includes('Free delivery') || bodyText.includes('Free Delivery')) {
        deliveryCharge = 'FREE';
      } else {
        const chg = bodyText.match(/\u20B9(\d+)\s*(?:Delivery|delivery|shipping)/);
        if (chg) deliveryCharge = '\u20B9' + chg[1];
      }

      if (bodyText.includes('Location not set') || bodyText.includes('Select delivery location')) {
        deliveryDate = 'Pincode not applied';
      }

      let available = !outOfStock && !notServiceable;
      if (deliveryDate !== 'N/A' && deliveryDate !== 'Not Serviceable' && deliveryDate !== 'Pincode not applied') {
        available = true;
      }

      return {
        name: name.substring(0, 200),
        mrp, sellingPrice, available,
        deliveryDate, deliveryCharge
      };
    });

    return data;
  } catch (e) {
    console.error(`  Error scraping ${fsn}/${pincode}: ${e.message}`);
    return {
      name: 'Error', mrp: 'N/A', sellingPrice: 'N/A',
      available: false, deliveryDate: 'Error', deliveryCharge: 'N/A'
    };
  }
}

async function scrapeWithRetry(page, fsn, pincode, retries = MAX_RETRIES) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    const result = await scrapeProduct(page, fsn, pincode);
    if (result.name !== 'Error') return result;
    if (attempt < retries) {
      console.log(`  Retry ${attempt + 1} for ${fsn}/${pincode}...`);
      await delay(3000);
    }
  }
  return { name: 'Error', mrp: 'N/A', sellingPrice: 'N/A', available: false, deliveryDate: 'Error', deliveryCharge: 'N/A' };
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
  console.log(`Estimated time: ~${Math.ceil(fsns.length * 15 / 60)} minutes`);
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
    if (['image', 'stylesheet', 'font', 'media'].includes(type)) {
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

  const batchBuffer = [];
  let processed = 0;
  let errors = 0;

  for (let i = 0; i < fsns.length; i++) {
    const fsn = fsns[i];
    const elapsed = ((Date.now() - startTime) / 60000).toFixed(1);
    console.log(`[${i + 1}/${fsns.length}] ${fsn} (${elapsed}m elapsed)`);

    const row = [fsn];
    let productName = 'N/A', mrp = 'N/A', sellingPrice = 'N/A';

    for (let p = 0; p < PINCODES.length; p++) {
      const data = await scrapeWithRetry(page, fsn, PINCODES[p]);

      if (p === 0) {
        productName = data.name;
        mrp = data.mrp;
        sellingPrice = data.sellingPrice;
        row.push(productName, mrp, sellingPrice);
      }

      row.push(
        data.available ? 'In Stock' : 'Out of Stock',
        data.deliveryDate,
        data.deliveryCharge
      );

      if (data.name === 'Error') errors++;

      console.log(`  ${CITY_NAMES[p]} (${PINCODES[p]}): ${data.available ? 'In Stock' : 'Out of Stock'} | ${data.deliveryDate} | ${data.deliveryCharge}`);
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
