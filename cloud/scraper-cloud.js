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
const MAX_RETRIES = 2;
const RETRY_DELAYS = [3000, 8000];
const PARALLEL_WORKERS = 3;            // Concurrent browser pages per chunk
const MAX_CONSECUTIVE_ERRORS = 20;     // Abort when IP is clearly blocked
const MAX_ELAPSED_MINUTES = 180;       // Exit gracefully before GH Actions timeout
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
    await delay(1000);

    // Close login popup
    try {
      const btns = await page.$$('button');
      for (const b of btns) {
        const t = await page.evaluate(el => el.textContent, b);
        if (t.includes('\u2715') || t.includes('\u00D7')) { await b.click(); break; }
      }
    } catch (e) {}
    await delay(200);

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

// Helper: check if pincode input is already visible in the DOM
function pincodeInputSelector() {
  return `() => {
    const inputs = document.querySelectorAll('input');
    for (const inp of inputs) {
      const ph = (inp.placeholder || '').toLowerCase();
      if (ph.includes('pincode') || ph.includes('pin code') || ph.includes('enter pin')) return true;
    }
    return false;
  }`;
}

// Enter/change pincode and extract delivery info (without reloading the page)
async function checkPincode(page, pincode) {
  try {
    // Bug C fix: check if pincode input is already visible (popup still open from previous pincode)
    let inputAlreadyVisible = false;
    try {
      inputAlreadyVisible = await page.evaluate(new Function('return (' + pincodeInputSelector() + ')()'));
    } catch (e) {}

    if (!inputAlreadyVisible) {
      // Click delivery location trigger
      // Bug A fix: exact match for 'change' to avoid matching 'exchange offer'
      const clickResult = await page.evaluate(() => {
        const partialTriggers = ['select delivery location', 'enter pincode'];
        let best = null;
        let bestLen = Infinity;
        const els = document.querySelectorAll('a, div, span, button');
        for (const el of els) {
          const t = (el.textContent || '').trim().toLowerCase();
          if (t.length > 50) continue;
          // Exact match for 'change' (avoids 'exchange offer')
          if (t === 'change' && t.length < bestLen) {
            best = el; bestLen = t.length;
            continue;
          }
          // Partial match for longer triggers
          for (const trigger of partialTriggers) {
            if (t.includes(trigger) && t.length < bestLen) {
              best = el; bestLen = t.length;
            }
          }
          // Match 6-digit pincode display (e.g. "110001")
          if (/^\d{6}$/.test(t) && t.length < bestLen) {
            best = el; bestLen = t.length;
          }
        }
        if (best) {
          best.click();
          return { clicked: true, text: best.textContent.trim().substring(0, 40) };
        }
        return { clicked: false, text: null };
      });
      console.log(`    Pin ${pincode}: trigger=${clickResult.text || 'NONE'}`);
    } else {
      console.log(`    Pin ${pincode}: input already visible`);
    }

    // Wait for pincode input to appear
    // Bug B fix: use 'enter pin' instead of bare 'enter' to avoid matching unrelated inputs
    let inputFound = inputAlreadyVisible;
    if (!inputFound) {
      try {
        await page.waitForFunction(pincodeInputSelector(), { timeout: 5000 });
        inputFound = true;
      } catch (e) {
        // Fallback: try clicking pincode display text
        try {
          await page.evaluate(() => {
            const els = document.querySelectorAll('span, div, a');
            for (const el of els) {
              const t = (el.textContent || '').trim();
              if (/^\d{6}$/.test(t)) { el.click(); return; }
            }
          });
          await page.waitForFunction(pincodeInputSelector(), { timeout: 3000 });
          inputFound = true;
        } catch (e2) {}
      }
    }

    if (inputFound) {
      // Clear existing value, then fill new pincode
      await page.evaluate((pin) => {
        const inputs = document.querySelectorAll('input');
        for (const inp of inputs) {
          const ph = (inp.placeholder || '').toLowerCase();
          if (ph.includes('pincode') || ph.includes('pin code') || ph.includes('enter pin')) {
            inp.focus();
            inp.select();
            const nativeSet = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
            nativeSet.call(inp, '');
            inp.dispatchEvent(new Event('input', { bubbles: true }));
            nativeSet.call(inp, pin);
            inp.dispatchEvent(new Event('input', { bubbles: true }));
            inp.dispatchEvent(new Event('change', { bubbles: true }));
            break;
          }
        }
      }, pincode);
      await delay(500);

      // Bug D fix: partial match for Apply button variants
      const applyClicked = await page.evaluate(() => {
        const actionWords = ['apply', 'check', 'submit', 'update'];
        const btns = document.querySelectorAll('button, span');
        for (const b of btns) {
          const t = (b.textContent || '').trim().toLowerCase();
          if (actionWords.some(w => t.includes(w)) && t.length < 30) {
            b.click();
            return true;
          }
        }
        return false;
      });
      if (!applyClicked) console.log(`    Pin ${pincode}: WARNING — Apply button not found`);
      await delay(1500);
    } else {
      console.log(`    Pin ${pincode}: WARNING — pincode input not found`);
    }

    // Extract delivery info
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

    console.log(`    Pin ${pincode}: delivery=${delivery.dd}, available=${delivery.available}`);
    return delivery;
  } catch (e) {
    console.log(`    Pin ${pincode}: ERROR — ${e.message}`);
    return { dd: 'Error', available: false };
  }
}

// ─── PAGE SETUP ─────────────────────────────────────────────

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

// ─── CONCURRENCY HELPERS ────────────────────────────────────

class Semaphore {
  constructor(max) { this.max = max; this.current = 0; this.queue = []; }
  async acquire() {
    if (this.current < this.max) { this.current++; return; }
    return new Promise(resolve => this.queue.push(resolve));
  }
  release() {
    this.current--;
    if (this.queue.length > 0) { this.current++; this.queue.shift()(); }
  }
}

class PagePool {
  constructor() { this.available = []; this.waiters = []; }
  add(page) { this.available.push(page); }
  async take() {
    if (this.available.length > 0) return this.available.pop();
    return new Promise(resolve => this.waiters.push(resolve));
  }
  release(page) {
    if (this.waiters.length > 0) this.waiters.shift()(page);
    else this.available.push(page);
  }
}

// ─── SCRAPE SINGLE FSN ─────────────────────────────────────

async function scrapeFSN(page, fsn, browser) {
  // Rotate User-Agent
  await page.setUserAgent(USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)]);

  // Load page with retry
  let info = null;
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    info = await loadProductPage(page, fsn);
    if (info.name !== 'Error') break;
    if (attempt < MAX_RETRIES) {
      const retryDelay = RETRY_DELAYS[attempt];
      console.log(`  Retry ${attempt + 1}/${MAX_RETRIES} after ${retryDelay / 1000}s...`);
      await page.goto('about:blank').catch(() => {});
      await delay(retryDelay);
    }
  }

  const row = [fsn, info.name, info.mrp, info.sp];
  const isError = info.name === 'Error';

  // Check each pincode
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
  return { row, isError };
}

// ─── PROCESS ONE CATEGORY ───────────────────────────────────

async function processCategory(catName, fsnList, pagePool, semaphore, state, browser) {
  await semaphore.acquire();
  let page = await pagePool.take();
  const workerId = state.nextWorkerId++;
  let localConsecutiveErrors = 0;

  console.log(`[W${workerId}] Starting category: ${catName} (${fsnList.length} FSNs)`);

  try {
    for (let i = 0; i < fsnList.length; i++) {
      const fsn = fsnList[i];

      // Check global abort conditions
      if (state.aborted) {
        console.log(`[W${workerId}] Aborted — stopping ${catName}`);
        break;
      }
      const elapsedMin = (Date.now() - state.startTime) / 60000;
      if (elapsedMin >= MAX_ELAPSED_MINUTES) {
        console.log(`[W${workerId}] TIME LIMIT reached — stopping ${catName}`);
        state.aborted = true;
        break;
      }

      console.log(`[W${workerId}] [${catName}] [${i + 1}/${fsnList.length}] ${fsn} (${elapsedMin.toFixed(1)}m)`);

      const { row, isError } = await scrapeFSN(page, fsn, browser);

      if (isError) {
        state.totalErrors++;
        localConsecutiveErrors++;
        state.consecutiveErrors++;

        // Recycle page on persistent errors
        if (localConsecutiveErrors >= 5) {
          console.log(`[W${workerId}] ${localConsecutiveErrors} consecutive errors — recycling page + waiting 15s`);
          await page.close().catch(() => {});
          await delay(15000);
          page = await createWorkerPage(browser);
          localConsecutiveErrors = 0;
        }

        // Global abort
        if (state.consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
          console.log(`[W${workerId}] IP BLOCKED — ${state.consecutiveErrors} global consecutive errors`);
          state.aborted = true;
          break;
        }
      } else {
        localConsecutiveErrors = 0;
        state.consecutiveErrors = 0;
      }

      state.totalProcessed++;

      // Queue writes
      const rowInfo = state.fsnRowMap[fsn];
      if (rowInfo) {
        state.pendingWrites.push({
          range: `Tracker!A${rowInfo.trackerRow}:M${rowInfo.trackerRow}`,
          values: [row]
        });
        if (rowInfo.catTab && rowInfo.catRow) {
          state.pendingWrites.push({
            range: `'${rowInfo.catTab}'!A${rowInfo.catRow}:M${rowInfo.catRow}`,
            values: [row]
          });
        }
      }

      // Flush if enough writes accumulated
      if (state.pendingWrites.length >= BATCH_SIZE * 2) {
        await state.flush();
      }

      // Inter-FSN delay (reduced — 4 runners with separate IPs provide natural spacing)
      if (i < fsnList.length - 1) {
        await delay(1500 + Math.floor(Math.random() * 1000));
      }
    }
  } finally {
    // Navigate away to free DOM memory
    await page.goto('about:blank').catch(() => {});
    pagePool.release(page);
    semaphore.release();
    // Flush remaining writes for this category
    await state.flush();
    console.log(`[W${workerId}] Finished category: ${catName}`);
  }
}

// ─── MAIN ───────────────────────────────────────────────────
async function main() {
  const startTime = Date.now();
  const opts = parseArgs();

  console.log('=== Flipkart Product Tracker (Cloud) ===');
  console.log(`Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`);
  console.log(`Environment: ${process.env.GITHUB_ACTIONS ? 'GitHub Actions' : 'Local'}`);
  console.log(`Mode: Parallel (${PARALLEL_WORKERS} workers by sub-category)`);

  // Load ALL FSNs
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

  // Get Sheets API token
  const saConfig = getServiceAccountConfig();
  let token = null;
  try {
    token = await getAccessToken(saConfig);
    if (token) console.log('Google Sheets: authenticated');
  } catch (e) {
    console.error('Failed to get Sheets token:', e.message);
    console.log('Continuing without sheet updates...');
  }

  // Setup category tabs (chunk 0 only)
  if (token && useCategories && (opts.chunk === null || opts.chunk === 0)) {
    const catNames = Object.keys(catData.catFSNs);
    try {
      await ensureCategoryTabs(catNames, token);
      await addRedFormatting(token);
    } catch (e) {
      console.error('Tab setup error:', e.message);
    }
  }

  if (token && useCategories && opts.chunk !== null && opts.chunk > 0) {
    console.log('Waiting 30s for chunk 0 to create tabs...');
    await delay(30000);
  }

  // Pre-assign row numbers for each FSN
  const fsnRowMap = {};
  const catCounters = {};
  if (useCategories) {
    for (const tab of Object.keys(catData.catFSNs)) catCounters[tab] = 2;
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

  // Group FSNs by category for this chunk, sort largest-first
  const chunkCatFSNs = {};
  for (const fsn of fsns) {
    const catTab = (useCategories && catData.map[fsn]) || 'Uncategorized';
    if (!chunkCatFSNs[catTab]) chunkCatFSNs[catTab] = [];
    chunkCatFSNs[catTab].push(fsn);
  }
  const sortedCategories = Object.entries(chunkCatFSNs).sort((a, b) => b[1].length - a[1].length);

  console.log(`\nCategories in this chunk: ${sortedCategories.length}`);
  for (const [cat, catFsns] of sortedCategories) {
    console.log(`  ${cat}: ${catFsns.length} FSNs`);
  }

  // Launch browser
  console.log(`\nLaunching Chromium with ${PARALLEL_WORKERS} worker pages...`);
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

  // Create page pool
  const pagePool = new PagePool();
  const numWorkers = Math.min(PARALLEL_WORKERS, sortedCategories.length);
  for (let w = 0; w < numWorkers; w++) {
    const page = await createWorkerPage(browser);
    pagePool.add(page);
    if (w < numWorkers - 1) await delay(1000); // stagger page creation
  }
  console.log(`${numWorkers} worker pages ready`);

  // Shared state with mutex-protected flush
  const state = {
    startTime,
    aborted: false,
    consecutiveErrors: 0,
    totalProcessed: 0,
    totalErrors: 0,
    nextWorkerId: 0,
    fsnRowMap,
    pendingWrites: [],
    _flushing: false,

    async flush(force = false) {
      if (this._flushing) return;
      if (!force && this.pendingWrites.length < BATCH_SIZE * 2) return;
      if (!token || this.pendingWrites.length === 0) return;

      this._flushing = true;
      const batch = this.pendingWrites.splice(0);
      try {
        await sheetsAPI('POST',
          `/v4/spreadsheets/${SPREADSHEET_ID}/values:batchUpdate`,
          { valueInputOption: 'RAW', data: batch }, token
        );
        console.log(`  >> Flushed ${batch.length} ranges`);
      } catch (e) {
        console.error(`  >> Sheet write error: ${e.message}`);
        try {
          token = await getAccessToken(saConfig);
          await sheetsAPI('POST',
            `/v4/spreadsheets/${SPREADSHEET_ID}/values:batchUpdate`,
            { valueInputOption: 'RAW', data: batch }, token
          );
        } catch (e2) {
          console.error(`  >> Retry failed: ${e2.message} — ${batch.length} writes lost`);
        }
      }
      this._flushing = false;
    }
  };

  // Periodic flush safety net (every 30s)
  const flushInterval = setInterval(() => state.flush(true), 30000);

  // Launch all categories as parallel tasks gated by semaphore
  const semaphore = new Semaphore(numWorkers);
  const promises = sortedCategories.map(([catName, catFsns]) =>
    processCategory(catName, catFsns, pagePool, semaphore, state, browser)
  );

  const results = await Promise.allSettled(promises);

  // Log any category-level failures
  for (let i = 0; i < results.length; i++) {
    if (results[i].status === 'rejected') {
      console.error(`Category "${sortedCategories[i][0]}" failed: ${results[i].reason}`);
    }
  }

  // Final flush
  clearInterval(flushInterval);
  await state.flush(true);

  await browser.close();

  const totalTime = ((Date.now() - startTime) / 60000).toFixed(1);
  console.log(`\n=== COMPLETE ===`);
  console.log(`Processed: ${state.totalProcessed}/${fsns.length} FSNs`);
  console.log(`Errors: ${state.totalErrors}`);
  console.log(`Workers: ${numWorkers} parallel pages`);
  console.log(`Time: ${totalTime} minutes`);
  console.log(`Sheet: https://docs.google.com/spreadsheets/d/${SPREADSHEET_ID}`);

  if (state.totalErrors > fsns.length * 0.5) {
    console.error('WARNING: More than 50% errors. Flipkart may be blocking this IP.');
    process.exit(1);
  }
}

main().catch(e => {
  console.error('Fatal:', e);
  process.exit(1);
});
