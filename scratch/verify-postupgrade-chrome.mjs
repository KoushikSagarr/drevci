import { spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';

function getDashboardPassword() {
  const envPath = path.resolve(process.cwd(), '.env');
  const content = fs.readFileSync(envPath, 'utf8');
  for (const line of content.split('\n')) {
    const trimmed = line.trim();
    if (trimmed.startsWith('DASHBOARD_PASSWORD=')) {
      return trimmed.slice('DASHBOARD_PASSWORD='.length).replace(/^["']|["']$/g, '');
    }
  }
  return null;
}

class CdpClient {
  constructor(wsUrl) {
    this.wsUrl = wsUrl;
    this.ws = null;
    this.id = 1;
    this.callbacks = new Map();
  }

  async connect() {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(this.wsUrl);
      this.ws.onopen = () => resolve();
      this.ws.onerror = (err) => reject(err);
      this.ws.onmessage = (event) => {
        const msg = JSON.parse(event.data);
        if (msg.id && this.callbacks.has(msg.id)) {
          const { resolve, reject } = this.callbacks.get(msg.id);
          this.callbacks.delete(msg.id);
          if (msg.error) {
            reject(new Error(msg.error.message));
          } else {
            resolve(msg.result);
          }
        }
      };
    });
  }

  send(method, params = {}) {
    const id = this.id++;
    return new Promise((resolve, reject) => {
      this.callbacks.set(id, { resolve, reject });
      this.ws.send(JSON.stringify({ id, method, params }));
    });
  }

  close() {
    if (this.ws) this.ws.close();
  }
}

async function runBrowserVerification() {
  console.log('=== REAL HEADLESS CHROME POST-UPGRADE VALIDATION ===\n');

  const password = getDashboardPassword();
  assert(password, 'DASHBOARD_PASSWORD missing');

  // Obtain session cookie via HTTP login
  const loginRes = await fetch('http://localhost:3001/api/auth/login', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Host': 'localhost:3001',
      'Origin': 'http://localhost:3001',
    },
    body: JSON.stringify({ password }),
  });
  assert.equal(loginRes.status, 200);
  const cookieHeader = loginRes.headers.get('set-cookie');
  const sessionCookieVal = cookieHeader.split(';')[0].split('=')[1];
  console.log('✓ Session cookie obtained.');

  // Launch Google Chrome in headless mode with debugging port 9228
  const chromePath = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
  const userDataDir = '/tmp/drev-chrome-postupgrade-' + Date.now();
  const chromeProc = spawn(chromePath, [
    '--headless',
    '--disable-gpu',
    '--remote-debugging-port=9228',
    `--user-data-dir=${userDataDir}`,
    'about:blank',
  ]);

  try {
    let versionData = null;
    for (let i = 0; i < 20; i++) {
      try {
        const res = await fetch('http://127.0.0.1:9228/json/version');
        versionData = await res.json();
        break;
      } catch {
        await new Promise((r) => setTimeout(r, 200));
      }
    }
    assert(versionData, 'Chrome CDP must be reachable');
    console.log(`✓ Connected to Chrome: ${versionData.Browser}`);

    // Create a new target/page
    const newPageRes = await fetch('http://127.0.0.1:9228/json/new', { method: 'PUT' });
    const pageTarget = await newPageRes.json();
    const cdp = new CdpClient(pageTarget.webSocketDebuggerUrl);
    await cdp.connect();

    await cdp.send('Network.enable');
    await cdp.send('Page.enable');
    await cdp.send('Runtime.enable');

    // Inject session cookie into Chrome
    await cdp.send('Network.setCookie', {
      name: 'drev_session',
      value: sessionCookieVal,
      domain: 'localhost',
      path: '/',
      httpOnly: true,
      sameSite: 'Lax',
    });
    console.log('✓ Session cookie injected into Chrome browser.');

    const evalInPage = async (expr) => {
      const res = await cdp.send('Runtime.evaluate', {
        expression: expr,
        returnByValue: true,
        awaitPromise: true,
      });
      return res.result ? res.result.value : undefined;
    };

    // --- TEST 1: Overview Page (/) ---
    console.log('\n--- 1. Testing Overview Page in Chrome (http://localhost:3001/) ---');
    await cdp.send('Page.navigate', { url: 'http://localhost:3001/' });
    await new Promise((r) => setTimeout(r, 2000));

    const overviewTitle = await evalInPage('(() => document.querySelector("h1")?.innerText)()');
    console.log(`  Page title: "${overviewTitle}"`);
    assert.equal(overviewTitle, 'System Overview');

    const statusBadges = await evalInPage('(() => Array.from(document.querySelectorAll("[role=status]")).map(el => el.innerText.trim()))()');
    console.log(`  Status badges rendered:`, statusBadges);
    assert(statusBadges.includes('Online'), 'Must show global Online indicator');
    const updateBadge = statusBadges.find((b) => b.startsWith('Updated') || b === 'Live');
    console.log(`  Live update badge: "${updateBadge}"`);
    assert(updateBadge, 'Overview must display live update timestamp');
    assert(
      !statusBadges.some((b) => b.includes('Fallback active')),
      'Overview must no longer display "Fallback active"'
    );

    const alertBanner = await evalInPage('(() => document.querySelector("[role=alert]")?.innerText)()');
    console.log(`  Alert banner present: ${alertBanner ? alertBanner : 'None (Correct)'}`);
    assert(!alertBanner, 'No error banner should be displayed');

    const cardValues = await evalInPage(`
      (() => Array.from(document.querySelectorAll('section span.tabular-nums')).map(el => el.innerText.trim()))()
    `);
    console.log('  Stat card values rendered:', cardValues);
    assert(cardValues.includes('100%'), 'Pass rate should be 100%');
    assert(cardValues.includes('1s'), 'Mean duration should be 1s');
    assert(cardValues.includes('0'), 'Queue depth should be 0');

    // --- TEST 2: Runs Page (/runs) ---
    console.log('\n--- 2. Testing Runs Explorer Page (/runs) ---');
    await cdp.send('Page.navigate', { url: 'http://localhost:3001/runs' });
    await new Promise((r) => setTimeout(r, 2000));

    const runsTitle = await evalInPage('(() => document.querySelector("h1")?.innerText)()');
    console.log(`  Page title: "${runsTitle}"`);
    assert.equal(runsTitle, 'Pipeline Runs');

    const runRowText = await evalInPage('(() => document.querySelector("table tbody tr")?.innerText)()');
    console.log(`  First run row contains: ${runRowText?.slice(0, 100)}...`);
    assert(runRowText && runRowText.includes('drev-self-test'), 'Runs table must show drev-self-test');
    assert(runRowText && runRowText.includes('fd30764f'), 'Runs table must show run ID prefix');

    // Test search filter
    await evalInPage(`
      (() => {
        const input = document.querySelector('input[placeholder*="Search"]');
        input.value = 'drev-self-test';
        input.dispatchEvent(new Event('input', { bubbles: true }));
      })()
    `);
    await new Promise((r) => setTimeout(r, 500));
    const searchMatchCount = await evalInPage('(() => document.querySelectorAll("table tbody tr").length)()');
    console.log(`  Search matching runs: ${searchMatchCount}`);
    assert.equal(searchMatchCount, 1);

    // Test non-matching search using native input setter so React onChange triggers
    await evalInPage(`
      (() => {
        const input = document.querySelector('input[placeholder*="Search"]');
        const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
        nativeSetter.call(input, 'nonexistent-query-xyz');
        input.dispatchEvent(new Event('input', { bubbles: true }));
      })()
    `);
    await new Promise((r) => setTimeout(r, 600));
    const noMatchCount = await evalInPage('(() => document.querySelectorAll("table tbody tr").length)()');
    const emptyStateText = await evalInPage('(() => document.body.innerText)()');
    console.log(`  Non-matching search rows: ${noMatchCount}, contains "No matching runs found": ${emptyStateText.includes('No matching runs found')}`);
    assert(emptyStateText.includes('No matching runs found'));

    // Reset search
    await evalInPage(`
      (() => {
        const input = document.querySelector('input[placeholder*="Search"]');
        const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
        nativeSetter.call(input, '');
        input.dispatchEvent(new Event('input', { bubbles: true }));
      })()
    `);
    await new Promise((r) => setTimeout(r, 600));

    // --- TEST 3: Browser History Back / Forward ---
    console.log('\n--- 3. Testing Browser History (Back / Forward) ---');
    await cdp.send('Page.navigate', { url: 'http://localhost:3001/' });
    await new Promise((r) => setTimeout(r, 1000));
    assert.equal(await evalInPage('(() => location.pathname)()'), '/');

    await cdp.send('Page.navigate', { url: 'http://localhost:3001/runs' });
    await new Promise((r) => setTimeout(r, 1000));
    assert.equal(await evalInPage('(() => location.pathname)()'), '/runs');

    await evalInPage('(() => window.history.back())()');
    await new Promise((r) => setTimeout(r, 1000));
    const backPath = await evalInPage('(() => location.pathname)()');
    console.log(`  After history.back(): "${backPath}"`);
    assert.equal(backPath, '/');

    await evalInPage('(() => window.history.forward())()');
    await new Promise((r) => setTimeout(r, 1000));
    const fwdPath = await evalInPage('(() => location.pathname)()');
    console.log(`  After history.forward(): "${fwdPath}"`);
    assert.equal(fwdPath, '/runs');

    cdp.close();
    console.log('\n✓ ALL POST-UPGRADE BROWSER CHECKS PASSED SUCCESSFULLY!');
  } finally {
    try { chromeProc.kill('SIGKILL'); } catch {}
    try { fs.rmSync(userDataDir, { recursive: true, force: true }); } catch {}
  }
}

runBrowserVerification().catch((err) => {
  console.error('\n❌ Post-upgrade browser verification failed:', err);
  process.exit(1);
});
