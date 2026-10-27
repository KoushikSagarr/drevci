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
    this.consoleErrors = [];
  }

  async connect() {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(this.wsUrl);
      this.ws.onopen = () => resolve();
      this.ws.onerror = (err) => reject(err);
      this.ws.onmessage = (event) => {
        const msg = JSON.parse(event.data);
        if (msg.method === 'Runtime.consoleAPICalled' && msg.params.type === 'error') {
          const text = msg.params.args.map((a) => a.value || a.description || '').join(' ');
          this.consoleErrors.push(text);
        }
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

async function runBrowserQa() {
  console.log('=== REAL CHROME BROWSER QA PASS 2 VALIDATION ===\n');

  const password = getDashboardPassword();
  assert(password, 'DASHBOARD_PASSWORD must be available in .env');

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
  console.log('✓ Session cookie acquired.');

  const chromePath = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
  const userDataDir = '/tmp/drev-chrome-qa-pass2-' + Date.now();
  const chromeProc = spawn(chromePath, [
    '--headless',
    '--disable-gpu',
    '--remote-debugging-port=9230',
    `--user-data-dir=${userDataDir}`,
    'about:blank',
  ]);

  try {
    let versionData = null;
    for (let i = 0; i < 20; i++) {
      try {
        const res = await fetch('http://127.0.0.1:9230/json/version');
        versionData = await res.json();
        break;
      } catch {
        await new Promise((r) => setTimeout(r, 200));
      }
    }
    assert(versionData, 'Chrome CDP must be reachable');
    console.log(`✓ Connected to Chrome: ${versionData.Browser}`);

    const newPageRes = await fetch('http://127.0.0.1:9230/json/new', { method: 'PUT' });
    const pageTarget = await newPageRes.json();
    const cdp = new CdpClient(pageTarget.webSocketDebuggerUrl);
    await cdp.connect();

    await cdp.send('Network.enable');
    await cdp.send('Page.enable');
    await cdp.send('Runtime.enable');

    await cdp.send('Network.setCookie', {
      name: 'drev_session',
      value: sessionCookieVal,
      domain: 'localhost',
      path: '/',
      httpOnly: true,
      sameSite: 'Lax',
    });

    const evalInPage = async (expr) => {
      const res = await cdp.send('Runtime.evaluate', {
        expression: expr,
        returnByValue: true,
        awaitPromise: true,
      });
      return res.result ? res.result.value : undefined;
    };

    // --- 1. OVERVIEW PAGE ---
    console.log('\n--- 1. Testing Overview Page ---');
    await cdp.send('Page.navigate', { url: 'http://localhost:3001/' });
    await new Promise((r) => setTimeout(r, 2000));

    const overviewTitle = await evalInPage('(() => document.querySelector("h1")?.innerText)()');
    assert.equal(overviewTitle, 'System Overview');

    const statusBadges = await evalInPage('(() => Array.from(document.querySelectorAll("[role=status]")).map(el => el.innerText.trim()))()');
    console.log('  Status badges:', statusBadges);
    assert(statusBadges.includes('Online'), 'Must show global Online indicator');
    assert(statusBadges.some((b) => b.startsWith('Updated') || b === 'Live'), 'Must show live update status');
    assert(!statusBadges.some((b) => b.includes('Fallback active')), 'Must not show fallback mode when live stats succeed');

    const cardValues = await evalInPage('(() => Array.from(document.querySelectorAll("section span.tabular-nums")).map(el => el.innerText.trim()))()');
    console.log('  Metric cards:', cardValues);
    assert(cardValues.includes('100%'), 'Pass rate must be 100%');
    assert(cardValues.includes('1s'), 'Mean duration must be 1s');
    assert(cardValues.includes('0'), 'Queue depth must be 0');
    assert(cardValues.includes('3'), 'Worker capacity must be 3');

    const failuresText = await evalInPage('(() => document.body.innerText)()');
    assert(failuresText.includes('Zero pipeline failures'), 'Failures panel must indicate zero failures');

    // --- 2. RUNS EXPLORER PAGE ---
    console.log('\n--- 2. Testing Runs Explorer Page ---');
    await cdp.send('Page.navigate', { url: 'http://localhost:3001/runs' });
    await new Promise((r) => setTimeout(r, 2000));

    const runsTitle = await evalInPage('(() => document.querySelector("h1")?.innerText)()');
    assert.equal(runsTitle, 'Pipeline Runs');

    // Filter pipeline by query string ?pipeline=all (must show all runs)
    await cdp.send('Page.navigate', { url: 'http://localhost:3001/runs?pipeline=all' });
    await new Promise((r) => setTimeout(r, 1500));
    const allPipelineRows = await evalInPage('(() => document.querySelectorAll("table tbody tr").length)()');
    console.log(`  Runs count with ?pipeline=all: ${allPipelineRows}`);
    assert.equal(allPipelineRows, 1);

    // Filter by branch
    const branchSelector = await evalInPage('(() => document.querySelector("select[aria-label=\\"Filter by branch\\"]") != null)()');
    assert(branchSelector, 'Branch filter select must be present');

    // Test text search
    await evalInPage(`
      (() => {
        const input = document.querySelector('input[placeholder*="Search"]');
        const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
        nativeSetter.call(input, 'fd30764f');
        input.dispatchEvent(new Event('input', { bubbles: true }));
      })()
    `);
    await new Promise((r) => setTimeout(r, 500));
    const matchedCount = await evalInPage('(() => document.querySelectorAll("table tbody tr").length)()');
    console.log(`  Search matching run ID prefix: ${matchedCount}`);
    assert.equal(matchedCount, 1);

    // --- 3. RUN DETAIL & LOG STREAMING ---
    console.log('\n--- 3. Testing Run Detail & Logs (/runs/fd30764f-8cf9-420f-a35b-33fcd34a996a) ---');
    await cdp.send('Page.navigate', { url: 'http://localhost:3001/runs/fd30764f-8cf9-420f-a35b-33fcd34a996a' });
    await new Promise((r) => setTimeout(r, 2500));

    const detailHeader = await evalInPage('(() => document.querySelector("h1")?.innerText)()');
    console.log(`  Run detail header: "${detailHeader}"`);
    assert.equal(detailHeader, 'DREV-SELF-TEST');

    const timelineText = await evalInPage('(() => document.body.innerText)()');
    assert(timelineText.includes('verify'), 'Timeline must show job name verify');

    const logLinesCount = await evalInPage('(() => document.querySelectorAll("[id^=\\"log-line-\\"]").length)()');
    console.log(`  Rendered log lines count: ${logLinesCount}`);
    assert(logLinesCount > 0, 'Log viewer must render streamed log lines');

    // --- 4. NON-EXISTENT RUN ID (404 NOT FOUND) ---
    console.log('\n--- 4. Testing Non-Existent Run (404 Not Found) ---');
    await cdp.send('Page.navigate', { url: 'http://localhost:3001/runs/00000000-0000-0000-0000-000000000000' });
    await new Promise((r) => setTimeout(r, 2000));

    const notFoundTitle = await evalInPage('(() => document.querySelector("h2")?.innerText)()');
    const notFoundBody = await evalInPage('(() => document.body.innerText)()');
    console.log(`  404 Heading: "${notFoundTitle}"`);
    assert.equal(notFoundTitle, 'Run Not Found');
    assert(notFoundBody.includes('does not exist or may have expired'));
    assert(notFoundBody.includes('Back to Pipeline Runs'));

    // --- 5. MOBILE / NARROW VIEWPORT (375x812) ---
    console.log('\n--- 5. Testing Mobile / Responsive Viewport (375x812) ---');
    await cdp.send('Emulation.setDeviceMetricsOverride', {
      width: 375,
      height: 812,
      deviceScaleFactor: 2,
      mobile: true,
    });
    await cdp.send('Page.navigate', { url: 'http://localhost:3001/' });
    await new Promise((r) => setTimeout(r, 2000));

    const mobileBodyWidth = await evalInPage('(() => document.body.scrollWidth)()');
    console.log(`  Mobile body scrollWidth at 375px: ${mobileBodyWidth}px`);
    assert(mobileBodyWidth <= 385, 'Mobile layout must fit without severe horizontal blowouts');

    // Reset viewport
    await cdp.send('Emulation.clearDeviceMetricsOverride');

    // Check captured console errors
    console.log('\n--- 6. Console Error Check ---');
    console.log(`  Captured console errors: ${cdp.consoleErrors.length}`);
    if (cdp.consoleErrors.length > 0) {
      console.log('  Errors:', cdp.consoleErrors);
    }
    // Filter out expected 404 test log:
    const fatalErrors = cdp.consoleErrors.filter((e) => !e.includes('404'));
    assert.equal(fatalErrors.length, 0, 'Must have zero unexpected console errors');

    cdp.close();
    console.log('\n✓ ALL CHROME BROWSER QA PASS 2 CHECKS PASSED SUCCESSFULLY!');
  } finally {
    try { chromeProc.kill('SIGKILL'); } catch {}
    try { fs.rmSync(userDataDir, { recursive: true, force: true }); } catch {}
  }
}

runBrowserQa().catch((err) => {
  console.error('\n❌ Browser QA failed:', err);
  process.exit(1);
});
