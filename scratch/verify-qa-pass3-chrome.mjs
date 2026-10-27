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

async function runQaPass3() {
  console.log('=== DREV CI — DASHBOARD QA PASS 3: CHROME CDP VERIFICATION ===\n');

  const password = getDashboardPassword();
  assert(password, 'DASHBOARD_PASSWORD must be available in .env');

  // 1. Session authentication
  const loginRes = await fetch('http://localhost:3001/api/auth/login', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Host': 'localhost:3001',
      'Origin': 'http://localhost:3001',
    },
    body: JSON.stringify({ password }),
  });
  assert.equal(loginRes.status, 200, 'HTTP Login must succeed');
  const cookieHeader = loginRes.headers.get('set-cookie');
  const sessionCookieVal = cookieHeader.split(';')[0].split('=')[1];
  console.log('✓ Session cookie acquired.');

  const chromePath = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
  const userDataDir = '/tmp/drev-chrome-qa-pass3-' + Date.now();
  const chromeProc = spawn(chromePath, [
    '--headless',
    '--disable-gpu',
    '--remote-debugging-port=9231',
    `--user-data-dir=${userDataDir}`,
    'about:blank',
  ]);

  try {
    let versionData = null;
    for (let i = 0; i < 20; i++) {
      try {
        const res = await fetch('http://127.0.0.1:9231/json/version');
        versionData = await res.json();
        break;
      } catch {
        await new Promise((r) => setTimeout(r, 200));
      }
    }
    assert(versionData, 'Chrome CDP must be reachable');
    console.log(`✓ Connected to Chrome: ${versionData.Browser}`);

    const newPageRes = await fetch('http://127.0.0.1:9231/json/new', { method: 'PUT' });
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

    const setViewport = async (width, height = 900) => {
      await cdp.send('Emulation.setDeviceMetricsOverride', {
        width,
        height,
        deviceScaleFactor: 1,
        mobile: width < 768,
      });
      await new Promise((r) => setTimeout(r, 150));
    };

    const evalInPage = async (expr) => {
      const res = await cdp.send('Runtime.evaluate', {
        expression: expr,
        returnByValue: true,
        awaitPromise: true,
      });
      return res.result ? res.result.value : undefined;
    };

    const breakpoints = [
      { name: 'Mobile', width: 375 },
      { name: 'Tablet', width: 768 },
      { name: 'Desktop', width: 1280 },
    ];

    // ==========================================
    // 1. OVERVIEW PAGE ACROSS BREAKPOINTS
    // ==========================================
    console.log('\n--- 1. Overview Page Responsiveness & Health ---');
    await cdp.send('Page.navigate', { url: 'http://localhost:3001/' });
    await new Promise((r) => setTimeout(r, 2000));

    for (const bp of breakpoints) {
      await setViewport(bp.width);
      const docScrollWidth = await evalInPage('document.documentElement.scrollWidth');
      const bodyScrollWidth = await evalInPage('document.body.scrollWidth');
      const h1Text = await evalInPage('document.querySelector("h1")?.innerText');
      const cards = await evalInPage('Array.from(document.querySelectorAll("section span.tabular-nums")).map(e => e.innerText.trim())');
      const statusBadges = await evalInPage('Array.from(document.querySelectorAll("[role=status]")).map(e => e.innerText.trim())');

      console.log(`  [${bp.name} - ${bp.width}px] h1="${h1Text}", scrollWidth=${docScrollWidth}px / ${bodyScrollWidth}px`);
      console.log(`    Cards: ${JSON.stringify(cards)}, Badges: ${JSON.stringify(statusBadges)}`);

      // Verify no horizontal overflow beyond viewport width
      assert(
        docScrollWidth <= bp.width + 1,
        `Page should not horizontally overflow viewport at ${bp.width}px (got ${docScrollWidth}px)`
      );
      assert.equal(h1Text, 'System Overview');
      assert(statusBadges.includes('Online'));
      assert(statusBadges.some((b) => b.startsWith('Updated')));
    }

    // ==========================================
    // 2. RUNS EXPLORER PAGE ACROSS BREAKPOINTS
    // ==========================================
    console.log('\n--- 2. Runs Explorer Responsiveness & 4-Way Filters ---');
    await cdp.send('Page.navigate', { url: 'http://localhost:3001/runs' });
    await new Promise((r) => setTimeout(r, 2000));

    for (const bp of breakpoints) {
      await setViewport(bp.width);
      const docScrollWidth = await evalInPage('document.documentElement.scrollWidth');
      const rowCount = await evalInPage('document.querySelectorAll("table tbody tr").length');
      const hasFilterBar = await evalInPage('document.querySelector("input[aria-label=\\"Search runs by commit, author, branch, or ID\\"]") != null');

      console.log(`  [${bp.name} - ${bp.width}px] scrollWidth=${docScrollWidth}px, rows=${rowCount}, filterBar=${hasFilterBar}`);
      assert(
        docScrollWidth <= bp.width + 1,
        `Runs page should not horizontally overflow viewport at ${bp.width}px (got ${docScrollWidth}px)`
      );
      assert(hasFilterBar, 'Search input should be accessible across breakpoints');
      assert.equal(rowCount, 1, 'Should display existing run');
    }

    // Interactive filter round-trip check
    console.log('\n--- 2b. Runs Explorer Interactive Filter Testing ---');
    await setViewport(1280);

    // Test Sort runs selector (duration-desc)
    await evalInPage(`
      (() => {
        const sortSelect = document.querySelector('select[aria-label="Sort runs"]');
        if (sortSelect) {
          const setter = Object.getOwnPropertyDescriptor(window.HTMLSelectElement.prototype, 'value').set;
          setter.call(sortSelect, 'duration-desc');
          sortSelect.dispatchEvent(new Event('change', { bubbles: true }));
        }
      })()
    `);
    await new Promise((r) => setTimeout(r, 800));
    let rowsAfterSort = await evalInPage('document.querySelectorAll("table tbody tr").length');
    let urlAfterSort = await evalInPage('window.location.search');
    console.log(`  Filtered by sort=duration-desc -> rows: ${rowsAfterSort}, query: ${urlAfterSort}`);
    assert.equal(rowsAfterSort, 1);
    assert(urlAfterSort.includes('sort=duration-desc'));

    // Matching text search
    await evalInPage(`
      (() => {
        const input = document.querySelector('input[aria-label="Search runs by commit, author, branch, or ID"]');
        if (input) {
          const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
          setter.call(input, 'drev');
          input.dispatchEvent(new Event('input', { bubbles: true }));
        }
      })()
    `);
    await new Promise((r) => setTimeout(r, 800));
    let rowsAfterSearchMatch = await evalInPage('document.querySelectorAll("table tbody tr").length');
    let urlAfterSearch = await evalInPage('window.location.search');
    console.log(`  Search matching 'drev' -> rows: ${rowsAfterSearchMatch}, query: ${urlAfterSearch}`);
    assert.equal(rowsAfterSearchMatch, 1);
    assert(urlAfterSearch.includes('q=drev'));

    // Non-matching search query
    await evalInPage(`
      (() => {
        const input = document.querySelector('input[aria-label="Search runs by commit, author, branch, or ID"]');
        if (input) {
          const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
          setter.call(input, 'non-existent-sha-query-xyz');
          input.dispatchEvent(new Event('input', { bubbles: true }));
        }
      })()
    `);
    await new Promise((r) => setTimeout(r, 800));
    let runRowsAfterSearch = await evalInPage('document.querySelectorAll("table tbody tr:not(:has(td[colspan]))").length');
    let hasEmptyRow = await evalInPage('document.body.innerText.includes("No matching runs found")');
    console.log(`  Search non-existent -> runRows: ${runRowsAfterSearch}, hasEmptyMessage: ${hasEmptyRow}`);
    assert.equal(runRowsAfterSearch, 0);
    assert.equal(hasEmptyRow, true);

    // Reset filters button
    await evalInPage(`
      (() => {
        const resetBtn = Array.from(document.querySelectorAll('button')).find(b => b.innerText.includes('Clear all filters'));
        if (resetBtn) resetBtn.click();
      })()
    `);
    await new Promise((r) => setTimeout(r, 800));
    let runRowsAfterReset = await evalInPage('document.querySelectorAll("table tbody tr:not(:has(td[colspan]))").length');
    let urlAfterReset = await evalInPage('window.location.search');
    console.log(`  After Reset -> runRows: ${runRowsAfterReset}, query: "${urlAfterReset}"`);
    assert.equal(runRowsAfterReset, 1);
    assert.equal(urlAfterReset, '');

    // ==========================================
    // 3. RUN DETAIL PAGE (EXISTING RUN)
    // ==========================================
    console.log('\n--- 3. Run Details & Log Viewer (fd30764f-8cf9-420f-a35b-33fcd34a996a) ---');
    await cdp.send('Page.navigate', { url: 'http://localhost:3001/runs/fd30764f-8cf9-420f-a35b-33fcd34a996a' });
    await new Promise((r) => setTimeout(r, 2000));

    for (const bp of breakpoints) {
      await setViewport(bp.width);
      const docScrollWidth = await evalInPage('document.body.scrollWidth');
      const runTitle = await evalInPage('document.querySelector("h1")?.innerText');
      const breadcrumbLink = await evalInPage('document.querySelector("a[href*=\\"pipeline=\\"]")?.getAttribute("href")');

      console.log(`  [${bp.name} - ${bp.width}px] scrollWidth=${docScrollWidth}px, title="${runTitle}", breadcrumb="${breadcrumbLink}"`);
      assert(
        docScrollWidth <= bp.width + 10,
        `Run detail should not horizontally overflow viewport at ${bp.width}px (got ${docScrollWidth}px)`
      );
      assert.equal(runTitle, 'DREV-SELF-TEST');
      assert.equal(breadcrumbLink, '/runs?pipeline=drev-self-test');
    }

    // Verify copy plain-text logs
    await setViewport(1280);
    const logLineCount = await evalInPage('document.querySelectorAll("[id^=\\"log-line-\\"]").length');
    console.log(`  Log output elements rendered: ${logLineCount}`);
    assert(logLineCount > 0, 'Log viewer must render lines');

    // ==========================================
    // 4. RUN DETAIL PAGE: 404 RUN NOT FOUND
    // ==========================================
    console.log('\n--- 4. Run Details: Non-existent Run (404 Classification) ---');
    await cdp.send('Page.navigate', { url: 'http://localhost:3001/runs/00000000-0000-0000-0000-000000000000' });
    await new Promise((r) => setTimeout(r, 2000));

    for (const bp of breakpoints) {
      await setViewport(bp.width);
      const notFoundHeading = await evalInPage('document.querySelector("h2")?.innerText');
      const hasBackLink = await evalInPage('Array.from(document.querySelectorAll("a")).some(a => a.innerText.includes("Back to Pipeline Runs") || a.href.includes("/runs"))');
      const docScrollWidth = await evalInPage('document.body.scrollWidth');

      console.log(`  [${bp.name} - ${bp.width}px] scrollWidth=${docScrollWidth}px, heading="${notFoundHeading}", backLink=${hasBackLink}`);
      assert.equal(notFoundHeading, 'Run Not Found');
      assert(hasBackLink, 'Must provide back link to /runs');
      assert(docScrollWidth <= bp.width + 10);
    }

    // ==========================================
    // 5. CONSOLE ERROR AUDIT
    // ==========================================
    console.log('\n--- 5. Console Error Audit ---');
    const filteredErrors = cdp.consoleErrors.filter((err) => {
      // 404 network fetch error when intentionally navigating to 404 is expected
      if (err.includes('404') || err.toLowerCase().includes('not found')) return false;
      return true;
    });

    console.log(`  Total browser console errors observed: ${cdp.consoleErrors.length}`);
    if (filteredErrors.length > 0) {
      console.log('  Unexpected console errors:', filteredErrors);
    } else {
      console.log('  ✓ Zero unexpected browser console errors.');
    }
    assert.equal(filteredErrors.length, 0, 'No unexpected console errors should occur');

    console.log('\n=== ALL PHASE E CHROME CDP CHECKS PASSED ===\n');
  } finally {
    try {
      chromeProc.kill('SIGTERM');
    } catch {
      // ignore
    }
    try {
      fs.rmSync(userDataDir, { recursive: true, force: true });
    } catch {
      // ignore
    }
  }
}

runQaPass3().catch((err) => {
  console.error('\n✖ QA PASS 3 FAILED:', err);
  process.exit(1);
});
