// Browser check of the built book: every diagram goes live with no console
// error (a live output that differs from catalog.json is reported as one), and
// a real pointer drag on the Join page changes its output.
//
// Usage: node check-site.mjs <book dir> [chrome binary] [profile dir] [screenshot dir]
// Exit code 0 only if every check passes.
import { createServer } from 'node:http';
import { readFile, readdir, writeFile, mkdtemp, mkdir } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { extname, join, normalize, resolve } from 'node:path';
import { tmpdir } from 'node:os';

const [bookArg, chromeBin = 'chromium', profileArg, shotDir] = process.argv.slice(2);
const root = resolve(bookArg);
const profile = profileArg ?? await mkdtemp(join(tmpdir(), 'trill-docs-'));
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json',
  '.wasm': 'application/wasm', '.svg': 'image/svg+xml', '.woff2': 'font/woff2' };
const sleep = ms => new Promise(r => setTimeout(r, ms));

const server = createServer(async (req, res) => {
  let path = normalize(join(root, decodeURIComponent(new URL(req.url, 'http://x').pathname)));
  if (!path.startsWith(root)) { res.writeHead(403).end(); return; }
  if (path.endsWith('/')) path += 'index.html';
  let body;
  try { body = await readFile(path); } catch { res.writeHead(404).end(); return; }
  res.writeHead(200, { 'content-type': MIME[extname(path)] ?? 'application/octet-stream' }).end(body);
}).listen(0, '127.0.0.1');
await new Promise(r => server.on('listening', r));
const base = `http://127.0.0.1:${server.address().port}`;

const port = 9300 + Math.floor(Math.random() * 500);
const chrome = spawn(chromeBin, ['--headless=new', `--remote-debugging-port=${port}`, '--no-first-run',
  `--user-data-dir=${profile}`, '--window-size=1000,1200', 'about:blank'], { stdio: 'ignore' });
// A page that never goes live (a script error, say) would make every wait run
// to its limit. Stop the whole check, and the browser with it, after 5 minutes.
const watchdog = setTimeout(() => {
  console.error('check-site: gave up after 5 minutes');
  chrome.kill('SIGTERM');
  process.exit(3);
}, 300000);
let target;
for (let i = 0; i < 150 && !target; i++) {
  try { target = (await (await fetch(`http://127.0.0.1:${port}/json/list`)).json()).find(t => t.type === 'page'); }
  catch { await sleep(200); }
}
if (!target) { console.error(`could not start ${chromeBin}`); chrome.kill('SIGTERM'); process.exit(2); }

const ws = new WebSocket(target.webSocketDebuggerUrl);
await new Promise(r => ws.addEventListener('open', r));
let id = 0; const waiting = new Map(); let errors = [];
ws.addEventListener('message', m => {
  const msg = JSON.parse(m.data);
  if (msg.id && waiting.has(msg.id)) { waiting.get(msg.id)(msg); waiting.delete(msg.id); }
  if (msg.method === 'Runtime.exceptionThrown') errors.push(msg.params.exceptionDetails.exception?.description ?? 'exception');
  if (msg.method === 'Runtime.consoleAPICalled' && msg.params.type === 'error')
    errors.push(msg.params.args.map(a => a.value ?? a.description).join(' '));
});
const cdp = (method, params = {}) => new Promise(r => { const i = ++id; waiting.set(i, r); ws.send(JSON.stringify({ id: i, method, params })); });
const evaluate = async expr => (await cdp('Runtime.evaluate', { expression: expr, returnByValue: true })).result?.result?.value;
async function shot(name) {
  if (!shotDir) return;
  await mkdir(shotDir, { recursive: true });
  const { width, height } = (await cdp('Page.getLayoutMetrics')).result.cssContentSize;
  const r = await cdp('Page.captureScreenshot', { format: 'png', captureBeyondViewport: true, clip: { x: 0, y: 0, width, height, scale: 1 } });
  await writeFile(join(shotDir, `${name}.png`), Buffer.from(r.result.data, 'base64'));
}
async function visit(path) {
  errors = [];
  await cdp('Page.navigate', { url: `${base}/${path}` });
  let state = {};
  for (let i = 0; i < 300; i++) {
    await sleep(100);
    state = await evaluate(`({ total: document.querySelectorAll('.trill-marbles').length,
      live: document.querySelectorAll('.trill-marbles[data-ready=live]').length })`) ?? {};
    if (state.total > 0 && state.live === state.total) break;
  }
  return state;
}

// Every event glyph (bar, marble, label, edge tick) must lie inside its
// diagram's viewBox: data that agrees with Trill is no use if it is clipped.
async function geometryProblems() {
  return await evaluate(`[...document.querySelectorAll('.trill-marbles svg')].flatMap(svg => {
    const vb = svg.viewBox.baseVal;
    return [...svg.querySelectorAll('.ev')].map(g => [g, g.getBBox()])
      .filter(([, b]) => b.x < -0.5 || b.x + b.width > vb.width + 0.5 || b.y < -0.5 || b.y + b.height > vb.height + 0.5)
      .map(([g, b]) => 'clipped glyph "' + g.textContent.trim() + '" spans x ' + b.x.toFixed(0) + '..' + (b.x + b.width).toFixed(0)
        + ', y ' + b.y.toFixed(0) + '..' + (b.y + b.height).toFixed(0) + ' of ' + vb.width + 'x' + vb.height);
  })`) ?? ['could not measure geometry'];
}

// Press on the element that the JavaScript expression `target` returns, move it
// by the given number of time units in small steps, release, and let the
// recompute finish.
async function drag(target, units, beforeRelease) {
  const box = await evaluate(`(() => { const t = (${target}); t.scrollIntoView({ block: 'center' });
    const r = t.getBoundingClientRect();
    const s = t.closest('svg');
    const vb = s.viewBox.baseVal;
    return { x: r.left + Math.min(10, r.width / 2), y: r.top + r.height / 2,
      unit: s.getBoundingClientRect().width / vb.width * (vb.width - 84 - 24) / Number(s.dataset.axisEnd) }; })()`);
  const mouse = (type, x) => cdp('Input.dispatchMouseEvent', { type, x, y: box.y, button: 'left', buttons: type === 'mouseReleased' ? 0 : 1, clickCount: 1 });
  await mouse('mousePressed', box.x);
  const steps = 2 * Math.abs(units), dir = Math.sign(units);
  for (let k = 1; k <= steps; k++) { await mouse('mouseMoved', box.x + dir * k * box.unit / 2); await sleep(30); }
  await sleep(100);
  if (beforeRelease) await beforeRelease(box.x + units * box.unit, box.unit);
  await mouse('mouseReleased', box.x + units * box.unit);
  await sleep(300);
}

// After a drag scenario, before the next visit() clears them: any console
// error or exception raised during the drags is a failure, and the diagram
// must still be live.
async function scenarioErrors(name) {
  for (const e of errors) fail(`${name}: console error: ${e}`);
  const live = await evaluate("document.querySelector('.trill-marbles')?.dataset.ready");
  if (live !== 'live') fail(`${name}: diagram is no longer live (${live})`);
}

let failures = 0;
const fail = msg => { failures++; console.log(`FAIL ${msg}`); };
try {
  await cdp('Runtime.enable'); await cdp('Page.enable'); await cdp('Network.enable');
  await cdp('Network.setCacheDisabled', { cacheDisabled: true });

  const pages = ['introduction.html', ...(await readdir(join(root, 'operators'))).filter(f => f.endsWith('.html')).map(f => `operators/${f}`)];
  for (const page of pages) {
    const { total, live } = await visit(page);
    if (!total || live !== total) fail(`${page}: ${live ?? 0} of ${total ?? 0} diagrams went live`);
    for (const e of errors) fail(`${page}: console error: ${e}`);
    const clipped = await geometryProblems();
    for (const c of clipped) fail(`${page}: ${c}`);
    if (total && live === total && !errors.length && !clipped.length) console.log(`ok   ${page}`);
  }

  // A drag that moves output past the inputs' 0..16 range must widen the axis,
  // not clip: move the last one-tick input of the tumbling window to the end.
  await visit('operators/tumbling-window.html');
  // Input marbles carry a <title>; legend swatches do not. The last one is the event at 9.
  // Moving it 5 ticks (9 to 14) takes its output past 16. The axis must stay
  // frozen during the drag, so the circle is still under the pointer just
  // before release, and the event must land exactly 5 ticks on.
  const lastMarble = "[...document.querySelectorAll('.trill-marbles g.ev')].filter(g => g.querySelector('title') && g.querySelector('circle')).at(-1)";
  await drag(lastMarble, 5, async (pointerX, tick) => {
    const cx = await evaluate(`(() => { const r = (${lastMarble}).querySelector('circle').getBoundingClientRect(); return r.left + r.width / 2; })()`);
    if (!(Math.abs(cx - pointerX) < tick / 2)) fail(`tumbling-window: during the drag the circle is ${(cx - pointerX).toFixed(1)} px from the pointer (axis rescaled mid-drag?)`);
    else console.log('ok   tumbling-window: the dragged circle stays under the pointer');
  });
  const movedTo = await evaluate("JSON.parse(document.querySelector('.trill-marbles').dataset.inputs)[0].map(e => e.start).at(-1)");
  if (movedTo !== 14) fail(`tumbling-window: a 5-tick drag of the event at 9 landed at ${movedTo}, not 14`);
  else console.log('ok   tumbling-window: a 5-tick drag lands 5 ticks on');
  await scenarioErrors('tumbling-window drag');
  const maxEnd = await evaluate("Math.max(...JSON.parse(document.querySelector('.trill-marbles').dataset.output).map(e => e.end ?? 0))");
  if (!(maxEnd > 16)) fail(`tumbling-window drag did not move output past 16 (max end ${maxEnd}), so the geometry check below proves nothing`);
  const wide = await geometryProblems();
  for (const c of wide) fail(`tumbling-window after drag: ${c}`);
  if (!wide.length) console.log('ok   tumbling-window: output past the input range stays on screen');

  // A short output at the right edge puts its label after the bar; the axis
  // must widen for the label too. Shorten LeftOuterJoin's a:3 [6,8) to one
  // tick, then move it to [15,16): its output a:3+none [15,16) has a label
  // wider than its bar.
  await visit('operators/left-outer-join.html');
  const a3 = "[...document.querySelectorAll('.trill-marbles g.ev')].find(g => g.querySelector('title')?.textContent.startsWith('[6, 8)  a:3'))";
  await drag(`${a3}.querySelector('.grip')`, -1);
  await drag("[...document.querySelectorAll('.trill-marbles g.ev')].find(g => g.querySelector('title')?.textContent.startsWith('[6, 7)  a:3')).querySelector('.fill')", 9);
  const placed = await evaluate("JSON.parse(document.querySelector('.trill-marbles').dataset.output).some(e => e.start === 15 && e.end === 16 && e.label === '3+none')");
  if (!placed) fail('left-outer-join drag did not produce a:3+none [15,16), so the label check below proves nothing');
  await scenarioErrors('left-outer-join drag');
  const edge = await geometryProblems();
  for (const c of edge) fail(`left-outer-join after drag: ${c}`);
  if (placed && !edge.length) console.log('ok   left-outer-join: a label after a short bar at the right edge stays on screen');

  // mdBook's print page holds every diagram. Widening one diagram's axis must
  // not change another's drag scale: widen Tumbling's axis, then a 3-tick drag
  // of Join's first input must move it from [1,5) to [4,8).
  await visit('print.html');
  const onPrint = id => `document.querySelector('.trill-marbles[data-op=${id}]')`;
  await drag(`[...${onPrint('tumbling-window')}.querySelectorAll('g.ev')].filter(g => g.querySelector('title') && g.querySelector('circle')).at(-1)`, 5);
  await drag(`${onPrint('join')}.querySelector('.ev .fill')`, 3, async (pointerX, tick) => {
    // The press was 10 px into the bar, so the bar's left edge should now sit 10 px left of the pointer.
    // Find a:1 by its label: lanes re-sort by start time, so it is no longer the first bar.
    const left = await evaluate(`[...${onPrint('join')}.querySelectorAll('g.ev')].find(g => g.querySelector('title')?.textContent.includes('  a:1  ')).querySelector('.fill').getBoundingClientRect().left`);
    if (!(Math.abs(left + 10 - pointerX) < tick / 2)) fail(`print page: during the Join drag its bar is ${(left + 10 - pointerX).toFixed(1)} px from the pointer`);
  });
  const joinFirst = await evaluate(`JSON.parse(${onPrint('join')}.dataset.inputs)[0][0]`);
  if (joinFirst?.start !== 4 || joinFirst?.end !== 8)
    fail(`print page: after Tumbling's axis widened, a 3-tick drag of Join's first input gave [${joinFirst?.start},${joinFirst?.end}), not [4,8)`);
  else console.log('ok   print page: one diagram\'s axis does not change another\'s drag');
  await scenarioErrors('print page drags');

  await visit('operators/join.html');
  await shot('join');
  const before = await evaluate("document.querySelector('.trill-marbles').dataset.output");
  await drag("document.querySelector('.trill-marbles .ev .fill')", 3);
  if (before === await evaluate("document.querySelector('.trill-marbles').dataset.output")) fail('join: dragging an input did not change the output');
  else console.log('ok   join: drag recomputed the output');
  await scenarioErrors('join drag');
  await shot('join-after-drag');
  await cdp('Emulation.setEmulatedMedia', { features: [{ name: 'prefers-color-scheme', value: 'dark' }] });
  await visit('operators/left-outer-join.html');
  await shot('left-outer-join-dark');
} finally {
  clearTimeout(watchdog); ws.close(); chrome.kill(); server.close();
}
console.log(failures ? `${failures} check(s) failed` : 'all checks passed');
process.exit(failures ? 1 : 0);
