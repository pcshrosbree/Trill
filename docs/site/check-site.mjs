// Browser check of the built book: every diagram goes live with no console
// error (a live output that differs from catalog.json is reported as one), the
// index page lists the catalogue's operators, and a real pointer drag on the Join
// page changes its output.
//
// Usage: node check-site.mjs <book dir> [chrome binary] [profile dir] [screenshot dir]
// Exit code 0 only if every check passes.
import { createServer } from 'node:http';
import { readFile, readdir, writeFile, mkdtemp, mkdir, stat } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { basename, dirname, extname, join, normalize, resolve, sep } from 'node:path';
import { tmpdir } from 'node:os';

const [bookArg, chromeBin = 'chromium', profileArg, shotDir] = process.argv.slice(2);
const root = resolve(bookArg);
const profile = profileArg ?? await mkdtemp(join(tmpdir(), 'trill-docs-'));
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json',
  '.wasm': 'application/wasm', '.svg': 'image/svg+xml', '.woff2': 'font/woff2' };
const sleep = ms => new Promise(r => setTimeout(r, ms));

// The file the server serves for a URL path, or null for a path outside the
// book (refused). The index check maps links through this too.
const fileFor = pathname => {
  const path = normalize(join(root, decodeURIComponent(pathname)));
  if (path !== root && !path.startsWith(root + sep)) return null;
  return path.endsWith('/') ? path + 'index.html' : path;
};
const server = createServer(async (req, res) => {
  const path = fileFor(new URL(req.url, 'http://x').pathname);
  if (!path) { res.writeHead(403).end(); return; }
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
  // Scroll the target into view and let layout settle before measuring it.
  await evaluate(`(${target}).scrollIntoView({ block: 'center' })`);
  await sleep(150);
  const box = await evaluate(`(() => { const t = (${target});
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

  // The index page must list exactly the catalogue's operators, in catalogue
  // order, each under its category heading. Reading the rendered page means the
  // check sees whatever mdBook made of operators.md, HTML included. An entry is
  // a link to an operator page in the first cell of a table row, under the
  // nearest second-level heading above it. Entries are taken in the order they
  // are drawn (top to bottom, then left to right), not in source order. A link
  // or heading counts only if some text or image inside it is drawn: not
  // hidden, not under an opacity(0) filter, text painted with a fill or stroke
  // colour that is not fully transparent, with a non-zero area. Headings are
  // compared by their displayed text: innerText without the text that is not
  // drawn. Some content is not supported, because the check cannot read what
  // it displays reliably, and the check fails on it rather than guess: CSS or
  // code brought by the page's own content (style, link, script, template,
  // iframe, object or embed elements, or style, dir or on* attributes);
  // anything but text and simple inline formatting in a heading; and, on or
  // inside a heading or first-cell link, generated text (::before or ::after
  // content other than an empty string) or ::first-letter or ::first-line
  // styling that changes the letters' fill colour, opacity or size. The book's
  // own stylesheets are trusted not to restyle index text in other ways the
  // check does not read, such as a text shadow or a layout that reorders
  // words. Links are resolved against the
  // page and mapped to a file by the server's own fileFor; a link is an entry
  // when that file exists in operators/, which is exactly when the server
  // serves that operator's page for it. Visibility is judged from styles and
  // layout, not pixels: an entry that draws nothing (an empty or transparent
  // image), or that is clipped, covered by another element or placed off the
  // page, still counts.
  //
  // indexLinks runs in the page (its source is sent to the browser). It returns
  // the drawn first-cell links in drawn order as [heading, pathname] pairs, for
  // links to this site, and the unsupported content it found.
  function indexLinks() {
    // Visibility is read from the text's own element below: a child can be
    // visible inside a hidden parent, so the boxed ancestor is not asked.
    const opts = { opacityProperty: true, checkOpacity: true };
    // An element with display: contents has no box; its nearest boxed ancestor decides display and opacity.
    const boxed = el => { while (el && getComputedStyle(el).display === 'contents') el = el.parentElement; return el; };
    // checkVisibility does not see an opacity(0) filter on the element or an ancestor.
    const zeroFilter = s => /opacity\(0(\.0*)?\)/.test(s.filter);
    const filtered = el => { for (; el; el = el.parentElement) if (zeroFilter(getComputedStyle(el))) return true;
      return false; };
    // A canvas reads any CSS colour's alpha, whatever notation it is written in.
    const ctx = document.createElement('canvas').getContext('2d', { willReadFrequently: true });
    const clear = c => { ctx.clearRect(0, 0, 1, 1); ctx.fillStyle = '#000'; ctx.fillStyle = c; ctx.fillRect(0, 0, 1, 1);
      return ctx.getImageData(0, 0, 1, 1).data[3] === 0; };
    // Glyphs are painted with the text-fill colour (which defaults to color) and any text stroke.
    const inked = s => !clear(s.webkitTextFillColor) || (parseFloat(s.webkitTextStrokeWidth) > 0 && !clear(s.webkitTextStrokeColor));
    // An element and everything inside it.
    const within = el => [el, ...el.querySelectorAll('*')];
    // Whether CSS generates content on el or inside it: ::before or ::after
    // content other than none, normal or an empty string.
    const generates = el => within(el).some(e => ['::before', '::after'].some(which =>
      !['none', 'normal', '""'].includes(getComputedStyle(e, which).content)));
    // Whether ::first-letter or ::first-line on el or inside it draws part of
    // the text differently: another fill colour, opacity or size. (Chrome does
    // not apply a text stroke there, and innerText already shows a case change.)
    const glyphStyle = ['webkitTextFillColor', 'fontSize'];
    const partStyled = el => within(el).some(e => ['::first-letter', '::first-line'].some(which => {
      const own = getComputedStyle(e), part = getComputedStyle(e, which);
      return part.opacity !== '1' || glyphStyle.some(p => part[p] !== own[p]);
    }));
    // The top-left of an element's first drawn piece and its non-blank text
    // nodes that draw nothing, or null if nothing in it is drawn.
    const drawn = el => {
      let first = null; const undrawn = [];
      const note = rs => { for (const r of rs) if (!first || r.top < first.top || (r.top === first.top && r.left < first.left)) first = r; };
      const walk = document.createTreeWalker(el, NodeFilter.SHOW_TEXT | NodeFilter.SHOW_ELEMENT);
      for (let n = walk.currentNode; n; n = walk.nextNode()) {
        const isText = n.nodeType === Node.TEXT_NODE;
        if (isText ? !n.data.trim() : !n.matches('img, svg')) continue;
        const owner = isText ? n.parentElement : n, style = getComputedStyle(owner);
        let rects = [];
        if (boxed(owner)?.checkVisibility(opts) && style.visibility === 'visible' && !filtered(owner)
            && (!isText || inked(style))) {
          if (isText) { const r = document.createRange(); r.selectNodeContents(n); rects = [...r.getClientRects()]; }
          else rects = [...n.getClientRects()];
          rects = rects.filter(r => r.width > 0 && r.height > 0);
        }
        if (!rects.length) { if (isText) undrawn.push(n); continue; }
        note(rects);
      }
      return first && { top: first.top, left: first.left, undrawn };
    };
    // An element's displayed text, with the text nodes that draw nothing blanked while it is read.
    const shownText = (el, undrawn) => {
      const saved = undrawn.map(n => n.data);
      undrawn.forEach(n => { n.data = ''; });
      const text = el.innerText;
      undrawn.forEach((n, i) => { n.data = saved[i]; });
      return text.replace(/\s+/g, ' ').trim();
    };
    const items = [], unsupported = [];
    // The page's content may not bring its own CSS or code: no style, link,
    // script, template or embedded-document element, and no style, dir or
    // event-handler attribute. The book's own stylesheets and scripts are trusted.
    const ownCode = 'style, link, script, template, iframe, object, embed';
    const ownAttr = a => a.name === 'style' || a.name === 'dir' || a.name.startsWith('on');
    for (const e of document.querySelectorAll('main *')) {
      const attr = [...e.attributes].find(ownAttr);
      if (e.matches(ownCode) || attr) unsupported.push(`<${e.localName}${attr ? ' ' + attr.name : ''}> in the page`);
    }
    // A heading may contain only text and simple inline formatting, so that
    // innerText holds everything it displays (an input or image would not be in it).
    const inHeading = 'a, b, strong, i, em, code, span, br, s, del, u, small, sub, sup';
    for (const e of document.querySelectorAll('main h2 *'))
      if (!e.matches(inHeading)) unsupported.push(`<${e.localName}> in heading "${e.closest('h2').textContent.trim()}"`);
    for (const el of document.querySelectorAll('main h2, main table tr > :is(td, th):first-child a[href]')) {
      if (generates(el) || partStyled(el)) unsupported.push(`CSS-generated text or ::first-letter/::first-line styling on ${el.tagName === 'H2' ? `heading "${el.textContent.trim()}"` : `link ${el.getAttribute('href')}`}`);
      const d = drawn(el);
      if (d) items.push({ el, top: d.top, left: d.left, text: el.tagName === 'H2' ? shownText(el, d.undrawn) : '' });
    }
    items.sort((a, b) => a.top - b.top || a.left - b.left);
    const out = []; let heading = '(no heading)';
    for (const { el, text } of items) {
      if (el.tagName === 'H2') { heading = text; continue; }
      const url = new URL(el.href);
      if (url.origin === location.origin) out.push([heading, url.pathname]);
    }
    return { linked: out, unsupported };
  }
  await cdp('Page.navigate', { url: `${base}/operators.html` });
  for (let i = 0; i < 100 && await evaluate("document.readyState") !== 'complete'; i++) await sleep(100);
  const { linked = [], unsupported = [] } = await evaluate(`(${indexLinks})()`) ?? {};
  // A link is an entry when the server would serve operators/<id>.html for it.
  const operatorDir = join(root, 'operators');
  const onIndex = [];
  for (const [heading, pathname] of linked) {
    let file;
    try { file = fileFor(pathname); } catch { continue; }
    const m = file && dirname(file) === operatorDir ? basename(file).match(/^(.+)\.html$/) : null;
    // The server answers a missing file with 404.
    if (m && (await stat(file).catch(() => null))?.isFile()) onIndex.push(`${heading}: ${m[1]}`);
  }
  const catalogue = JSON.parse(await readFile(join(root, 'marbles', 'catalog.json'), 'utf8')).map(e => `${e.category}: ${e.id}`);
  const at = catalogue.findIndex((want, i) => onIndex[i] !== want);
  if (unsupported.length) {
    fail(`index: not supported: ${unsupported.join('; ')}`);
  } else if (at >= 0 || onIndex.length !== catalogue.length) {
    const i = at >= 0 ? at : catalogue.length;
    fail(`index: entry ${i + 1} is "${onIndex[i] ?? 'missing'}", the catalogue has "${catalogue[i] ?? 'nothing'}" (${onIndex.length} entries on the page, ${catalogue.length} in the catalogue)`);
  } else console.log(`ok   operators.html (${onIndex.length} entries)`);

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
  // Without this precondition the scenario proves nothing: Join's drag can
  // only be skewed if Tumbling's axis really widened first.
  const tumblingAxis = await evaluate(`Number(${onPrint('tumbling-window')}.querySelector('svg').dataset.axisEnd)`);
  if (tumblingAxis !== 20) fail(`print page: the Tumbling drag did not widen its axis to 20 (got ${tumblingAxis}), so the Join check below proves nothing`);
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
