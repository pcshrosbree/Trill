// Trill marbles: interactive diagrams of Trill operators.
//
// Every <div class="trill-marbles" data-op="ID"> becomes a diagram of catalogue
// entry ID. It is drawn at once from catalog.json (the recorded engine run that
// docs/site/DiagramTests checks), then switches to live mode: real Trill, in
// row mode under .NET WASM, recomputes the output whenever an input event is
// dragged (move) or its right end is dragged (resize). On the switch, the live
// output for the default inputs must equal the recorded one; a difference is
// reported as a console error, which the browser check treats as a failure.

const INPUT_T = 16;              // inputs can be dragged within 0..INPUT_T
const W = 760, LEFT = 84, RIGHT = 24;
// The axis runs 0..T, widened per drawing so that every finite output end is
// on screen (a window can move an event past the inputs' range).
let T = INPUT_T, UNIT = (W - LEFT - RIGHT) / T;
function setDomain(events) {
  const avail = W - LEFT - RIGHT;
  const even = t => Math.ceil(t / 2) * 2;
  const finite = events.filter(e => e.end != null);
  T = Math.max(INPUT_T, even(Math.max(0, ...finite.map(e => e.end))));
  UNIT = avail / T;
  // A label too wide for its bar is drawn after the bar, so widen the axis
  // until every such label also ends inside the drawing. Widening shrinks the
  // bars, which can push more labels outside, so repeat until stable.
  for (let i = 0; i < 8; i++) {
    let need = T;
    for (const e of finite) {
      if (labelFits(e)) continue;
      const room = W - LEFT - labelWidth(e) - 4;   // where the bar must end, in px from LEFT
      need = Math.max(need, even(e.end * avail / room));
    }
    if (need === T) break;
    T = need;
    UNIT = avail / T;
  }
}
const ROW = 24, BAR = 16, LANE_PAD = 14;
const NS = 'http://www.w3.org/2000/svg';
const KEYS = ['a', 'b', 'c'];    // categorical slots 1..3, fixed order

const x = t => LEFT + t * UNIT;

function el(name, attrs = {}, parent) {
  const n = document.createElementNS(NS, name);
  for (const [k, v] of Object.entries(attrs)) n.setAttribute(k, v);
  if (parent) parent.appendChild(n);
  return n;
}

function keyClass(key) {
  const i = KEYS.indexOf(key);
  return i >= 0 ? `k${i + 1}` : 'k0';
}

// Trill emits some results as a start edge [s, inf) and, later, an end edge
// whose start is the same s. Pair them into one logical event so the bar
// shows the lifetime, and remember that it arrived as edges.
function logical(events) {
  const out = [], open = [];
  for (const e of events) {
    if (e.kind === 'Interval') out.push({ ...e, form: 'interval' });
    else if (e.kind === 'Start') {
      const o = { ...e, end: null, form: 'edges' };
      open.push(o); out.push(o);
    } else if (e.kind === 'End') {
      const i = open.findIndex(o => o.start === e.start && o.key === e.key && o.label === e.label);
      if (i >= 0) { open[i].end = e.end; open.splice(i, 1); }
      else out.push({ ...e, form: 'orphan-end' });
    }
  }
  return out;
}

const labelText = e => (e.key ? e.key + ':' : '') + e.label;
const labelWidth = e => labelText(e).length * 7.5 + 10;
const labelFits = e => labelWidth(e) < ((e.end ?? T) - e.start) * UNIT;

// The time an event's drawing reaches: a label that does not fit inside its
// bar is drawn after it, and must not run into the next event on the row.
function occupiedEnd(e) {
  if (e.end == null) return Infinity;
  return labelFits(e) ? e.end : e.end + (labelWidth(e) + 4) / UNIT;
}

// Greedy interval stacking: each event takes the first row it fits in.
function stack(events) {
  const rowsEnd = [];
  const placed = events.map(e => {
    const end = occupiedEnd(e);
    let r = rowsEnd.findIndex(re => re <= e.start);
    if (r < 0) { r = rowsEnd.length; rowsEnd.push(end); } else rowsEnd[r] = end;
    return { e, row: r };
  });
  return { placed, rows: Math.max(1, rowsEnd.length) };
}

function describe(e, form) {
  const end = e.end == null ? '∞' : e.end;
  const how = form === 'edges' ? (e.end == null ? 'start edge, no end edge yet' : 'start edge + end edge')
            : form === 'orphan-end' ? 'end edge' : 'interval';
  return `[${e.start}, ${end})  ${e.key ? e.key + ':' : ''}${e.label}  (${how})`;
}

function drawLane(svg, y, name, events, opts) {
  const { placed, rows } = stack([...events].sort((a, b) => a.start - b.start));
  const h = rows * ROW + LANE_PAD;
  el('text', { x: LEFT - 12, y: y + h / 2 + 4, class: 'lane-name', 'text-anchor': 'end' }, svg).textContent = name;
  el('line', { x1: LEFT, x2: W - RIGHT + 8, y1: y + h - 4, y2: y + h - 4, class: 'axis' }, svg);
  for (const { e, row } of placed) {
    const form = e.form ?? 'interval';
    const cy = y + 6 + row * ROW + BAR / 2;
    const g = el('g', { class: `ev ${keyClass(e.key)} ${form}` }, svg);
    el('title', {}, g).textContent = describe(e, form);
    if (opts.points) {
      // A lane of one-tick inputs is drawn as marbles at their start times.
      // They can be moved, not resized.
      el('circle', { cx: x(e.start), cy, r: 10, class: 'fill' }, g);
      el('text', { x: x(e.start), y: cy + 4, class: 'in-label', 'text-anchor': 'middle' }, g).textContent = e.label;
    } else {
      const x1 = x(e.start), x2 = e.end == null ? W - RIGHT : x(e.end);
      el('rect', { x: x1, y: cy - BAR / 2, width: Math.max(2, x2 - x1 - 2), height: BAR, rx: form === 'interval' ? 4 : 0, class: 'fill' }, g);
      if (form === 'edges') el('rect', { x: x1, y: cy - BAR / 2 - 3, width: 3, height: BAR + 6, class: 'edge' }, g);
      if (form !== 'interval') {
        if (e.end != null) el('rect', { x: x2 - 5, y: cy - BAR / 2 - 3, width: 3, height: BAR + 6, class: 'edge' }, g);
        else el('text', { x: x2 + 2, y: cy + 4, class: 'inf' }, g).textContent = '→∞';
      }
      // A label that does not fit inside its bar goes after it, in text ink.
      const fits = labelFits(e);
      // An open-ended bar runs to the axis end, so its label stays inside it.
      const inside = fits || e.end == null;
      el('text', { x: inside ? x1 + 6 : x2 + 4, y: cy + 4, class: inside ? 'in-label' : 'out-label' }, g).textContent = labelText(e);
      if (opts.input) el('rect', { x: x2 - 8, y: cy - BAR / 2, width: 8, height: BAR, class: 'grip' }, g);
    }
    if (opts.bind) opts.bind(g, e);
  }
  return h;
}

function drawAxis(svg, y) {
  for (let t = 0; t <= T; t += 2) {
    el('line', { x1: x(t), x2: x(t), y1: y, y2: y + 4, class: 'tick' }, svg);
    el('text', { x: x(t), y: y + 16, class: 'tick-label', 'text-anchor': 'middle' }, svg).textContent = t;
  }
  return 22;
}

// Legend: one swatch per key present (identity is never colour alone), then
// the glyphs for how an output event was emitted.
function drawLegend(svg, y, events) {
  let lx = LEFT;
  const ly = y + 14;
  const item = (cls, draw, label) => {
    draw(el('g', { class: `ev ${cls}` }, svg), lx);
    el('text', { x: lx + 22, y: ly + 4, class: 'legend' }, svg).textContent = label;
    lx += 30 + label.length * 6.5;
  };
  for (const k of KEYS.filter(k => events.some(e => e.key === k)))
    item(keyClass(k), (g, at) => el('circle', { cx: at + 8, cy: ly, r: 7, class: 'fill' }, g), `key ${k}`);
  lx += 16;
  item('k0 interval', (g, at) => el('rect', { x: at, y: ly - 6, width: 16, height: 12, rx: 3, class: 'fill' }, g), 'interval');
  item('k0 edges', (g, at) => {
    el('rect', { x: at, y: ly - 6, width: 16, height: 12, class: 'fill' }, g);
    el('rect', { x: at, y: ly - 9, width: 3, height: 18, class: 'edge' }, g);
    el('rect', { x: at + 13, y: ly - 9, width: 3, height: 18, class: 'edge' }, g);
  }, 'start edge … end edge');
  return 26;
}

function mount(host, entry) {
  const inputs = entry.defaultInputs.map(evs => evs.map(e => ({ ...e })));
  // Decided once from the defaults, so a bar shortened to one tick stays a
  // bar and keeps its resize handle.
  const pointLanes = entry.defaultInputs.map(evs => evs.every(e => e.end === e.start + 1));
  const svg = el('svg', { class: 'marbles', role: 'img', 'aria-label': `${entry.title} diagram` });
  const status = document.createElement('div');
  status.className = 'status';
  host.replaceChildren(svg, status);
  let output = entry.output, run = null, drag = null, pending = false;

  function draw() {
    svg.replaceChildren();
    setDomain(inputs.flat().concat(output));
    let y = 4;
    inputs.forEach((evs, i) => { y += drawLane(svg, y, entry.inputNames[i], evs, { input: true, points: pointLanes[i], bind: run && bind }); });
    const lines = entry.code.split('\n');
    const boxH = 14 + lines.length * 16;
    el('rect', { x: LEFT, y: y + 6, width: W - LEFT - RIGHT, height: boxH, rx: 4, class: 'op-box' }, svg);
    lines.forEach((l, i) => { el('text', { x: LEFT + 12, y: y + 24 + i * 16, class: 'code' }, svg).textContent = l; });
    y += boxH + 16;
    y += drawLane(svg, y, 'output', logical(output), {});
    y += drawAxis(svg, y);
    y += drawLegend(svg, y, inputs.flat().concat(output));
    svg.setAttribute('viewBox', `0 0 ${W} ${y + 4}`);
    svg.setAttribute('width', W);
    svg.dataset.axisEnd = T;     // read by check-site.mjs to convert ticks to pixels
    host.dataset.output = JSON.stringify(output);
  }

  function recompute() {
    const t0 = performance.now();
    output = JSON.parse(run(entry.id, JSON.stringify(inputs)));
    status.textContent = `Live: computed by Trill (row mode, .NET WASM) in ${(performance.now() - t0).toFixed(1)} ms. Drag an input event to move it${pointLanes.every(p => p) ? '' : ', or a bar\'s right end to resize it'}.`;
    draw();
  }

  function bind(g, ev) {
    g.addEventListener('pointerdown', p => {
      drag = { ev, resize: p.target.classList.contains('grip'), x0: p.clientX, s0: ev.start, e0: ev.end };
      svg.setPointerCapture(p.pointerId);
      p.preventDefault();
    });
  }

  svg.addEventListener('pointermove', p => {
    if (!drag) return;
    const dt = Math.round((p.clientX - drag.x0) / (UNIT * svg.getBoundingClientRect().width / W));
    const len = drag.e0 - drag.s0;
    if (drag.resize) drag.ev.end = Math.max(drag.s0 + 1, Math.min(INPUT_T, drag.e0 + dt));
    else { drag.ev.start = Math.max(0, Math.min(INPUT_T - len, drag.s0 + dt)); drag.ev.end = drag.ev.start + len; }
    if (!pending) { pending = true; requestAnimationFrame(() => { pending = false; recompute(); }); }
  });
  svg.addEventListener('pointerup', () => { drag = null; });

  status.textContent = 'Recorded run. Loading Trill to make it interactive…';
  draw();
  host.dataset.ready = 'recorded';

  return function goLive(runFn) {
    run = runFn;
    const recorded = JSON.stringify(entry.output);
    recompute();
    if (JSON.stringify(output) !== recorded) {
      host.dataset.mismatch = 'true';
      console.error(`trill-marbles: live output for '${entry.id}' differs from catalog.json`, { recorded: entry.output, live: output });
    }
    host.dataset.ready = 'live';
  };
}

const catalog = await (await fetch(new URL('catalog.json', import.meta.url))).json();
const live = [];
for (const host of document.querySelectorAll('.trill-marbles')) {
  const entry = catalog.find(o => o.id === host.dataset.op);
  if (entry) live.push(mount(host, entry));
  else host.textContent = `Unknown diagram: ${host.dataset.op}`;
}
try {
  const { dotnet } = await import(new URL('_framework/dotnet.js', import.meta.url).href);
  const { getAssemblyExports, getConfig } = await dotnet.create();
  const api = (await getAssemblyExports(getConfig().mainAssemblyName)).Interop;
  for (const goLive of live) goLive(api.Run);
} catch (err) {
  console.error('trill-marbles: could not start Trill; diagrams stay on the recorded run', err);
  for (const s of document.querySelectorAll('.trill-marbles .status')) s.textContent = 'Recorded run (Trill could not start in this browser).';
}
