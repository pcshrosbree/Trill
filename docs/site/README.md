# Operator reference with diagrams

The source of the operator reference published on this fork's GitHub Pages. It has a page per operator and an interactive diagram on each operator page, in the manner of [RxMarbles](https://rxmarbles.com/). Every diagram is computed by Trill: the page first draws a recorded run, then loads Trill into the browser (row mode, under .NET WebAssembly) and re-runs the query whenever an input is dragged.

The documentation, its build and its tests live under `docs/site/`, and its workflow is `.github/workflows/pages.yml`. None of it is under `Sources/`, so that commits there stay easy to offer upstream.

## Layout

| Path | What it is |
|---|---|
| `Catalog/` | The diagrams: each entry's inputs and query. The code a page shows is captured from the query lambda by `[CallerArgumentExpression]`, so it is the code that runs. |
| `Marbles/` | The browser host: Trill under .NET WebAssembly, exposing one `[JSExport]` call. Trimmed (framework only) and with Roslyn lazy-loaded, since row mode never invokes Roslyn. |
| `DiagramTests/` | Checks that `src/marbles/catalog.json`, the recorded runs the pages draw first, equals a fresh native run, and that the check can fail. |
| `src/` | The mdBook pages. `src/marbles/` holds the widget script, its stylesheet and `catalog.json`. |
| `build.sh` | Publishes `Marbles`, builds the book into `book/`, and copies the WebAssembly files next to the widget. |
| `check-site.mjs` | Loads every page of the built book in headless Chrome. Each diagram must run its query with no console error, its live output must equal the recorded run, and no event glyph may extend outside its diagram. Two drags check that the axis widens for output, and for a label, past its end; a third must change Join's output. It checks the data and the bounds of what is drawn, not whether the drawing is otherwise right. |

## Build and check

Prerequisites: the .NET 10 SDK, [mdBook](https://github.com/rust-lang/mdBook) 0.5, Node 22 or later, and Chrome or Chromium.

```bash
dotnet build docs/site/DiagramTests/DiagramTests.csproj -c Release
dotnet docs/site/DiagramTests/bin/Release/net10.0/DiagramTests.dll
bash docs/site/build.sh
node docs/site/check-site.mjs docs/site/book chromium
```

Run the test executable directly rather than through `dotnet test`. `DiagramTests` uses Microsoft.Testing.Platform, and `dotnet test` needs an SDK opt-in for that, which the repository's VSTest-based tests do not use.

## Adding or changing a diagram

1. Add or change an entry in `Catalog/Operators.cs`. Choose inputs that show the operator's behaviour at a glance.
2. Regenerate the recorded runs by running the test executable with the environment variable `TRILL_DOCS_UPDATE=1`. That run rewrites `src/marbles/catalog.json` and fails on purpose. Review the diff, then run the tests again without the variable.
3. Add or edit the page in `src/operators/`, with `<div class="trill-marbles" data-op="ID"></div>` where the diagram goes, and list it in `src/SUMMARY.md` and `src/operators.md`. A test checks that the pages and the catalogue name the same diagrams.
4. If a page claims more than its own diagram shows, such as what a drag will do or how other parameters behave, add a test for that claim to `PageClaimTests`.

The colours are a categorical palette checked for colour-vision deficiency and for contrast against the light and dark surfaces. A diagram never relies on colour alone: every event is labelled and every diagram has a legend.
