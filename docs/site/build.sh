#!/usr/bin/env bash
# Build the operator reference into docs/site/book/.
#
#   1. Publish the browser host (Trill under .NET WASM) to obj/marbles-publish.
#   2. Build the mdBook.
#   3. Copy the published _framework/ next to the widget script in the book.
#
# Run docs/site/DiagramTests first: it checks that src/marbles/catalog.json,
# which the pages draw before Trill loads, matches the engine.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

dotnet publish "$here/Marbles/Marbles.csproj" -c Release -o "$here/obj/marbles-publish" --disable-build-servers -nodeReuse:false
mdbook build "$here"
rm -rf "$here/book/marbles/_framework"
cp -r "$here/obj/marbles-publish/wwwroot/_framework" "$here/book/marbles/_framework"
echo "Built $here/book"
