// mdBook runs additional-js files as classic scripts. Load the widget module
// only on pages that have a diagram. mdBook defines path_to_root on each page;
// it is "" on top-level pages, which would make a bare module specifier, so
// resolve it against the page URL first.
if (document.querySelector('.trill-marbles')) {
  import(new URL(path_to_root + 'marbles/marbles.js', document.baseURI).href);
}
