# Vendored assets

- `htmx.min.js` — htmx v1.9.12, downloaded from
  https://raw.githubusercontent.com/bigskysoftware/htmx/v1.9.12/dist/htmx.min.js
  (BSD 2-Clause license, bundled with the htmx project). No CDN dependency at runtime.

- `fonts/IBMPlexSans-{Regular,Medium,SemiBold}.woff2`, `fonts/IBMPlexMono-{Regular,Medium}.woff2`
  — IBM Plex (master branch snapshot, 2026-07-10), downloaded from
  https://raw.githubusercontent.com/IBM/plex/master/packages/plex-sans/fonts/complete/woff2/
  and
  https://raw.githubusercontent.com/IBM/plex/master/packages/plex-mono/fonts/complete/woff2/
  (SIL Open Font License 1.1, Copyright IBM Corp — see `fonts/OFL.txt`). No CDN dependency
  at runtime; served locally via `@font-face` in `style.css`.
