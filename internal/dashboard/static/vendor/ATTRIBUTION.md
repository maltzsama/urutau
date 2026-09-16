# Vendored assets

These files are vendored so the dashboard works offline (air-gapped, no CDN)
and ships inside the coordinator binary via `//go:embed`. Versions are pinned
by the URLs below; upgrade by re-downloading the same paths.

| File | Project | License |
| --- | --- | --- |
| `pico.min.css` | [Pico.css](https://github.com/picocss/pico) v2 | MIT |
| `alpine.min.js` | [Alpine.js](https://github.com/alpinejs/alpine) v3 | MIT |
| `chart.umd.min.js` | [Chart.js](https://github.com/chartjs/Chart.js) v4 | MIT |

Source URLs:

- `https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.min.css`
- `https://cdn.jsdelivr.net/npm/alpinejs@3/dist/cdn.min.js`
- `https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js`

All three are MIT-licensed; the full license texts live in each project's
repository (linked above).
