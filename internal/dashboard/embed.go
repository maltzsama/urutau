package dashboard

import (
	"embed"
	"io/fs"
	"net/http"
)

// staticFS carries the SPA shell and its vendored libraries (Pico.css,
// Alpine.js, Chart.js) into the binary — no CDN, no build step.
//
//go:embed static
var staticFS embed.FS

// spaHandler serves the embedded SPA from the root: / → index.html, and
// /app.js, /style.css, /vendor/* from the static tree.
func spaHandler() http.Handler {
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		// The embed directive guarantees the directory exists; a failure here
		// is a build-time bug, not a runtime condition.
		panic(err)
	}
	return http.FileServerFS(sub)
}
