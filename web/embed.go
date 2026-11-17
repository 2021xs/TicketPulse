package web

import (
	"embed"
	"net/http"
	"strings"
)

//go:embed index.html app.js app.css
var assets embed.FS

func Handler() http.Handler {
	files := http.FileServer(http.FS(assets))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/app.js", "/app.css":
			files.ServeHTTP(w, r)
		case "/", "/login", "/workspace", "/tickets/new", "/search", "/analytics":
			serveIndex(w)
		default:
			if strings.HasPrefix(r.URL.Path, "/tickets/") {
				serveIndex(w)
				return
			}
			http.NotFound(w, r)
		}
	})
}

func serveIndex(w http.ResponseWriter) {
	data, err := assets.ReadFile("index.html")
	if err != nil {
		http.Error(w, "Web assets unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(data)
}
