package main

import (
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
)

func main() {
	drevTarget, _ := url.Parse("http://localhost:9090")
	otherTarget, _ := url.Parse("http://localhost:3001") // Change 3001 to your other project's port

	drevProxy := httputil.NewSingleHostReverseProxy(drevTarget)
	otherProxy := httputil.NewSingleHostReverseProxy(otherTarget)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Route logic: Drev CI handles /webhooks and /api
		if strings.HasPrefix(r.URL.Path, "/webhooks") || strings.HasPrefix(r.URL.Path, "/api") {
			log.Printf("[Router] --> Drev CI: %s %s", r.Method, r.URL.Path)
			drevProxy.ServeHTTP(w, r)
			return
		}

		// Everything else goes to your other project
		log.Printf("[Router] --> Other Project: %s %s", r.Method, r.URL.Path)
		otherProxy.ServeHTTP(w, r)
	})

	port := ":8888"
	log.Printf("⚡ Drev Router active on %s", port)
	log.Printf("Point ngrok here: ngrok http --domain=picked-indirectly-cheetah.ngrok-free.app 8888")
	
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatal(err)
	}
}
