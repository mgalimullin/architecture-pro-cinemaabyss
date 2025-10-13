package main

import (
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	port := getEnv("PORT", "8000")
	monolithURL := getEnv("MONOLITH_URL", "http://monolith:8080")
	moviesServiceURL := getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081")
	migrationPercentStr := getEnv("MOVIES_MIGRATION_PERCENT", "50")

	migrationPercent, err := strconv.Atoi(migrationPercentStr)
	if err != nil || migrationPercent < 0 || migrationPercent > 100 {
		log.Fatalf("Invalid MOVIES_MIGRATION_PERCENT: %v", migrationPercentStr)
	}

	rand.Seed(time.Now().UnixNano())

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"status":true}`)
	})

	http.HandleFunc("/api/movies", func(w http.ResponseWriter, r *http.Request) {
		target := monolithURL
		if rand.Intn(100) < migrationPercent {
			target = moviesServiceURL
		}

		log.Printf("➡️  Proxying request to: %s", target)

		proxyReq, err := http.NewRequest(r.Method, target+"/api/movies", r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		proxyReq.Header = r.Header.Clone()
		client := &http.Client{}
		resp, err := client.Do(proxyReq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		for key, values := range resp.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	})

	http.HandleFunc("/api/users", func(w http.ResponseWriter, r *http.Request) {
		target := monolithURL

		log.Printf("➡️  Proxying request to: %s", target)

		proxyReq, err := http.NewRequest(r.Method, target+"/api/users", r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		proxyReq.Header = r.Header.Clone()
		client := &http.Client{}
		resp, err := client.Do(proxyReq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		for key, values := range resp.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	})

	log.Printf("🚀 Proxy service started on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func getEnv(key, def string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return def
}
