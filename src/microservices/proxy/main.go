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

	rand.Seed(time.Now().UnixNano())
	
	mux := http.NewServeMux()
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/api/movies", proxyHandler)
	mux.HandleFunc("/api/users", proxyHandler)

	loggedMux := loggingMiddleware(mux)

	log.Printf("🚀 Proxy service started on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, loggedMux))
}

func getEnv(key, def string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return def
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, `{"status":true}`)
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	monolithURL := getEnv("MONOLITH_URL", "http://monolith:8080")
	moviesServiceURL := getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081")
	migrationPercentStr := getEnv("MOVIES_MIGRATION_PERCENT", "50")

	migrationPercent, err := strconv.Atoi(migrationPercentStr)
	if err != nil || migrationPercent < 0 || migrationPercent > 100 {
		log.Fatalf("Invalid MOVIES_MIGRATION_PERCENT: %v", migrationPercentStr)
	}

	target := monolithURL
	if rand.Intn(100) < migrationPercent {
		target = moviesServiceURL
	}

	log.Printf("➡️  Proxying request to: %s", target)
	
	proxyURL := target + r.URL.Path
	if r.URL.RawQuery != "" {
		proxyURL += "?" + r.URL.RawQuery
	}

	proxyReq, err := http.NewRequest(r.Method, proxyURL, r.Body)
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
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Фиксируем время начала запроса
		start := time.Now()

		// Логируем информацию о запросе
		log.Printf(
			"[%s] %s %s (from %s)",
			r.Method,
			r.URL.Path,
			r.URL.Query().Encode(), // параметры запроса (?id=123&...)
			r.RemoteAddr,
		)

		// Вызываем следующий обработчик в цепочке
		next.ServeHTTP(w, r)

		// После обработки запроса логируем длительность
		log.Printf(
			"[%s] %s completed in %v",
			r.Method,
			r.URL.Path,
			time.Since(start),
		)
	})
}
