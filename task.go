package main

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

const halfHourInSeconds int = 1800

type Promotion struct {
	ID             string  `json:"id"`
	Price          float64 `json:"price"`
	ExpirationDate string  `json:"expiration_date"`
}

type PromotionStorage struct {
	sync.RWMutex
	data map[int]Promotion
}

var storage = PromotionStorage{
	data: make(map[int]Promotion),
}

func loadPromotionsFromCSV(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	r := csv.NewReader(file)
	for {
		// Streaming reading line by line
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		id := record[0]
		price, _ := strconv.ParseFloat(record[1], 64)
		expirationDate := record[2]

		promotion := Promotion{
			ID:             id,
			Price:          price,
			ExpirationDate: expirationDate,
		}

		storage.Lock()
		storage.data[len(storage.data)+1] = promotion
		storage.Unlock()
	}
	log.Println("New CSV file loaded")

	return nil
}

func getPromotionHandler(w http.ResponseWriter, r *http.Request) {
	idStr := r.URL.Path[len("/promotions/"):]
	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	storage.RLock()
	promotion, found := storage.data[id]
	storage.RUnlock()

	if !found {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(promotion)
}

func main() {
	csvFilename := "promotions.csv"
	err := loadPromotionsFromCSV(csvFilename)
	if err != nil {
		log.Fatal(err)
	}

	// A goroutine to load new data from CSV in regular time interval
	ticker := time.NewTicker(time.Duration(halfHourInSeconds) * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				err := loadPromotionsFromCSV(csvFilename)
				if err != nil {
					log.Fatal(err)
				}
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()

	http.HandleFunc("/promotions/", getPromotionHandler)

	server := &http.Server{
		Addr:         ":1321",
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	log.Println("Server is listening on :1321")
	log.Fatal(server.ListenAndServe())
}
