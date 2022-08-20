package main

import (
	"load_multiple_rows/database"
	"load_multiple_rows/workerpool"
	"log"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

func main() {
	// Define variables
	var wg sync.WaitGroup
	var max_load int = 1000 // Maximum insert postgresql
	var total_data int = 1000000 // Total rows
	var total_thread int = int(math.Ceil(float64(total_data) / float64(max_load)))
	rand.Seed(time.Now().Unix())

	// Set db connection
	db, err := database.NewDB("postgres://postgres:password@localhost/testgo?sslmode=disable")
	if err != nil {
		log.Panic(err)
	}

	// Fill worker pool
	numberOfWorkers := 100 // Can adjust it later
	workerArray := make([]workerpool.Worker, 0) // Fill with worker interface
	for i := 0; i < numberOfWorkers; i++ {
		w := &workerpool.LoadWorker{Id: strconv.FormatInt(int64(i), 10)}
		workerArray = append(workerArray, w) // Fill with LoadWorker
	}

	// Create worker pool struct
	workerPool := &workerpool.WorkerPool{Pool: workerArray, Wg: &wg}

	// Run job
	for i := 0; i < total_thread; i++ {
		idx_start := i * max_load
		idx_end := max_load + i * max_load

		workerPool.Job(db, "building.csv", idx_start, idx_end)
	}

	// Wait job
	wg.Wait()
}