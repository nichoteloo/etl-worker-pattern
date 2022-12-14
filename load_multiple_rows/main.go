package main

import (
	"fmt"
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
	start := time.Now()

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
	// db.SetMaxOpenConns(50)

	// Fill worker pool
	numberOfWorkers := 100 // Max connection postgres
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

	elapsed := time.Since(start)
	fmt.Printf("Total execution time %s\n", elapsed.String())

	// Benchmarking
	// Load 1 million rows from 1 file
	// With setMaxOpenConn 50, worker 100 ==> 4m56.72s
	// Without setMaxOpenConn, worker 100 ==> 4m54.90s
}