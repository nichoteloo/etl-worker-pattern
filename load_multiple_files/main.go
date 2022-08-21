package main

import (
	"fmt"
	"load_multiple_files/database"
	"load_multiple_files/workerpool"
	"log"
	"math/rand"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

func main() {
	start := time.Now()

	// Define variables
	var wg sync.WaitGroup
	rand.Seed(time.Now().Unix())

	// Set db connection
	db, err := database.NewDB("postgres://postgres:password@localhost/testgo?sslmode=disable")
	if err != nil {
		log.Panic(err)
	}
	db.SetMaxOpenConns(50)

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
	files, err := filepath.Glob("./data/*") // 1000 files with 1000 rows foreach
	if err != nil {
        log.Fatal(err)
    }

	for _, file := range files {
		workerPool.Job(db, file)
	}

	// Wait job
	wg.Wait()

	elapsed := time.Since(start)
	fmt.Printf("Total execution time %s\n", elapsed.String())

	// Benchmarking
	// Load 1 million rows from 1000 files with 1000 rows foreach
	// MaxOpenConn: 5, worker: 5 ==> 22.51s, 23.43s
	// MaxOpenConn: 5, worker: 100 ==> 22.87s, 22.01
	// MaxOpenConn: 50, worker: 50 ==> 19.52s, 20.43s
	// MaxOpenConn: 50, worker: 100 ==> 18.62s, 19.83s
	// MaxOpenConn: 80, worker: 80 ==> 20.10s, 20.31s
	// MaxOpenConn: 80, worker: 100 ==> 19.43s, 20.05s
}