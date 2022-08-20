package workerpool

import (
	"load_multiple_rows/database"
	"math/rand"
	"sync"
)

type Worker interface {
	LoadCsv(*database.DB, string, int, int, *sync.WaitGroup)
}

type WorkerPool struct {
	Pool []Worker
	Wg *sync.WaitGroup
}

func (wp *WorkerPool) Job(db *database.DB, path string, start_index int, end_index int) error {
	worker := wp.Pool[rand.Intn(len(wp.Pool))]
	wp.Wg.Add(1)
	go worker.LoadCsv(db, path, start_index, end_index, wp.Wg)
	return nil
}