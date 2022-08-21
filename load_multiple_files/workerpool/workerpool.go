package workerpool

import (
	"load_multiple_files/database"
	"math/rand"
	"sync"
)

type Worker interface {
	LoadCsv(*database.DB, string, *sync.WaitGroup)
}

type WorkerPool struct {
	Pool []Worker
	Wg *sync.WaitGroup
}

func (wp *WorkerPool) Job(db *database.DB, path string) error {
	worker := wp.Pool[rand.Intn(len(wp.Pool))]
	wp.Wg.Add(1)
	go worker.LoadCsv(db, path, wp.Wg)
	return nil
}