## Load 1 million rows data with 100 workers

### ***Benchmarking***

Load 1 million rows from 1 file

With setMaxOpenConn 50, worker 100 ==> 4m56.72s

Without setMaxOpenConn, worker 100 ==> 4m54.90s
