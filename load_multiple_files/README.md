## Load thousand rows data from thousand files

***Benchmarking***
Load 1 million rows from 1000 files with 1000 rows foreach
MaxOpenConn: 5, worker: 5 ==> 22.51s, 23.43s
MaxOpenConn: 5, worker: 100 ==> 22.87s, 22.01
MaxOpenConn: 50, worker: 50 ==> 19.52s, 20.43s
MaxOpenConn: 50, worker: 100 ==> 18.62s, 19.83s
MaxOpenConn: 80, worker: 80 ==> 20.10s, 20.31s
MaxOpenConn: 80, worker: 100 ==> 19.43s, 20.05s