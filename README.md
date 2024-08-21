# godb

Implementation inspired by:
* https://github.com/facebook/rocksdb
* https://github.com/cockroachdb/pebble 
* https://github.com/google/leveldb

### Running the source code 

To run the source code firt clone this repository. After that run `go mod tidy` to install all required dependecies. 

To run all unit tests with their coverage run command `./test.sh`

There is also an example of working with database in the directory called `example`. To run the example, `cd` into example directory and run `go run main.go`.

### Instalation 

To install the package simply run 
```
go get -u github.com/jakub-galecki/godb
```

### Compaction

* https://www.youtube.com/watch?v=6jF2xWTi2hs
* https://www.youtube.com/watch?v=YhGiw9fRil8&t=23s
* https://www.diva-portal.org/smash/get/diva2:946772/FULLTEXT02.pdf
* https://www.scylladb.com/2018/01/31/compaction-series-leveled-compaction/
* https://www.google.com/search?channel=fs&client=ubuntu-sn&q=scyllaDB%E2%80%99s+Compaction+Strategies+Series
* https://github.com/facebook/rocksdb/wiki/Leveled-Compaction
