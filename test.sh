#!/bin/bash

packages=("common" "bloom" "compaction" "internal/skiplist" "internal/cache" "memtable" "sst" "wal")

go test ./ -cover -count=1 
for p in ${packages[@]}; do 
    go test ./$p -cover -count=1 
done

