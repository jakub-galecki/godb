=== Structure ====
```
Size    {k: key, v: value}  # ... #
```

1. Block Restrictions
- Fixed Maximum Size => BLOCK_SIZE 

2. On disk layout 
For simplicity one block group will be stored in one file. So if memtable 
generates more then one block group - say n then n files will be created. 
Maximum number blocks than can fit inside the group is XXXX.  

3. Block Builder 
Given a MemTable, BlockBuilder should create blocks based on the 
memtable's data and save them into buffer. When the memtable is 
processed fully - blocks should be flushed on disk.
