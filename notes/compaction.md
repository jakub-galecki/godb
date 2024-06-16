> Notes based on the https://github.com/facebook/rocksdb/wiki/Leveled-Compaction

## Level Target Sizes 
```
- L1 : 300 MB 
- L2 : 3GB
- L3 : 30GB
- L4 : 300GB
...
```

### Calculation Target Sizes
let TargetSize(L1) = `max_bytes_for_level_base`
then TargetSize(Ln+1) = TargetSize(n) * `max_bytes_for_level_multiplier` 

## Compactions

When number of L0 files reaches `level0_file_num_compaction_trigger` compaction is triggered. We merge all L0 files as 
they are usually overlapping.
If level excceds maximum size we pick at least one file from this level and merge it with overlapping range of next level. 
Multiple compactions can be ran in parallel but the maximum number of allowed compactions in backgroud  is controlled by `max_background_compactions`.
Compaction of L1 and L2 is not parallelized.

## Picking Compaction

If multiple compaction trigger compation we must choose wich compaction to run first. We generate score for each level:
1. For level != 0: 
    - Score equals to the size dived by max size. If some files are currently being compacted they are excluded.
2. For level == 0:
    - max(num_of_files / `level0_file_num_compaction_trigger`, total_size/`max_bytes_for_level_base`)

*where `max_bytes_for_level_base` is L1 target size*

Level with the highest score takes the priortity to compact.   



### Choose Level Compaction Files

Possible future improvements:
- TTL
- Intra-L0 Compaction
- level_compaction_dynamic_level_bytes
- Compaction Filter