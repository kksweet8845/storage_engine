# Storage engine


# Implementation

- filename.c
    - Return the filename
- skiplist.c
    - Implementation of skiplist
- memtable.c
    - 
- sstable.c
    - store the immutable memory table into sstable
    - structure
```
    <Begin of the file>
    [Data block 1]
    ...
    [Data block N]
    [meta block 1]
    ...
    [meta block N]
    [metablock indexing]
    [datablock indexing]
    [footer]
```
- block_builder.c
    - Used to store the file