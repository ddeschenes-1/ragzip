
# Frequently asked questions


---------

## Why not using ints for offsets in indexes?

It's fair to question the cost of the pointers when using ints vs longs. Using long doubles the metadata size.
However, even for tiny 1kB pages (4MB covered per level1 index), the 32kB of metadata allowed is still merely 1/128th overhead.

Let's erroneously assume the span of compressed offsets from 1st page start to last page start, within one index,
could be restricted to 4GB, so that we can use unsigned ints for offsets.

If we were to use 4 bytes ints offsets, a payload of 32kb would hold up to 8k records.
When a level1 index would address an uncompressed (logical) offset beyond 4G, (when far away from the uncompressed file start),
the index would need to indicate the 1st page offset as a long, and all other pages relative to that offset with ints.

Thus, the max compressed page size would need to be no more than 4G/8k pages = 2^(32-13) = 2^19 = 512kB per page.
Assuming a worst possible inflation (not compression) of 2x (usually about 1.3x max inflation with deflate),
then the max logical size of a page should be 256kB. 
Although it seems a pretty big offset to 'skip' inside a gzip page once opened, this is limitative for the future
with huge pages that might not need to be so 'small', particularly when the purpose is not to do random access but
merely do parallel compression/decompression.

While a level1 index of 4096 pages of 1MB would be covering already 4GB or more of the compressed file,
a level 2+ index would be using absolute offsets (to lower indexes) distant from each other by more than the int range.
We must use something larger.

When using 7 or 6 bytes per offset, notice that 8k records (instead of 4k) could fit respectively in 56kB 
and 48kB of a gzip extra field (which can hold 65531 bytes). There isn't much space to save in those high rank indexes
because they are quite rare. So it's simpler to use 8-bytes long for level 2+ indexes. At that point, we've shown that
there is no reason to make the spec more difficult for a futile attempt to save very little indexing metadata.




---------

## Why is this not an RFC yet?

It may be some day. Life is so short.
My first contact with the RFC process was quite discouraging.

If you understood the [ragzip spec](README.md), you are proving my point: a document, whether it is source or else, sure must be 
readable, but **it must also be writable**.

Here are some ascii art I made before my diagrams.
For your viewing pleasure:

A book:

```
    (1st page of book offset)                                  (Level 1 index offset)
    |                                                                    |
    v                                                                    v
----+============+==========+==============================+=============+===========+----
####|gz page 0   |gz page 1 | gz pages 2 to $ffe...        |gz page $fff |gz L1 index|####
####|            |          |                              |             |           |####
----+============+==========+==============================+=============+===========+----
    ^            ^          ^                              ^         
    |            +-----+    |                              |    
    |                  |    |               +--------------+
    +-------------+    |    |               |      
                  |    |    |               |
                +----+----+----+---------+----+
      L1 index: |    |    |    |         |    |    absolute offsets to pages in file
      metadata  |    |    |    |   ...   |    |
                +----+----+----+---------+----+
         slot#:   0    1    2      ...    fff  

```


A shelf:

```
                                                               (Level 2 index offset)
                                                                       |
                                                                       v
----+============+===========+=============================+===========+===========+----
####| book 0   |i| book 1  |i| book $002 to $0fc...        |book $0fd|i|gz L2 index|####
####|          | |         | |                             |         | |           |####
----+============+===========+=============================+===========+===========+----
               ^           ^                                        ^
               |           |                                        |
               +--+     +--+                +-----------------------+
                  |     |                   |
                +----+----+----+---------+----+
      L2 index  |    |    |    |         |    |    offsets to level 1 indexes of books in file.
      metadata  |    |    |    |   ...   |    |    
                +----+----+----+---------+----+
         slot#:   0    1    2      ...    $0fd
```



A footer:

```
     
----+==============+======================+====================+===============+============+
####|gz top index  |gz ext 0              |gz ext 1            | gz ext 2      | gz footer  |
####|              |                      |                    |               |  64 bytes  |
----+==============+======================+====================+===============+============+
    ^              ^                      ^                    ^                    
    |              |                      |                    |                              
    |              |                      |                    +---------------+           
    |              +-------------------+  +-----------------+                  |          
    |                                  |                    |                  |           
    |           +----+-+--+-------+  +----+-+--+-------+  +----+-+--+-------+  |
    |  ext.     |prev|f|id| ext 0 |  |prev|f|id| ext 1 |  |prev|f|id| ext 2 |  |
    |  metadata | -1 | |  | bytes |  |    | |  | bytes |  |    | |  | bytes |  |
    |           +----+-+--+-------+  +----+-+--+-------+  +----+-+--+-------+  |
    |                                                                          |
    |                                                                          |
    +------------------------------------------------+                   +-----+
                                                     |                   |
     footer metadata:                                |                   |
     +----------+-----------+-----------------+-----------------------------------+-------------+
     |version   |treespec   |uncompressed size|top index offset |ext.tail offfset |padding*     |
     |MMMM mmmm |00 LL II PP|uuuuuuuu uuuuuuuu|tttttttt tttttttt|eeeeeeee eeeeeeee|???????? ????|
     +----------+-----------+-----------------+-----------------+-----------------+-------------+

```




---------

## Why that source style?

Admit it, everybody can be happy viewing indentation with tabs because they can make it the width they desire.
TABs were invented for that purpose.

You have to become good at reading someone else's code. This is 95% of your job.
