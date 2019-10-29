# CSMqGraph
CSMqGraph: Coarse-Grained And Multi-External- Storage Multi-queue I/O Management for Graph Computing
## Compilation
Compilers supporting basic C++11 features (lambdas, threads, etc.) and OpenMP are required.

To compile:
```
make
```
## Preprocessing
Before running applications on a graph, GridGraph needs to partition the original edge list into the grid format.

Two types of edge list files are supported:
- Unweighted. Edges are tuples of <4 byte source, 4 byte destination>.
- Weighted. Edges are tuples of <4 byte source, 4 byte destination, 4 byte float typed weight>.

To partition the edge list:
```
./bin/preprocess -i [input path] -o [output path] -v [vertices] -p [partitions] -t [edge type: 0=unweighted, 1=weighted]  –d [number of disks] -s [stripe depth(KB)]
```
For example, we want to partition the unweighted [LiveJournal](http://snap.stanford.edu/data/soc-LiveJournal1.html) graph into a 36x36 grid and strip it to 4 external storage devices with a stripe depth of 6144KB.
```
./bin/preprocess -i /data/LiveJournal -o /data/LiveJournal_Grid -v 4847571 -p 36 -t 0  -d 4 -s 6144
```
After preprocessing, the following file will be generated：
(1) block-*-* : edge block file;
(2) column : An edge file that aggregates all edge blocks in column order;
(3) column_offset : Offset of each edge block in the column file;
(4) row : An edge file that aggregates all edge blocks in column order;
(5) row_offset : Offset of each edge block in the row file
(6) meta : Record metadata information such as edge type, number of vertices, number of sides, number of partitions, etc.
(7) column_disk*_* :  The resulting striped edge file, the first * indicates the current striped edge file id, the second * indicates the total number of striped edge files, that is, the total number of external storage devices storing the edge data, in the above specific example that is 4.

Then the generated striping edge file column_disk*_* is moved according to its id to the above four target external storage devices（disk0~disk3）.
> You may need to raise the limit of maximum open file descriptors (./tools/raise\_ulimit\_n.sh).

## Running Applications
To run the applications, just give the path of the grid format and the memory budge (unit in GB), as well as other necessary program parameters (e.g. the starting vertex of BFS, the number of iterations of PageRank, etc.):

### BFS
```
./bin/bfs [path] [start vertex id] [memory budget] [number of disks] [stripe depth]
```

### WCC
```
./bin/wcc [path] [memory budget] [number of disks] [stripe depth]
```

### SpMV
```
./bin/spmv [path] [memory budget] [number of disks] [stripe depth]
```

### PageRank
```
./bin/pagerank [path] [number of iterations] [memory budget] [number of disks] [stripe depth]
```

For example, to run 20 iterations of PageRank on the (grid partitioned) [LiveJournal](http://snap.stanford.edu/data/soc-LiveJournal1.html) graph using a machine with 8 GB RAM:
```
./bin/pagerank /data/LiveJournal_Grid 20 8 4 6144
```
## Resources
Shuo Chen, Zhan Shi, Dan Feng, Fang Wang, Shang Liu, Lei Yang and Ruili Yu. [CSMqGraph: Coarse-Grained And Multi-External-
Storage Multi-queue I/O Management for Graph Computing]. Proceedings of the 2019 International Journal of Parallel Programming
