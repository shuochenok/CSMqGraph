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
./bin/preprocess -i [input path] -o [output path] -v [vertices] -p [partitions] -t [edge type: 0=unweighted, 1=weighted]
```
For example, we want to partition the unweighted [LiveJournal](http://snap.stanford.edu/data/soc-LiveJournal1.html) graph into a 4x4 grid:
```
./bin/preprocess -i /data/LiveJournal -o /data/LiveJournal_Grid -v 4847571 -p 4 -t 0
```

> You may need to raise the limit of maximum open file descriptors (./tools/raise\_ulimit\_n.sh).

## Running Applications
To run the applications, just give the path of the grid format and the memory budge (unit in GB), as well as other necessary program parameters (e.g. the starting vertex of BFS, the number of iterations of PageRank, etc.):

### BFS
```
./bin/bfs [path] [start vertex id] [memory budget]
```

### WCC
```
./bin/wcc [path] [memory budget]
```

### SpMV
```
./bin/spmv [path] [memory budget]
```

### PageRank
```
./bin/pagerank [path] [number of iterations] [memory budget]
```

For example, to run 20 iterations of PageRank on the (grid partitioned) [LiveJournal](http://snap.stanford.edu/data/soc-LiveJournal1.html) graph using a machine with 8 GB RAM:
```
./bin/pagerank /data/LiveJournal_Grid 20 8
```
