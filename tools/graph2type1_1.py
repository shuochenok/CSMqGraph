#Convert graph files into x-stream type 1 inputs
#choose to convert graph to bidirectional graph
#the minist id of vertices is not 0
#Arguments <name of matrix market file> <output file name>
import sys
import struct
import random
import re

#Choose one
add_rev_edges = False
#add_rev_edges = True

random.seed(0)

infile=file(sys.argv[1], "r")
outfile=file(sys.argv[2], "wtb")
outfile_meta=file(sys.argv[2]+".ini", "wt")

outfile_meta.write("[graph]\n")
outfile_meta.write("type=1\n")
outfile_meta.write("name="+sys.argv[2]+"\n")

s = struct.Struct('@IIf')
edges=0
vertices=0
for line in infile:
    if line[0] == '%' or line[0] == '#':
        pass
    else:
        #vector = line.strip().split(" ")
        vector = re.split(r'[,\t ]+',line.strip())
        vector = list(map(int, vector))
        vector[0] = vector[0] - 1 # the minist id of vertices is not 0 
        vector[1] = vector[1] - 1
        vector.append(random.random()) # Edge weight
        if vector[0] > vertices:
            vertices = vector[0]
        if vector[1] > vertices:
            vertices = vector[1]
        edges = edges + 1
        outfile.write(s.pack(*vector))
        if add_rev_edges:
            edges = edges + 1
            tmp = vector[0]
            vector[0] = vector[1]
            vector[1] = tmp
            outfile.write(s.pack(*vector))

vertices = vertices + 1
outfile_meta.write("vertices="+str(vertices)+"\n")
outfile_meta.write("edges="+str(edges)+"\n")
infile.close()
outfile.close()
outfile_meta.close()
