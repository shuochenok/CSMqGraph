#arguments <name of directed graph file> <output file name>
#this program is to transform directed graph to  bidirectional graph
import sys
import re #to Specify multiple delimiters
#add_rev_edges = False
add_rev_edges = True
infile=file(sys.argv[1], "r")
outfile=file(sys.argv[2], "w")
edges=0
vertices=0
flag=False
for line in infile:
    if line[0] == '%' or line[0] == '#': #ignore the lines as the begaining of '%'and '#'
        pass
    else:
        #vector = line.strip().split(" ")
        vector = re.split(r'[,\t ]+',line.strip())#separated string to a list as the delimiter of ' ','\t',','
        vector = list(map(int, vector))#convert every element of vector to int
        if vector[0] == 0 or vector[1] == 0:
           #print str(vector[0])+"\t"+str(vector[1])
           flag=True
        if vector[0] > vertices:
            vertices = vector[0]
        if vector[1] > vertices:
            vertices = vector[1]
        edges = edges + 1
        outfile.write(str(vector[0])+"\t"+str(vector[1])+"\n")
        if add_rev_edges:
            edges = edges + 1
            tmp = vector[0]
            vector[0] = vector[1]
            vector[1] = tmp
            outfile.write(str(vector[0])+"\t"+str(vector[1])+"\n")

vertices = vertices + 1
if flag==False: 
    print "there is not the vertex 0"
infile.close()
outfile.close()
