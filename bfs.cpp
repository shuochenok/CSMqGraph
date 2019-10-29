/*
Copyright (c) 2014-2015 Xiaowei Zhu, Tsinghua University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include <iostream>
#include <time.h>
#include "core/graph.hpp"

int main(int argc, char ** argv) {
	if (argc<3) {
		fprintf(stderr, "usage: bfs [path] [start vertex id] [memory budget in GB] [worker_threads_size] [disks number] [stripe_depth in KB]\n");
		exit(-1);
	}
	std::string path = argv[1];
	VertexId start_vid = atoi(argv[2]);
	long memory_bytes = (argc>=4)?atol(argv[3])*1024l*1024l*1024l:8l*1024l*1024l*1024l;
	int worker_threads_size = (argc>=5)?atoi(argv[4]):(std::thread::hardware_concurrency());
	int disks_n = (argc>=6)?atoi(argv[5]):1;
	int stripe_depth = (argc>=7)?atoi(argv[6])*1024l:(IOSIZE);

	Graph graph(path,worker_threads_size);
	graph.set_memory_bytes(memory_bytes);
	graph.set_worker_threads_size(worker_threads_size);//set the number of worker thread when streaming edges
	graph.set_disks_number(disks_n);
	graph.set_stripe_depth(stripe_depth);

	Bitmap * active_in = graph.alloc_bitmap();
	Bitmap * active_out = graph.alloc_bitmap();
	BigVector<VertexId> parent(graph.path+"/parent", graph.vertices);
	graph.set_vertex_data_bytes( graph.vertices * sizeof(VertexId) );

	active_out->clear();
	active_out->set_bit(start_vid);
	parent.fill(-1);
	parent[start_vid] = start_vid;
	VertexId active_vertices = 1;

	double start_time = get_time();

        time_t nowtime = time(NULL);
        struct tm *local = localtime(&nowtime);
        char buf[80];
        strftime(buf,80,"The start time of running is :%D %H:%M:%S",local);
        std::cout << buf << std::endl;

	int iteration = 0;
	while (active_vertices!=0) {
		iteration++;
		printf("%7d: %d\n", iteration, active_vertices);
		std::swap(active_in, active_out);
		active_out->clear();
		graph.hint(parent);
		active_vertices = graph.stream_edges<VertexId>([&](Edge & e){
			if (parent[e.target]==-1) {
				if (cas(&parent[e.target], -1, e.source)) {
					active_out->set_bit(e.target);
					return 1;
				}
			}
			return 0;
		}, active_in);
	}
	double end_time = get_time();
	
	nowtime = time(NULL);
        local = localtime(&nowtime);
        strftime(buf,80,"The end time of running is: %D %H:%M:%S",local);
        std::cout << buf << std::endl;

	int discovered_vertices = graph.stream_vertices<VertexId>([&](VertexId i){
		return parent[i]!=-1;
	});
	printf("discovered %d vertices from %d in %.2f seconds.\n", discovered_vertices, start_vid, end_time - start_time);

	return 0;
}
