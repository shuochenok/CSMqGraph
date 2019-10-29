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
	if (argc<2) {
		fprintf(stderr, "usage: spmv [path] [memory budget in GB] [worker_threads_size] [disks number] [stripe_depth in KB]\n");
		exit(-1);
	}
	std::string path = argv[1];
	long memory_bytes = ((argc>=3)?atol(argv[2]):8l)*1024l*1024l*1024l;
	int worker_threads_size = (argc>=4)?atoi(argv[3]):(std::thread::hardware_concurrency());
	int disks_n = (argc>=5)?atoi(argv[4]):1;
	int stripe_depth = (argc>=6)?atoi(argv[5])*1024l:(IOSIZE);

	Graph graph(path,worker_threads_size);
	assert(graph.edge_type==1);
	graph.set_memory_bytes(memory_bytes);
	graph.set_worker_threads_size(worker_threads_size);//set the number of worker thread when streaming edges
	graph.set_disks_number(disks_n);
	graph.set_stripe_depth(stripe_depth);

	BigVector<float> input(graph.path+"/input", graph.vertices);
	BigVector<float> output(graph.path+"/output", graph.vertices);
	graph.set_vertex_data_bytes( (long) graph.vertices * ( sizeof(float) * 2 ) );

	double begin_time = get_time();
	time_t nowtime;
        nowtime = time(NULL);
        struct tm *local = localtime(&nowtime);
        char buf[80];
        strftime(buf,80,"The start time of running is :%D %H:%M:%S",local);
        std::cout << buf << std::endl;

	graph.hint(input, output);
	graph.stream_vertices<float>(
		[&](VertexId i){
			input[i] = i;
			output[i] = 0;
			return 0;
		}, nullptr, 0,
		[&](std::pair<VertexId,VertexId> vid_range){
			input.load(vid_range.first, vid_range.second);
			output.load(vid_range.first, vid_range.second);
		},
		[&](std::pair<VertexId,VertexId> vid_range){
			input.save();
			output.save();
		}
	);
	graph.hint(input);
	graph.stream_edges<float>(
		[&](Edge & e){
			write_add(&output[e.target], input[e.source] * e.weight);
			return 0;
		}, nullptr, 0, 1,
		[&](std::pair<VertexId,VertexId> source_vid_range){
			input.lock(source_vid_range.first, source_vid_range.second);
		},
		[&](std::pair<VertexId,VertexId> source_vid_range){
			input.unlock(source_vid_range.first, source_vid_range.second);
		}
	);
	double end_time = get_time();
	
	nowtime = time(NULL);
        local = localtime(&nowtime);
        strftime(buf,80,"The end time of running is: %D %H:%M:%S",local);
        std::cout << buf << std::endl;
	printf("spmv took %.2f seconds\n", end_time - begin_time);
}
