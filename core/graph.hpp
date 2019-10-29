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

#ifndef GRAPH_H
#define GRAPH_H

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <unistd.h>
#include <malloc.h>
#include <omp.h>
#include <string.h>

#include <thread>
#include <vector>

#include "core/constants.hpp"
#include "core/type.hpp"
#include "core/bitmap.hpp"
#include "core/atomic.hpp"
#include "core/queue.hpp"
#include "core/partition.hpp"
#include "core/bigvector.hpp"
#include "core/time.hpp"

bool f_true(VertexId v) {
	return true;
}

void f_none_1(std::pair<VertexId,VertexId> vid_range) {

}

void f_none_2(std::pair<VertexId,VertexId> source_vid_range, std::pair<VertexId,VertexId> target_vid_range) {

}

class Graph {
	int parallelism;
	int edge_unit;
	bool * should_access_shard;
	long ** fsize;
	char ** buffer_pool;
	long * column_offset;
	long * row_offset;
	long memory_bytes;
	int partition_batch;
	long vertex_data_bytes;
	long PAGESIZE;

	int worker_threads_size;
	int disks_n;
	long stripe_depth;
public:
	std::string path;

	int edge_type;
	VertexId vertices;
	EdgeId edges;
	int partitions;

	Graph (std::string path,int worker_threads_size = std::thread::hardware_concurrency()) {
		PAGESIZE = 4096;
		parallelism = std::thread::hardware_concurrency();
		//parallelism = 1;
		buffer_pool = new char * [worker_threads_size*1];
		for (int i=0;i<worker_threads_size*1;i++) {
			buffer_pool[i] = (char *)memalign(PAGESIZE, IOSIZE);
			assert(buffer_pool[i]!=NULL);
			memset(buffer_pool[i], 0, IOSIZE);
		}
		init(path);
	}

	void set_memory_bytes(long memory_bytes) {
		this->memory_bytes = memory_bytes;
	}

	void set_vertex_data_bytes(long vertex_data_bytes) {
		this->vertex_data_bytes = vertex_data_bytes;
	}
	
	void set_worker_threads_size(int worker_threads_size){
		this->worker_threads_size = worker_threads_size;
	}

	void set_disks_number(int disks_n){
		this->disks_n = disks_n;
	}
	
	void set_stripe_depth(int stripe_depth){
		this->stripe_depth = stripe_depth;
	}

	void init(std::string path) {
		this->path = path;

		FILE * fin_meta = fopen((path+"/meta").c_str(), "r");
		fscanf(fin_meta, "%d %d %ld %d", &edge_type, &vertices, &edges, &partitions);
		fclose(fin_meta);

		if (edge_type==0) {
			PAGESIZE = 4096;
		} else {
			PAGESIZE = 12288;
		}

		should_access_shard = new bool[partitions];

		if (edge_type==0) {
			edge_unit = sizeof(VertexId) * 2;
		} else {
			edge_unit = sizeof(VertexId) * 2 + sizeof(Weight);
		}

		memory_bytes = 1024l*1024l*1024l*1024l; // assume RAM capacity is very large
		partition_batch = partitions;
		vertex_data_bytes = 0;

		char filename[1024];
		fsize = new long * [partitions];
		for (int i=0;i<partitions;i++) {
			fsize[i] = new long [partitions];
			for (int j=0;j<partitions;j++) {
				sprintf(filename, "%s/block-%d-%d", path.c_str(), i, j);
				fsize[i][j] = file_size(filename);
			}
		}

		long bytes;

		column_offset = new long [partitions*partitions+1];
		int fin_column_offset = open((path+"/column_offset").c_str(), O_RDONLY);
		bytes = read(fin_column_offset, column_offset, sizeof(long)*(partitions*partitions+1));
		assert(bytes==sizeof(long)*(partitions*partitions+1));
		close(fin_column_offset);

		row_offset = new long [partitions*partitions+1];
		int fin_row_offset = open((path+"/row_offset").c_str(), O_RDONLY);
		bytes = read(fin_row_offset, row_offset, sizeof(long)*(partitions*partitions+1));
		assert(bytes==sizeof(long)*(partitions*partitions+1));
		close(fin_row_offset);
	}

	Bitmap * alloc_bitmap() {
		return new Bitmap(vertices);
	}

	template <typename T>
	T stream_vertices(std::function<T(VertexId)> process, Bitmap * bitmap = nullptr, T zero = 0,
		std::function<void(std::pair<VertexId,VertexId>)> pre = f_none_1,
		std::function<void(std::pair<VertexId,VertexId>)> post = f_none_1) {
		T value = zero;
		if (bitmap==nullptr && vertex_data_bytes > (0.8 * memory_bytes)) {
			for (int cur_partition=0;cur_partition<partitions;cur_partition+=partition_batch) {
				VertexId begin_vid, end_vid;
				begin_vid = get_partition_range(vertices, partitions, cur_partition).first;
				if (cur_partition+partition_batch>=partitions) {
					end_vid = vertices;
				} else {
					end_vid = get_partition_range(vertices, partitions, cur_partition+partition_batch).first;
				}
				pre(std::make_pair(begin_vid, end_vid));
				#pragma omp parallel for schedule(dynamic) num_threads(parallelism)
				for (int partition_id=cur_partition;partition_id<cur_partition+partition_batch;partition_id++) {
					if (partition_id < partitions) {
						T local_value = zero;
						VertexId begin_vid, end_vid;
						std::tie(begin_vid, end_vid) = get_partition_range(vertices, partitions, partition_id);
						for (VertexId i=begin_vid;i<end_vid;i++) {
							local_value += process(i);
						}
						write_add(&value, local_value);
					}
				}
				#pragma omp barrier
				post(std::make_pair(begin_vid, end_vid));
			}
		} else {
			#pragma omp parallel for schedule(dynamic) num_threads(parallelism)
			for (int partition_id=0;partition_id<partitions;partition_id++) {
				T local_value = zero;
				VertexId begin_vid, end_vid;
				std::tie(begin_vid, end_vid) = get_partition_range(vertices, partitions, partition_id);
				if (bitmap==nullptr) {
					for (VertexId i=begin_vid;i<end_vid;i++) {
						local_value += process(i);
					}
				} else {
					VertexId i = begin_vid;
					while (i<end_vid) {
						unsigned long word = bitmap->data[WORD_OFFSET(i)];
						if (word==0) {
							i = (WORD_OFFSET(i) + 1) << 6;
							continue;
						}
						size_t j = BIT_OFFSET(i);
						word = word >> j;
						while (word!=0) {
							if (word & 1) {
								local_value += process(i);
							}
							i++;
							j++;
							word = word >> 1;
							if (i==end_vid) break;
						}
						i += (64 - j);
					}
				}
				write_add(&value, local_value);
			}
			#pragma omp barrier
		}
		return value;
	}

	void set_partition_batch(long bytes) {
		int x = (int)ceil(bytes / (0.8 * memory_bytes));
		partition_batch = partitions / x;
	}

	template <typename... Args>
	void hint(Args... args);

	template <typename A>
	void hint(BigVector<A> & a) {
		long bytes = sizeof(A) * a.length;
		set_partition_batch(bytes);
	}

	template <typename A, typename B>
	void hint(BigVector<A> & a, BigVector<B> & b) {
		long bytes = sizeof(A) * a.length + sizeof(B) * b.length;
		set_partition_batch(bytes);
	}

	template <typename A, typename B, typename C>
	void hint(BigVector<A> & a, BigVector<B> & b, BigVector<C> & c) {
		long bytes = sizeof(A) * a.length + sizeof(B) * b.length + sizeof(C) * c.length;
		set_partition_batch(bytes);
	}

	int prefetch_merge(long end_offset, long cur_offset_new, long su_end_offset, int cur_partition_row, int cur_partition_column, int row_window_start,int update_mode,long & cur_length, long & prefetch_offset){
		long max_prefetch_offset;
		if(IOSIZE > su_end_offset - cur_offset_new){
			max_prefetch_offset = su_end_offset;
		}
		else{
			max_prefetch_offset = cur_offset_new + IOSIZE;
		}
		//printf("max_prefetch_offset = %ld \n",max_prefetch_offset);
		if(end_offset >= max_prefetch_offset){
			prefetch_offset = max_prefetch_offset;
		}
		else{
			prefetch_offset = end_offset;
			int i,j,k;
			long* row_column_offset;
			if(update_mode == 0){//source oriented update
				i = cur_partition_row;
				j = cur_partition_column + 1;
				k = partitions;
				row_column_offset = row_offset;
			}
			else{// target oriented update
				i = cur_partition_column;
				j = cur_partition_row + 1;
				k = row_window_start + partition_batch;
				row_column_offset = column_offset;
			}
			int flag = 0;
			for(;i < partitions; i++){//traverse the next active edge block(i,j) in the current stripe cell
				if(update_mode == 0){
					if(!should_access_shard[i]){
						flag = 1;
						break;
					}
				}
				for(;j < k; j++){
					if(update_mode == 1){
						if(j >= partitions) break;
						if(!should_access_shard[j]){
							flag = 1;
							break;
						}
					}
					end_offset = row_column_offset[i*partitions+j+1];//
					if(end_offset >= max_prefetch_offset){
						prefetch_offset = max_prefetch_offset;
						flag = 1;
						break;
					}
					else{
						prefetch_offset = end_offset;
					}
				}
				if(update_mode && j!= partitions) break;
				j = 0;
				if(flag) break;
			}
		}
		cur_length = (prefetch_offset - cur_offset_new + PAGESIZE -1)/PAGESIZE * PAGESIZE;
		prefetch_offset = cur_offset_new + cur_length;
		//printf("prefetch_offset = %ld \n",prefetch_offset);
		//printf("cur_length = %ld \n",cur_length);
		return 0;
	}
	template <typename T>
	T stream_edges(std::function<T(Edge&)> process, Bitmap * bitmap = nullptr, T zero = 0, int update_mode = 1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> pre_source_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> post_source_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> pre_target_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> post_target_window = f_none_1) {
		if (bitmap==nullptr) {
			for (int i=0;i<partitions;i++) {
				should_access_shard[i] = true;
			}
		} else {
			for (int i=0;i<partitions;i++) {
				should_access_shard[i] = false;
			}
			#pragma omp parallel for schedule(dynamic) num_threads(parallelism)
			for (int partition_id=0;partition_id<partitions;partition_id++) {
				VertexId begin_vid, end_vid;
				std::tie(begin_vid, end_vid) = get_partition_range(vertices, partitions, partition_id);
				VertexId i = begin_vid;
				while (i<end_vid) {
					unsigned long word = bitmap->data[WORD_OFFSET(i)];
					if (word!=0) {
						should_access_shard[partition_id] = true;
						break;
					}
					i = (WORD_OFFSET(i) + 1) << 6;
				}
			}
			#pragma omp barrier
		}

		T value = zero;
		Queue<std::tuple<int, long, long> > tasks(65536);//65536
		
		std::vector<std::thread> threads;
		long read_bytes = 0;
		/*
		long equal_IOSIZE_count = 0;
                long less_IOSIZE_count = 0;	
		double IO_time = 0;
		double compute_time = 0;
		double IO_compute_time = 0;
		double process_time_start = 0;
                double process_time_end = 0;
		double push_task_time = 0;
		*/

		long total_bytes = 0;
		for (int i=0;i<partitions;i++) {
			if (!should_access_shard[i]) continue;
			for (int j=0;j<partitions;j++) {
				total_bytes += fsize[i][j];
			}
		}
		int read_mode;
		if (memory_bytes < total_bytes) {
			read_mode = O_RDONLY | O_DIRECT;
			// printf("use direct I/O\n");
		} else {
			read_mode = O_RDONLY;
			// printf("use buffered I/O\n");
		}

		int fin;
		long offset = 0;
		
		//printf("disks number = %d\n",disks_n);	
		int* fin_disks = new int[disks_n];
		Queue<std::tuple<int, long, long> > * multi_tasks = new Queue<std::tuple<int, long, long> >[disks_n];
		int size = 0;
		for(int i = 0; i < disks_n; i++){
			multi_tasks[i] = Queue<std::tuple<int, long, long> >(65536/disks_n);//the number of task queue equals disks_n,the size of each task queue is 65536/disks_n
		}
		switch(update_mode) {
		case 0: // source oriented update
			threads.clear();
			for (int ti=0;ti<worker_threads_size;ti++) {
				threads.emplace_back([&](int thread_id){
					T local_value = zero;
					long local_read_bytes = 0;
					while (true) {
						int fin;
						long offset, length;
						std::tie(fin, offset, length) = tasks.pop();
						if (fin==-1) break;
						char * buffer = buffer_pool[thread_id];
						long bytes = pread(fin, buffer, length, offset);
						assert(bytes>0);
						local_read_bytes += bytes;
						// CHECK: start position should be offset % edge_unit
						for (long pos=offset % edge_unit;pos+edge_unit<=bytes;pos+=edge_unit) {
							Edge & e = *(Edge*)(buffer+pos);
							if (bitmap==nullptr || bitmap->get_bit(e.source)) {
								local_value += process(e);
							}
						}
					}
					write_add(&value, local_value);
					write_add(&read_bytes, local_read_bytes);
				}, ti);
			}
			fin = open((path+"/row").c_str(), read_mode);
			posix_fadvise(fin, 0, 0, POSIX_FADV_SEQUENTIAL);
			for (int i=0;i<partitions;i++) {
				if (!should_access_shard[i]) continue;
				for (int j=0;j<partitions;j++) {
					long begin_offset = row_offset[i*partitions+j];
					if (begin_offset - offset >= PAGESIZE) {
						offset = begin_offset / PAGESIZE * PAGESIZE;
					}
					long end_offset = row_offset[i*partitions+j+1];
					if (end_offset <= offset) continue;
					while (end_offset - offset >= IOSIZE) {
						tasks.push(std::make_tuple(fin, offset, IOSIZE));
						offset += IOSIZE;
					}
					if (end_offset > offset) {
						tasks.push(std::make_tuple(fin, offset, (end_offset - offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE));
						offset += (end_offset - offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE;
					}
				}
			}
			for (int i=0;i<worker_threads_size;i++) {
				tasks.push(std::make_tuple(-1, 0, 0));
			}
			for (int i=0;i<worker_threads_size;i++) {
				threads[i].join();
			}
			
		        close(fin);
			break;
		case 1: // target oriented update
			//fin = open((path+"/column").c_str(), read_mode);
			//posix_fadvise(fin, 0, 0, POSIX_FADV_SEQUENTIAL);
			for(int i = 0; i < disks_n; i++){
				char filename[4096];
				sprintf(filename,"%s/column_disk%d_%d",path.c_str(), i, disks_n);
				fin_disks[i] = open(filename, read_mode);
				//printf("fin_disk%d = %d\n",i,fin_disks[i]);
                                posix_fadvise(fin_disks[i], 0, 0, POSIX_FADV_SEQUENTIAL);
                        }

			//process_time_start = get_time();
			for (int cur_partition=0;cur_partition<partitions;cur_partition+=partition_batch) {
				VertexId begin_vid, end_vid;
				begin_vid = get_partition_range(vertices, partitions, cur_partition).first;
				if (cur_partition+partition_batch>=partitions) {
					end_vid = vertices;
				} else {
					end_vid = get_partition_range(vertices, partitions, cur_partition+partition_batch).first;
				}
				pre_source_window(std::make_pair(begin_vid, end_vid));
				//printf("pre %d %d\n", begin_vid, end_vid);
				threads.clear();
				for (int ti=0;ti<worker_threads_size;ti++) {
					threads.emplace_back([&](int thread_id){
						T local_value = zero;
						int thread_num_per_disk0 = worker_threads_size / disks_n;
						int thread_num_per_disk1 = worker_threads_size / disks_n + 1;
						int split_point = worker_threads_size % disks_n * thread_num_per_disk1;
						int task_id = (thread_id < split_point) ? thread_id / thread_num_per_disk1 : (thread_id - split_point) / thread_num_per_disk0 + worker_threads_size % disks_n;

						long local_read_bytes = 0;
						/*
						double local_IO_time = 0;
						double local_compute_time = 0;
						double local_IO_compute_time = 0;
						long local_equal_IOSIZE_count = 0;
						long local_less_IOSIZE_count = 0;
						*/
						while (true) {
							int fin;
							long offset, length;
							std::tie(fin, offset, length) = multi_tasks[task_id].pop();
							
							//printf("the fin is %d\n",fin);
							//printf("the offset is %ld\n",offset);
							//printf("the length is %ld\n",length);
							//if(fin == -1) printf("failed to open the fin file\n");
							
							if (fin==-1) break;
							char * buffer = buffer_pool[thread_id];
							//double start_IO_time = get_time();

							long bytes = pread(fin, buffer, length, offset);
							//if(bytes <= 0) printf("bytes <= 0!\n");
							assert(bytes>0);
							/*
							if(bytes == IOSIZE) local_equal_IOSIZE_count++;
							else local_less_IOSIZE_count++;
							double end_IO_time = get_time();
							local_IO_time += end_IO_time - start_IO_time;
							*/
							local_read_bytes += bytes;

							//double start_compute_time = get_time();
							// CHECK: start position should be offset % edge_unit
							for (long pos=offset % edge_unit;pos+edge_unit<=bytes;pos+=edge_unit) {
								Edge & e = *(Edge*)(buffer+pos);
								if (e.source < begin_vid || e.source >= end_vid) {
									continue;
								}
								if (bitmap==nullptr || bitmap->get_bit(e.source)) {
									local_value += process(e);
								}
							}
							/*
							double end_compute_time = get_time();
							local_compute_time += end_compute_time - start_compute_time;
							local_IO_compute_time += end_compute_time - start_IO_time;
							*/
						}
						
						//printf("the thread %d compute %d edges\n",thread_id,local_value);
						//printf("the thread %d read %ld bytes\n",thread_id,local_read_bytes);
						write_add(&value, local_value);
						write_add(&read_bytes, local_read_bytes);
						/*
						write_add(&equal_IOSIZE_count,local_equal_IOSIZE_count);
						write_add(&less_IOSIZE_count,local_less_IOSIZE_count);

						write_add(&IO_time,local_IO_time);
						write_add(&compute_time,local_compute_time);
						write_add(&IO_compute_time,local_IO_compute_time);
						*/
					}, ti);
				}
				offset = 0;
				long striped_offset_new = 0;
				long striped_length_new = 0;

				double push_task_start = get_time();
				for (int j=0;j<partitions;j++) {
					for (int i=cur_partition;i<cur_partition+partition_batch;i++) {
						if (i>=partitions) break;
						if (!should_access_shard[i]) continue;
						long begin_offset = column_offset[j*partitions+i];
						if (begin_offset - offset >= PAGESIZE) {
							offset = begin_offset / PAGESIZE * PAGESIZE;
						}
						long end_offset = column_offset[j*partitions+i+1];
						//printf("partition (%d,%d):begin_offset = %ld\tend_offset = %ld\n",i,j,begin_offset,end_offset);
						if (end_offset <= offset) continue;
						long IO_size;
						while (end_offset - offset >= IOSIZE) {
							striped_offset_new = offset / stripe_depth / disks_n * stripe_depth + offset % stripe_depth;
							IO_size = IOSIZE;
							if(IO_size <= stripe_depth - offset % stripe_depth){//don't break
								striped_length_new = IO_size;
								multi_tasks[(offset/stripe_depth)%disks_n].push(std::make_tuple(fin_disks[(offset/stripe_depth)%disks_n], striped_offset_new, striped_length_new));
							}
							else{
								striped_length_new = stripe_depth - offset % stripe_depth;
								multi_tasks[(offset/stripe_depth)%disks_n].push(std::make_tuple(fin_disks[(offset/stripe_depth)%disks_n], striped_offset_new, striped_length_new));
								long rest_length_IO = IO_size - striped_length_new;
								int i = 1;
								while(rest_length_IO >= stripe_depth){
									striped_offset_new = (offset + i* stripe_depth)/ stripe_depth / disks_n * stripe_depth;
									striped_length_new = stripe_depth;
									multi_tasks[(offset/stripe_depth+i)%disks_n].push(std::make_tuple(fin_disks[(offset/stripe_depth+i)%disks_n], striped_offset_new, striped_length_new));
									i++;
									rest_length_IO -= stripe_depth;
								}
								if(rest_length_IO >0){
									striped_offset_new = (offset + i* stripe_depth)/ stripe_depth / disks_n * stripe_depth;
									striped_length_new = rest_length_IO;
									multi_tasks[(offset/stripe_depth+i)%disks_n].push(std::make_tuple(fin_disks[(offset/stripe_depth+i)%disks_n], striped_offset_new, striped_length_new));
								}
							}
						  	/*
							printf("offset/IOSIZE = %ld\n",offset/(IOSIZE));
							printf("disks_n = %d",disks_n);
							printf("offset/IOSIZE \% disks_n",(offset/(IOSIZE))%disks_n);
							*/
							/*
							printf("offset_disk_a = %ld\n",offset_disk_a);
							printf("length_disk_a = %ld\n",length_disk_a);
							printf("the disk_id = %d\n",(offset/(IOSIZE))%disks_n);
							printf("the fin_disk = %d\n",fin_disks[(offset/(IOSIZE))%disks_n]);
							*/
							offset += IO_size;
								
						}
						if (end_offset > offset) {
							IO_size = (end_offset - offset + PAGESIZE -1) / PAGESIZE * PAGESIZE;
							striped_offset_new = offset / stripe_depth / disks_n * stripe_depth + offset % stripe_depth;
							if(IO_size <= stripe_depth - offset % stripe_depth){//don't break
								striped_length_new = IO_size;
								multi_tasks[(offset/stripe_depth)%disks_n].push(std::make_tuple(fin_disks[(offset/stripe_depth)%disks_n], striped_offset_new, striped_length_new));
							}
							else{
								striped_length_new = stripe_depth - offset % stripe_depth;
								multi_tasks[(offset/stripe_depth)%disks_n].push(std::make_tuple(fin_disks[(offset/stripe_depth)%disks_n], striped_offset_new, striped_length_new));
								long rest_length_IO = IO_size - striped_length_new;
								int i = 1;
								while(rest_length_IO >= stripe_depth){
									striped_offset_new = (offset + i* stripe_depth)/ stripe_depth / disks_n * stripe_depth;
									striped_length_new = stripe_depth;
									multi_tasks[(offset/stripe_depth+i)%disks_n].push(std::make_tuple(fin_disks[(offset/stripe_depth+i)%disks_n], striped_offset_new, striped_length_new));
									i++;
									rest_length_IO -= stripe_depth;
								}
								if(rest_length_IO >0){
									striped_offset_new = (offset + i* stripe_depth)/ stripe_depth / disks_n * stripe_depth;
									striped_length_new = rest_length_IO;
									multi_tasks[(offset/stripe_depth+i)%disks_n].push(std::make_tuple(fin_disks[(offset/stripe_depth+i)%disks_n], striped_offset_new, striped_length_new));
								}
							}
						  	/*
							printf("offset/IOSIZE = %ld\n",offset/(IOSIZE));
							printf("disks_n = %d",disks_n);
							printf("offset/IOSIZE \% disks_n",(offset/(IOSIZE))%disks_n);
							*/
							/*
							printf("offset_disk_a = %ld\n",offset_disk_a);
							printf("length_disk_a = %ld\n",length_disk_a);
							printf("the disk_id = %d\n",(offset/(IOSIZE))%disks_n);
							printf("the fin_disk = %d\n",fin_disks[(offset/(IOSIZE))%disks_n]);
							*/
							offset += IO_size;
						}
					}
				}
/*
				for (int i=0;i<worker_threads_size;i++) {
					tasks.push(std::make_tuple(-1, 0, 0));
				}
*/
				//Adds a termination task for each thread to the respective task queue.
				for(int i = 0; i < disks_n; i++){
					int thread_num_per_disk = (i < worker_threads_size % disks_n) ? (worker_threads_size / disks_n + 1) : (worker_threads_size / disks_n); 
					for(int j= 0; j < thread_num_per_disk; j++){
						multi_tasks[i].push(std::make_tuple(-1, 0, 0));
					}
				}
				for (int i=0;i<worker_threads_size;i++) {
					threads[i].join();
				}
				//double push_task_end = get_time();
                                //push_task_time += push_task_end - push_task_start;

				post_source_window(std::make_pair(begin_vid, end_vid));
				//printf("post %d %d\n", begin_vid, end_vid);
			}
			for(int i = 0; i < disks_n; i++) close(fin_disks[i]);
			/*
			process_time_end = get_time();
                        printf("process time = %.2fseconds\n",process_time_end - process_time_start);
                        printf("push task takes %.2fseconds\n",push_task_time);
			*/
			break;
		default:
			assert(false);
		}

		printf("streamed %ld bytes of edges\n", read_bytes);
		/*
		printf("IO issue size equal %ld :%ld\n",IOSIZE,equal_IOSIZE_count);
                printf("IO issue size less %ld :%ld\n",IOSIZE,less_IOSIZE_count);

                printf("IO_time = %.2f seconds\n",IO_time);
                printf("compute_time = %.2f seconds\n",compute_time);
                printf("IO_compute_time = %.2f seconds\n",IO_compute_time);
		*/
		return value;
	}
};

#endif
