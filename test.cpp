#include "core/graph.hpp"
int main(int argc, char ** argv){
	if (argc<3) {
		fprintf(stderr, "usage: test [path] [vertices]\n");
		exit(-1);
	}
	std::string path = argv[1];
    size_t vertices =  argv[2];
    Graph graph(path);
    Bigvector 
}
