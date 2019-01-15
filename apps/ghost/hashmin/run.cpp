#include "ghost_app_hashmin.h"

int main(int argc, char* argv[]){
	init_workers();
	set_ghost_threshold(2);//set to at least 100 for real large graphs
	ghost_hashmin("/toyFolder", "/toyOutput", true);
	worker_finalize();
	return 0;
}
