#include "pregel_app_sssp.h"

int main(int argc, char* argv[]){
	init_workers();
	pregel_sssp(2, "/toyFolder", "/toyOutput", true);
	worker_finalize();
	return 0;
}
