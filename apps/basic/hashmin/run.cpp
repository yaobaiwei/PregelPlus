#include "pregel_app_hashmin.h"

int main(int argc, char* argv[]){
	init_workers();
	pregel_hashmin("/toyFolder", "/toyOutput", true);
	worker_finalize();
	return 0;
}
