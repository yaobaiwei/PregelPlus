#include "pregel_app_svplus.h"

int main(int argc, char* argv[]){
	init_workers();
	pregel_sv("/toyFolder", "/toyOutput");
	worker_finalize();
	return 0;
}
