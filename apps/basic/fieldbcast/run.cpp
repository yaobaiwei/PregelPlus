#include "pregel_app_fieldbcast.h"

int main(int argc, char* argv[]){
	init_workers();
	bool directed=false;
	pregel_fieldbcast("/toyFolder", "/toyOutput", directed);
	worker_finalize();
	return 0;
}
