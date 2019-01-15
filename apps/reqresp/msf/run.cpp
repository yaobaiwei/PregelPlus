#include "req_app_mst.h"

int main(int argc, char* argv[]){
	init_workers();
	req_mst("/msf", "/msf_out");
	worker_finalize();
	return 0;
}
