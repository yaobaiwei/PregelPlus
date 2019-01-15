#include "pregel_app_sv.h"

int main(int argc, char* argv[]){
	init_workers();
	pregel_sv("/sv", "/sv_out");
	worker_finalize();
	return 0;
}
