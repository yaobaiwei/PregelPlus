#include "pregel_scc_owcty.h"
#include "pregel_scc_minlabel.h"
#include "pregel_scc_mingdecom.h"

//vid \t color=-2 sccTag=0 in_num in1 in2 ... out_num out1 out2 ...

int main(int argc, char* argv[]){
	init_workers();
	pregel_owcty("/scc_input", "/scc_owcty");
	scc_minlabel("/scc_owcty", "/scc_minlabel");
	scc_minGDecom("/scc_minlabel", "/scc_output");

	pregel_owcty("/scc_output", "/scc_owcty");
	scc_minlabel("/scc_owcty", "/scc_minlabel");
	scc_minGDecom("/scc_minlabel", "/scc_output");
	worker_finalize();
	return 0;
}
