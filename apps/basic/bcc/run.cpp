#include "pregel_ppa_hashmin.h"
#include "pregel_app_sv.h"
#include "pregel_ppa_spantree.h"
#include "pregel_ppa_etour.h"
#include "pregel_ppa_hashminAndSpantree.h"
#include "pregel_ppa_listrank1.h"
#include "pregel_ppa_listrank2.h"
#include "pregel_ppa_merge.h"
#include "pregel_ppa_minmax.h"
#include "pregel_ppa_auxgraph.h"
#include "pregel_ppa_triphashmin.h"
#include "pregel_ppa_tripsv.h"
#include "pregel_ppa_case1mark.h"
#include "pregel_ppa_edgeback.h"

int main(int argc, char* argv[]){
	init_workers();

	//////group 1 option 1 BEGIN
	ppa_hashmin("/toyFolder", "/hashmin_ppa", true);
	ppa_spantree("/hashmin_ppa", "/spantree_ppa", true);
	pregel_sv("/toyFolder", "/sv_ppa");//to test time only
	//////group 1 option 1 END
	//ppa_hashmin_spantree("/toyFolder", "/spantree_ppa", true);//////group 1 option 2
	ppa_etour("/spantree_ppa", "/etour_ppa");
	ppa_listrank1("/etour_ppa", "/listrank1_ppa");
	ppa_listrank2("/listrank1_ppa", "/listrank2_ppa");

	MultiInputParams param;
	param.add_input_path("/toyFolder");
	param.add_input_path("/spantree_ppa");
	param.add_input_path("/listrank2_ppa");
	param.output_path="/merge_ppa";
	param.force_write=true;
	param.native_dispatcher=false;
	ppa_merge(param);

	ppa_minmax("/merge_ppa", "/minmax_ppa");
	ppa_auxgraph("/minmax_ppa", "/auxg_ppa");
	ppa_triphashmin("/auxg_ppa", "/triphashmin_ppa", true);//////group 2 option 1
	//ppa_tripsv("/auxg_ppa", "/triphashmin_ppa", true);//////group 2 option 2

	MultiInputParams param1;
	param1.add_input_path("/minmax_ppa");
	param1.add_input_path("/triphashmin_ppa");
	param1.output_path="/case1mark_ppa";
	param1.force_write=true;
	param1.native_dispatcher=false;
	ppa_case1(param1);

	MultiInputParams param2;
	param2.add_input_path("/spantree_ppa");
	param2.add_input_path("/listrank2_ppa");
	param2.add_input_path("/case1mark_ppa");
	param2.output_path="/edgeback_ppa";
	param2.force_write=true;
	param2.native_dispatcher=false;
	ppa_eback(param2);

	worker_finalize();
	return 0;
}
