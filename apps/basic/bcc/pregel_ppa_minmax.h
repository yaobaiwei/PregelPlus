#include "basic/pregel-dev.h"
#include "utils/type.h"
using namespace std;

//<E> = <preorder_number, tag(tree/non-tree)>
struct MinMaxEdge_ppa
{
	int no;
	bool is_tree;
};

ibinstream & operator<<(ibinstream & m, const MinMaxEdge_ppa & v){
	m<<v.no;
	m<<v.is_tree;
	return m;
}

obinstream & operator>>(obinstream & m, MinMaxEdge_ppa & v){
	m>>v.no;
	m>>v.is_tree;
	return m;
}

//<V> = <color, parent, vec<E>>
struct MinMaxValue_ppa
{
	int nd;
	int min;
	int max;
	int little;
	int big;
	int globMin;
	int globMax;
	vector<MinMaxEdge_ppa> edges;
};

ibinstream & operator<<(ibinstream & m, const MinMaxValue_ppa & v){
	m<<v.nd;
	m<<v.min;
	m<<v.max;
	m<<v.little;
	m<<v.big;
	m<<v.globMin;
	m<<v.globMax;
	m<<v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, MinMaxValue_ppa & v){
	m>>v.nd;
	m>>v.min;
	m>>v.max;
	m>>v.little;
	m>>v.big;
	m>>v.globMin;
	m>>v.globMax;
	m>>v.edges;
	return m;
}

//====================================
//the following two updates are done in parallel
//- update min(v)/max(v) for iteraion i
//- update global(v) for iteraion (i+1)
class MinMaxVertex_ppa:public Vertex<intpair, MinMaxValue_ppa, inttriplet, IntPairHash>
{
	enum types{INIT=1, LITTLE=2, BIG=3, GLOB=4};

	bool is_isolated()
	{
		//return id.v2==0 && value().nd==1; //also a correct choice
		return value().edges.size()==0;
	}

	int get_i()
	{
		return (step_num()-3)/2;
	}

	public:

		virtual void compute(MessageContainer & messages)
		{
			if(step_num()==1)
			{
				if(is_isolated())
				{
					//initialization
					MinMaxValue_ppa & val=value();
					val.min=0;
					val.max=0;
				}
				else
				{//Assumption: iteration variable i=0
					int pre=id.v2;
					MinMaxValue_ppa & val=value();
					//initializing little(v)/big(v) (note that 2^i=2^0=1)
					val.little=pre;
					val.big=pre+val.nd-1;
					//computing localMin(v)/localMax(v)
					int min=pre;
					int max=pre;
					vector<MinMaxEdge_ppa> & edges=val.edges;
					for(int i=0; i<edges.size(); i++)
					{
						if(edges[i].is_tree==0)//non-tree edge
						{
							int pre_nb=edges[i].no;
							if(pre_nb<min) min=pre_nb;
							if(pre_nb>max) max=pre_nb;
						}
					}
					//initializing globalMin(v)/globalMax(v)
					val.globMin=min;
					val.globMax=max;
					//initializing min(v)/max(v)
					val.min=val.globMin;
					val.max=val.globMax;
					//========================================
					//get the following (note that 2^i=2^0=1)
					//v.min=min(local(me), local(big(v)))
					//v.max=min(local(me), local(big(v)))
					//----------------------------------------
					int color=id.v1;
					if(val.little < val.big)
					{
						//- send request to big for local(big)
						//init purpose
						intpair tgt(color, val.big);
						inttriplet msg(INIT, pre, -1);//msg type 5: <TYPE5, sender, ->
						send_message(tgt, msg);
						//----------------------------------------
						//app logic
						if(val.big % 2 != 0)
						{//ask w=big(v)-1 for globMin(w), globMax(w)
							intpair tgt(color, val.big-1);
							inttriplet msg(BIG, pre, -1);//msg type 1: <TYPE1, sender, ->
							send_message(tgt, msg);
						}
					}
					//========================================
					//get the following (step length = 2^0 = 1)
					//v.globalMin=min(globalMin(me), globalMin(me+1))
					//v.globalMax=max(globalMax(me), globalMax(me+1))
					//- send request to (me+1)
					if((pre & 1)==0) //2^(i+1)=2
					{
						intpair tgt1(color, pre+1);
						inttriplet msg1(GLOB, pre, -1);//msg type 2: <TYPE2, sender, ->
						send_message(tgt1, msg1);
					}
				}
			}
			else if((step_num() & 1) == 0)
			{
				int color=id.v1;
				MinMaxValue_ppa & val=value();
				for(int i=0; i<messages.size(); i++)
				{
					inttriplet in_msg=messages[i];
					if(in_msg.v1==LITTLE)
					{
						int requester=in_msg.v2;
						intpair tgt(color, requester);
						inttriplet msg(LITTLE, val.globMin, val.globMax);
						send_message(tgt, msg);
					}
					else if(in_msg.v1==BIG)
					{
						int requester=in_msg.v2;
						intpair tgt(color, requester);
						inttriplet msg(BIG, val.globMin, val.globMax);
						send_message(tgt, msg);
					}
					else if(in_msg.v1==GLOB)
					{
						int requester=in_msg.v2;
						intpair tgt(color, requester);
						inttriplet msg(GLOB, val.globMin, val.globMax);
						send_message(tgt, msg);
					}
					else
					{
						int requester=in_msg.v2;
						intpair tgt(color, requester);
						inttriplet msg(INIT, val.globMin, val.globMax);
						send_message(tgt, msg);
					}
				}
			}
			else if ((step_num() & 1) == 1) {
				int cur_i = get_i();
				int cur_pow = (1 << cur_i);
				MinMaxValue_ppa & val=value();
				for(int i=0; i<messages.size(); i++)
				{
					inttriplet in_msg=messages[i];
					if(in_msg.v1==LITTLE)
					{
						if(val.min > in_msg.v2) val.min = in_msg.v2;//set min(v) = min{min(v), msg.globMin}
						if(val.max < in_msg.v3) val.max = in_msg.v3;//set max(v) = max{max(v), msg.globMax}
						val.little += cur_pow;
					}
					else if(in_msg.v1==BIG)
					{
						if(val.min > in_msg.v2) val.min = in_msg.v2;//set min(v) = min{min(v), msg.globMin}
						if(val.max < in_msg.v3) val.max = in_msg.v3;//set max(v) = max{max(v), msg.globMax}
						val.big -= cur_pow;
					}
					else if(in_msg.v1==GLOB)
					{
						if(val.globMin > in_msg.v2) val.globMin = in_msg.v2;
						if(val.globMax < in_msg.v3) val.globMax = in_msg.v3;
					}
					else
					{
						if(val.min > in_msg.v2) val.min = in_msg.v2;//set min(v) = min{min(v), msg.globMin}
						if(val.max < in_msg.v3) val.max = in_msg.v3;//set max(v) = max{max(v), msg.globMax}
					}
				}
				////////
				int next_i = cur_i + 1;
				int power = (1 << next_i);
				int nxt = (power << 1);
				int pre = id.v2;
				if((val.little < val.big) && (val.little % nxt != 0))
				{//ask w=little(v) for globMin(w), globMax(w)
					intpair tgt(id.v1, val.little);
					inttriplet msg(LITTLE, pre, -1);//msg type 1: <TYPE1, sender, ->
					send_message(tgt, msg);
				}
				if((val.little < val.big) && (val.big % nxt != 0))
				{//ask w=big(v) for globMin(w), globMax(w)
					intpair tgt(id.v1, val.big-power);
					inttriplet msg(BIG, pre, -1);//msg type 1: <TYPE1, sender, ->
					send_message(tgt, msg);
				}
				if(pre % nxt==0)
				{//send msg to (me+2^(i+1))
					intpair tgt(id.v1, pre+power);
					inttriplet msg(GLOB, pre, -1);//msg type 2: <TYPE2, sender, ->
					send_message(tgt, msg);
				}
			}
			vote_to_halt();//isolated vertices vote to halt in step 1, and are never activated again
		}
};

class MinMaxWorker_ppa:public Worker<MinMaxVertex_ppa>
{
	char buf[100];

	public:

		virtual MinMaxVertex_ppa* toVertex(char* line)
		{
			char * pch;
			pch=strtok(line, " ");
			MinMaxVertex_ppa* v=new MinMaxVertex_ppa;
			v->id.v1=atoi(pch);
			pch=strtok(NULL, "\t");
			v->id.v2=atoi(pch);
			pch=strtok(NULL, " ");
			v->value().nd=atoi(pch);
			vector<MinMaxEdge_ppa> & edges=v->value().edges;
			while(pch=strtok(NULL, " "))
			{
				MinMaxEdge_ppa edge;
				edge.no=atoi(pch);
				pch=strtok(NULL, " ");
				edge.is_tree=atoi(pch);
				edges.push_back(edge);
			}
			return v;
		}

		virtual void toline(MinMaxVertex_ppa* v, BufferedWriter & writer)
		{//output u v \t v w color
			sprintf(buf, "%d %d\t%d %d %d ", v->id.v1, v->id.v2, v->value().nd, v->value().min, v->value().max);
			writer.write(buf);
			vector<MinMaxEdge_ppa> & edges=v->value().edges;
			for(int i=0; i<edges.size(); i++)
			{
				sprintf(buf, "%d %d ", edges[i].no, edges[i].is_tree);
				writer.write(buf);
			}
			writer.write("\n");
		}
};

void ppa_minmax(string in_path, string out_path)
{
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=true;
	MinMaxWorker_ppa worker;
	worker.run(param);
}
