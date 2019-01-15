/**
*  Updated by Chen Hongzhi on 2015.10.7, bug-fixed by yanda on 2015.10.18
*  change 1: delete the Star Hooking step
*  change 2: the stop condition checks whether pre_D[v] = D[v] for every vertex
*/

#include "basic/pregel-dev.h"
#include "utils/type.h"
using namespace std;

#define ROUND_STEP_NUM 7

//input line format: vertexID \t numOfNeighbors neighbor1 neighbor2 ...
//output line format: v \t min_vertexID(v's connected component)

//naming rules:
//G(get): receive messages
//D(do): process vertex
//S(send): send messages
//R(respond): including GDS

//<V>=<D[v] of prev-round, D[v] of cur-round>
//initially, D[v]=v, Pre_D[v] = v
struct SVValue_pregel
{
	int pre_D;
	int D;
	vector<VertexID> edges;
};

ibinstream & operator<<(ibinstream & m, const SVValue_pregel & v){
	m<<v.D;
	m<<v.pre_D;
	m<<v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, SVValue_pregel & v){
	m>>v.D;
	m>>v.pre_D;
	m>>v.edges;
	return m;
}

//====================================

class SVVertex_pregel:public Vertex<int, SVValue_pregel, int>
{
	void treeInit_D() {
		//set D[u]=min{v} to allow fastest convergence, though any v is ok (assuming (u, v) is accessed last)
		vector<VertexID> & edges=value().edges;
		for(int i=0; i<edges.size(); i++)
		{
			int nb=edges[i];
			if(nb<value().D) value().D=nb;
		}
	}

	void rtHook_1S()// = shortcut's request to w
	{// request to w
		int Du=value().D;
		send_message(Du, id);
	}

	void rtHook_2R(MessageContainer & msgs)// = shortcut's respond by w
	{// respond by w
		int Dw=value().D;
		for(int i=0; i<msgs.size(); i++)
		{
			int requester=msgs[i];
			send_message(requester, Dw);
		}
	}

	void rtHook_2S()
	{// send negated D[v]
		int Dv=value().D;
		vector<VertexID> & edges=value().edges;
		for(int i=0; i<edges.size(); i++)
		{
			int nb=edges[i];
			send_message(nb, -Dv-1);//negate Dv to differentiate it from other msg types
		}
	}//in fact, a combiner with MIN operator can be used here

	void rtHook_3GDS(MessageContainer & msgs)//return whether a msg is sent
	{//set D[w]=min_v{D[v]} to allow fastest convergence, though any D[v] is ok (assuming (u, v) is accessed last)
		int Dw=-1;
		int Du=value().D;
		int Dv=-1;//pick the min
		for(int i=0; i<msgs.size(); i++)
		{
			int msg=msgs[i];
			if(msg>=0) Dw=msg;
			else// from rtHook_2R
			{
				int cur=-msg-1;
				if(Dv==-1 || cur<Dv) Dv=cur;
			}
		}
		if(Dw==Du && Dv!=-1 && Dv<Du)//condition checking
		{
			send_message(Du, Dv);
		}
	}

	void rtHook_4GD(MessageContainer & msgs)
	{//set D[w]=min_v{D[v]} to allow fastest convergence, though any D[v] is ok (assuming (u, v) is accessed last)
		int Dv=-1;
		for(int i=0; i<msgs.size(); i++)
		{
			int cur=msgs[i];
			if(Dv==-1 || cur<Dv) Dv=cur;
		}
		if(Dv!=-1) value().D=Dv;
	}

	void shortcut_3GD(MessageContainer & msgs) {//D[u]=D[D[u]]
		value().D=msgs[0];  //Once update the D[v], we should also update the Pre_D to keep the pre_D[v]
	}

	public:

	virtual void compute(MessageContainer & messages)
	{
		if(step_num() == 1)
		{
			treeInit_D();
			rtHook_1S();
		}
		else if(step_num() % ROUND_STEP_NUM == 2)
		{
			rtHook_2R(messages);
			rtHook_2S();
		}
		else if(step_num() % ROUND_STEP_NUM == 3)
		{
			rtHook_3GDS(messages);
		}
		else if(step_num() % ROUND_STEP_NUM == 4)
		{
			rtHook_4GD(messages);
		}
		else if(step_num() % ROUND_STEP_NUM == 5)
		{
			rtHook_1S();
		}
		else if(step_num() % ROUND_STEP_NUM == 6)
		{
			rtHook_2R(messages);
		}
		else if(step_num() % ROUND_STEP_NUM == 0)
		{
			shortcut_3GD(messages);
		}

		else if(step_num() % ROUND_STEP_NUM == 1)
		{
			bool* agg=(bool*)getAgg();
			if(*agg)
			{
				vote_to_halt();
				return;
			}
			rtHook_1S();
		}
	}
};

//====================================

class SVAgg_pregel:public Aggregator<SVVertex_pregel, bool, bool>
{
	private:
		bool AND;
	public:
		virtual void init(){
			AND = true;
		}

		virtual void stepPartial(SVVertex_pregel* v)
		{
			if(step_num() % 7 == 0)
			{
				if(v->value().pre_D != v->value().D){
					AND=false;
					v->value().pre_D = v->value().D;//to prepare for next round
				}
			}
		}

		virtual void stepFinal(bool* part)
		{
			if(*part==false) AND=false;
		}

		virtual bool* finishPartial(){ return &AND; }
		virtual bool* finishFinal(){ return &AND; }
};

//====================================

class SVWorker_pregel:public Worker<SVVertex_pregel,  SVAgg_pregel>
{
	char buf[100];

	public:
	virtual SVVertex_pregel* toVertex(char* line)
	{
		char * pch;
		pch=strtok(line, "\t");
		SVVertex_pregel* v=new SVVertex_pregel;
		v->id=atoi(pch);
		pch=strtok(NULL, " ");
		int num=atoi(pch);
		for(int i=0; i<num; i++)
		{
			pch=strtok(NULL, " ");
			v->value().edges.push_back(atoi(pch));
		}
		v->value().D=v->id;
		v->value().pre_D = v->id; //record the D[u] in the previous superstep in order to check whether the ALG come into convergence
		return v;
	}

	virtual void toline(SVVertex_pregel* v, BufferedWriter & writer)
	{
		sprintf(buf, "%d\t%d\n", v->id, v->value().D);
		writer.write(buf);
	}
};

void pregel_sv(string in_path, string out_path)
{
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=false;
	SVWorker_pregel worker;
	SVAgg_pregel agg;
	worker.setAggregator(&agg);
	worker.run(param);
}
