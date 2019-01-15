#include "basic/pregel-dev.h"
#include "utils/type.h"
using namespace std;

//<V> = <dist_to_head, changing_prev_edge=(u, v), color, init_prev_edge=(u, v)>
struct LR2Value_ppa
{
	int dist2head;
	intpair pred;//shorthand notation for predecessor
	int color;
	bool isForward;
	int nd;//nd(v)
};

ibinstream & operator<<(ibinstream & m, const LR2Value_ppa & v){
	m<<v.dist2head;
	m<<v.pred;
	m<<v.color;
	m<<v.isForward;
	m<<v.nd;
	return m;
}

obinstream & operator>>(obinstream & m, LR2Value_ppa & v){
	m>>v.dist2head;
	m>>v.pred;
	m>>v.color;
	m>>v.isForward;
	m>>v.nd;
	return m;
}

//v.pred=null, use intpair(-1, -)
bool isnull(intpair pointer)
{
	return pointer.v1==-1;
}

//====================================
//<I> = edge (u, v)
//<M> = inttriplet (for the info of <dist2head, pred> in phase 1)
class LR2Vertex_ppa:public Vertex<intpair, LR2Value_ppa, inttriplet, IntPairHash>
{
	public:
		virtual void compute(MessageContainer & messages)
		{
			if(phase_num()==1)
			{
				if(step_num()==1)
				{
					if(!isnull(value().pred))
					{//send request to pred (with requester's id)
						send_message(value().pred, inttriplet(id.v1, id.v2, 0));//msg=<me=(u, v), 0>
					}
				}
				else if((step_num() & 1) == 0)//even
				{
					for(int i=0; i<messages.size(); i++)//at most one in fact
					{
						intpair requester(messages[i].v1, messages[i].v2);
						inttriplet msg(value().dist2head, value().pred.v1, value().pred.v2);//msg=<dist2head, pred>
						//respond the requester (with info of me)
						send_message(requester, msg);
					}
				}
				else//odd
				{
					for(int i=0; i<messages.size(); i++)//at most one in fact
					{//update
						value().dist2head+=messages[i].v1;
						value().pred.v1=messages[i].v2;
						value().pred.v2=messages[i].v3;
					}
					if(isnull(value().pred)) vote_to_halt();
					else send_message(value().pred, inttriplet(id.v1, id.v2, 0));//send request
				}
			}
			else
			{
				if(step_num()==1)
				{//(u, v) sends its dist2head to (v, u)
					if(value().isForward==false)//v->p(v) = (p(v), v)
					{
						intpair vu(id.v2, id.v1);
						inttriplet msg(value().dist2head, 0, 0);//msg=<dist2head, -, ->
						send_message(vu, msg);
					}
				}
				else
				{
					//(v, u) sets its direction, using the sign of color
					if(messages.size()>0) value().nd = messages[0].v1 - value().dist2head + 1;
					vote_to_halt();
				}
			}
		}
};

//====================================
//agg for collecting (CC, size)
typedef hash_map<int, int> LR2Map;
typedef LR2Map::iterator LR2Iter;

class LR2Agg_ppa:public Aggregator<LR2Vertex_ppa, LR2Map, LR2Map>
{
	private:
		LR2Map map;
	public:
		virtual void init(){}

		virtual void stepPartial(LR2Vertex_ppa* v)
		{
			if(phase_num()==2 && step_num()==2)
			{
				int color=v->value().color;
				if(color<0) color=-color-1;
				LR2Iter it=map.find(color);
				if(it==map.end()) map[color]=1;
				else it->second++;
			}
		}

		virtual void stepFinal(LR2Map* part)
		{
			if(phase_num()==2 && step_num()==2)
			{
				for(LR2Iter it0=part->begin(); it0!=part->end(); it0++)
				{
					int color=it0->first;
					int count=it0->second;
					LR2Iter it=map.find(color);
					if(it==map.end()) map[color]=count;
					else it->second+=count;
				}
			}
		}

		virtual LR2Map* finishPartial(){ return &map; }
		virtual LR2Map* finishFinal(){ return &map; }
};

class LR2Worker_ppa:public Worker<LR2Vertex_ppa, LR2Agg_ppa>
{
	char buf[1000];

	public:
		//C version
		virtual LR2Vertex_ppa* toVertex(char* line)
		{
			char * pch;
			pch=strtok(line, " ");
			LR2Vertex_ppa* v=new LR2Vertex_ppa;
			v->id.v1=atoi(pch);
			pch=strtok(NULL, "\t");
			v->id.v2=atoi(pch);
			//////
			pch=strtok(NULL, " ");
			v->value().pred.v1=atoi(pch);
			pch=strtok(NULL, " ");
			v->value().pred.v2=atoi(pch);
			pch=strtok(NULL, " ");//skip old_dist2head
			pch=strtok(NULL, " ");
			int color=atoi(pch);
			v->value().color=color;
			pch=strtok(NULL, " ");
			bool forward=atoi(pch);
			v->value().isForward=forward;
			if(color<0)
			{
				v->value().dist2head=0;
				v->value().pred.v1=-1;
				v->value().pred.v2=-1;
			}
			else{
				if(forward) v->value().dist2head=1;
				else v->value().dist2head=0;
			}
			return v;
		}

		virtual void toline(LR2Vertex_ppa* v, BufferedWriter & writer)
		{//output format: $ v \t pre(v) nd(v)
			if(v->value().isForward)
			{
				sprintf(buf, "%d\t%d %d $\n", v->id.v1, (v->value().dist2head+1), v->value().nd);
				writer.write(buf);
				int color=v->value().color;
				if(color<0)
				{//first edge
					LR2Map* agg=(LR2Map*)getAgg();
					color=-color-1;
					int count=(*agg)[color];
					int num=count/2+1;
					sprintf(buf, "%d\t0 %d $\n", v->id.v2, num);
					writer.write(buf);
				}
			}
		}
};

void ppa_listrank2(string in_path, string out_path)
{
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=true;
	LR2Worker_ppa worker;
	LR2Agg_ppa agg;
	worker.setAggregator(&agg);
	worker.run(param, 2);
}
