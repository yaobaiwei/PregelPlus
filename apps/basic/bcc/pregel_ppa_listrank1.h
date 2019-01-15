#include "basic/pregel-dev.h"
#include "utils/type.h"
using namespace std;

//<V> = <dist_to_head, changing_prev_edge=(u, v), color, init_prev_edge=(u, v)>
struct LR1Value_ppa
{
	int dist2head;
	intpair pred;//shorthand notation for predecessor
	int color;
	intpair prev_edge;
};

ibinstream & operator<<(ibinstream & m, const LR1Value_ppa & v){
	m<<v.dist2head;
	m<<v.pred;
	m<<v.color;
	m<<v.prev_edge;
	return m;
}

obinstream & operator>>(obinstream & m, LR1Value_ppa & v){
	m>>v.dist2head;
	m>>v.pred;
	m>>v.color;
	m>>v.prev_edge;
	return m;
}

//v.pred=null, use intpair(-1, -)
bool isNull(intpair pointer)
{
	return pointer.v1==-1;
}

//====================================
//<I> = edge (u, v)
//<M> = inttriplet (for the info of <dist2head, pred> in phase 1)
class LR1Vertex_ppa:public Vertex<intpair, LR1Value_ppa, inttriplet, IntPairHash>
{
	public:
		virtual void compute(MessageContainer & messages)
		{
			if(phase_num()==1)
			{
				if(step_num()==1)
				{
					if(!isNull(value().pred))
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
					if(isNull(value().pred)) vote_to_halt();
					else send_message(value().pred, inttriplet(id.v1, id.v2, 0));//send request
				}
			}
			else
			{
				if(step_num()==1)
				{//(u, v) sends its dist2head to (v, u)
					intpair vu(id.v2, id.v1);
					inttriplet msg(value().dist2head, 0, 0);//msg=<dist2head, -, ->
					send_message(vu, msg);
				}
				else
				{
					//(v, u) sets its direction, using the sign of color
					if(messages[0].v1>value().dist2head) value().color=-value().color-1;
					vote_to_halt();
				}
			}
		}
};

class LR1Worker_ppa:public Worker<LR1Vertex_ppa>
{
	char buf[1000];

	public:
		//C version
		virtual LR1Vertex_ppa* toVertex(char* line)
		{
			char * pch;
			pch=strtok(line, " ");
			LR1Vertex_ppa* v=new LR1Vertex_ppa;
			v->id.v1=atoi(pch);
			pch=strtok(NULL, "\t");
			v->id.v2=atoi(pch);
			//////
			pch=strtok(NULL, " ");
			int prev_v1=atoi(pch);
			v->value().prev_edge.v1=prev_v1;
			pch=strtok(NULL, " ");
			int prev_v2=atoi(pch);
			v->value().prev_edge.v2=prev_v2;
			pch=strtok(NULL, " ");
			int color=atoi(pch);
			if(color<0)
			{
				v->value().dist2head=0;
				v->value().color=-color-1;
				v->value().pred.v1=-1;
				v->value().pred.v2=-1;
			}
			else
			{
				v->value().dist2head=1;
				v->value().color=color;
				v->value().pred.v1=prev_v1;
				v->value().pred.v2=prev_v2;
			}
			return v;
		}

		virtual void toline(LR1Vertex_ppa* v, BufferedWriter & writer)
		{//output format: (u, v) \t next(u, v)_dist2head_color_isForward (1=forward, 0=backward)
			//here, one needs to regard (u, v) as (v, u), as we go from tail to head
			int color=v->value().color, isForward;
			int dist=v->value().dist2head;
			if(dist!=0)
			{
				if(color>=0) isForward=0;
				else
				{
					color=-color-1;
					isForward=1;
				}
			}
			else
			{
				isForward=1;
			}
			sprintf(buf, "%d %d\t%d %d %d %d %d\n", v->id.v1, v->id.v2, v->value().prev_edge.v1, v->value().prev_edge.v2, dist, color, isForward);
			writer.write(buf);
		}
};

void ppa_listrank1(string in_path, string out_path)
{
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=true;
	LR1Worker_ppa worker;
	worker.run(param, 2);
}
