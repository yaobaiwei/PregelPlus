#include "basic/pregel-dev.h"
using namespace std;

struct CCSPValue_ppa
{
	int color;
	int parent;
	vector<VertexID> edges;
};

ibinstream & operator<<(ibinstream & m, const CCSPValue_ppa & v){
	m<<v.color;
	m<<v.parent;
	m<<v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, CCSPValue_ppa & v){
	m>>v.color;
	m>>v.parent;
	m>>v.edges;
	return m;
}

//====================================

class CCSPVertex_ppa:public Vertex<VertexID, CCSPValue_ppa, VertexID>
{
	public:
		void broadcast(VertexID msg)
		{
			vector<VertexID> & nbs=value().edges;
			for(int i=0; i<nbs.size(); i++)
			{
				send_message(nbs[i], msg);
			}
		}

		virtual void compute(MessageContainer & messages)
		{
			if(phase_num()==1)
			{
				if(step_num()==1)
				{
					VertexID min=id;
					vector<VertexID> & nbs=value().edges;
					for(int i=0; i<nbs.size(); i++)
					{
						if(min>nbs[i]) min=nbs[i];
					}
					value().color=min;
					broadcast(min);
					vote_to_halt();
				}
				else
				{
					VertexID min=messages[0];
					for(int i=1; i<messages.size(); i++)
					{
						if(min>messages[i]) min=messages[i];
					}
					if(min<value().color)
					{
						value().color=min;
						broadcast(min);
					}
					vote_to_halt();
				}
			}
			else
			{
				if(step_num()==1)
				{
					if(id==value().color){
						value().parent=-1;
						broadcast(id);
					}
					else value().parent=-2;
				}
				else
				{
					if(value().parent==-2)//-2 means not yet reached
					{
						if(messages.size()>0)
						{
							value().parent=messages[0];
							broadcast(id);
						}
					}
				}
				vote_to_halt();
			}
		}
};

class CCSPWorker_ppa:public Worker<CCSPVertex_ppa>
{
	char buf[100];

	public:
		//C version
		virtual CCSPVertex_ppa* toVertex(char* line)
		{
			char * pch;
			pch=strtok(line, "\t");
			CCSPVertex_ppa* v=new CCSPVertex_ppa;
			v->id=atoi(pch);
			pch=strtok(NULL, " ");
			int num=atoi(pch);
			for(int i=0; i<num; i++)
			{
				pch=strtok(NULL, " ");
				v->value().edges.push_back(atoi(pch));
			}
			return v;
		}

		virtual void toline(CCSPVertex_ppa* v, BufferedWriter & writer)
		{
			sprintf(buf, "%d\t%d %d #\n", v->id, v->value().color, v->value().parent);
			writer.write(buf);
		}
};

class CCSPCombiner_ppa:public Combiner<VertexID>
{
	public:
		virtual void combine(VertexID & old, const VertexID & new_msg)
		{
			if(old>new_msg) old=new_msg;
		}
};

void ppa_hashmin_spantree(string in_path, string out_path, bool use_combiner)
{
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=false;
	CCSPWorker_ppa worker;
	CCSPCombiner_ppa combiner;
	if(use_combiner) worker.setCombiner(&combiner);
	worker.run(param, 2);
}
