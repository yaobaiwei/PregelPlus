#include "basic/pregel-dev.h"
using namespace std;

//<V> = <color, parent, adj-list>
//for src (id==color), parent=-1
//otherwise, parent=-2
struct SpanValue_ppa
{
	int color;
	int parent;
	vector<VertexID> edges;
};

ibinstream & operator<<(ibinstream & m, const SpanValue_ppa & v){
	m<<v.color;
	m<<v.parent;
	m<<v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, SpanValue_ppa & v){
	m>>v.color;
	m>>v.parent;
	m>>v.edges;
	return m;
}

//====================================
//<M> = <sender>
class SpanVertex_ppa:public Vertex<VertexID, SpanValue_ppa, VertexID>
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
			if(step_num()==1)
			{
				if(id==value().color) broadcast(id);
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
};

class SpanWorker_ppa:public Worker<SpanVertex_ppa>
{
	char buf[100];

	public:
		//C version
		virtual SpanVertex_ppa* toVertex(char* line)
		{
			char * pch;
			pch=strtok(line, "\t");
			SpanVertex_ppa* v=new SpanVertex_ppa;
			v->id=atoi(pch);
			pch=strtok(NULL, " ");
			int color=atoi(pch);
			v->value().color=color;
			if(v->id==color) v->value().parent=-1;
			else v->value().parent=-2;
			while(pch=strtok(NULL, " "))
			{
				v->value().edges.push_back(atoi(pch));
			}
			return v;
		}

		virtual void toline(SpanVertex_ppa* v, BufferedWriter & writer)
		{
			sprintf(buf, "%d\t%d %d #\n", v->id, v->value().color, v->value().parent);
			writer.write(buf);
		}
};

class SpanCombiner_ppa:public Combiner<VertexID>
{
	public:
		virtual void combine(VertexID & old, const VertexID & new_msg){} //drop new_msg
};

void ppa_spantree(string in_path, string out_path, bool use_combiner)
{
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=true;
	SpanWorker_ppa worker;
	SpanCombiner_ppa combiner;
	if(use_combiner) worker.setCombiner(&combiner);
	worker.run(param);
}
