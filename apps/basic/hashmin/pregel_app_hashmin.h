#include "basic/pregel-dev.h"
using namespace std;

//input line format: vertexID \t numOfNeighbors neighbor1 neighbor2 ...
//output line format: v \t min_vertexID(v's connected component)

struct CCValue_pregel
{
	int color;
	vector<VertexID> edges;
};

ibinstream & operator<<(ibinstream & m, const CCValue_pregel & v){
	m<<v.color;
	m<<v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, CCValue_pregel & v){
	m>>v.color;
	m>>v.edges;
	return m;
}

//====================================

class CCVertex_pregel:public Vertex<VertexID, CCValue_pregel, VertexID>
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
};

class CCWorker_pregel:public Worker<CCVertex_pregel>
{
	char buf[100];

	public:
		//C version
		virtual CCVertex_pregel* toVertex(char* line)
		{
			char * pch;
			pch=strtok(line, "\t");
			CCVertex_pregel* v=new CCVertex_pregel;
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

		virtual void toline(CCVertex_pregel* v, BufferedWriter & writer)
		{
			sprintf(buf, "%d\t%d\n", v->id, v->value().color);
			writer.write(buf);
		}
};

class CCCombiner_pregel:public Combiner<VertexID>
{
	public:
		virtual void combine(VertexID & old, const VertexID & new_msg)
		{
			if(old>new_msg) old=new_msg;
		}
};

void pregel_hashmin(string in_path, string out_path, bool use_combiner)
{
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=false;
	CCWorker_pregel worker;
	CCCombiner_pregel combiner;
	if(use_combiner) worker.setCombiner(&combiner);
	worker.run(param);
}
