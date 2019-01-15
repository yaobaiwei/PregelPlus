#include "basic/pregel-dev.h"
#include <set>
using namespace std;

//MINForward+MinBackward

//input format:
//vid \t color sccTag in_num in1 in2 ... out_num out1 out2 ...
//-1 \t nextColorToAssign

//output format:
//vid \t color sccTag minForward minBackward in_num in1 in2 ... out_num out1 out2 ...
//-1 \t nextColorToAssign

struct MinLabelValue_scc
{
	int color;
	//color=-1, means trivial SCC
	//color=-2, means not assigned
	//otherwise, color>=0
	int sccTag;
	VertexID minForward;
	VertexID minBackward;
	vector<VertexID> in_edges;
	vector<VertexID> out_edges;
};

ibinstream & operator<<(ibinstream & m, const MinLabelValue_scc & v){
	m<<v.color;
	m<<v.sccTag;
	m<<v.minForward;
	m<<v.minBackward;
	m<<v.in_edges;
	m<<v.out_edges;
	return m;
}

obinstream & operator>>(obinstream & m, MinLabelValue_scc & v){
	m>>v.color;
	m>>v.sccTag;
	m>>v.minForward;
	m>>v.minBackward;
	m>>v.in_edges;
	m>>v.out_edges;
	return m;
}

//====================================

class MinLabelVertex_scc:public Vertex<VertexID, MinLabelValue_scc, VertexID>
{
	public:
		void bcast_to_in_nbs(VertexID msg)
		{
			vector<VertexID> & nbs=value().in_edges;
			for(int i=0; i<nbs.size(); i++)
			{
				send_message(nbs[i], msg);
			}
		}

		void bcast_to_out_nbs(VertexID msg)
		{
			vector<VertexID> & nbs=value().out_edges;
			for(int i=0; i<nbs.size(); i++)
			{
				send_message(nbs[i], msg);
			}
		}

		virtual void compute(MessageContainer & messages)
		{
			if(phase_num()==1)//minForward
			{
				if(step_num() == 1) {
					if(id!=-1)
					{//not ctrl
						MinLabelValue_scc & val=value();
						if(val.sccTag==0)
						{//not in SCCs found
							val.minForward=id;
							bcast_to_out_nbs(id);
						}
					}
				}
				else
				{
					MinLabelValue_scc & val=value();
					if(val.sccTag==0)
					{
						int min=INT_MAX;
						for(int i=0; i<messages.size(); i++)
						{
							VertexID message=messages[i];
							if(message<min) min=message;
						}
						if(val.minForward==-1 || min<val.minForward)//label=-1 is treated as infinity
						{
							val.minForward=min;
							bcast_to_out_nbs(min);
						}
					}
				}
				vote_to_halt();//ctrl votes to halt directly
			}
			/////////////////////
			else if(phase_num()==2)//minBackward
			{
				if(step_num() == 1)
				{
					if(id!=-1)
					{//not ctrl
						MinLabelValue_scc & val=value();
						if(val.sccTag==0 && val.minForward==id)
						{//not in SCCs found
							val.minBackward=id;
							bcast_to_in_nbs(id);
						}
					}
				}
				else
				{
					MinLabelValue_scc & val=value();
					if(val.sccTag==0)
					{
						int min=INT_MAX;
						for(int i=0; i<messages.size(); i++)
						{
							VertexID message=messages[i];
							if(message<min) min=message;
						}
						if(val.minBackward==-1 || min<val.minBackward)//label=-1 is treated as infinity
						{
							val.minBackward=min;
							bcast_to_in_nbs(min);
						}
					}
				}
				vote_to_halt();//ctrl votes to halt directly
			}
		}
};

class MinLabelWorker_scc:public Worker<MinLabelVertex_scc>
{
	char buf[100];

	public:
		//C version
		virtual MinLabelVertex_scc* toVertex(char* line)
		{
			char * pch;
			pch=strtok(line, "\t");
			MinLabelVertex_scc* v=new MinLabelVertex_scc;
			v->id=atoi(pch);
			pch=strtok(NULL, " ");
			v->value().color=atoi(pch);
			if(v->id==-1) return v;
			pch=strtok(NULL, " ");
			v->value().sccTag=atoi(pch);
			v->value().minForward=-1;
			v->value().minBackward=-1;
			pch=strtok(NULL, " ");
			int num=atoi(pch);
			for(int i=0; i<num; i++)
			{
				pch=strtok(NULL, " ");
				v->value().in_edges.push_back(atoi(pch));
			}
			pch=strtok(NULL, " ");
			num=atoi(pch);
			for(int i=0; i<num; i++)
			{
				pch=strtok(NULL, " ");
				v->value().out_edges.push_back(atoi(pch));
			}
			return v;
		}

		virtual void toline(MinLabelVertex_scc* v, BufferedWriter & writer)
		{
			if(v->id==-1)
			{
				sprintf(buf, "-1\t%d\n", v->value().color);
				writer.write(buf);
				return;
			}
			vector<VertexID> & in_edges=v->value().in_edges;
			vector<VertexID> & out_edges=v->value().out_edges;
			sprintf(buf, "%d\t%d %d %d %d %d ", v->id, v->value().color, v->value().sccTag, v->value().minForward, v->value().minBackward, in_edges.size());
			writer.write(buf);
			for(int i=0; i<in_edges.size(); i++)
			{
				sprintf(buf, "%d ", in_edges[i]);
				writer.write(buf);
			}
			sprintf(buf, "%d ", out_edges.size());
			writer.write(buf);
			for(int i=0; i<out_edges.size(); i++)
			{
				sprintf(buf, "%d ", out_edges[i]);
				writer.write(buf);
			}
			writer.write("\n");
		}
};

class MinLabelCombiner_scc:public Combiner<VertexID>
{
	public:
		virtual void combine(VertexID & old, const VertexID & new_msg)
		{
			if(old>new_msg) old=new_msg;
		}
};

void scc_minlabel(string in_path, string out_path)
{
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=false;
	MinLabelWorker_scc worker;
	MinLabelCombiner_scc combiner;
	worker.setCombiner(&combiner);
	worker.run(param, 2);
}
