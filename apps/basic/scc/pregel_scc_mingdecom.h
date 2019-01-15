#include "basic/pregel-dev.h"
#include "utils/type.h"
#include "scc_config.h"
#include <set>
using namespace std;

//input format:
//vid \t color sccTag minForward minBackward in_num in1 in2 ... out_num out1 out2 ...
//-1 \t nextColorToAssign

//output format:
//vid \t newColor sccTag in_num in1 in2 ... out_num out1 out2 ...
//-1 \t nextColorToAssign

//<V> = <newColor, sccTag, minForward, minBackward>
//color=-1, means trivial SCC
//color=-2, means not assigned
//<ctrl> = <nextColorToAssign, -, -, ->

//round1:
//- if id=-1, aggregator gets nextColorToAssign
//- if sccTag=0, aggregator obtains all (forward_backward, count) data
//- if sccTag=1, vertex vote to halt
//- aggregator finally computes (forward_backward, newColor) map, report max|Scc|
//round2:
//- ctrl gets nextColorToAssign
//- if not ctrl, and sccTag=0, set newColor; get color's count, if <Config.threshold, set sccTag
//- if not ctrl, sccTag=0, and minForward=minBackward, set sccTag
//- send <sender, newColor> to all neighbors
//round3:
//- if not the same color, delete the edge

struct MinGDecomValue_scc
{
	int color;
	int sccTag;
	VertexID minForward;
	VertexID minBackward;
	//color=-1, means trivial SCC
	//color=-2, means not assigned
	//otherwise, color>=0
	vector<VertexID> in_edges;
	vector<VertexID> out_edges;
};

ibinstream & operator<<(ibinstream & m, const MinGDecomValue_scc & v){
	m<<v.color;
	m<<v.sccTag;
	m<<v.minForward;
	m<<v.minBackward;
	m<<v.in_edges;
	m<<v.out_edges;
	return m;
}

obinstream & operator>>(obinstream & m, MinGDecomValue_scc & v){
	m>>v.color;
	m>>v.sccTag;
	m>>v.minForward;
	m>>v.minBackward;
	m>>v.in_edges;
	m>>v.out_edges;
	return m;
}

//====================================

struct MinGDAggValue_scc
{
	int nxtColor;
	int max;
	hash_map<intpair, int> cntMap;
	hash_map<intpair, int> colorMap;
};

ibinstream & operator<<(ibinstream & m, const MinGDAggValue_scc & v){
	m<<v.nxtColor;
	m<<v.max;
	m<<v.cntMap;
	m<<v.colorMap;
	return m;
}

obinstream & operator>>(obinstream & m, MinGDAggValue_scc & v){
	m>>v.nxtColor;
	m>>v.max;
	m>>v.cntMap;
	m>>v.colorMap;
	return m;
}

class MinGDecomVertex_scc:public Vertex<VertexID, MinGDecomValue_scc, intpair>
{
	public:
		void bcast_to_in_nbs(intpair msg)
		{
			vector<VertexID> & nbs=value().in_edges;
			for(int i=0; i<nbs.size(); i++)
			{
				send_message(nbs[i], msg);
			}
		}

		void bcast_to_out_nbs(intpair msg)
		{
			vector<VertexID> & nbs=value().out_edges;
			for(int i=0; i<nbs.size(); i++)
			{
				send_message(nbs[i], msg);
			}
		}

		void bcast_to_all_nbs(intpair msg)
		{
			bcast_to_in_nbs(msg);
			bcast_to_out_nbs(msg);
		}

		virtual void compute(MessageContainer & messages)
		{
			if(step_num() == 1)
			{
				if(id!=-1)
				{//not ctrl
					if(value().sccTag!=0) vote_to_halt();
				}
			}
			else if(step_num() == 2)
			{
				if(id==-1)
				{
					MinGDAggValue_scc* agg=(MinGDAggValue_scc*)getAgg();
					value().color=agg->nxtColor;
				}
				else if(value().sccTag==0)
				{
					MinGDAggValue_scc* agg=(MinGDAggValue_scc*)getAgg();
					intpair pair(value().minForward, value().minBackward);
					if(pair.v1==pair.v2) value().sccTag=1;//set sccTag
					else
					{
						int cnt=agg->cntMap[pair];
						if(cnt<=SCC_THRESHOLD) value().sccTag=-1;//set sccTag
					}
					int newColor=agg->colorMap[pair];
					value().color=newColor;
					intpair msg(id, newColor);
					bcast_to_all_nbs(msg);
				}
			}
			else
			{
				hash_map<int, int> map;
				for(int i=0; i<messages.size(); i++)
				{
					intpair & message=messages[i];
					map[message.v1]=message.v2;
				}
				vector<VertexID> & in_edges=value().in_edges;
				vector<VertexID> in_new;
				for(int i=0; i<in_edges.size(); i++)
				{
					int nbColor=map[in_edges[i]];
					if(nbColor==value().color) in_new.push_back(in_edges[i]);
				}
				in_edges.swap(in_new);
				vector<VertexID> & out_edges=value().out_edges;
				vector<VertexID> out_new;
				for(int i=0; i<out_edges.size(); i++)
				{
					int nbColor=map[out_edges[i]];
					if(nbColor==value().color) out_new.push_back(out_edges[i]);
				}
				out_edges.swap(out_new);
				vote_to_halt();
			}
		}
};

//====================================

class MinGDAgg_scc:public Aggregator<MinGDecomVertex_scc, MinGDAggValue_scc, MinGDAggValue_scc>
{
	private:
		MinGDAggValue_scc state;
	public:
		virtual void init(){
			state.nxtColor=-1;
			state.cntMap.clear();
			state.colorMap.clear();
		}

		virtual void stepPartial(MinGDecomVertex_scc* v)
		{
			MinGDecomValue_scc & val=v->value();
			if(step_num()==1)
			{
				if(v->id==-1)
				{
					state.nxtColor=val.color;
				}
				else if(val.sccTag==0)
				{
					intpair pair(val.minForward, val.minBackward);
					hash_map<intpair, int>::iterator it=state.cntMap.find(pair);
					if(it==state.cntMap.end())
					{
						state.cntMap[pair]=1;
					}
					else
					{
						it->second++;
					}
				}
			}
			else if(step_num()==3)
			{
				if(v->id==-1)
				{
					state.nxtColor=val.color;
				}
			}
		}

		virtual void stepFinal(MinGDAggValue_scc* part)
		{
			if(step_num()==1)
			{
				if(part->nxtColor!=-1) state.nxtColor=part->nxtColor;
				for(hash_map<intpair, int>::iterator it=part->cntMap.begin(); it!=part->cntMap.end(); it++)
				{
					intpair key=it->first;
					int cnt=it->second;
					hash_map<intpair, int>::iterator it1=state.cntMap.find(key);
					if(it1!=state.cntMap.end())
					{
						int myCnt=it1->second;
						state.cntMap[key]=cnt+myCnt;
					}
					else
					{
						state.cntMap[key]=cnt;
					}
				}
			}
			else if(step_num()==3)
			{
				if(part->nxtColor!=-1) state.nxtColor=part->nxtColor;
			}
		}

		virtual MinGDAggValue_scc* finishPartial(){ return &state; }
		virtual MinGDAggValue_scc* finishFinal(){
			if(step_num()==1)
			{
				int max=-1;
				int nxtColor=state.nxtColor;
				for(hash_map<intpair, int>::iterator it=state.cntMap.begin(); it!=state.cntMap.end(); it++)
				{
					intpair key=it->first;
					int cnt=it->second;
					if(key.v1!=key.v2 && cnt>SCC_THRESHOLD && cnt>max) max=cnt;//key.K1!=key.K2 ===> do not count SCC
					state.colorMap[key]=nxtColor;
					nxtColor++;
				}
				cout<<"%%%%%%%%%% Max Subgraph Size = "<<max<<endl;
				state.nxtColor=nxtColor;
			}
			return &state;
		}
};

//====================================

class MinGDecomWorker_scc:public Worker<MinGDecomVertex_scc, MinGDAgg_scc>
{
	char buf[100];

	public:
		//C version
		virtual MinGDecomVertex_scc* toVertex(char* line)
		{
			char * pch;
			pch=strtok(line, "\t");
			MinGDecomVertex_scc* v=new MinGDecomVertex_scc;
			v->id=atoi(pch);
			pch=strtok(NULL, " ");
			v->value().color=atoi(pch);
			if(v->id==-1) return v;
			pch=strtok(NULL, " ");
			v->value().sccTag=atoi(pch);
			pch=strtok(NULL, " ");
			v->value().minForward=atoi(pch);
			pch=strtok(NULL, " ");
			v->value().minBackward=atoi(pch);
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

		virtual void toline(MinGDecomVertex_scc* v, BufferedWriter & writer)
		{
			if(v->id==-1)
			{
				sprintf(buf, "-1\t%d\n", v->value().color);
				writer.write(buf);
				return;
			}
			vector<VertexID> & in_edges=v->value().in_edges;
			vector<VertexID> & out_edges=v->value().out_edges;
			sprintf(buf, "%d\t%d %d %d ", v->id, v->value().color, v->value().sccTag, in_edges.size());
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

void scc_minGDecom(string in_path, string out_path)
{
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=false;
	MinGDecomWorker_scc worker;
	MinGDAgg_scc agg;
	worker.setAggregator(&agg);
	worker.run(param);
}
