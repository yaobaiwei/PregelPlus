#include "basic/pregel-dev.h"
#include "utils/type.h"
using namespace std;

//input line format:
//(<color, pre1, pre2>) \t (dst1=<pre1, pre2>)_(dst2=<pre1, pre2>)_...

//output line format:
//(<color, pre1, pre2>) \t newColor_pair (dst1=<pre1, pre2>)_(dst2=<pre1, pre2>)_...

//true color is decided by triplet=(color, newColor_pair)

struct TCCValue_ppa
{
	intpair color;
	vector<intpair> edges;
};

ibinstream & operator<<(ibinstream & m, const TCCValue_ppa & v){
	m<<v.color;
	m<<v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, TCCValue_ppa & v){
	m>>v.color;
	m>>v.edges;
	return m;
}

//====================================

class TCCVertex_ppa:public Vertex<inttriplet, TCCValue_ppa, intpair, IntTripletHash>
{
	public:
		void broadcast(intpair msg)
		{
			vector<intpair> & nbs=value().edges;
			for(int i=0; i<nbs.size(); i++)
			{
				inttriplet dst(id.v1, nbs[i].v1, nbs[i].v2);
				send_message(dst, msg);
			}
		}

		virtual void compute(MessageContainer & messages)
		{
			if(step_num()==1)
			{
				intpair min(id.v2, id.v3);
				vector<intpair> & nbs=value().edges;
				for(int i=0; i<nbs.size(); i++)
				{
					if(nbs[i]<min) min=nbs[i];
				}
				value().color=min;
				broadcast(min);
				vote_to_halt();
			}
			else
			{
				intpair min=messages[0];
				for(int i=1; i<messages.size(); i++)
				{
					if(messages[i]<min) min=messages[i];
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

class TCCWorker_ppa:public Worker<TCCVertex_ppa>
{
	char buf[100];

	public:
		//C version
		virtual TCCVertex_ppa* toVertex(char* line)
		{
			char * pch;
			pch=strtok(line, " ");
			TCCVertex_ppa* v=new TCCVertex_ppa;
			v->id.v1=atoi(pch);
			pch=strtok(NULL, " ");
			v->id.v2=atoi(pch);
			pch=strtok(NULL, "\t");
			v->id.v3=atoi(pch);
			while(pch=strtok(NULL, " "))
			{
				intpair edge;
				edge.v1=atoi(pch);
				pch=strtok(NULL, " ");
				edge.v2=atoi(pch);
				v->value().edges.push_back(edge);
			}
			return v;
		}

		virtual void toline(TCCVertex_ppa* v, BufferedWriter & writer)
		{
			sprintf(buf, "%d %d %d\t%d %d ", v->id.v1, v->id.v2, v->id.v3, v->value().color.v1, v->value().color.v2);
			writer.write(buf);
			vector<intpair> & nbs=v->value().edges;
			for(int i=0; i<nbs.size(); i++)
			{
				sprintf(buf, "%d %d ", nbs[i].v1, nbs[i].v2);
				writer.write(buf);
			}
			writer.write("\n");
		}
};

class TCCCombiner_ppa:public Combiner<intpair>
{
	public:
		virtual void combine(intpair & old, const intpair & new_msg)
		{
			if(new_msg<old) old=new_msg;
		}
};

void ppa_triphashmin(string in_path, string out_path, bool use_combiner)
{
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=false;
	TCCWorker_ppa worker;
	TCCCombiner_ppa combiner;
	if(use_combiner) worker.setCombiner(&combiner);
	worker.run(param);
}
