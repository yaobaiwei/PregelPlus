#include "basic/pregel-dev.h"
#include "utils/type.h"
#include <set>
using namespace std;

//<V> = <color, pre(v), nd(v), parent, adj-list>
//<E> = <vertexID/preorder_number, tag(tree/non-tree)>
struct MergeEdge_ppa
{
	int no;
	bool is_tree;
};

ibinstream & operator<<(ibinstream & m, const MergeEdge_ppa & v){
	m<<v.no;
	m<<v.is_tree;
	return m;
}

obinstream & operator>>(obinstream & m, MergeEdge_ppa & v){
	m>>v.no;
	m>>v.is_tree;
	return m;
}

//-----------------------------------------

struct MergeValue_ppa
{
	int color;
	int pre;
	int nd;
	int parent;
	vector<MergeEdge_ppa> edges;
};

ibinstream & operator<<(ibinstream & m, const MergeValue_ppa & v){
	m<<v.color;
	m<<v.pre;
	m<<v.nd;
	m<<v.parent;
	m<<v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, MergeValue_ppa & v){
	m>>v.color;
	m>>v.pre;
	m>>v.nd;
	m>>v.parent;
	m>>v.edges;
	return m;
}

//====================================
class MergeVertex_ppa:public Vertex<intpair, MergeValue_ppa, intpair, IntPairHash>
{
	public:
		virtual void compute(MessageContainer & messages)
		{
			if(step_num()==1)
			{
				if(id.v2==1)//Type 1 vertex sends info to Type 3 vertex
				{
					intpair tgt(id.v1, 3);
					intpair msg(value().pre, value().nd);
					send_message(tgt, msg);
					vote_to_halt();
				}
				else if(id.v2==2)//Type 2 vertex sends info to Type 3 vertex
				{
					intpair tgt(id.v1, 3);
					intpair msg(-value().color-1, value().parent);
					send_message(tgt, msg);
					vote_to_halt();
				}
			}
			else if(step_num()==2)
			{
				//============= recv msgs =============
				for(int i=0; i<messages.size(); i++)//exactly two
				{
					intpair & msg=messages[i];
					if(msg.v1<0)//Type 3 vertex recvs info from Type 2 vertex
					{
						value().color=-msg.v1-1;
						value().parent=msg.v2;
					}
					else//Type 3 vertex recvs info from Type 1 vertex
					{
						value().pre=msg.v1;
						value().nd=msg.v2;
					}
				}
				//============= notify parent about tree-edge =============
				int parent=value().parent;
				if(parent!=-1)
				{
					intpair tgt(value().parent, 3);
					intpair msg(id.v1, 0);//msg = (me, -)
					send_message(tgt, msg);
				}
			}
			else if(step_num()==3)
			{
				set<int> tree_nbs;
				int parent=value().parent;
				if(parent!=-1) tree_nbs.insert(parent);
				for(int i=0; i<messages.size(); i++) tree_nbs.insert(messages[i].v1);
				vector<MergeEdge_ppa> & edges=value().edges;
				for(int i=0; i<edges.size(); i++)
				{
					if(tree_nbs.find(edges[i].no)!=tree_nbs.end()) edges[i].is_tree=true;//set tree-edges
					//notify nbs with pre(me)
					intpair tgt(edges[i].no, 3);
					intpair msg(id.v1, value().pre);//msg = (me, pre(me))
					send_message(tgt, msg);
				}
			}
			else
			{
				//replace nb with pre(nb)
				hash_map<int, int> lut;//lut[nb]=pre(nb)
				for(int i=0; i<messages.size(); i++)
				{
					intpair & msg=messages[i];
					lut[msg.v1]=msg.v2;
				}
				vector<MergeEdge_ppa> & edges=value().edges;
				for(int i=0; i<edges.size(); i++)
				{
					edges[i].no = lut[edges[i].no];
				}
				vote_to_halt();
			}
		}
};

class MergeWorker_ppa:public Worker<MergeVertex_ppa>
{
	char buf[100];

	public:
		//C version
		virtual MergeVertex_ppa* toVertex(char* line)
		{
			MergeVertex_ppa* v=new MergeVertex_ppa;
			char * pch;
			int len=strlen(line);
			char c=line[len-1];
			if(c=='$')
			{
				pch=strtok(line, "\t");
				v->id.v1=atoi(pch);
				v->id.v2=1;//denote that pre(v) are nd(v) available
				pch=strtok(NULL, " ");
				v->value().pre=atoi(pch);
				pch=strtok(NULL, " ");
				v->value().nd=atoi(pch);
			}
			else if(c=='#')
			{
				pch=strtok(line, "\t");
				v->id.v1=atoi(pch);
				v->id.v2=2;//denote that color and parent available
				pch=strtok(NULL, " ");
				v->value().color=atoi(pch);
				pch=strtok(NULL, " ");
				v->value().parent=atoi(pch);
			}
			else
			{
				pch=strtok(line, "\t");
				v->id.v1=atoi(pch);
				v->id.v2=3;//denote that edges are available
				//===========================
				//the following fields are set as if v is isolated
				//if it doesn't get fields from TYPE1/TYPE2 vertices, the default fields are dumped
				v->value().pre=0;
				v->value().nd=1;
				v->value().color=v->id.v1;
				v->value().parent=-1;
				//===========================
				pch=strtok(NULL, " ");
				int num=atoi(pch);
				for(int i=0; i<num; i++)
				{
					pch=strtok(NULL, " ");
					MergeEdge_ppa edge;
					edge.no=atoi(pch);
					edge.is_tree=false;
					v->value().edges.push_back(edge);
				}
			}
			return v;
		}

		virtual void toline(MergeVertex_ppa* v, BufferedWriter & writer)
		{//color pre(v) \t nd(v) pre(nb1) tag(nb1) pre(nb2) tag(nb2)...
			if(v->id.v2==3)
			{
				sprintf(buf, "%d %d\t%d ", v->value().color, v->value().pre, v->value().nd);
				writer.write(buf);
				vector<MergeEdge_ppa> & edges=v->value().edges;
				for(int i=0; i<edges.size(); i++)
				{
					sprintf(buf, "%d %d ", edges[i].no, edges[i].is_tree);
					writer.write(buf);
				}
				writer.write("\n");
			}
		}
};

void ppa_merge(MultiInputParams & param)
{
	MergeWorker_ppa worker;
	worker.run(param);
}
