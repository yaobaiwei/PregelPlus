#include "basic/pregel-dev.h"
#include "utils/type.h"
#include <set>
using namespace std;

//only consider tree-edge vertices

//input file
//- color pre(v) \t nd(v)_min(v)_max(v)_pre(dst1)_edgeType_pre(dst2)_edgeType... (from minmax)

//output format:
//- (new vertex=<color, pre1, pre2>) \t (dst1=<pre1, pre2>)_(dst2=<pre1, pre2>)_...

//<V> = <nd(v)_min(v)_max(v)_pa(v)>
struct AuxGValue_ppa
{
	int nd;
	int min;
	int max;
	int pa;
	vector<intpair> edges;

	AuxGValue_ppa()
	{
		nd=-1;
		min=-1;
		max=-1;
		pa=-1;
	}
};

ibinstream & operator<<(ibinstream & m, const AuxGValue_ppa & v){
	m<<v.nd;
	m<<v.min;
	m<<v.max;
	m<<v.pa;
	m<<v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, AuxGValue_ppa & v){
	m>>v.nd;
	m>>v.min;
	m>>v.max;
	m>>v.pa;
	m>>v.edges;
	return m;
}

//====================================
int ADDEDGE=0;
int CASE2=1;
int CASE3_REQ=2;
int CASE3_RESP=3;

class AuxGVertex_ppa:public Vertex<inttriplet, AuxGValue_ppa, inttriplet, IntTripletHash>
{
	public:
		virtual void compute(MessageContainer & messages)
		{
			if(step_num()==1)
			{
				//round 1: for each vertex u, construct new tree-edge vertex (p(u), u)
				if(value().pa!=-1)
				{
					//add new vertex
					inttriplet newId(id.v1, value().pa, id.v2);
					AuxGVertex_ppa * newV=new AuxGVertex_ppa();
					newV->id=newId;
					add_vertex(newV);
					/////////////
					inttriplet pa(id.v1, value().pa, -1);
					inttriplet requester(CASE3_REQ, id.v2, -1);
					send_message(pa, requester);
					return;//cannot halt
				}
			}
			else if(step_num()==2)
			{
				//round 2: for each old vertex u with p(u)!=-1
				//--------------------------------------------
				//* for each non-tree-edge (u, v) (v>u), check case 2
				//if v>=u+nd(u), u sends (p(u), u) to v,
				//v (that has p(v)) will then add edge ((p(u), u), (p(v), v))
				if(id.v3==-1 && value().pa!=-1)
				{
					vector<intpair> & edges=value().edges;
					int me=id.v2;
					for(int i=0; i<edges.size(); i++)
					{
						int dst=edges[i].v1;
						if(me<dst)
						{
							if(!edges[i].v2)//!is-tree
							{//case 2
								if(dst>=me+value().nd)
								{
									inttriplet v(id.v1, dst, -1);
									inttriplet uEdge(CASE2, value().pa, me);//msg type 1: <TYPE1, p(u), u>
									send_message(v, uEdge);
								}
							}
						}
					}
					//-------------------------------------------
					for(int i=0; i<messages.size(); i++)
					{
						int sender=messages[i].v2;
						inttriplet requester(id.v1, sender, -1);
						inttriplet response(CASE3_RESP, value().nd, value().pa);
						send_message(requester, response);
					}
				}
			}
			else if(step_num()==3)
			{
				if(id.v3==-1 && value().pa!=-1)
				{
					for(int i=0; i<messages.size(); i++)
					{
						inttriplet & msg=messages[i];
						if(msg.v1==CASE2)
						{
							int pu=msg.v2;
							int u=msg.v3;
							//pv=pa, v=me
							/////////////
							//add edge ((p(u), u), (p(v), v))
							inttriplet tmp(id.v1, pu, u);
							inttriplet msg(ADDEDGE, value().pa, id.v2);
							send_message(tmp, msg);
							/////////////
							//add edge ((p(v), v), (p(u), u))
							tmp.set(id.v1, value().pa, id.v2);
							msg.set(ADDEDGE, pu, u);
							send_message(tmp, msg);
						}
						else
						{//CASE3_RESP
							int ndu=msg.v2;
							int ppu=msg.v3;
							if(value().min<value().pa || value().max>=value().pa+ndu)
							{
								//add edge ((p(u), u), (p(p(u)), p(u)))
								inttriplet tmp(id.v1, value().pa, id.v2);
								inttriplet msg(ADDEDGE, ppu, value().pa);
								send_message(tmp, msg);
								/////////////
								//add edge ((p(p(u)), p(u), (p(u), u))
								tmp.set(id.v1, ppu, value().pa);
								msg.set(ADDEDGE, value().pa, id.v2);
								send_message(tmp, msg);
							}
						}
					}
				}
			}
			else
			{
				set<intpair> set;
				for(int i=0; i<messages.size(); i++)
				{
					inttriplet & msg=messages[i];
					intpair key(msg.v2, msg.v3);
					set.insert(key);
				}
				for(std::set<intpair>::iterator it=set.begin(); it!=set.end(); it++)
				{
					value().edges.push_back(*it);
				}
			}
			vote_to_halt();
		}
};

class AuxGWorker_ppa:public Worker<AuxGVertex_ppa>
{
	char buf[100];

	public:

		virtual AuxGVertex_ppa* toVertex(char* line)
		{
			char * pch;
			pch=strtok(line, " ");
			AuxGVertex_ppa* v=new AuxGVertex_ppa;
			v->id.v1=atoi(pch);
			pch=strtok(NULL, "\t");
			v->id.v2=atoi(pch);
			v->id.v3=-1;
			pch=strtok(NULL, " ");
			v->value().nd=atoi(pch);
			pch=strtok(NULL, " ");
			v->value().min=atoi(pch);
			pch=strtok(NULL, " ");
			v->value().max=atoi(pch);
			while(pch=strtok(NULL, " "))
			{
				intpair edge;
				edge.v1=atoi(pch);
				pch=strtok(NULL, " ");
				edge.v2=atoi(pch);
				v->value().edges.push_back(edge);
				if(edge.v2==1 && edge.v1<v->id.v2) v->value().pa=edge.v1;
			}
			return v;
		}

		virtual void toline(AuxGVertex_ppa* v, BufferedWriter & writer)
		{
			if(v->id.v3!=-1)
			{
				sprintf(buf, "%d %d %d\t", v->id.v1, v->id.v2, v->id.v3);
				writer.write(buf);
				vector<intpair> & edges=v->value().edges;
				for(int i=0; i<edges.size(); i++)
				{
					sprintf(buf, "%d %d ", edges[i].v1, edges[i].v2);
					writer.write(buf);
				}
				writer.write("\n");
			}
		}
};

void ppa_auxgraph(string in_path, string out_path)
{
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=true;
	AuxGWorker_ppa worker;
	worker.run(param);
}
