#include "basic/pregel-dev.h"
#include "utils/type.h"
using namespace std;

//input 1 (from minmax):
//color pre(v) \t nd(v) min(v) max(v) pre(nb1) tag(nb1) pre(nb2) tag(nb2)...

//input 2 (from triphashmin):
//(<color, pre1, pre2>) \t newColor_pair (dst1=<pre1, pre2>)_(dst2=<pre1, pre2>)_...

//output format:
//- <color, pre1, pre2> \t newColor_pair

//<V> = <nd(v)_min(v)_max(v)_pa(v)>, or <newColor_pair_X_X>
struct C1Value_ppa
{
	int nd;//newColor_pair.v1
	int min;//newColor_pair.v2
	int max;
	int pa;
	vector<intpair> edges;
};

ibinstream & operator<<(ibinstream & m, const C1Value_ppa & v){
	m<<v.nd;
	m<<v.min;
	m<<v.max;
	m<<v.pa;
	m<<v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, C1Value_ppa & v){
	m>>v.nd;
	m>>v.min;
	m>>v.max;
	m>>v.pa;
	m>>v.edges;
	return m;
}

//====================================
class C1Vertex_ppa:public Vertex<inttriplet, C1Value_ppa, inttriplet, IntTripletHash>
{
	public:
		virtual void compute(MessageContainer & messages)
		{
			if(step_num()==1)
			{
				if(id.v3==-1)
				{//input 1
					//for each old vertex u, construct new non-tree-edge vertex (v, u), (v<u)
					int color=id.v1;
					int me=id.v2;
					int pa=value().pa;
					vector<intpair> & edges=value().edges;
					for(int i=0; i<edges.size(); i++)
					{
						intpair & edge=edges[i];
						if(edge.v2==0)//non-tree-edge
						{
							int dst=edge.v1;
							if(me>dst)
							{
								//step 1: u sends "v" to (p(u), u) requesting for its newColor
								inttriplet tmp(color, pa, me);
								inttriplet msg(dst, -1, -1);
								send_message(tmp, msg);
								//step 2: (p(u), u) sends <newColor, v> back to u
								//step 3: u adds vertex <color, v, u> with newColor
							}
						}
					}
				}
			}
			else if(step_num()==2)
			{
				int color=id.v1;
				int pu=id.v2;
				int u=id.v3;
				for(int i=0; i<messages.size(); i++)
				{
					inttriplet & message=messages[i];
					inttriplet tmp(color, u, -1);
					inttriplet msg(value().nd, value().min, message.v1);//here value().nd stores new color
					send_message(tmp, msg);
				}
			}
			else if(step_num()==3)
			{
				int color=id.v1;
				int u=id.v2;
				for(int i=0; i<messages.size(); i++)
				{
					inttriplet & message=messages[i];
					int v=message.v3;
					C1Vertex_ppa * vertex=new C1Vertex_ppa();
					vertex->id.v1=color;
					vertex->id.v2=v;
					vertex->id.v3=u;
					vertex->value().nd=message.v1;
					vertex->value().min=message.v2;
					add_vertex(vertex);
				}
			}
			vote_to_halt();
		}
};

class C1Worker_ppa:public Worker<C1Vertex_ppa>
{
	char buf[100];

	public:

		virtual C1Vertex_ppa* toVertex(char* line)
		{
			char * pch;
			char * outer_ptr=NULL;
			char * inner_ptr=NULL;
			pch=strtok_r(line, "\t", &outer_ptr);//get key part before /t
			pch=strtok_r(pch, " ", &inner_ptr);
			int color=atoi(pch);
			pch=strtok_r(NULL, " ", &inner_ptr);
			int pre1=atoi(pch);
			pch=strtok_r(NULL, " ", &inner_ptr);
			if(pch)
			{//input 2
				int pre2=atoi(pch);
				inttriplet key(color, pre1, pre2);
				C1Vertex_ppa * v=new C1Vertex_ppa;
				v->id=key;
				pch=strtok_r(NULL, "\t", &outer_ptr);//get value part after /t
				pch=strtok_r(pch, " ", &inner_ptr);
				v->value().nd=atoi(pch);//for this kind, nd is used for recording newColor
				pch=strtok_r(NULL, " ", &inner_ptr);
				v->value().min=atoi(pch);//for this kind, nd is used for recording newColor
				while(pch=strtok_r(NULL, " ", &inner_ptr))
				{
					intpair edge;
					edge.v1=atoi(pch);
					pch=strtok_r(NULL, " ", &inner_ptr);
					edge.v2=atoi(pch);
					v->value().edges.push_back(edge);
				}
				return v;
			}
			else
			{//input 1
				inttriplet key(color, pre1, -1);
				C1Vertex_ppa * v=new C1Vertex_ppa;
				v->id=key;
				pch=strtok_r(NULL, "\t", &outer_ptr);//get value part after /t
				pch=strtok_r(pch, " ", &inner_ptr);
				v->value().nd=atoi(pch);
				pch=strtok_r(NULL, " ", &inner_ptr);
				v->value().min=atoi(pch);
				pch=strtok_r(NULL, " ", &inner_ptr);
				v->value().max=atoi(pch);
				while(pch=strtok_r(NULL, " ", &inner_ptr))
				{
					intpair edge;
					edge.v1=atoi(pch);
					pch=strtok_r(NULL, " ", &inner_ptr);
					edge.v2=atoi(pch);
					v->value().edges.push_back(edge);
					if(edge.v2==1 && edge.v1<v->id.v2) v->value().pa=edge.v1;
				}
				return v;
			}
		}

		virtual void toline(C1Vertex_ppa* v, BufferedWriter & writer)
		{
			if(v->id.v3!=-1)
			{
				sprintf(buf, "%d %d %d\t %d %d\n", v->id.v1, v->id.v2, v->id.v3, v->value().nd, v->value().min);
				writer.write(buf);
			}
		}
};

void ppa_case1(MultiInputParams & param)
{
	C1Worker_ppa worker;
	worker.run(param);
}
