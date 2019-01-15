#include "basic/pregel-dev.h"
#include "utils/type.h"
#include <set>
using namespace std;

//<V> = <color, parent, vec<E>>
//<E> for vertex u = <v, w>, indicating that <u, v>'s next edge is <v, w>
struct ETourValue_ppa
{
	int color;
	int parent;
	vector<intpair> edges;
};

ibinstream & operator<<(ibinstream & m, const ETourValue_ppa & v){
	m<<v.color;
	m<<v.parent;
	m<<v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, ETourValue_ppa & v){
	m>>v.color;
	m>>v.parent;
	m>>v.edges;
	return m;
}

//====================================
class ETourVertex_ppa:public Vertex<VertexID, ETourValue_ppa, intpair>
{
	public:
		virtual void compute(MessageContainer & messages)
		{
			if(step_num()==1)
			{
				int parent=value().parent;
				//send (me, -) to parent
				if(parent!=-1) send_message(parent, intpair(id, 0));
			}
			else if(step_num()==2)
			{
				vector<int> adj_list;
				int parent=value().parent;
				if(parent!=-1) adj_list.push_back(parent);
				//collect msgs (child, -) to adj-list
				for(int i=0; i<messages.size(); i++) adj_list.push_back(messages[i].v1);
				//sort adj-list
				sort(adj_list.begin(), adj_list.end());
				//notify neighbors about next-edge info
				//- me is v
				//- send (v, w) to each neighbor u
				if(adj_list.size()>0)
				{
					for(int i=0; i<adj_list.size()-1; i++)
					{
						//u=adj_list[i]
						//v=me
						//w=adj_list[i+1]
						send_message(adj_list[i], intpair(id, adj_list[i+1]));
					}
					//u=adj_list[adj_list.size()-1]
					//v=me
					//w=adj_list[0]
					send_message(adj_list[adj_list.size()-1], intpair(id, adj_list[0]));
				}
			}
			else
			{
				vector<intpair> & edges=value().edges;
				for(int i=0; i<messages.size(); i++)
				{
					edges.push_back(messages[i]);
				}
				vote_to_halt();
			}
		}
};

class ETourWorker_ppa:public Worker<ETourVertex_ppa>
{
	char buf[100];

	public:

		virtual ETourVertex_ppa* toVertex(char* line)
		{
			char * pch;
			pch=strtok(line, "\t");
			ETourVertex_ppa* v=new ETourVertex_ppa;
			v->id=atoi(pch);
			pch=strtok(NULL, " ");
			v->value().color=atoi(pch);
			pch=strtok(NULL, " ");
			v->value().parent=atoi(pch);
			return v;
		}

		virtual void toline(ETourVertex_ppa* v, BufferedWriter & writer)
		{//output u v \t v w color
			int color=v->value().color;
			vector<intpair> & edges=v->value().edges;
			for(int i=0; i<edges.size(); i++)
			{
				if(v->id==color && i==edges.size()-1) color=-color-1;
				sprintf(buf, "%d %d\t%d %d %d\n", v->id, edges[i].v1, edges[i].v1, edges[i].v2, color);
				writer.write(buf);
			}
		}
};

void ppa_etour(string in_path, string out_path)
{
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=true;
	ETourWorker_ppa worker;
	worker.run(param);
}
