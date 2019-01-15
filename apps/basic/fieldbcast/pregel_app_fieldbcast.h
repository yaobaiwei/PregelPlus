#include "basic/pregel-dev.h"
#include "utils/type.h"

using namespace std;

//input line format: vertexID \t numOfNeighbors neighbor1 neighbor2 ...
//output line format: v field(v) \t neighbor1 field(neighbor1) neighbor2 field(neighbor2) ...

bool DIRECTED = true;

struct FieldValue_pregel
{
	int field;
	vector<intpair> edges;
};

ibinstream & operator<<(ibinstream & m, const FieldValue_pregel & v)
{
	m << v.field;
	m << v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, FieldValue_pregel & v)
{
	m >> v.field;
	m >> v.edges;
	return m;
}

//====================================

class FieldVertex_pregel: public Vertex<VertexID, FieldValue_pregel, intpair>
{
	public:
		virtual void compute(MessageContainer & messages)
		{
			if (DIRECTED)
			{

				if (step_num() == 1)
				{
					// request
					vector<intpair>& edges = value().edges;
					for (int i = 0; i < edges.size(); i++)
					{
						send_message(edges[i].v1, intpair(id, -1));
					}
					vote_to_halt();
				}
				else if (step_num() == 2)
				{
					// respond
					for (int i = 0; i < messages.size(); i++)
					{
						send_message(messages[i].v1, intpair(id, value().field));
					}
					vote_to_halt();
				}
				else
				{
					vector<intpair>& edges = value().edges;
					edges.clear();
					for (int i = 0; i < messages.size(); i++)
					{
						edges.push_back(messages[i]);
					}
					vote_to_halt();
				}
			}
			else
			{
				if (step_num() == 1)
				{
					vector<intpair>& edges = value().edges;
					// respond
					for (int i = 0; i < edges.size(); i++)
					{
						send_message(edges[i].v1, intpair(id, value().field));
					}
					vote_to_halt();
				}
				else
				{
					vector<intpair>& edges = value().edges;
					edges.clear();
					for (int i = 0; i < messages.size(); i++)
					{
						edges.push_back(messages[i]);
					}
					vote_to_halt();
				}
			}
		}
};

class FieldWorker_pregel: public Worker<FieldVertex_pregel>
{
	char buf[100];

public:
	//C version
	virtual FieldVertex_pregel* toVertex(char* line)
	{
		char * pch;
		pch = strtok(line, "\t");
		FieldVertex_pregel* v = new FieldVertex_pregel;
		v->id = atoi(pch);
		v->value().field = v->id; //set field of v as id(v)
		pch = strtok(NULL, " ");
		int num = atoi(pch);
		for (int i = 0; i < num; i++)
		{
			pch = strtok(NULL, " ");
			int vid = atoi(pch);
			v->value().edges.push_back(intpair(vid, -1));
		}
		return v;
	}

	virtual void toline(FieldVertex_pregel* v, BufferedWriter & writer)
	{
		sprintf(buf, "%d %d\t", v->id, v->value().field);
		writer.write(buf);
		vector<intpair> & edges = v->value().edges;
		for (int i = 0; i < edges.size(); i++)
		{
			sprintf(buf, "%d %d ", edges[i].v1, edges[i].v2);
			writer.write(buf);
		}
		writer.write("\n");
	}
};

void pregel_fieldbcast(string in_path, string out_path, bool directed = true)
{

	DIRECTED = directed;
	WorkerParams param;
	param.input_path = in_path;
	param.output_path = out_path;
	param.force_write = true;
	param.native_dispatcher = false;
	FieldWorker_pregel worker;
	worker.run(param);
}

