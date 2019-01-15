#include "reqresp/req-dev.h"
#include "utils/type.h"
using namespace std;

//input line format: vertexID \t numOfNeighbors neighbor1 neighbor2 ...
//output line format: v field(v) \t neighbor1 field(neighbor1) neighbor2 field(neighbor2) ...

bool DIRECTED = true;

struct FieldValue_req
{
	int field;
	vector<intpair> edges;
};

ibinstream & operator<<(ibinstream & m, const FieldValue_req & v){
	m<<v.field;
	m<<v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, FieldValue_req & v){
	m>>v.field;
	m>>v.edges;
	return m;
}

//====================================

class FieldVertex_req: public RVertex<VertexID, FieldValue_req, char, int> {
	public:
		virtual int respond()
		{
			return value().field;
		}

		virtual void compute(MessageContainer & messages) {
			vector<intpair> & edges=value().edges;
			if (step_num() == 1) {
				if (DIRECTED) for(int i=0; i<edges.size(); i++) request(edges[i].v1);
				else for(int i=0; i<edges.size(); i++) exp_respond(edges[i].v1);
			}
			else
			{
				for(int i=0; i<edges.size(); i++) edges[i].v2=get_respond(edges[i].v1);
				vote_to_halt();
			}
		}
};

class FieldWorker_req: public RWorker<FieldVertex_req> {
	char buf[100];
	public:

		virtual FieldVertex_req * toVertex(char* line) {
			char * pch;
			pch = strtok(line, "\t");
			FieldVertex_req * v = new FieldVertex_req;
			v->id = atoi(pch);
			v->value().field = v->id; //set field of v as id(v)
			pch = strtok(NULL, " ");
			int num = atoi(pch);
			vector<intpair> & edges = v->value().edges;
			for (int i = 0; i < num; i++) {
				pch = strtok(NULL, " ");
				int vid = atoi(pch);
				edges.push_back(intpair(vid, -1));
			}
			return v;
		}

		virtual void toline(FieldVertex_req * v, BufferedWriter & writer) {
			sprintf(buf, "%d %d\t", v->id, v->value().field);
			writer.write(buf);
			vector<intpair> & edges=v->value().edges;
			for(int i=0; i<edges.size(); i++)
			{
				sprintf(buf, "%d %d ", edges[i].v1, edges[i].v2);
				writer.write(buf);
			}
			writer.write("\n");
		}
};

void req_fieldbcast(string in_path, string out_path, bool directed = true) {
	DIRECTED = directed;
	WorkerParams param;
	param.input_path = in_path;
	param.output_path = out_path;
	param.force_write = true;
	param.native_dispatcher = false;
	FieldWorker_req worker;
	worker.run(param);
}
