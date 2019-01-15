#include "ghost/ghost-dev.h"
#include <float.h>
using namespace std;

//input line format: vertexID \t numOfNeighbors neighbor1 neighbor2 ...
//edge lengths are assumed to be 1

//output line format: v \t shortest_path_length  previous_vertex_on_shorest_path
//previous_vertex_on_shorest_path=-1 for source vertex

int src = 0;

//====================================

struct SPMsg_ghost {
	double dist;
	int from;

	SPMsg_ghost(double _dist = 0, int _from = 0) :
			dist(_dist), from(_from) {
	}
};

ibinstream & operator<<(ibinstream & m, const SPMsg_ghost & v) {
	m << v.dist;
	m << v.from;
	return m;
}

obinstream & operator>>(obinstream & m, SPMsg_ghost & v) {
	m >> v.dist;
	m >> v.from;
	return m;
}

struct SPGEdge: public GEdge<VertexID, SPMsg_ghost, double> {

	void relay(MessageType & msg)
	{
		msg.dist += eval;
	}
};

class SPVertex_ghost: public GVertex<VertexID, SPMsg_ghost, SPMsg_ghost, SPGEdge> {
public:
	virtual void compute(MessageContainer & messages) {
		if (step_num() == 1) {
			if (id == src) {
				//value().dist = 0; //done during init
				//value().from = -1;
				broadcast(SPMsg_ghost(value().dist, id));
			} //else {
				//value().dist = DBL_MAX;
				//value().from = -1;
			//}
		} else {
			SPMsg_ghost min;
			min.dist = DBL_MAX;
			for (int i = 0; i < messages.size(); i++) {
				SPMsg_ghost msg = messages[i];
				if (min.dist > msg.dist) {
					min = msg;
				}
			}
			if (min.dist < value().dist) {
				value().dist = min.dist;
				value().from = min.from;
				broadcast(SPMsg_ghost(value().dist, id));
			}
		}
		vote_to_halt();
	}

};

class SPWorker_ghost: public GWorker<SPVertex_ghost> {
	char buf[100];
public:

	virtual SPVertex_ghost* toVertex(char* line) {
		char * pch;
		pch = strtok(line, "\t");
		SPVertex_ghost* v = new SPVertex_ghost;
		int id = atoi(pch);
		v->id = id;
		v->value().from = -1;
		if (id == src)
			v->value().dist = 0;
		else {
			v->value().dist = DBL_MAX;
			v->vote_to_halt();
		}
		EdgeContainer & edges = v->neighbors();
		while(pch = strtok(NULL, " "))
		{
			EdgeT edge;
			edge.id = atoi(pch);
			edge.eval = 1;
			edges.push_back(edge);
		}
		return v;
	}

	virtual void toline(SPVertex_ghost* v, BufferedWriter & writer) {
		if (v->value().dist != DBL_MAX) sprintf(buf, "%d\t%f %d\n", v->id, v->value().dist, v->value().from);
		else sprintf(buf, "%d\tunreachable\n", v->id);
		writer.write(buf);
	}
};

class SPCombiner_ghost: public Combiner<SPMsg_ghost> {
public:
	virtual void combine(SPMsg_ghost & old, const SPMsg_ghost & new_msg) {
		if (old.dist > new_msg.dist)
			old = new_msg;
	}
};

void ghost_sssp(int srcID, string in_path, string out_path, bool use_combiner) {
	src=srcID;

	WorkerParams param;
	param.input_path = in_path;
	param.output_path = out_path;
	param.force_write = true;
	param.native_dispatcher = false;
	SPWorker_ghost worker;
	SPCombiner_ghost combiner;
	if(use_combiner) worker.setCombiner(&combiner);
	worker.run(param);
}
