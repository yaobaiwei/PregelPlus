#include "ghost/ghost-dev.h"
using namespace std;

//input line format: vertexID \t numOfNeighbors neighbor1 neighbor2 ...
//output line format: v \t min_vertexID(v's connected component)

class CCVertex_ghost: public GVertex<VertexID, VertexID, VertexID> {
	public:
		virtual void compute(MessageContainer & messages) {
			if (step_num() == 1) {
				// Initialize in toVertex
				broadcast(value());
				vote_to_halt();
			} else {
				VertexID min = messages[0];
				for (int i = 1; i < messages.size(); i++) {
					if (min > messages[i])
						min = messages[i];
				}
				if (min < value()) {
					value() = min;
					broadcast(min);
				}
				vote_to_halt();
			}
		}

};

class CCWorker_ghost: public GWorker<CCVertex_ghost> {
	char buf[100];
	public:

		virtual CCVertex_ghost* toVertex(char* line) {
			char * pch;
			pch = strtok(line, "\t");
			CCVertex_ghost* v = new CCVertex_ghost;
			v->id = atoi(pch);
			pch = strtok(NULL, " ");
			int num = atoi(pch);
			EdgeContainer & edges = v->neighbors();
			VertexID min = v->id;
			for (int i = 0; i < num; i++) {
				pch = strtok(NULL, " ");
				EdgeT edge;
				edge.id = atoi(pch);
				if(edge.id < min) min = edge.id;
				edges.push_back(edge);
			}
			v->value() = min;
			return v;
		}

		virtual void toline(CCVertex_ghost* v, BufferedWriter & writer) {
			sprintf(buf, "%d\t%d\n", v->id, v->value());
			writer.write(buf);
		}
};

class CCCombiner_ghost: public Combiner<VertexID>
{
	public:
		virtual void combine(VertexID & old, const VertexID & new_msg) {
			if (old > new_msg)
				old = new_msg;
		}
};

void ghost_hashmin(string in_path, string out_path, bool use_combiner) {
	WorkerParams param;
	param.input_path = in_path;
	param.output_path = out_path;
	param.force_write = true;
	param.native_dispatcher = false;
	CCWorker_ghost worker;
	CCCombiner_ghost combiner;
	if(use_combiner) worker.setCombiner(&combiner);
	worker.run(param);
}
