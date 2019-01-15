#include "basic/pregel-dev.h"
#include "utils/type.h"
using namespace std;

//Note that we assume integer edge weight, you may want to change it to double in your app

enum MSTPhase
{
    DistributedMinEdgePicking_LocalPickingAndSendToRoot,
    DistributedMinEdgePicking_RootPickMinEdge,
    DistributedMinEdgePicking_RespondEdgeRoot,
    DistributedMinEdgePicking_Finalize,
    Supervertex_Finding,
    PointerJumping_Request,
    PointerJumping_Respond,
    SupervertexFormation_Request,
    SupervertexFormation_Respond,
    SupervertexFormation_Update,
    EdgeCleaning_Exchange,
    EdgeCleanning_RemoveEdge
};

enum VertexType
{
    SuperVertex, PointsAtSupervertex, PointsAtSubvertex, Unknown
};

struct MSTValue
{
    int root;
    VertexType type;
    std::vector<inttriplet> edges; // from to weight
    std::vector<inttriplet> output;
};

ibinstream& operator<<(ibinstream& m, const MSTValue& v)
{
    int type = v.type;
    m << v.root;
    m << type;
    m << v.edges;
    m << v.output;
    return m;
}

obinstream& operator>>(obinstream& m, MSTValue& v)
{
    int type;
    m >> v.root;
    m >> type;
    m >> v.edges;
    m >> v.output;
    v.type = (VertexType)type;
    return m;
}

//====================================
struct MSTAggType
{
    MSTPhase phase;
    bool ifPointsAtSupervertex;
    bool haltAlgorithm;
};


ibinstream& operator<<(ibinstream& m, const MSTAggType& v)
{
    int phase = v.phase;
    m << phase;
    m << v.ifPointsAtSupervertex;
    m << v.haltAlgorithm;
    return m;
}

obinstream& operator>>(obinstream& m, MSTAggType& v)
{
    int phase;
    m >> phase;
    v.phase = (MSTPhase)phase;
    m >> v.ifPointsAtSupervertex;
    m >> v.haltAlgorithm;
    return m;
}

class MSTVertex : public Vertex<VertexID, MSTValue, inttriplet>
{
public:

	//edges must have total order to form conjoined-tree //$$$$$$$$$$$$$$$$$$$$$$$$$$$$
	//convert intput edge (u, v, weight) to ensure u < v //$$$$$$$$$$$$$$$$$$$$$$$$$$$$
	bool edgecmp(inttriplet p1, inttriplet p2)
	{
		if(p1.v3 < p2.v3)
		{
			return true;
		}
		else
		{
			if(p1.v1 > p1.v2)
			{
				int tmp = p1.v1;
				p1.v1 = p1.v2;
				p1.v2 = tmp;
			}
			if(p2.v1 > p2.v2)
			{
				int tmp = p2.v1;
				p2.v1 = p2.v2;
				p2.v2 = tmp;
			}
			//------
			if(p1.v3 == p2.v3 && p1.v2 < p2.v2)
			{
				return true;
			}
			else if (p1.v3 == p2.v3 && p1.v2 == p2.v2 &&  p1.v1 < p2.v1)
			{
				return true;
			}
		}
		return false;
	}

    /* assume edges.size() > 0  */
    inttriplet minElement(std::vector<inttriplet>& edges)
    {
        int idx = 0;
        for(int i = 1 ; i <  edges.size(); i ++)
        {
            if(edgecmp(edges[i], edges[idx]))
                idx = i;
        }
        return edges[idx];
    }

    inttriplet minElement(std::vector<inttriplet>& edges,MessageContainer& messages)
    {
        if(edges.size() == 0)
            return minElement(messages);
        else if(messages.size() == 0)
            return minElement(edges);
        else
        {
            inttriplet edge = minElement(messages);
            int idx = -1;
            for(int i = 0 ; i <  edges.size(); i ++)
            {
                if(idx == -1 && edgecmp(edges[i], edge))
                    idx = i;
                else if (idx != -1 && edgecmp(edges[i], edges[idx]))
                    idx = i;
            }
            return idx == -1 ? edge : edges[idx];
        }
    }

    void sendMsg(const int id, const int value)
    {
        send_message(id, inttriplet(value,-1,-1) );
    }

    void sendMsg(const int id, const intpair& value)
    {
        send_message(id, inttriplet(value.v1,value.v2,-1) );
    }
    void sendMsg(const int id, const inttriplet& value)
    {
        send_message(id, value);
    }

    virtual void compute(MessageContainer& messages)
    {
    	MSTAggType* agg= (MSTAggType*) getAgg();
		std::vector<inttriplet>& edges = value().edges;
		int& root = value().root;
		VertexType& type = value().type;
		std::vector<inttriplet>& output = value().output;

        if(step_num() == 1)
        {
        	if(edges.size() == 0) vote_to_halt();
            return;
        }

        switch(agg->phase)
        {
        case DistributedMinEdgePicking_LocalPickingAndSendToRoot:
            if(type == PointsAtSupervertex && edges.size() > 0)
            {
                inttriplet edge = minElement(edges);
                sendMsg(root, edge);
                type = Unknown; // change to PointsAtSupervertex when new supervertex is found
            }
            break;
        case DistributedMinEdgePicking_RootPickMinEdge:
            if(type == SuperVertex)
            {
                inttriplet minEdge = minElement(edges, messages);
                sendMsg(minEdge.v2, id); // request root;
                output.push_back(minEdge);
            }
            break;
        case DistributedMinEdgePicking_RespondEdgeRoot:
            for(int i = 0 ;i < messages.size() ; i ++)
            {
                sendMsg(messages[i].v1, root);
            }
            break;
        case DistributedMinEdgePicking_Finalize:
            if(type == SuperVertex)
            {
                inttriplet minEdge = output.back();
                assert(messages.size() == 1);
                sendMsg(messages[0].v1, id);
                root = messages[0].v1;
            }
            break;
        case Supervertex_Finding:
        	if(messages.size() > 0)
			{
				bool nearvertex = false;
				for(int i = 0 ;i < messages.size(); i ++)
				{
					if(messages[i].v1 == root)
					{
						if(id < root)
						{
							type = SuperVertex;
							root = id;
						}
						else
						{
							type = PointsAtSupervertex;
							output.pop_back(); // remove duplicate
						}
						nearvertex = true;
						break;
					}
				}
				if(!nearvertex)
				{
					type = PointsAtSubvertex;
				}
			}
			else
			{
				type = PointsAtSubvertex;
			}
            break;

        case PointerJumping_Request:
            if(type == PointsAtSubvertex)
            {
                if(messages.size() != 0)
                {
                    assert(messages.size() == 1); // only one msg
                    root = messages[0].v1;
                    if(messages[0].v2 == true)
                    {
                        type = PointsAtSupervertex;
                        break; // switch break;
                    }
                }
                sendMsg(root, id);
            }
            break;
        case PointerJumping_Respond:
            for(int i = 0 ;i < messages.size() ; i ++)
            {
                sendMsg(messages[i].v1, intpair(root, type == SuperVertex ) );
            }

            break;
        case    SupervertexFormation_Request:
            if(type == Unknown)
            {
                sendMsg(root, id); // only one int is used.
            }
            break;
        case    SupervertexFormation_Respond:
            for(int i = 0 ;i < messages.size(); i ++)
            {
                sendMsg(messages[i].v1,root); // only one int is used.
            }
            break;
        case SupervertexFormation_Update:
            if(type == Unknown)
            {
                root = messages[0].v1;
                type = PointsAtSubvertex;
            }
            break;
        case    EdgeCleaning_Exchange:
            for(int i = 0 ;i < edges.size(); i ++)
            {
                sendMsg(edges[i].v2, inttriplet(id, root, edges[i].v3));
            }
            break;

        case    EdgeCleanning_RemoveEdge:
            std::vector<inttriplet> updatedEdges;
            for(int i = 0 ;i < messages.size(); i ++)
            {
                if(messages[i].v2 != root)
                {
                    updatedEdges.push_back(inttriplet(id,messages[i].v1, messages[i].v3));
                }
            }
            edges.swap(updatedEdges);
            if(edges.size()==0) vote_to_halt();//!!! important in order for agg.ifPointsAtSupervertex to work
            break;
        }
    }
};

class MSTAgg : public Aggregator<MSTVertex, MSTAggType, MSTAggType>
{
private:
    MSTAggType value;
public:
    virtual void init()
    {
        if(step_num() > 1)
        {
        	value.phase = ((MSTAggType*) getAgg())->phase;
            value.ifPointsAtSupervertex = true;
            value.haltAlgorithm = true;
        }
    }

    virtual void stepPartial(MSTVertex* v)
    {
        if(value.phase == PointerJumping_Respond && v->value().type == PointsAtSubvertex)
        {
        	value.ifPointsAtSupervertex= false;
        }
        if(value.phase == EdgeCleanning_RemoveEdge && v->value().edges.size() != 0)
        {
            value.haltAlgorithm = false;
        }
    }

    virtual void stepFinal(MSTAggType* part)
    {
        if(part->ifPointsAtSupervertex == false)
            value.ifPointsAtSupervertex = false;
        if(part->haltAlgorithm == false)
            value.haltAlgorithm = false;
    }

    virtual MSTAggType* finishPartial()
    {
        return &value;
    }
    virtual MSTAggType* finishFinal()
    {
        if(step_num() == 1)
        {
            value.phase = DistributedMinEdgePicking_LocalPickingAndSendToRoot;
        }
        else
        {
        	if(value.phase == EdgeCleanning_RemoveEdge  && value.haltAlgorithm)
            {
                forceTerminate();
            }

            else if(value.phase == PointerJumping_Respond)
            {
                if(value.ifPointsAtSupervertex == false)
                {
                    value.phase = PointerJumping_Request;
                }
                else
                {
                    value.phase = (MSTPhase)((value.phase + 1) % 12);
                }
            }
            else
            {
                value.phase = (MSTPhase)((value.phase + 1) % 12);
            }
        }
        cout << "Phase " << value.phase << endl;
        return &value;
    }
};

class MSTWorker : public Worker<MSTVertex,MSTAgg>
{
    char buf[100];

public:
    // vid \t num v1 d1 v2 d2
    virtual MSTVertex* toVertex(char* line)
    {
        char* pch;
        pch = strtok(line, "\t");
        MSTVertex* v = new MSTVertex;
        v->id = atoi(pch);

        pch = strtok(NULL, " ");
        int num = atoi(pch);
        hash_set<int> nbset;//redundant edge removal //$$$$$$$$$$$$$$$$$$$$$$$$$$$$
        while (num -- )
        {
            pch = strtok(NULL, " ");
            int vid = atoi(pch);
            pch = strtok(NULL, " ");
            if(vid != v->id)//self-loop removal //$$$$$$$$$$$$$$$$$$$$$$$$$$$$
			{
				if(nbset.find(vid)==nbset.end())//redundant edge removal //$$$$$$$$$$$$$$$$$$$$$$$$$$$$
				{
					int dis = atoi(pch);
					v->value().edges.push_back(inttriplet(v->id, vid, dis));
					nbset.insert(vid);//redundant edge removal //$$$$$$$$$$$$$$$$$$$$$$$$$$$$
				}
				//else cout<<"Edge ("<<v->id<<", "<<vid<<") is redundant !"<<endl;//$$$$$$$$$$$$$$$$$$$$$$$$$$$$
			}
			else cout<<"Vertex "<<vid<<" has a self-loop !"<<endl;//$$$$$$$$$$$$$$$$$$$$$$$$$$$$
        }
        v->value().type = SuperVertex;
        v->value().root = v->id;
        return v;
    }

    virtual void toline(MSTVertex* v, BufferedWriter& writer)
	{
		std::vector<inttriplet>& output = v->value().output;
		for(int i = 0 ;i < output.size(); i ++)
		{
			sprintf(buf, " %d %d %d\n", output[i].v1, output[i].v2,  output[i].v3 );
			writer.write(buf);
		}
	}
};

void pregel_mst(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    MSTWorker worker;
    MSTAgg agg;
    worker.setAggregator(&agg);
    worker.run(param);
}
