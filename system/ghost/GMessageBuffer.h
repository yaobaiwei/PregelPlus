#ifndef GMESSAGEBUFFER_H_
#define GMESSAGEBUFFER_H_

#include <vector>
#include "../utils/global.h"
#include "../utils/Combiner.h"
#include "../utils/communication.h"
#include "../utils/vecs.h"
using namespace std;

template <class EdgeT, class MsgVecPointer>
struct GhostEntry {
    EdgeT edge;
    MsgVecPointer msgvec; //pointer

    GhostEntry(EdgeT edge, MsgVecPointer msgvec)
    {
        this->edge = edge;
        this->msgvec = msgvec;
    }
};

template <class GVertexT>
class GMessageBuffer {
public:
    typedef typename GVertexT::KeyType KeyT;
    typedef typename GVertexT::MessageType MessageT;
    typedef typename GVertexT::HashType HashT;
    typedef vector<MessageT> MessageContainerT;
    typedef hash_map<KeyT, int> Map; //int = position in v_msg_bufs //CHANGED FOR VADD
    typedef Vecs<KeyT, MessageT, HashT> VecsT;
    typedef typename VecsT::Vec Vec;
    typedef typename VecsT::VecGroup VecGroup;
    typedef typename Map::iterator MapIter;

    typedef typename GVertexT::EdgeType EdgeT;
    typedef vector<EdgeT> EdgeContainer;
    typedef hash_map<int, EdgeContainer> NeighborTable;
    typedef typename NeighborTable::iterator TableIter;

    typedef GhostEntry<EdgeT, int> GEntry; //int = position in v_msg_bufs //CHANGED FOR VADD
    typedef vector<GEntry> GEntryList;
    typedef hash_map<KeyT, GEntryList> GhostTable;

    void init(vector<GVertexT*> vertexes)
    {
        v_msg_bufs.resize(vertexes.size());
        for (int i = 0; i < vertexes.size(); i++) {
            GVertexT* v = vertexes[i];
            in_messages[v->id] = i; //CHANGED FOR VADD
        }
    }

    void add_message(const KeyT& id, const MessageT& msg)
    {
        hasMsg(); //cannot end yet even every vertex halts
        out_messages.append(id, msg);
    }

    void add_gmessage(GVertexT* vertex, const MessageT& field)
    {
        hasMsg(); //cannot end yet even if every vertex halts
        vector<int>& mac = vertex->machines();
        for (int i = 0; i < mac.size(); i++) {
            out_gmessages.vecs[mac[i]].push_back(msgpair<KeyT, MessageT>(vertex->id, field));
        }
    }

    Map& get_messages()
    {
        return in_messages;
    }

    void combine()
    {
        //apply combiner
        Combiner<MessageT>* combiner = (Combiner<MessageT>*)get_combiner();
        if (combiner != NULL)
            out_messages.combine();
    }

    vector<GVertexT*>& sync_messages(vector<GhostTable>& ghosts)
    {
        int np = get_num_workers();
        int me = get_worker_id();

        //------------------------------------------------
        // get messages from remote
        vector<vector<GVertexT*> > add_buf(_num_workers);
        //set send buffer
        for (int i = 0; i < to_add.size(); i++) {
            GVertexT* v = to_add[i];
            add_buf[hash(v->id)].push_back(v);
        }

        //================================================

        //exchange msgs
        //exchange vertices to add
        all_to_all_cat(out_messages.getBufs(), add_buf, out_gmessages.vecs);

        //================================================

        //------------------------------------------------
        //delete sent vertices
        for (int i = 0; i < to_add.size(); i++) {
            GVertexT* v = to_add[i];
            if (hash(v->id) != _my_rank)
                delete v;
        }
        to_add.clear();
        //collect new vertices to add
        for (int i = 0; i < np; i++) {
            to_add.insert(to_add.end(), add_buf[i].begin(), add_buf[i].end());
        }

        //================================================
        //Change of G33
        int oldsize = v_msg_bufs.size();
        v_msg_bufs.resize(oldsize + to_add.size());
        for (int i = 0; i < to_add.size(); i++) {
            int pos = oldsize + i;
            in_messages[to_add[i]->id] = pos; //CHANGED FOR VADD
        }

        //------------------------------------------------

        VecGroup& gbufs = out_gmessages.vecs;
        for (int i = 0; i < _num_workers; i++) {
            Vec& buf = gbufs[i];
            for (int j = 0; j < buf.size(); j++) {
                msgpair<KeyT, MessageT>& pair = buf[j];
                GEntryList& local_nbs = ghosts[hash(pair.key)][pair.key];
                for (int k = 0; k < local_nbs.size(); k++) {
                    GEntry nb = local_nbs[k];
                    MessageT msg = pair.msg;
                    nb.edge.relay(msg);
                    v_msg_bufs[nb.msgvec].push_back(msg); //merge ghost msg to the ordinary in-msg pool
                }
            }
        }
        out_gmessages.clear();

        // gather all messages
        for (int i = 0; i < np; i++) {
            Vec& msgBuf = out_messages.getBuf(i);
            for (int i = 0; i < msgBuf.size(); i++) {
                MapIter it = in_messages.find(msgBuf[i].key);
                if (it != in_messages.end()) //filter out msgs to non-existent vertices
                    v_msg_bufs[it->second].push_back(msgBuf[i].msg); //CHANGED FOR VADD
            }
        }
        //clear out-msg-buf
        out_messages.clear();

        return to_add;
    }

    void add_vertex(GVertexT* v)
    {
        hasMsg(); //cannot end yet even every vertex halts
        to_add.push_back(v);
    }

    long long get_total_msg()
    {
        return out_messages.get_total_msg();
    }

    long long get_total_gmsg()
    {
        return out_gmessages.get_total_msg();
    }

    int get_total_vadd()
    {
        return to_add.size();
    }

    vector<MessageContainerT>& get_v_msg_bufs()
    {
        return v_msg_bufs;
    }

private:
    VecsT out_messages;
    VecsT out_gmessages;
    Map in_messages;
    vector<GVertexT*> to_add;
    vector<MessageContainerT> v_msg_bufs;
    HashT hash;
};

#endif /* GMESSAGEBUFFER_H_ */
