#ifndef MESSAGEBUFFER_H
#define MESSAGEBUFFER_H

#include <vector>
#include "../utils/global.h"
#include "../utils/Combiner.h"
#include "../utils/communication.h"
#include "../utils/vecs.h"
using namespace std;

template <class VertexT>
class MessageBuffer {
public:
    typedef typename VertexT::KeyType KeyT;
    typedef typename VertexT::MessageType MessageT;
    typedef typename VertexT::HashType HashT;
    typedef vector<MessageT> MessageContainerT;
    typedef hash_map<KeyT, int> Map; //int = position in v_msg_bufs //CHANGED FOR VADD
    typedef Vecs<KeyT, MessageT, HashT> VecsT;
    typedef typename VecsT::Vec Vec;
    typedef typename VecsT::VecGroup VecGroup;
    typedef typename Map::iterator MapIter;

    VecsT out_messages;
    Map in_messages;
    vector<VertexT*> to_add;
    vector<MessageContainerT> v_msg_bufs;
    HashT hash;

    void init(vector<VertexT*> vertexes)
    {
        v_msg_bufs.resize(vertexes.size());
        for (int i = 0; i < vertexes.size(); i++) {
            VertexT* v = vertexes[i];
            in_messages[v->id] = i; //CHANGED FOR VADD
        }
    }
    void reinit(vector<VertexT*> vertexes)
    {
        v_msg_bufs.resize(vertexes.size());
	 in_messages.clear();
        for (int i = 0; i < vertexes.size(); i++) {
            VertexT* v = vertexes[i];
            in_messages[v->id] = i; //CHANGED FOR VADD
        }
    }
    void add_message(const KeyT& id, const MessageT& msg)
    {
        hasMsg(); //cannot end yet even every vertex halts
        out_messages.append(id, msg);
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

    vector<VertexT*>& sync_messages()
    {
        int np = get_num_workers();
        int me = get_worker_id();

        //------------------------------------------------
        // get messages from remote
        vector<vector<VertexT*> > add_buf(_num_workers);
        //set send buffer
        for (int i = 0; i < to_add.size(); i++) {
            VertexT* v = to_add[i];
            add_buf[hash(v->id)].push_back(v);
        }
        //================================================
        //exchange msgs
        //exchange vertices to add
        all_to_all_cat(out_messages.getBufs(), add_buf);

        //------------------------------------------------
        //delete sent vertices
        for (int i = 0; i < to_add.size(); i++) {
            VertexT* v = to_add[i];
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

        //================================================
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

    void add_vertex(VertexT* v)
    {
        hasMsg(); //cannot end yet even every vertex halts
        to_add.push_back(v);
    }

    long long get_total_msg()
    {
        return out_messages.get_total_msg();
    }

    int get_total_vadd()
    {
        return to_add.size();
    }

    vector<MessageContainerT>& get_v_msg_bufs()
    {
        return v_msg_bufs;
    }
};

#endif
