#ifndef RMESSAGEBUFFER_H
#define RMESSAGEBUFFER_H

#include <vector>
#include "../utils/global.h"
#include "../utils/Combiner.h"
#include "../utils/communication.h"
#include "../utils/vecs.h"
using namespace std;

template <class RVertexT>
class RMessageBuffer {
public:
    typedef typename RVertexT::KeyType KeyT;
    typedef typename RVertexT::MessageType MessageT;
    typedef typename RVertexT::HashType HashT;
    typedef typename RVertexT::RespondType RespondT;
    typedef vector<MessageT> MessageContainerT;
    typedef hash_map<KeyT, int> Map; //int = position in v_msg_bufs //CHANGED FOR VADD
    typedef hash_map<KeyT, RVertexT*> VMap;
    typedef Vecs<KeyT, MessageT, HashT> VecsT;
    typedef typename VecsT::Vec Vec;
    typedef typename VecsT::VecGroup VecGroup;
    typedef typename Map::iterator MapIter;

    typedef hash_set<KeyT> ReqSet;
    typedef vector<ReqSet> ReqSets;
    typedef hash_map<KeyT, RespondT> RespMap;
    typedef typename RespMap::iterator RespIter;
    typedef vector<RespMap> RespMaps;

    VecsT out_messages;
    Map in_messages;
    vector<RVertexT*> to_add;
    vector<MessageContainerT> v_msg_bufs;
    ReqSets in_req_sets; //added for RVertex
    ReqSets out_req_sets; //added for RVertex
    RespMaps in_resp_maps; //added for RVertex
    RespMaps out_resp_maps; //added for RVertex
    HashT hash;

    VMap vmap;

    void init(vector<RVertexT*> vertexes)
    {
        v_msg_bufs.resize(vertexes.size());
        in_req_sets.resize(_num_workers);
        out_req_sets.resize(_num_workers);
        in_resp_maps.resize(_num_workers);
        out_resp_maps.resize(_num_workers);
        for (int i = 0; i < vertexes.size(); i++) {
            RVertexT* v = vertexes[i];
            in_messages[v->id] = i; //CHANGED FOR VADD
            vmap[v->id] = v;
        }
    }

    void add_message(const KeyT& id, const MessageT& msg)
    {
        hasMsg(); //cannot end yet even every vertex halts
        out_messages.append(id, msg);
    }

    void add_request(const KeyT& id)
    { //added for RVertex
        hasMsg(); //cannot end yet even every vertex halts
        out_req_sets[hash(id)].insert(id);
    }

    void add_respond_explicit(const KeyT& me, const KeyT& id, const RespondT& respond)
    { //added for RVertex
        hasMsg(); //cannot end yet even every vertex halts
        RespMap& rmap = out_resp_maps[hash(id)];
        RespIter it = rmap.find(me);
        if (it == rmap.end())
            rmap[me] = respond;
    }

    void add_respond_implicit(const KeyT& me, int wid, const RespondT& respond)
    { //added for RVertex
        hasMsg(); //cannot end yet even every vertex halts
        RespMap& rmap = out_resp_maps[wid];
        RespIter it = rmap.find(me);
        if (it == rmap.end())
            rmap[me] = respond;
    }

    RespondT getRespond(const KeyT& id)
    {
        return in_resp_maps[hash(id)][id];
    }

    RespondT* getRespond_safe(const KeyT& id)
    {
        RespMap& map = in_resp_maps[hash(id)];
        RespIter it = map.find(id);
        if (it == map.end())
            return NULL;
        else
            return &(it->second);
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

    void sync_reqs()
    {
        for (int i = 0; i < _num_workers; i++)
            in_req_sets[i].clear(); //added for RVertex
        all_to_all(out_req_sets, in_req_sets); //added for RVertex
        out_req_sets[_my_rank].swap(in_req_sets[_my_rank]);
        for (int i = 0; i < _num_workers; i++)
            out_req_sets[i].clear(); //added for RVertex
    }

    void sync_resps()
    {
        for (int i = 0; i < _num_workers; i++)
            in_resp_maps[i].clear(); //added for RVertex
        all_to_all(out_resp_maps, in_resp_maps); //added for RVertex
        out_resp_maps[_my_rank].swap(in_resp_maps[_my_rank]);
        for (int i = 0; i < _num_workers; i++)
            out_resp_maps[i].clear(); //added for RVertex
    }

    vector<RVertexT*>& sync_messages()
    {
        int np = get_num_workers();
        int me = get_worker_id();

        //------------------------------------------------
        // get messages from remote
        vector<vector<RVertexT*> > add_buf(_num_workers);
        //set send buffer
        for (int i = 0; i < to_add.size(); i++) {
            RVertexT* v = to_add[i];
            add_buf[hash(v->id)].push_back(v);
        }

        //================================================

        //exchange msgs
        //exchange vertices to add
        all_to_all_cat(out_messages.getBufs(), add_buf);

        //================================================

        //------------------------------------------------
        //delete sent vertices
        for (int i = 0; i < to_add.size(); i++) {
            RVertexT* v = to_add[i];
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

    void add_vertex(RVertexT* v)
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

    long long get_total_req()
    {
        long long sum = 0;
        for (int i = 0; i < _num_workers; i++) {
            sum += out_req_sets[i].size();
        }
        return sum;
    }

    long long get_total_resp()
    {
        long long sum = 0;
        for (int i = 0; i < _num_workers; i++) {
            sum += out_resp_maps[i].size();
        }
        return sum;
    }

    vector<MessageContainerT>& get_v_msg_bufs()
    {
        return v_msg_bufs;
    }

    bool hasReqResp()
    {
        return (get_total_req() > 0LL) || (get_total_resp() > 0LL);
    }
};

#endif
