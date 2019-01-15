#ifndef GVERTEX_H_
#define GVERTEX_H_

#include "../utils/global.h"
#include <vector>
#include "../utils/serialization.h"
#include "GMessageBuffer.h"
#include "../basic/Vertex.h" //for DefaultHash
using namespace std;

//GEdge
template <class KeyT, class MessageT, class EValT>
class GEdge {
public:
    typedef MessageT MessageType;

    KeyT id;
    EValT eval;

    void relay(MessageT& msg)
    {
        //to process msg into a new value
        //relay is done on sender-side if deg<tau
        //otherwise, done on receiver-side
        //here, the default implementation is to do nothing to msg
    }

    friend ibinstream& operator<<(ibinstream& m, const GEdge& e)
    {
        m << e.id;
        m << e.eval;
        return m;
    }

    friend obinstream& operator>>(obinstream& m, GEdge& e)
    {
        m >> e.id;
        m >> e.eval;
        return m;
    }
};

template <class KeyT, class MessageT>
class DefaultGEdge {
public:
    typedef MessageT MessageType;

    KeyT id;

    void relay(MessageT& msg)
    {
        //to process msg into a new value
        //relay is done on sender-side if deg<tau
        //otherwise, done on receiver-side
        //here, the default implementation is to do nothing to msg
    }

    friend ibinstream& operator<<(ibinstream& m, const DefaultGEdge& e)
    {
        m << e.id;
        return m;
    }

    friend obinstream& operator>>(obinstream& m, DefaultGEdge& e)
    {
        m >> e.id;
        return m;
    }
};

//--------------------------
//GVertex
template <class KeyT, class ValueT, class MessageT, class EdgeT = DefaultGEdge<KeyT, MessageT>, class HashT = DefaultHash<KeyT> >
class GVertex {
public:
    typedef KeyT KeyType;
    typedef ValueT ValueType;
    typedef EdgeT EdgeType;
    typedef MessageT MessageType;
    typedef HashT HashType;
    typedef vector<MessageType> MessageContainer;
    typedef typename MessageContainer::iterator MessageIter;
    typedef GVertex<KeyT, ValueT, MessageT, EdgeT, HashT> GVertexT;
    typedef MessageBuffer<GVertexT> MessageBufT;

    typedef vector<EdgeT> EdgeContainer;
    typedef hash_map<int, EdgeContainer> NeighborTable;
    typedef typename NeighborTable::iterator TableIter;

    KeyT id;
    void* _neighbors;
    bool tag; //whether neighbors->machines already
    ValueT _value;
    bool active;

    GVertex()
    {
        tag = false;
        active = true;
        _neighbors = new EdgeContainer;
    }

    ~GVertex()
    {
        if (tag)
            delete (vector<int>*)_neighbors;
        else
            delete (EdgeContainer*)_neighbors;
    }

    EdgeContainer& neighbors()
    {
        return *((EdgeContainer*)_neighbors);
    }

    vector<int>& machines()
    {
        return *((vector<int>*)_neighbors);
    }

    void split(NeighborTable& table)
    {
        //set the empty table with local_neighbor info to sent
        //do the neighbors->machines conversion
        tag = true;
        EdgeContainer& nbs = neighbors();
        HashT hash;
        for (int i = 0; i < nbs.size(); i++) {
            int worker = hash(nbs[i].id);
            table[worker].push_back(nbs[i]);
        }
        delete (EdgeContainer*)_neighbors;
        _neighbors = new vector<int>;
        vector<int>& macs = machines();
        for (TableIter it = table.begin(); it != table.end(); it++) {
            macs.push_back(it->first);
        }
    }

    void broadcast(const MessageT& field)
    {
        GMessageBuffer<GVertexT>* gmsgbuf = (GMessageBuffer<GVertexT>*)global_message_buffer;
        if (tag) {
            vector<int>& macs = machines();
            gmsgbuf->add_gmessage(this, field);
        } else {
            EdgeContainer& nbs = neighbors();
            for (int i = 0; i < nbs.size(); i++) {
                EdgeT nb = nbs[i];
                MessageT msg = field;
                nb.relay(msg);
                gmsgbuf->add_message(nb.id, msg);
            }
        }
    }

    //-----------

    virtual void compute(MessageContainer& messages) = 0;
    inline ValueT& value()
    {
        return _value;
    }
    inline const ValueT& value() const
    {
        return _value;
    }

    inline bool operator<(const GVertexT& rhs) const
    {
        return id < rhs.id;
    }
    inline bool operator==(const GVertexT& rhs) const
    {
        return id == rhs.id;
    }
    inline bool operator!=(const GVertexT& rhs) const
    {
        return id != rhs.id;
    }

    inline bool is_active()
    {
        return active;
    }
    inline void activate()
    {
        active = true;
    }
    inline void vote_to_halt()
    {
        active = false;
    }

    friend ibinstream& operator<<(ibinstream& m, const GVertexT& v)
    { //just for using by sync_graph()
        m << v.id;
        m << v._value;
        m << *((EdgeContainer*)v._neighbors);
        m << v.tag;
        m << v.active;
        return m;
    }

    friend obinstream& operator>>(obinstream& m, GVertexT& v)
    { //just for using by sync_graph()
        m >> v.id;
        m >> v._value;
        m >> *((EdgeContainer*)v._neighbors);
        m >> v.tag;
        m >> v.active;
        return m;
    }
};

#endif /* GVERTEX_H_ */
