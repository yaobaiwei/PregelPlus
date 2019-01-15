#ifndef VERTEX_H
#define VERTEX_H

#include "../utils/global.h"
#include <vector>
#include "../utils/serialization.h"
#include "MessageBuffer.h"
using namespace std;

//Default Hash Function =====================
template <class KeyT>
class DefaultHash {
public:
    inline int operator()(KeyT key)
    {
        if (key >= 0)
            return key % _num_workers;
        else
            return (-key) % _num_workers;
    }
};
//==========================================

template <class KeyT, class ValueT, class MessageT, class HashT = DefaultHash<KeyT> >
class Vertex {
public:
    KeyT id;

    typedef KeyT KeyType;
    typedef ValueT ValueType;
    typedef MessageT MessageType;
    typedef HashT HashType;
    typedef vector<MessageType> MessageContainer;
    typedef typename MessageContainer::iterator MessageIter;
    typedef Vertex<KeyT, ValueT, MessageT, HashT> VertexT;
    typedef MessageBuffer<VertexT> MessageBufT;

    friend ibinstream& operator<<(ibinstream& m, const VertexT& v)
    {
        m << v.id;
        m << v._value;
        return m;
    }

    friend obinstream& operator>>(obinstream& m, VertexT& v)
    {
        m >> v.id;
        m >> v._value;
        return m;
    }

    virtual void compute(MessageContainer& messages) = 0;
    inline ValueT& value()
    {
        return _value;
    }
    inline const ValueT& value() const
    {
        return _value;
    }

    Vertex()
        : active(true)
    {
    }

    inline bool operator<(const VertexT& rhs) const
    {
        return id < rhs.id;
    }
    inline bool operator==(const VertexT& rhs) const
    {
        return id == rhs.id;
    }
    inline bool operator!=(const VertexT& rhs) const
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

    void send_message(const KeyT& id, const MessageT& msg)
    {
        ((MessageBufT*)get_message_buffer())->add_message(id, msg);
    }

    void add_vertex(VertexT* v)
    {
        ((MessageBufT*)get_message_buffer())->add_vertex(v);
    }

private:
    ValueT _value;
    bool active;
};

#endif
