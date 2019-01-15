#ifndef RVERTEX_H
#define RVERTEX_H

#include "../utils/global.h"
#include <vector>
#include "../utils/serialization.h"
#include "RMessageBuffer.h"
#include "../basic/Vertex.h" //for DefaultHash
using namespace std;

template <class KeyT, class ValueT, class MessageT, class RespondT, class HashT = DefaultHash<KeyT> >
class RVertex {
public:
    KeyT id;

    typedef KeyT KeyType;
    typedef ValueT ValueType;
    typedef MessageT MessageType;
    typedef RespondT RespondType;
    typedef HashT HashType;
    typedef vector<MessageType> MessageContainer;
    typedef typename MessageContainer::iterator MessageIter;
    typedef RVertex<KeyT, ValueT, MessageT, RespondT, HashT> RVertexT;
    typedef RMessageBuffer<RVertexT> MessageBufT;
    typedef typename MessageBufT::RespMap RespMap;

    friend ibinstream& operator<<(ibinstream& m, const RVertexT& v)
    {
        m << v.id;
        m << v._value;
        return m;
    }

    friend obinstream& operator>>(obinstream& m, RVertexT& v)
    {
        m >> v.id;
        m >> v._value;
        return m;
    }

    virtual void compute(MessageContainer& messages) = 0;
    virtual RespondT respond() = 0; //added for RVertex

    inline ValueT& value()
    {
        return _value;
    }
    inline const ValueT& value() const
    {
        return _value;
    }

    RVertex()
        : active(true)
    {
    }

    inline bool operator<(const RVertexT& rhs) const
    {
        return id < rhs.id;
    }
    inline bool operator==(const RVertexT& rhs) const
    {
        return id == rhs.id;
    }
    inline bool operator!=(const RVertexT& rhs) const
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

    void send_message(KeyT& id, const MessageT& msg)
    {
        ((MessageBufT*)get_message_buffer())->add_message(id, msg);
    }

    void request(KeyT& tgt)
    { //added for RVertex
        ((MessageBufT*)get_message_buffer())->add_request(tgt);
    }

    void exp_respond(KeyT& tgt)
    { //added for RVertex
        ((MessageBufT*)get_message_buffer())->add_respond_explicit(id, tgt, respond());
    }

    RespondT get_respond(KeyT& tgt)
    {
        return ((MessageBufT*)get_message_buffer())->getRespond(tgt);
    }

    RespondT* get_respond_safe(KeyT& tgt)
    {
        return ((MessageBufT*)get_message_buffer())->getRespond_safe(tgt);
    }

    void add_vertex(RVertexT* v)
    {
        ((MessageBufT*)get_message_buffer())->add_vertex(v);
    }

private:
    ValueT _value;
    bool active;
};

#endif
