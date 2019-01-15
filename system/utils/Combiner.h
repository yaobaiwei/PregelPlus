#ifndef COMBINER_H
#define COMBINER_H

template <class MessageT>
class Combiner {

public:
    virtual void combine(MessageT& old, const MessageT& new_msg) = 0;
};

#endif
