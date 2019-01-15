#ifndef VECS_H_
#define VECS_H_

#include "serialization.h"
#include "Combiner.h"
#include "global.h"
#include <vector>
using namespace std;

template <class KeyT, class MessageT>
struct msgpair {
    KeyT key;
    MessageT msg;

    msgpair()
    {
    }

    msgpair(KeyT v1, MessageT v2)
    {
        key = v1;
        msg = v2;
    }

    inline bool operator<(const msgpair& rhs) const
    {
        return key < rhs.key;
    }
};

template <class KeyT, class MessageT>
ibinstream& operator<<(ibinstream& m, const msgpair<KeyT, MessageT>& v)
{
    m << v.key;
    m << v.msg;
    return m;
}

template <class KeyT, class MessageT>
obinstream& operator>>(obinstream& m, msgpair<KeyT, MessageT>& v)
{
    m >> v.key;
    m >> v.msg;
    return m;
}

//===============================================

template <class KeyT, class MessageT, class HashT>
class Vecs {
public:
    typedef vector<msgpair<KeyT, MessageT> > Vec;
    typedef vector<Vec> VecGroup;

    int np;
    VecGroup vecs;
    HashT hash;

    Vecs()
    {
        int np = _num_workers;
        this->np = np;
        vecs.resize(np);
    }

    void append(const KeyT key, const MessageT msg)
    {
        msgpair<KeyT, MessageT> item(key, msg);
        vecs[hash(key)].push_back(item);
    }

    Vec& getBuf(int pos)
    {
        return vecs[pos];
    }

    VecGroup& getBufs()
    {
        return vecs;
    }

    void clear()
    {
        for (int i = 0; i < np; i++) {
            vecs[i].clear();
        }
    }

    //============================
    //apply combiner logic

    void combine()
    {
        Combiner<MessageT>* combiner = (Combiner<MessageT>*)get_combiner();
        for (int i = 0; i < np; i++) {
            sort(vecs[i].begin(), vecs[i].end());
            Vec newVec;
            int size = vecs[i].size();
            if (size > 0) {
                newVec.push_back(vecs[i][0]);
                KeyT preKey = vecs[i][0].key;
                for (int j = 1; j < size; j++) {
                    msgpair<KeyT, MessageT>& cur = vecs[i][j];
                    if (cur.key != preKey) {
                        newVec.push_back(cur);
                        preKey = cur.key;
                    } else {
                        combiner->combine(newVec.back().msg, cur.msg);
                    }
                }
            }
            newVec.swap(vecs[i]);
        }
    }

    long long get_total_msg()
    {
        long long sum = 0;
        for (int i = 0; i < vecs.size(); i++) {
            sum += vecs[i].size();
        }
        return sum;
    }
};

#endif
