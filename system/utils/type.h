#ifndef TYPE_H_
#define TYPE_H_

#include "serialization.h"
#include "global.h"

//equivalent to boost::hash_combine
void hash_combine(size_t& seed, int v)
{
    seed ^= v + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

//============= int pair =============
struct intpair {
    int v1;
    int v2;

    intpair()
    {
    }

    intpair(int v1, int v2)
    {
        this->v1 = v1;
        this->v2 = v2;
    }

    void set(int v1, int v2)
    {
        this->v1 = v1;
        this->v2 = v2;
    }

    inline bool operator<(const intpair& rhs) const
    {
        return (v1 < rhs.v1) || ((v1 == rhs.v1) && (v2 < rhs.v2));
    }

    inline bool operator>(const intpair& rhs) const
    {
        return (v1 > rhs.v1) || ((v1 == rhs.v1) && (v2 > rhs.v2));
    }

    inline bool operator==(const intpair& rhs) const
    {
        return (v1 == rhs.v1) && (v2 == rhs.v2);
    }

    inline bool operator!=(const intpair& rhs) const
    {
        return (v1 != rhs.v1) || (v2 != rhs.v2);
    }

    int hash()
    {
        size_t seed = 0;
        hash_combine(seed, v1);
        hash_combine(seed, v2);
        return seed % ((unsigned int)_num_workers);
    }
};

ibinstream& operator<<(ibinstream& m, const intpair& v)
{
    m << v.v1;
    m << v.v2;
    return m;
}

obinstream& operator>>(obinstream& m, intpair& v)
{
    m >> v.v1;
    m >> v.v2;
    return m;
}

class IntPairHash {
public:
    inline int operator()(intpair key)
    {
        return key.hash();
    }
};

namespace __gnu_cxx {
template <>
struct hash<intpair> {
    size_t operator()(intpair pair) const
    {
        size_t seed = 0;
        hash_combine(seed, pair.v1);
        hash_combine(seed, pair.v2);
        return seed;
    }
};
}

//============= int triplet =============
struct inttriplet {
    int v1;
    int v2;
    int v3;

    inttriplet()
    {
    }

    inttriplet(int v1, int v2, int v3)
    {
        this->v1 = v1;
        this->v2 = v2;
        this->v3 = v3;
    }

    void set(int v1, int v2, int v3)
    {
        this->v1 = v1;
        this->v2 = v2;
        this->v3 = v3;
    }

    inline bool operator<(const inttriplet& rhs) const
    {
        return (v1 < rhs.v1) || ((v1 == rhs.v1) && (v2 < rhs.v2)) || ((v1 == rhs.v1) && (v2 == rhs.v2) && (v3 < rhs.v3));
    }

    inline bool operator>(const inttriplet& rhs) const
    {
        return (v1 > rhs.v1) || ((v1 == rhs.v1) && (v2 > rhs.v2)) || ((v1 == rhs.v1) && (v2 == rhs.v2) && (v3 > rhs.v3));
    }

    inline bool operator==(const inttriplet& rhs) const
    {
        return (v1 == rhs.v1) && (v2 == rhs.v2) && (v3 == rhs.v3);
    }

    inline bool operator!=(const inttriplet& rhs) const
    {
        return (v1 != rhs.v1) || (v2 != rhs.v2) || (v3 != rhs.v3);
    }

    int hash()
    {
        size_t seed = 0;
        hash_combine(seed, v1);
        hash_combine(seed, v2);
        hash_combine(seed, v3);
        return seed % ((unsigned int)_num_workers);
    }
};

ibinstream& operator<<(ibinstream& m, const inttriplet& v)
{
    m << v.v1;
    m << v.v2;
    m << v.v3;
    return m;
}

obinstream& operator>>(obinstream& m, inttriplet& v)
{
    m >> v.v1;
    m >> v.v2;
    m >> v.v3;
    return m;
}

class IntTripletHash {
public:
    inline int operator()(inttriplet key)
    {
        return key.hash();
    }
};

namespace __gnu_cxx {
template <>
struct hash<inttriplet> {
    size_t operator()(inttriplet triplet) const
    {
        size_t seed = 0;
        hash_combine(seed, triplet.v1);
        hash_combine(seed, triplet.v2);
        hash_combine(seed, triplet.v3);
        return seed;
    }
};
}

//============= vertex-worker pair =============
struct vwpair {
    VertexID vid;
    int wid;

    vwpair()
    {
    }

    vwpair(int vid, int wid)
    {
        this->vid = vid;
        this->wid = wid;
    }

    void set(int vid, int wid)
    {
        this->vid = vid;
        this->wid = wid;
    }

    inline bool operator<(const vwpair& rhs) const
    {
        return vid < rhs.vid;
    }

    inline bool operator==(const vwpair& rhs) const
    {
        return vid == rhs.vid;
    }

    inline bool operator!=(const vwpair& rhs) const
    {
        return vid != rhs.vid;
    }

    int hash()
    {
        return wid; //the only difference from intpair
    }
};

ibinstream& operator<<(ibinstream& m, const vwpair& v)
{
    m << v.vid;
    m << v.wid;
    return m;
}

obinstream& operator>>(obinstream& m, vwpair& v)
{
    m >> v.vid;
    m >> v.wid;
    return m;
}

class VWPairHash {
public:
    inline int operator()(vwpair key)
    {
        return key.hash();
    }
};

namespace __gnu_cxx {
template <>
struct hash<vwpair> {
    size_t operator()(vwpair pair) const
    {
        return pair.vid;
    }
};
}

#endif /* TYPE_H_ */
