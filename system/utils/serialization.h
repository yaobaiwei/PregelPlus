//Acknowledgements: this code is implemented by referencing pregel-mpi (https://code.google.com/p/pregel-mpi/) by Chuntao Hong.

#ifndef SERIALIZATION_H
#define SERIALIZATION_H

#include <vector>
#include <set>
#include <string>
#include <map>
#include "global.h"

using namespace std;

class ibinstream {
private:
    vector<char> buf;

public:
    char* get_buf()
    {
        return &buf[0];
    }

    size_t size()
    {
        return buf.size();
    }

    void raw_byte(char c)
    {
        buf.push_back(c);
    }

    void raw_bytes(const void* ptr, int size)
    {
        buf.insert(buf.end(), (const char*)ptr, (const char*)ptr + size);
    }
};

ibinstream& operator<<(ibinstream& m, size_t i)
{ //unsigned int
    m.raw_bytes(&i, sizeof(size_t));
    return m;
}

ibinstream& operator<<(ibinstream& m, bool i)
{
    m.raw_bytes(&i, sizeof(bool));
    return m;
}

ibinstream& operator<<(ibinstream& m, int i)
{
    m.raw_bytes(&i, sizeof(int));
    return m;
}

ibinstream& operator<<(ibinstream& m, double i)
{
    m.raw_bytes(&i, sizeof(double));
    return m;
}

ibinstream& operator<<(ibinstream& m, char c)
{
    m.raw_byte(c);
    return m;
}

template <class T>
ibinstream& operator<<(ibinstream& m, const T* p)
{
    return m << *p;
}

template <class T>
ibinstream& operator<<(ibinstream& m, const vector<T>& v)
{
    m << v.size();
    for (typename vector<T>::const_iterator it = v.begin(); it != v.end(); ++it) {
        m << *it;
    }
    return m;
}

template <>
ibinstream& operator<<(ibinstream& m, const vector<int>& v)
{
    m << v.size();
    m.raw_bytes(&v[0], v.size() * sizeof(int));
    return m;
}

template <>
ibinstream& operator<<(ibinstream& m, const vector<double>& v)
{
    m << v.size();
    m.raw_bytes(&v[0], v.size() * sizeof(double));
    return m;
}

template <class T>
ibinstream& operator<<(ibinstream& m, const set<T>& v)
{
    m << v.size();
    for (typename set<T>::const_iterator it = v.begin(); it != v.end(); ++it) {
        m << *it;
    }
    return m;
}

ibinstream& operator<<(ibinstream& m, const string& str)
{
    m << str.length();
    m.raw_bytes(str.c_str(), str.length());
    return m;
}

template <class KeyT, class ValT>
ibinstream& operator<<(ibinstream& m, const map<KeyT, ValT>& v)
{
    m << v.size();
    for (typename map<KeyT, ValT>::const_iterator it = v.begin(); it != v.end(); ++it) {
        m << it->first;
        m << it->second;
    }
    return m;
}
template <class KeyT, class ValT>
ibinstream& operator<<(ibinstream& m, const hash_map<KeyT, ValT>& v)
{
    m << v.size();
    for (typename hash_map<KeyT, ValT>::const_iterator it = v.begin(); it != v.end(); ++it) {
        m << it->first;
        m << it->second;
    }
    return m;
}

template <class T>
ibinstream& operator<<(ibinstream& m, const hash_set<T>& v)
{
    m << v.size();
    for (typename hash_set<T>::const_iterator it = v.begin(); it != v.end(); ++it) {
        m << *it;
    }
    return m;
}

class obinstream {
private:
    char* buf; //responsible for deleting the buffer, do not delete outside
    size_t size;
    size_t index;

public:
    obinstream(char* b, size_t s)
        : buf(b)
        , size(s)
        , index(0) {};
    obinstream(char* b, size_t s, size_t idx)
        : buf(b)
        , size(s)
        , index(idx) {};
    ~obinstream()
    {
        delete[] buf;
    }

    char raw_byte()
    {
        return buf[index++];
    }

    void* raw_bytes(unsigned int n_bytes)
    {
        char* ret = buf + index;
        index += n_bytes;
        return ret;
    }
};

obinstream& operator>>(obinstream& m, size_t& i)
{
    i = *(size_t*)m.raw_bytes(sizeof(size_t));
    return m;
}

obinstream& operator>>(obinstream& m, bool& i)
{
    i = *(bool*)m.raw_bytes(sizeof(bool));
    return m;
}

obinstream& operator>>(obinstream& m, int& i)
{
    i = *(int*)m.raw_bytes(sizeof(int));
    return m;
}

obinstream& operator>>(obinstream& m, double& i)
{
    i = *(double*)m.raw_bytes(sizeof(double));
    return m;
}

obinstream& operator>>(obinstream& m, char& c)
{
    c = m.raw_byte();
    return m;
}

template <class T>
obinstream& operator>>(obinstream& m, T*& p)
{
    p = new T;
    return m >> (*p);
}

template <class T>
obinstream& operator>>(obinstream& m, vector<T>& v)
{
    size_t size;
    m >> size;
    v.resize(size);
    for (typename vector<T>::iterator it = v.begin(); it != v.end(); ++it) {
        m >> *it;
    }
    return m;
}

template <>
obinstream& operator>>(obinstream& m, vector<int>& v)
{
    size_t size;
    m >> size;
    v.resize(size);
    int* data = (int*)m.raw_bytes(sizeof(int) * size);
    v.assign(data, data + size);
    return m;
}

template <>
obinstream& operator>>(obinstream& m, vector<double>& v)
{
    size_t size;
    m >> size;
    v.resize(size);
    double* data = (double*)m.raw_bytes(sizeof(double) * size);
    v.assign(data, data + size);
    return m;
}

template <class T>
obinstream& operator>>(obinstream& m, set<T>& v)
{
    size_t size;
    m >> size;
    for (size_t i = 0; i < size; i++) {
        T tmp;
        m >> tmp;
        v.insert(v.end(), tmp);
    }
    return m;
}

obinstream& operator>>(obinstream& m, string& str)
{
    size_t length;
    m >> length;
    str.clear();
    char* data = (char*)m.raw_bytes(length);
    str.append(data, length);
    return m;
}
template <class KeyT, class ValT>
obinstream& operator>>(obinstream& m, map<KeyT, ValT>& v)
{
    size_t size;
    m >> size;
    for (int i = 0; i < size; i++) {
        KeyT key;
        m >> key;
        m >> v[key];
    }
    return m;
}
template <class KeyT, class ValT>
obinstream& operator>>(obinstream& m, hash_map<KeyT, ValT>& v)
{
    size_t size;
    m >> size;
    for (int i = 0; i < size; i++) {
        KeyT key;
        m >> key;
        m >> v[key];
    }
    return m;
}

template <class T>
obinstream& operator>>(obinstream& m, hash_set<T>& v)
{
    size_t size;
    m >> size;
    for (int i = 0; i < size; i++) {
        T key;
        m >> key;
        v.insert(key);
    }
    return m;
}

#endif
