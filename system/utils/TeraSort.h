#ifndef TERASORT_H_
#define TERASORT_H_

//if "prefix sum" is activated, content is prefixed with "orderID "

#include "ydhdfs.h"
#include "global.h"
#include "serialization.h"
#include "communication.h"
#include "time.h"
#include <vector>
#include <iostream>

template <class T>
struct TeraItem {
    T key;
    string content;
};

template <class T>
ibinstream& operator<<(ibinstream& m, const TeraItem<T>& v)
{
    m << v.key;
    m << v.content;
    return m;
}

template <class T>
obinstream& operator>>(obinstream& m, TeraItem<T>& v)
{
    m >> v.key;
    m >> v.content;
    return m;
}

template <class T>
class TeraWorker {
public:
    typedef TeraItem<T> VertexT;
    typedef vector<VertexT*> VertexContainer;
    typedef typename VertexContainer::iterator VertexIter;

    VertexContainer vertexes;
    double samp_rate;
    bool prefix;

    TeraWorker(double sampling_rate, bool prefixsum)
    {
        srand((unsigned)time(NULL));
        samp_rate = sampling_rate;
        prefix = prefixsum;
    }

    ~TeraWorker()
    {
        for (int i = 0; i < vertexes.size(); i++)
            delete vertexes[i];
    }

    inline void add_vertex(TeraItem<T>* vertex)
    {
        vertexes.push_back(vertex);
    }

    //user-defined graphLoader ==============================
    virtual TeraItem<T>* toVertex(char* line) = 0; //this is what user specifies!!!!!!

    void load_vertex(TeraItem<T>* v)
    { //called by load_graph
        add_vertex(v);
    }

    void load_graph(const char* inpath)
    {
        hdfsFS fs = getHdfsFS();
        hdfsFile in = getRHandle(inpath, fs);
        LineReader reader(fs, in);
        while (true) {
            reader.readLine();
            if (!reader.eof())
                load_vertex(toVertex(reader.getLine()));
            else
                break;
        }
        hdfsCloseFile(fs, in);
        hdfsDisconnect(fs);
        //cout<<"Worker "<<_my_rank<<": \""<<inpath<<"\" loaded"<<endl;//DEBUG !!!!!!!!!!
    }
    //=======================================================

    //user-defined graphDumper ==============================
    virtual void toline(VertexT* v) = 0; //this is what user specifies!!!!!!

    vector<char> buf;
    void write(const char* content)
    {
        int len = strlen(content);
        buf.insert(buf.end(), content, content + len);
    }

    void dump_partition(const char* outpath)
    {
        hdfsFS fs = getHdfsFS();
        char tmp[5];
        sprintf(tmp, "%d", _my_rank);
        hdfsFile hdl = getWHandle((string(outpath) + "/part_" + string(tmp)).c_str(), fs);
        for (int i = 0; i < vertexes.size(); i++) {
            VertexT* v = vertexes[i];
            if (buf.size() >= HDFS_BLOCK_SIZE) {
                tSize numWritten = hdfsWrite(fs, hdl, &buf[0], buf.size());
                if (numWritten == -1) {
                    fprintf(stderr, "Failed to write file!\n");
                    exit(-1);
                }
                buf.clear();
            }
            toline(v);
        }
        tSize numWritten = hdfsWrite(fs, hdl, &buf[0], buf.size());
        if (numWritten == -1) {
            fprintf(stderr, "Failed to write file!\n");
            exit(-1);
        }
        buf.clear();
        if (hdfsFlush(fs, hdl)) {
            fprintf(stderr, "Failed to flush");
            exit(-1);
        }
        hdfsCloseFile(fs, hdl);
        hdfsDisconnect(fs);
    }
    //=======================================================

    void key_sampling(vector<T>& splits) //key<=splits[i], to machine i
    {
        ResetTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << "============= Step 1: Key Sampling =============" << endl;
        vector<T> samples;
        for (int i = 0; i < vertexes.size(); i++) {
            double samp = ((double)rand()) / RAND_MAX;
            if (samp <= samp_rate) {
                samples.push_back(vertexes[i]->key);
            }
        }
        if (_my_rank != MASTER_RANK) {
            //collect samples
            slaveGather(samples);
            samples.clear();
            //bcast samples
            slaveBcast(splits);
        } else {
            //collect samples
            vector<vector<T> > sampBufs(_num_workers);
            masterGather(sampBufs);
            for (int i = 0; i < _num_workers; i++) {
                if (i != MASTER_RANK) {
                    samples.insert(samples.end(), sampBufs[i].begin(), sampBufs[i].end());
                }
            }
            //sort samples
            sort(samples.begin(), samples.end());
            //compute splits
            int gap = samples.size() / _num_workers;
            if (gap == 0)
                splits.swap(samples);
            else {
                int residual = samples.size() % _num_workers;
                int pos = 0;
                for (int i = 0; i < _num_workers - 1; i++) //last sample may not be the largest item
                {
                    int step = gap;
                    if (residual > 0) {
                        step++;
                        residual--;
                    }
                    splits.push_back(samples[pos + step - 1]);
                    pos += step;
                }
            }
            //bcast samples
            masterBcast(splits);
        }
        StopTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << get_timer(4) << " seconds elapsed" << endl;
    }

    //=======================================================
    int getWorker(T key, vector<T>& splits)
    {
        if (splits.size() == 0)
            return 0;
        T last = splits.back();
        if (key > last)
            return _num_workers - 1;
        //////
        int pos = 0;
        for (int i = 0; i < splits.size(); i++) {
            if (key < splits[i] || key == splits[i])
                return i;
        }
        return _num_workers - 1;
    }

    //--------------------------------------------------
    static bool VPointerComp(const VertexT* lhs, const VertexT* rhs)
    {
        return lhs->key < rhs->key;
    };

    void vertexExchange(vector<T>& splits)
    {
        ResetTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << "============= Step 2: Vertex Exchange and Sort =============" << endl;
        vector<VertexContainer> buf(_num_workers);
        for (int i = 0; i < vertexes.size(); i++) {
            VertexT* v = vertexes[i];
            int wid = getWorker(v->key, splits);
            buf[wid].push_back(v);
        }
        all_to_all(buf);
        //free sent vertices
        for (int i = 0; i < vertexes.size(); i++) {
            VertexT* v = vertexes[i];
            int wid = getWorker(v->key, splits);
            if (wid != _my_rank)
                delete v;
        }
        //collect received vertices
        vertexes.clear();
        for (int i = 0; i < _num_workers; i++) {
            vertexes.insert(vertexes.end(), buf[i].begin(), buf[i].end());
        }
        //sort assigned vertices
        sort(vertexes.begin(), vertexes.end(), VPointerComp);
        StopTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << get_timer(4) << " seconds elapsed" << endl;
    }

    //=======================================================
    void prefixOrder()
    {
        ResetTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << "============= Step 3: Prefix Ordering =============" << endl;
        vector<int> sizes;
        int pre = 0;
        if (_my_rank != MASTER_RANK) {
            //collect sizes
            int size = vertexes.size();
            slaveGather(size);
            //get prefix number
            slaveScatter(pre);
        } else {
            //collect sizes
            vector<int> sizebuf(_num_workers);
            sizebuf[_my_rank] = vertexes.size();
            masterGather(sizebuf);
            //compute prefixes
            vector<int> pref(_num_workers);
            pref[0] = 0;
            for (int i = 1; i < _num_workers; i++) {
                pref[i] = pref[i - 1] + sizebuf[i - 1];
            }
            //bcast prefix number
            masterScatter(pref);
            pre = pref[_my_rank];
        }
        //add pre to get order, append to content string
        char tmp[20];
        for (int i = 0; i < vertexes.size(); i++) {
            int order = pre + i;
            sprintf(tmp, "%d", order);
            VertexT* v = vertexes[i];
            v->content = string(tmp) + " " + v->content;
        }
        StopTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << get_timer(4) << " seconds elapsed" << endl;
    }

    // run the worker
    void run(const WorkerParams& params)
    {
        //check path + init
        if (_my_rank == MASTER_RANK) {
            if (dirCheck(params.input_path.c_str(), params.output_path.c_str(), _my_rank == MASTER_RANK, params.force_write) == -1)
                exit(-1);
        }
        init_timers();

        //dispatch splits
        ResetTimer(WORKER_TIMER);
        vector<vector<string> >* arrangement;
        if (_my_rank == MASTER_RANK) {
            arrangement = params.native_dispatcher ? dispatchLocality(params.input_path.c_str()) : dispatchRan(params.input_path.c_str());
            //reportAssignment(arrangement);//DEBUG !!!!!!!!!!
            masterScatter(*arrangement);
            vector<string>& assignedSplits = (*arrangement)[0];
            //reading assigned splits (map)
            for (vector<string>::iterator it = assignedSplits.begin();
                 it != assignedSplits.end(); it++)
                load_graph(it->c_str());
            delete arrangement;
        } else {
            vector<string> assignedSplits;
            slaveScatter(assignedSplits);
            //reading assigned splits (map)
            for (vector<string>::iterator it = assignedSplits.begin();
                 it != assignedSplits.end(); it++)
                load_graph(it->c_str());
        }
        worker_barrier();
        StopTimer(WORKER_TIMER);
        PrintTimer("Load Time", WORKER_TIMER);

        //=========================================================

        init_timers();
        ResetTimer(WORKER_TIMER);
        vector<T> splits;
        key_sampling(splits);
        vertexExchange(splits);
        if (prefix)
            prefixOrder();
        worker_barrier();
        StopTimer(WORKER_TIMER);
        PrintTimer("Communication Time", COMMUNICATION_TIMER);
        PrintTimer("- Serialization Time", SERIALIZATION_TIMER);
        PrintTimer("- Transfer Time", TRANSFER_TIMER);
        PrintTimer("Total Computational Time", WORKER_TIMER);

        // dump graph
        ResetTimer(WORKER_TIMER);
        dump_partition(params.output_path.c_str());
        worker_barrier();
        StopTimer(WORKER_TIMER);
        PrintTimer("Dump Time", WORKER_TIMER);
    }
};

#endif