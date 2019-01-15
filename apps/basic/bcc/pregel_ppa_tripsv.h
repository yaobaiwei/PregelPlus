#include "basic/pregel-dev.h"
#include "utils/type.h"
using namespace std;

//input line format:
//(<color, pre1, pre2>) \t (dst1=<pre1, pre2>)_(dst2=<pre1, pre2>)_...

//output line format:
//(<color, pre1, pre2>) \t color (dst1=<pre1, pre2>)_(dst2=<pre1, pre2>)_...

//color is originally intpair
//converted to int by aggregator at last

struct TSVValue_ppa
{
	intpair D;
	bool star;
	vector<intpair> edges;
};

ibinstream & operator<<(ibinstream & m, const TSVValue_ppa & v)
{
	m << v.D;
	m << v.star;
	m << v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, TSVValue_ppa & v)
{
	m >> v.D;
	m >> v.star;
	m >> v.edges;
	return m;
}

//====================================

intpair nullintpair = intpair(-1, -1);

class TSVVertex_ppa: public Vertex<inttriplet, TSVValue_ppa, intpair,
		IntTripletHash>
{

	intpair to_intpair(const inttriplet& trip)
	{
		return intpair(trip.v2, trip.v3);
	}
	void sendMsg(intpair vertex, intpair msg)
	{
		inttriplet trip = inttriplet(id.v1, vertex.v1, vertex.v2);
		send_message(trip, msg);
	}
	void treeInit_D()
	{
		//set D[u]=min{v} to allow fastest convergence, though any v is ok (assuming (u, v) is accessed last)
		vector<intpair> & edges = value().edges;
		for (int i = 0; i < edges.size(); i++)
		{
			intpair nb = edges[i];
			if (nb < value().D)
				value().D = nb;
		}
	}

	// ========================================

	//w = Du

	void rtHook_1S() // = shortcut's request to w
	{ // request to w
		intpair Du = value().D;
		sendMsg(Du, to_intpair(id));
	}

	void rtHook_2R(MessageContainer & msgs) // = shortcut's respond by w
	{ // respond by w
		intpair Dw = value().D;
		for (int i = 0; i < msgs.size(); i++)
		{
			intpair requester = msgs[i];
			sendMsg(requester, Dw);
		}
	}

	void rtHook_2S() // = starhook's send D[v]
	{ // send negated D[v]
		intpair Dv = value().D;
		vector<intpair> & edges = value().edges;
		for (int i = 0; i < edges.size(); i++)
		{
			intpair nb = edges[i];
			//send_message(nb, -Dv - 1); //negate Dv to differentiate it from other msg types
			sendMsg(nb, intpair(-Dv.v1 - 1, -Dv.v2 - 1));
		}
	} //in fact, a combiner with MIN operator can be used here

	void rtHook_3GDS(MessageContainer & msgs)
	{ //set D[w]=min_v{D[v]} to allow fastest convergence, though any D[v] is ok (assuming (u, v) is accessed last)
		intpair Dw = nullintpair;
		intpair Du = value().D;
		intpair Dv = nullintpair; //pick the min
		for (int i = 0; i < msgs.size(); i++)
		{
			intpair msg = msgs[i];
			if (msg.v1 >= 0)
				Dw = msg;
			else // type==rtHook_2R_v
			{
				intpair cur = intpair(-msg.v1 - 1, -msg.v2 - 1);
				if (Dv == nullintpair || cur < Dv)
					Dv = cur;
			}
		}
		if (Dw == Du && Dv != nullintpair && Dv < Du) //condition checking
		{
			sendMsg(Du, Dv);
		}
	}

	void rtHook_4GD(MessageContainer & msgs) // = starhook's write D[D[u]]
	{ //set D[w]=min_v{D[v]} to allow fastest convergence, though any D[v] is ok (assuming (u, v) is accessed last)
		intpair Dv = nullintpair;
		for (int i = 0; i < msgs.size(); i++)
		{
			intpair cur = msgs[i];
			if (Dv == nullintpair || cur < Dv)
				Dv = cur;
		}
		if (Dv != nullintpair)
			value().D = Dv;
	}

	// ========================================

	// call rtHook_2S()

	void starHook_3GDS(vector<intpair> & msgs) // set star[u] first
	{ //set D[w]=min_v{D[v]} to allow fastest convergence
		if (value().star)
		{
			intpair Du = value().D;
			intpair Dv = nullintpair;
			for (int i = 0; i < msgs.size(); i++)
			{
				intpair cur = msgs[i];
				if (Dv == nullintpair || cur < Dv)
					Dv = cur;
			}
			if (Dv != nullintpair && Dv < Du) //condition checking
			{
				sendMsg(Du, Dv);
			}
		}
	}

	// call rtHook_4GD

	// ========================================

	// call rtHook_1S
	// call rtHook_2R

	void shortcut_3GD(MessageContainer & msgs)
	{ //D[u]=D[D[u]]
		value().D = msgs[0];
	}

	// ========================================

	void setStar_1S()
	{
		value().star = true;
		intpair Du = value().D;
		sendMsg(Du, to_intpair(id));
	}

	void setStar_2R(MessageContainer & msgs)
	{
		intpair Dw = value().D;
		for (int i = 0; i < msgs.size(); i++)
		{
			intpair requester = msgs[i];
			sendMsg(requester, Dw);
		}
	}

	void setStar_3GDS(MessageContainer & msgs)
	{
		intpair Du = value().D;
		intpair Dw = msgs[0];
		if (Du != Dw)
		{
			value().star = false;
			//notify Du
			sendMsg(Du, nullintpair); //-1 means star_notify
			//notify Dw
			sendMsg(Dw, nullintpair);
		}
		sendMsg(Du, to_intpair(id));
	}

	void setStar_4GDS(MessageContainer & msgs)
	{
		vector<intpair> requesters;
		for (int i = 0; i < msgs.size(); i++)
		{
			intpair msg = msgs[i];
			if (msg == nullintpair)
				value().star = false;
			else
				requesters.push_back(msg); //star_request
		}
		bool star = value().star;
		for (int i = 0; i < requesters.size(); i++)
		{
			sendMsg(requesters[i], intpair(star, star));
		}
	}

	void setStar_5GD(MessageContainer & msgs)
	{
		for (int i = 0; i < msgs.size(); i++) //at most one
				{
			value().star = msgs[i].v1;
		}
	}

	void setStar_5GD_starhook(MessageContainer & messages,
			vector<intpair> & msgs)
	{
		for (int i = 0; i < messages.size(); i++)
		{
			intpair msg = messages[i];
			if (msg.v1 >= 0)
				value().star = msg.v1;
			else
				msgs.push_back(intpair(-msg.v1 - 1, -msg.v2 - 1));
		}
	}

public:

	virtual void compute(MessageContainer & messages)
	{

		int cycle = 14;
		if (step_num() == 1)
		{
			treeInit_D();
			rtHook_1S();
		}
		else if (step_num() % cycle == 2)
		{
			//============== end condition ==============
			bool* agg = (bool*)getAgg();
			if (*agg)
			{
				vote_to_halt();
				return;
			}
			//===========================================
			rtHook_2R(messages);
			rtHook_2S();
		}
		else if (step_num() % cycle == 3)
		{
			rtHook_3GDS(messages);
		}
		else if (step_num() % cycle == 4)
		{
			rtHook_4GD(messages);
			setStar_1S();
		}
		else if (step_num() % cycle == 5)
		{
			setStar_2R(messages);
		}
		else if (step_num() % cycle == 6)
		{
			setStar_3GDS(messages);
		}
		else if (step_num() % cycle == 7)
		{
			setStar_4GDS(messages);
			rtHook_2S();
		}
		else if (step_num() % cycle == 8)
		{
			vector<intpair> msgs;
			setStar_5GD_starhook(messages, msgs);
			starHook_3GDS(msgs); //set star[v] first
		}
		else if (step_num() % cycle == 9)
		{
			rtHook_4GD(messages);
			rtHook_1S();
		}
		else if (step_num() % cycle == 10)
		{
			rtHook_2R(messages);
		}
		else if (step_num() % cycle == 11)
		{
			shortcut_3GD(messages);
			setStar_1S();
		}
		else if (step_num() % cycle == 12)
		{
			setStar_2R(messages);
		}
		else if (step_num() % cycle == 13)
		{
			setStar_3GDS(messages);
		}
		else if (step_num() % cycle == 0)
		{
			setStar_4GDS(messages);
		}
		else if (step_num() % cycle == 1)
		{
			setStar_5GD(messages);
			rtHook_1S();
		}

	}
};

class TSVAgg_ppa: public Aggregator<TSVVertex_ppa, bool, bool>
{
private:
	bool AND;
public:
	virtual void init()
	{
		AND = true;
	}

	virtual void stepPartial(TSVVertex_ppa* v)
	{
		if (v->value().star == false)
			AND = false;
	}

	virtual void stepFinal(bool* part)
	{
		if (part == false)
			AND = false;
	}

	virtual bool* finishPartial()
	{
		return &AND;
	}
	virtual bool* finishFinal()
	{
		return &AND;
	}
};

class TSVWorker_ppa: public Worker<TSVVertex_ppa, TSVAgg_ppa>
{
	char buf[100];

public:
	//C version
	virtual TSVVertex_ppa* toVertex(char* line)
	{
		char * pch;
		pch = strtok(line, " ");
		TSVVertex_ppa* v = new TSVVertex_ppa;
		v->id.v1 = atoi(pch);
		pch = strtok(NULL, " ");
		v->id.v2 = atoi(pch);
		pch = strtok(NULL, "\t");
		v->id.v3 = atoi(pch);
		while (pch = strtok(NULL, " "))
		{
			intpair edge;
			edge.v1 = atoi(pch);
			pch = strtok(NULL, " ");
			edge.v2 = atoi(pch);
			v->value().edges.push_back(edge);
		}
		v->value().D = intpair(v->id.v2, v->id.v3);
		v->value().star = false; //strictly speaking, this should be true
		//after treeInit_D(), should do star-checking
		//however, this is time-consuming, and it's very unlikely that treeInit_D() gives stars
		//therefore, set false here to save the first star-checking
		return v;
	}

	virtual void toline(TSVVertex_ppa* v, BufferedWriter & writer)
	{
		sprintf(buf, "%d %d %d\t%d %d ", v->id.v1, v->id.v2, v->id.v3,
				v->value().D.v1,v->value().D.v2);
		writer.write(buf);
		vector<intpair> & nbs = v->value().edges;
		for (int i = 0; i < nbs.size(); i++)
		{
			sprintf(buf, "%d %d ", nbs[i].v1, nbs[i].v2);
			writer.write(buf);
		}
		writer.write("\n");
	}
};

void ppa_tripsv(string in_path, string out_path)
{
	WorkerParams param;
	param.input_path = in_path;
	param.output_path = out_path;
	param.force_write = true;
	param.native_dispatcher = false;
	TSVWorker_ppa worker;
	TSVAgg_ppa agg;
	worker.setAggregator(&agg);
	worker.run(param);
}
