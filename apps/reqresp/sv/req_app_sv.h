#include "reqresp/req-dev.h"
#include "utils/type.h"
using namespace std;

//input line format: vertexID \t numOfNeighbors neighbor1 neighbor2 ...
//output line format: v \t min_vertexID(v's connected component)

//<V>=<D[v], star[v]>
//initially, D[v]=v, star[v]=false
struct SVValue_req
{
	int D;
	bool star;
	vector<VertexID> edges;
};

ibinstream & operator<<(ibinstream & m, const SVValue_req & v){
	m<<v.D;
	m<<v.star;
	m<<v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, SVValue_req & v){
	m>>v.D;
	m>>v.star;
	m>>v.edges;
	return m;
}

//====================================
class SVVertex_req:public RVertex<int, SVValue_req, int, int>
{
	void treeInit_D() {
		//set D[u]=min{v} to allow fastest convergence, though any v is ok (assuming (u, v) is accessed last)
		vector<VertexID> & edges=value().edges;
		for(int i=0; i<edges.size(); i++)
		{
			int nb=edges[i];
			if(nb<value().D) value().D=nb;
		}
	}

	void req_Du()
	{// request to w
		int Du=value().D;
		request(Du);
	} //respond logic: (1)int Dw=value().D; (2)return Dw

	void bcast_Dv()//in fact, one can use the ghost technique
	{// send negated D[v]
		int Dv=value().D;
		vector<VertexID> & edges=value().edges;
		for(int i=0; i<edges.size(); i++)
		{
			int nb=edges[i];
			send_message(nb, Dv);
		}
	}//in fact, a combiner with MIN operator can be used here

	// ========================================

	bool rtHook_check(MessageContainer & msgs) //return whether a msg is sent
	{//set D[w]=min_v{D[v]} to allow fastest convergence, though any D[v] is ok (assuming (u, v) is accessed last)
		int Du=value().D;
		int Dw=get_respond(Du);
		int Dv=-1;//pick the min
		for(int i=0; i<msgs.size(); i++)
		{
			int cur=msgs[i];
			if(Dv==-1 || cur<Dv) Dv=cur;
		}
		if(Dw==Du && Dv!=-1 && Dv<Du)//condition checking
		{
			send_message(Du, Dv);
			return true;
		}
		return false;
	}

	void rtHook_update(MessageContainer & msgs)// = starhook's write D[D[u]]
	{//set D[w]=min_v{D[v]} to allow fastest convergence, though any D[v] is ok (assuming (u, v) is accessed last)
		int Dv=-1;
		for(int i=0; i<msgs.size(); i++)
		{
			int cur=msgs[i];
			if(Dv==-1 || cur<Dv) Dv=cur;
		}
		if(Dv!=-1) value().D=Dv;
	}

	// ========================================

	void starHook_update(MessageContainer & msgs)// set star[u] first
	{//set D[w]=min_v{D[v]} to allow fastest convergence
		if(value().star)
		{
			int Du=value().D;
			int Dv=-1;
			for(int i=0; i<msgs.size(); i++)
			{
				int cur=msgs[i];
				if(Dv==-1 || cur<Dv) Dv=cur;
			}
			if(Dv!=-1 && Dv<Du)//condition checking
			{
				send_message(Du, Dv);
			}
		}
	}

	// ========================================

	void shortcut_update() {//D[u]=D[D[u]]
		value().D=get_respond(value().D);
	}

	// ========================================

	void setStar_notify(MessageContainer & msgs) {
		int Du=value().D;
		int Dw=get_respond(Du);
		if(Du!=Dw)
		{
			value().star=false;
			//notify Du
			send_message(Du, -1);//-1 means star_notify
			//notify Dw
			send_message(Dw, -1);
		}
	}

	void setStar_update(MessageContainer & msgs) {
		if(msgs.size()>0) value().star=false;
		request(value().D);
	} //respond logic: (1)bool star=value().star; (2)return star

	void setStar_final()
	{
		value().star=get_respond(value().D);
	}

	//==================================================

	public:

		virtual int respond()
		{
			int step=step_num();
			if(step%5==0) return value().star;
			else return value().D;
		}

		virtual void compute(MessageContainer & messages)
		{
			int cycle = 10;
			if(step_num() == 1)
			{
				treeInit_D();
				req_Du();
				//resp D
				bcast_Dv();
			}
			else if(step_num() % cycle == 2)
			{
				//============== end condition ==============
				bool* agg=(bool*)getAgg();
				bool msg_sent = rtHook_check(messages);
				if(msg_sent)
				{
					wakeAll();
				}
				else if(*agg)
				{
					vote_to_halt();
					return;
				}
				//===========================================
			}
			else if(step_num() % cycle == 3)
			{
				rtHook_update(messages);
				value().star=true;
				req_Du();
				//resp D
			}
			else if(step_num() % cycle == 4)
			{
				setStar_notify(messages);
			}
			else if(step_num() % cycle == 5)
			{
				setStar_update(messages);
				//resp star
				bcast_Dv();
			}
			else if(step_num() % cycle == 6)
			{
				setStar_final();
				starHook_update(messages);//set star[v] first
			}
			else if(step_num() % cycle == 7)
			{
				rtHook_update(messages);
				req_Du();
				//resp D
			}
			else if(step_num() % cycle == 8)
			{
				shortcut_update();
				value().star=true;
				req_Du();
				//resp D
			}
			else if(step_num() % cycle == 9)
			{
				setStar_notify(messages);
			}
			else if(step_num() % cycle == 0)
			{
				setStar_update(messages);
				//resp star
			}
			else if(step_num() % cycle == 1)
			{
				setStar_final();
				req_Du();
				//resp D
				bcast_Dv();
			}
		}
};

//====================================

class SVAgg_req:public Aggregator<SVVertex_req, bool, bool>
{
	private:
		bool AND;
	public:
		virtual void init(){
			AND=true;
		}

		virtual void stepPartial(SVVertex_req* v)
		{
			if(v->value().star==false) AND=false;
		}

		virtual void stepFinal(bool* part)
		{
			if(*part==false) AND=false;
		}

		virtual bool* finishPartial(){ return &AND; }
		virtual bool* finishFinal(){ return &AND; }
};

//====================================

class SVWorker_req:public RWorker<SVVertex_req, SVAgg_req>
{
	char buf[100];

	public:

	virtual SVVertex_req* toVertex(char* line)
	{
		char * pch;
		pch=strtok(line, "\t");
		SVVertex_req* v=new SVVertex_req;
		v->id=atoi(pch);
		pch=strtok(NULL, " ");
		int num=atoi(pch);
		for(int i=0; i<num; i++)
		{
			pch=strtok(NULL, " ");
			v->value().edges.push_back(atoi(pch));
		}
		v->value().D=v->id;
		v->value().star=false;//strictly speaking, this should be true
		//after treeInit_D(), should do star-checking
		//however, this is time-consuming, and it's very unlikely that treeInit_D() gives stars
		//therefore, set false here to save the first star-checking
		return v;
	}

	virtual void toline(SVVertex_req* v, BufferedWriter & writer)
	{
		sprintf(buf, "%d\t%d\n", v->id, v->value().D);
		writer.write(buf);
	}
};

void req_sv(string in_path, string out_path)
{
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=false;
	SVWorker_req worker;
	SVAgg_req agg;
	worker.setAggregator(&agg);
	worker.run(param);
}
