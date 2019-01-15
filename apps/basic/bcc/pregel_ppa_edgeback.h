#include "basic/pregel-dev.h"
#include "utils/type.h"
using namespace std;

//input 1 (from spantree):
//v \t color parent #

//input 2 (from listrank2):
//v \t pre(v) nd(v) $

//input 3 (from case1mark):
//<color, pre1, pre2> \t newColor_pair

//output format:
//- (u, v) \t color newColor_pair

//<I> = <ID, -2, -2>, or <ID, -1, -1>, or <color, pre1, pre2>

//step 1:
//- <ID, -2, -2> sends (color) to <ID, -1, -1>, and vote to halt
//step 2:
//- <ID, -1, -1> gets (color), creates vertices <color, pre, -1> with value (ID), and vote to halt
//- <color, pre1, pre2> sends to <color, pre1, -1> for <ID1>
//- <color, pre1, pre2> sends to <color, pre2, -1> for <ID2>
//step 3:
//- <color, pre1, -1> responses, and vote to halt
//step 4:
//- <color, pre1, pre2> records <ID1, ID2, newColor_pair>

//<V> = <ID1, ID2, newColor_pair>

struct quad
{
	int v1;
	int v2;
	int v3;
	int v4;

	quad(){}

	quad(int v1, int v2, int v3, int v4)
	{
		this->v1=v1;
		this->v2=v2;
		this->v3=v3;
		this->v4=v4;
	}
};

ibinstream & operator<<(ibinstream & m, const quad & v){
	m<<v.v1;
	m<<v.v2;
	m<<v.v3;
	m<<v.v4;
	return m;
}

obinstream & operator>>(obinstream & m, quad & v){
	m>>v.v1;
	m>>v.v2;
	m>>v.v3;
	m>>v.v4;
	return m;
}

class EBackVertex_ppa:public Vertex<inttriplet, quad, inttriplet, IntTripletHash>
{
	public:
		virtual void compute(MessageContainer & messages)
		{
			if(step_num()==1)
			{
				if(id.v3==-2)
				{
					int vid=id.v1;
					inttriplet tmp(vid, -1, -1);
					inttriplet msg(value().v1, -1, -1);
					send_message(tmp, msg);
					vote_to_halt();
				}
			}
			else if(step_num()==2)
			{
				if(id.v3==-1)
				{
					int pre=value().v1;
					for(int i=0; i<messages.size(); i++)
					{//at most one
						inttriplet message=messages[i];
						int color=message.v1;
						inttriplet nid(color, pre, -1);
						quad val(id.v1, -1, -1, -1);//vid
						EBackVertex_ppa * vertex=new EBackVertex_ppa();
						vertex->id=nid;
						vertex->value()=val;
						add_vertex(vertex);
						vote_to_halt();
					}
				}
				else//<color, pre1, pre2>
				{
					int color=id.v1;
					int pre1=id.v2;
					int pre2=id.v3;
					inttriplet tmp(color, pre1, -1);
					inttriplet msg(pre1, pre2, 1);//(pre1, pre2) denotes the sender, 1 means request for ID1
					send_message(tmp, msg);
					tmp.set(color, pre2, -1);
					msg.set(pre1, pre2, 2);//2 means request for ID1
					send_message(tmp, msg);
				}
			}
			else if(step_num()==3)
			{
				if(id.v3==-1)
				{
					int color=id.v1;
					int vid=value().v1;
					for(int i=0; i<messages.size(); i++)
					{
						inttriplet message=messages[i];
						if(message.v3==1)
						{
							inttriplet tmp(color, message.v1, message.v2);
							inttriplet msg(vid, -1, 1);
							send_message(tmp, msg);
						}
						else//message.K3==2
						{
							inttriplet tmp(color, message.v1, message.v2);
							inttriplet msg(vid, -1, 2);
							send_message(tmp, msg);
						}
					}
					vote_to_halt();
				}
			}
			else
			{
				for(int i=0; i<messages.size(); i++)
				{
					inttriplet message=messages[i];
					if(message.v3==1) value().v1=message.v1;
					else value().v2=message.v1;
				}
				vote_to_halt();
			}
		}
};

class EBackWorker_ppa:public Worker<EBackVertex_ppa>
{
	char buf[100];

	public:

		virtual EBackVertex_ppa* toVertex(char* line)
		{
			EBackVertex_ppa* v=new EBackVertex_ppa;
			char * pch;
			int len=strlen(line);
			char c=line[len-1];
			if(c=='$')
			{
				pch=strtok(line, "\t");
				v->id.v1=atoi(pch);
				v->id.v2=-1;
				v->id.v3=-1;
				pch=strtok(NULL, " ");
				v->value().v1=atoi(pch);//color
			}
			else if(c=='#')
			{
				pch=strtok(line, "\t");
				v->id.v1=atoi(pch);
				v->id.v2=-2;
				v->id.v3=-2;
				pch=strtok(NULL, " ");
				v->value().v1=atoi(pch);//pre
			}
			else
			{
				//<color, pre1, pre2> \t newColor
				pch=strtok(line, " ");
				v->id.v1=atoi(pch);
				pch=strtok(NULL, " ");
				v->id.v2=atoi(pch);
				pch=strtok(NULL, "\t");
				v->id.v3=atoi(pch);
				pch=strtok(NULL, " ");
				v->value().v3=atoi(pch);
				pch=strtok(NULL, " ");
				v->value().v4=atoi(pch);
			}
			return v;
		}

		virtual void toline(EBackVertex_ppa* v, BufferedWriter & writer)
		{
			if(v->id.v3>=0)
			{
				sprintf(buf, "%d %d\t%d %d %d\n", v->value().v1, v->value().v2, v->id.v1, v->value().v3, v->value().v4);
				writer.write(buf);
			}
		}
};

void ppa_eback(MultiInputParams & param)
{
	EBackWorker_ppa worker;
	worker.run(param);
}
