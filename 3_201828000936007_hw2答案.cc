#include <set>
#include <map>
#include <cmath>
#include <cstdio>
#include <random>
#include <vector>
#include <string>
#include <climits>
#include <cstdlib>
#include <iostream>
#include <algorithm>
using namespace std;
typedef unsigned long long ull;

#include "GraphLite.h"

#define VERTEX_CLASS_NAME(name) DirectedTriangleCount##name

class VERTEX_CLASS_NAME(InputFormatter) : public InputFormatter
{
	public:
		int64_t getVertexNum()
		{
			ull n;
			sscanf(m_ptotal_vertex_line, "%lld", &n);
			m_total_vertex = n;
			return m_total_vertex;
		}
		int64_t getEdgeNum()
		{
			ull n;
			sscanf(m_ptotal_edge_line, "%lld", &n);
			m_total_edge = n;
			return m_total_edge;
		}
		int getVertexValueSize()
		{
			m_n_value_size = sizeof(double);
			return m_n_value_size;
		}
		int getEdgeValueSize()
		{
			m_e_value_size = sizeof(double);
			return m_e_value_size;
		}
		int getMessageValueSize()
		{
			m_m_value_size = sizeof(double);
			return m_m_value_size;
		}
		void loadGraph()
		{
			ull last_vertex;
			ull from;
			ull to;
			double weight = 0;

			double value = 1;
			int outdegree = 0;

			const char *line = getEdgeLine();

			// Note: modify this if an edge weight is to be read
			//       modify the 'weight' variable

			sscanf(line, "%lld %lld", &from, &to);
			addEdge(from, to, &weight);

			last_vertex = from;
			++outdegree;
			for (int64_t i = 1; i < m_total_edge; ++i)
			{
				line = getEdgeLine();

				// Note: modify this if an edge weight is to be read
				//       modify the 'weight' variable

				sscanf(line, "%lld %lld", &from, &to);
				if (last_vertex != from)
				{
					addVertex(last_vertex, &value, outdegree);
					last_vertex = from;
					outdegree = 1;
				}
				else
				{
					++outdegree;
				}
				addEdge(from, to, &weight);
			}
			addVertex(last_vertex, &value, outdegree);
		}
};

int cntA = 0, cntB = 0;
int testA = 0, testB = 0;

class VERTEX_CLASS_NAME(OutputFormatter) : public OutputFormatter
{
	public:
		void writeResult()
		{
			int n = 0;
			char s[1024];
			const char *ss[10] = {"in", "out", "through", "cycle"};
			for (int i = 0; i < 4; i++)
			{
				n = sprintf(s, "%s: %d\n", ss[i], (i < 3 ? cntB : cntA));
				writeNextResLine(s, n);
			}
		}
};

// An aggregator that records a double value tom compute sum
class VERTEX_CLASS_NAME(Aggregator) : public Aggregator<ull>
{
	public:
		void init()
		{
			m_global = 0;
			m_local = 0;
		}
		void *getGlobal()
		{
			return &m_global;
		}
		void setGlobal(const void *p)
		{
			m_global = *(int *)p;
		}
		void *getLocal()
		{
			return &m_local;
		}
		void merge(const void *p)
		{
			m_global += *(int *)p;
		}
		void accumulate(const void *p)
		{
			m_local += *(int *)p;
		}
};

class VERTEX_CLASS_NAME() : public Vertex<double, double, double>
{
	public:
		map<int64_t, vector<int64_t>> miv; // fatherSet
		void compute(MessageIterator *pmsgs)
		{
			int64_t val = getVertexId();
			if (getSuperstep() == 0)
			{
				sendMessageToAllNeighbors(getVertexId());
			}
			else if (getSuperstep() == 1)
			{
				val = getVertexId();
				vector<int64_t> &neigh = miv[val];

				for (; !pmsgs->done(); pmsgs->next())
				{
					int64_t msg = pmsgs->getValue();
					neigh.push_back(msg);
					msg = (msg << 1); // lowesrt bit 0 is In
					sendMessageToAllNeighbors(msg);
				}

				OutEdgeIterator it = getOutEdgeIterator();
				for (; !it.done(); it.next())
				{
					// Get current edge target vertex id
					int64_t msg = it.target();
					msg = (msg << 1 | 1); //lowsrt 1 is Out
					//sendMessageToAllNeighbors(msg);
				}
			}
			else if (getSuperstep() == 2)
			{
				set<int64_t> sin, sout, stol, scur, uni;
				sin.clear();
				sout.clear();
				stol.clear();
				scur.clear();

				val = getVertexId();
				vector<int64_t> &neigh = miv[val];
				vector<int64_t> TwoJumps, store;

				for (; !pmsgs->done(); pmsgs->next())
				{
					int64_t msg = pmsgs->getValue();
					// if(msg<=1) continue;
					int64_t dir = (msg & 1), tar = (msg >> 1);

					stol.insert(tar);
					if (dir == 0)
						sin.insert(tar);
					else if (dir == 1)
						sout.insert(tar);
					TwoJumps.push_back(tar);
				}

				OutEdgeIterator it = getOutEdgeIterator();
				for (; !it.done(); it.next())
				{
					// cout << it.target() << endl;
					int64_t tit = it.target();
					scur.insert(tit);
					for (vector<int64_t>::iterator iter = TwoJumps.begin();
							iter != TwoJumps.end(); ++iter)
					{
						if (*iter == tit)
						{
							int addition = 1;
							accumulateAggr(0, &addition);
						}
					}
				}

				for (vector<int64_t>::iterator jit = TwoJumps.begin(); jit != TwoJumps.end(); ++jit)
				{
					if (find(neigh.begin(), neigh.end(), *jit) != neigh.end())
					{
						int addition = 1;
						accumulateAggr(1, &addition);
					}
				}
			}
			else if (getSuperstep() >= 3)
			{
				cntA = *(int *)getAggrGlobal(0);
				cntB = *(int *)getAggrGlobal(1);
				voteToHalt();
				return;
			}
			*mutableValue() = val;
		}
};

class VERTEX_CLASS_NAME(Graph) : public Graph
{
	public:
		VERTEX_CLASS_NAME(Aggregator) * aggregator;

	public:
		// argv[0]: PageRankVertex.so
		// argv[1]: <input path>
		// argv[2]: <output path>
		void init(int argc, char *argv[])
		{

			setNumHosts(5);
			setHost(0, "localhost", 1411);
			setHost(1, "localhost", 1421);
			setHost(2, "localhost", 1431);
			setHost(3, "localhost", 1441);
			setHost(4, "localhost", 1451);

			if (argc < 3)
			{
				printf("Usage: %s <input path> <output path>\n", argv[0]);
				exit(1);
			}

			m_pin_path = argv[1];
			m_pout_path = argv[2];

			aggregator = new VERTEX_CLASS_NAME(Aggregator)[2];
			regNumAggr(2);
			regAggr(0, &aggregator[0]);
			regAggr(1, &aggregator[1]);
		}

		void term()
		{
			delete[] aggregator;
		}
};

/* STOP: do not change the code below. */
extern "C" Graph *create_graph()
{
	Graph *pgraph = new VERTEX_CLASS_NAME(Graph);

	pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
	pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
	pgraph->m_pver_base = new VERTEX_CLASS_NAME();

	return pgraph;
}

extern "C" void destroy_graph(Graph *pobject)
{
	delete (VERTEX_CLASS_NAME() *)(pobject->m_pver_base);
	delete (VERTEX_CLASS_NAME(OutputFormatter) *)(pobject->m_pout_formatter);
	delete (VERTEX_CLASS_NAME(InputFormatter) *)(pobject->m_pin_formatter);
	delete (VERTEX_CLASS_NAME(Graph) *)pobject;
}
