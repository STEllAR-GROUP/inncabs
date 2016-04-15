#ifndef USE_SRB
  #ifdef INNCABS_USE_HPX
    #define srb hpx
  #else
    #define srb std
  #endif
#else
  #define hpx srb
#endif

#if defined(INNCABS_USE_HPX)
#ifdef USE_SRB
#error "Can't have both USE_HPX and USE_SRB"
#endif
#include <hpx/hpx_main.hpp>
#include <hpx/hpx.hpp>
#endif

#include "../include/inncabs.h"

#include <fstream>
#include <utility>
#include <cassert>
#include <set>
#include <queue>
#include <sstream>
#ifdef INNCABS_USE_HPX
#include <hpx/lcos/local/composable_guard.hpp>
#else
#define NO_HPX
#include "/home/sbrandt/src/hpx/hpx/lcos/local/composable_guard.hpp"
#include "/home/sbrandt/src/hpx/src/lcos/local/composable_guard.cpp"
#endif

#define MAX_PORTS 3

#ifdef INNCABS_USE_HPX
typedef boost::shared_ptr<hpx::lcos::local::guard> guard_ptr;
using hpx::lcos::local::guard_set;
#else
typedef hpx::lcos::local::guard_set guard_set;
typedef smem::shared_ptr<hpx::lcos::local::guard> guard_ptr;
#endif

struct guard : public guard_ptr {
  guard() : guard_ptr(new hpx::lcos::local::guard()) {}
  ~guard() {}
};

using namespace std;

// some type aliases
typedef char Symbol;

class Cell;

// a type forming half a wire connection
struct Port {
	std::shared_ptr<Cell> cell;
	unsigned port;
	Port(std::shared_ptr<Cell> cell = 0, unsigned port = 0)
	 : cell(cell), port(port) {}

	operator std::shared_ptr<Cell>() const { return cell; }
	std::shared_ptr<Cell> operator->() const { return cell; }
};

/**
 * An abstract base class for all cells.
 */
class Cell {
	Symbol symbol;
	unsigned numPorts;
	Port ports[MAX_PORTS];
  guard glock;
	bool alive;

public:
	Cell(Symbol symbol, unsigned numPorts)
		: symbol(symbol), numPorts(numPorts), alive(true) {
		assert(numPorts > 0 && numPorts <= MAX_PORTS);
		for(unsigned i=0; i<numPorts; i++) {
			ports[i] = Port();
		}
	}

	virtual ~Cell() {};

	void die() {
		alive = false;
    for(int i=0;i<MAX_PORTS;i++)
      ports[i].cell = nullptr;
	}

	bool dead() {
		return !alive;
	}

	Symbol getSymbol() const { return symbol; }

	unsigned getNumPorts() const { return numPorts; }

	const Port& getPrinciplePort() const {
		return getPort(0);
	}

	void setPrinciplePort(const Port& wire) {
		setPort(0, wire);
	}

	const Port& getPort(unsigned index) const {
		assert(index < numPorts);
		return ports[index];
	}

	void setPort(unsigned index, const Port& wire) {
    assert(this != nullptr);
		assert(index < numPorts);
		ports[index] = wire;
	}

	guard getLock() {
    assert(this != nullptr);
		return glock;
	}
};


void link(std::shared_ptr<Cell> a, unsigned portA, std::shared_ptr<Cell> b, unsigned portB) {
	a->setPort(portA, Port(b, portB));
	b->setPort(portB, Port(a, portA));
}

void link(std::shared_ptr<Cell> a, unsigned portA, const Port& portB) {
	link(a, portA, portB.cell, portB.port);
}

void link(const Port& portA, std::shared_ptr<Cell> b, unsigned portB) {
	link(portA.cell, portA.port, b, portB);
}

void link(const Port& a, const Port& b) {
	link(a.cell, a.port, b);
}


// ------ Special Cell Nodes ---------


struct End : public Cell {
	End() : Cell('#',1) {}
};

struct Zero : public Cell {
	Zero() : Cell('0',1) {}
};

struct Succ : public Cell {
	Succ() : Cell('s',2) {}
};

struct Add : public Cell {
	Add() : Cell('+',3) {}
};

struct Mul : public Cell {
	Mul() : Cell('*',3) {}
};

struct Eraser : public Cell {
	Eraser() : Cell('e', 1) {}
};

struct Duplicator : public Cell {
	Duplicator() : Cell('d', 3) {}
};


// ------- Net Construction Operations -------

std::shared_ptr<Cell> toNet(unsigned value) {

	// terminal case
	if (value == 0) {
    std::shared_ptr<Cell> z{new Zero()};
    return z;
  }

	// other cases
	std::shared_ptr<Cell> inner{toNet(value - 1)};
	std::shared_ptr<Cell> succ{new Succ()};
	link(succ, 1, inner, 0);
	return succ;
}

int toValue(std::shared_ptr<Cell> net) {
	// compute value
	if (net->getSymbol() == '0') return 0;
	if (net->getSymbol() == 's') {
		int res = toValue(net->getPort(1));
		return (res == -1) ? -1 : res + 1;
	}
	return -1;
}

Port add(Port a, Port b) {
	std::shared_ptr<Cell> res{new Add()};
	link(res, 0, a);
	link(res, 2, b);
	return Port(res, 1);
}

Port mul(Port a, Port b) {
	std::shared_ptr<Cell> res{new Mul()};
	link(res, 0, a);
	link(res, 2, b);
	return Port(res, 1);
}

std::shared_ptr<Cell> end(Port a) {
	std::shared_ptr<Cell> res{new End()};
	link(res, 0, a);
	return res;
}

// ------- Computation Operation --------

void compute(std::shared_ptr<Cell> net);


// other utilities
void destroy(const std::shared_ptr<Cell> net);
void plotGraph(const std::shared_ptr<Cell> cell, const string& filename = "net.dot");

// -----------------------------------------------------------------------

namespace {
	void collectClosure(std::shared_ptr<Cell> cell, set<std::shared_ptr<Cell>>& res) {
		if (!cell) return;

		// add current cell to resulting set
		bool newElement = res.insert(cell).second;
		if (!newElement) return;

		// continue with connected ports
		for(unsigned i=0; i<cell->getNumPorts(); i++) {
			collectClosure(cell->getPort(i), res);
		}
	}
}

set<std::shared_ptr<Cell>> getClosure(const std::shared_ptr<Cell> cell) {
	set<std::shared_ptr<Cell>> res;
	collectClosure(cell, res);
	return res;
}

void plotGraph(std::shared_ptr<Cell> cell, const string& filename) {

	// step 0: open file
	ofstream file;
	file.open(filename);

	// step 1: get set of all cells (convex closure)
	auto cells = getClosure(cell);

	// step 2: print dot header
	file << "digraph test {\n";

	// step 3: print node and edge description
	for(const std::shared_ptr<Cell> cur : cells) {
		// add node description
		file << "\tn" << (cur) << " [label=\"" << cur->getSymbol() << "\"];\n";

		// add ports
		for(unsigned i=0; i<cur->getNumPorts(); i++) {
			const std::shared_ptr<Cell> other = cur->getPort(i).cell;

			if (other) {
				file << "\tn" << (cur) << " -> n" << (other)
					<< " [label=\"\"" << ((i==0)?", color=\"blue\"":"") << "];\n";
			}
		}
	}

	// step 4: finish description
	file << "}\n";

	// step 5: close file
	file.close();
}

void destroy(const std::shared_ptr<Cell> net) {

	// step 1: get all cells
	auto cells = getClosure(net);

	// step 2: destroy them
	for(const std::shared_ptr<Cell> cur : cells) {
		//delete cur;
    cur->die();
	}
}

namespace {

	bool isCut(const std::shared_ptr<Cell> a, const std::shared_ptr<Cell> b) {
		return a->getPrinciplePort().cell == b && b->getPrinciplePort().cell == a;
	}

	template<typename T>
	void unlockAll(T& head) {
		head.unlock();
	}
	template<typename T, typename... Types>
	void unlockAll(T& head, Types&... args) {
		head.unlock();
		unlockAll(args...);
	}
}


hpx::future<void> handleCell_async(const inncabs::launch l, std::shared_ptr<Cell> a);
void handleCell(const inncabs::launch l, std::shared_ptr<Cell> a) {
  handleCell_async(l,a).wait();
}
hpx::future<void> handleCell_async(const inncabs::launch l, std::shared_ptr<Cell> a) {
  std::shared_ptr<srb::promise<void>> p{new srb::promise<void>()};
	std::shared_ptr<Cell> b{a->getPrinciplePort().cell};
  assert(b.get() != nullptr);
	//std::lock(a->getLock(), b->getLock());
  guard_set gs;
  gs.add(a->getLock());
  gs.add(b->getLock());

  run_guarded(gs,[l,a,b,p](){

	Symbol aSym = a->getSymbol();
	Symbol bSym = b->getSymbol();

	// ensure proper order
	if(aSym < bSym) {
		//unlockAll(a->getLock(), b->getLock());
    #ifdef USE_SRB
    std::function<void()> f=[l,b,p](){
		  handleCell(l, b);
		  //return;
      p->set_value();
    };
    srb::async(f);
    #else
    srb::async(srb::launch::async, [l,b,p](){
		  handleCell(l, b);
		  //return;
      p->set_value();
    });
    #endif
    return;
	}

	/*std::cout << "starting on " << a << ", " << b << std::endl;*/

	std::shared_ptr<Cell> x{a->getNumPorts()>1 ? a->getPort(1).cell : nullptr};
	std::shared_ptr<Cell> y{a->getNumPorts()>2 ? a->getPort(2).cell : nullptr};
	std::shared_ptr<Cell> z{b->getNumPorts()>1 ? b->getPort(1).cell : nullptr};
	std::shared_ptr<Cell> w{b->getNumPorts()>2 ? b->getPort(2).cell : nullptr};

  /*
	inncabs::mutex xLT, yLT, zLT, wLT;
	inncabs::mutex* aLock = &a->getLock();
	inncabs::mutex* bLock = &b->getLock();
	inncabs::mutex* xLock = x ? &x->getLock() : &xLT;
	inncabs::mutex* yLock = y ? &y->getLock() : &yLT;
	inncabs::mutex* zLock = z ? &z->getLock() : &zLT;
	inncabs::mutex* wLock = w ? &w->getLock() : &wLT;
  */
  guard_set gs;
  gs.add(a->getLock());
  gs.add(b->getLock());
  if(x.get() != nullptr) gs.add(x->getLock());
  if(y.get() != nullptr) gs.add(y->getLock());
  if(z.get() != nullptr) gs.add(z->getLock());
  if(w.get() != nullptr) gs.add(w->getLock());

	//unlockAll(*aLock, *bLock);
	//std::lock(*aLock, *bLock, *xLock, *yLock, *zLock, *wLock);
  run_guarded(gs,[a,b,x,y,z,w,l,p,aSym,bSym](){

	if(    a->getPrinciplePort().cell != b
		|| a->dead() || b->dead()
		|| (x && x->dead()) || (y && y->dead()) || (z && z->dead()) || (w && w->dead())
		|| x != (a->getNumPorts()>1 ? a->getPort(1).cell : nullptr)
		|| y != (a->getNumPorts()>2 ? a->getPort(2).cell : nullptr)
		|| z != (b->getNumPorts()>1 ? b->getPort(1).cell : nullptr)
		|| w != (b->getNumPorts()>2 ? b->getPort(2).cell : nullptr)
		|| aSym != a->getSymbol()
		|| bSym != b->getSymbol()) {
		// need to retry later
		//unlockAll(*aLock, *bLock, *xLock, *yLock, *zLock, *wLock);
    #ifdef USE_SRB
    std::function<void()> f =[l,a,p]() {
		  if(!a->dead()) handleCell(l, a);
      p->set_value();
		  return;
    };
    srb::async(f);
    #else
    srb::async(srb::launch::async,[l,a,p]() {
		  if(!a->dead()) handleCell(l, a);
      p->set_value();
		  return;
    });
    #endif
    return;
	}

	/*std::cout << "fully locked on " << a << ", " << b << std::endl;*/

	/*
		// debugging:
		stringstream file;
		file << "step" << counter++ << ".dot";
		plotGraph(net, file.str());
		std::cout << "Step: " << counter << " - Processing: " << a->getSymbol() << " vs. " << b->getSymbol() << "\n";
	*/
	std::shared_ptr<std::vector<std::shared_ptr<Cell>>> newTasks{new std::vector<std::shared_ptr<Cell>>()};
	switch(a->getSymbol()) {
		case '0': {
			switch(b->getSymbol()) {
			case '+': {
				// implement the 0+ rule
				auto x = b->getPort(1);
				auto y = b->getPort(2);

				link(x, y);

				a->die();
				b->die();

				// check whether this is producing a cut
				if(isCut(x, y)) newTasks->push_back(x);

				break;
			}
			case '*': {
				// implement s* rule
				auto x = b->getPort(1);
				auto y = b->getPort(2);

				// create a new cell
				std::shared_ptr<Cell> e{new Eraser()};

				// alter wiring
				link(a, 0, x);
				link(e, 0, y);

				// eliminate nodes
				b->die();

				// check for new cuts
				if(isCut(a, x)) newTasks->push_back(x);
				if(isCut(e, y)) newTasks->push_back(y);

				break;
			}
			case 'd': {
				// implement the 0+ rule
				auto x = b->getPort(1);
				auto y = b->getPort(2);

				// creat new cell
				std::shared_ptr<Cell> n{new Zero()};

				// update links
				link(a, 0, b, 2);
				link(n, 0, b, 1);

				// remove old cell
				b->die();

				// check whether this is producing a cut
				if(isCut(a, y)) newTasks->push_back(y);
				if(isCut(n, x)) newTasks->push_back(x);

				break;
			}
			default: break;
			}
			break;
		}
		case 's': {
			switch(b->getSymbol()) {
			case '+': {
				// implement the s+ rule
				auto x = a->getPort(1);
				auto y = b->getPort(1);

				// alter wiring
				link(x, b, 0);
				link(y, a, 0);
				link(a,1,b,1);

				// check for new cuts
				if(isCut(x, b)) newTasks->push_back(x);
				if(isCut(y, a)) newTasks->push_back(y);

				break;
			}
			case '*': {
				// implement s* rule
				auto x = a->getPort(1);
				auto y = b->getPort(1);
				auto z = b->getPort(2);

				// create new cells
				std::shared_ptr<Cell> p{new Add()};
				std::shared_ptr<Cell> d{new Duplicator()};

				// alter wiring
				link(b, 0, x);
				link(b, 1, p, 0);
				link(b, 2, d, 1);

				link(p, 1, y);
				link(p, 2, d, 2);

				link(d, 0, z);

				// eliminate nodes
				a->die();

				// check for new
				if(isCut(x, b)) newTasks->push_back(x);
				if(isCut(d, z)) newTasks->push_back(d);

				break;
			}
			case 'd': {
				// implement the sd rule
				auto x = a->getPort(1);
				auto y = b->getPort(1);
				auto z = b->getPort(2);

				// crate new cells
				std::shared_ptr<Cell> s{new Succ()};

				// alter wiring
				link(b, 0, x);
				link(b, 1, s, 1);
				link(b, 2, a, 1);

				link(s, 0, z);
				link(a, 0, y);

				// check for new cuts
				if(isCut(x, b)) newTasks->push_back(x);
				if(isCut(y, a)) newTasks->push_back(y);
				if(isCut(z, s)) newTasks->push_back(z);

				break;
			}
			case 'e': {
				// implement the sd rule
				auto x = a->getPort(1);

				// alter wiring
				link(b, 0, x);

				// delete old cell
				a->die();

				// check for new cuts
				if(isCut(x, b)) newTasks->push_back(x);

				break;
			}
			default: break;
			}
			break;
		}
		case '+': break;
		case 'd': {
			switch(b->getSymbol()) {
			case '0': {
				// implement the 0+ rule
				auto x = a->getPort(1);
				auto y = a->getPort(2);

				// creat new cell
				std::shared_ptr<Cell> n{new Zero()};

				// update links
				link(b, 0, x);
				link(n, 0, y);

				// remove old cell
				a->die();

				// check whether this is producing a cut
				if(isCut(b, x)) newTasks->push_back(x);
				if(isCut(n, y)) newTasks->push_back(y);

				break;
			}
			default: break;
			}
			break;
		}
		case 'e': {
			switch(b->getSymbol()) {
			case '0': {
				// just delete cells
				a->die();
				b->die();
				break;
			}
			default: break;
			}
			break;
		}
		default: break;
	}

	/*std::cout << "pre-unlocked on " << a << ", " << b << " locks: " <<  *(int*)&a->getLock() << " / " <<  *(int*)&b->getLock() << std::endl;*/
	//unlockAll(*aLock, *bLock, *xLock, *yLock, *zLock, *wLock);
	/*std::cout << "unlocked on " << a << ", " << b << " locks: " <<  *(int*)&a->getLock() << " / " <<  *(int*)&b->getLock() << std::endl;*/
  #ifdef USE_SRB
  std::function<void()> f=[newTasks,l,p](){

	  std::vector<srb::future<void>> futures;
	  for(std::shared_ptr<Cell> c : *newTasks) {
      std::function<void()> f2 = std::bind(handleCell,l,c);
      futures.push_back(srb::async(l,f2));
    }
	  for(auto& f : futures) f.wait();
    p->set_value();
  };
  srb::async(f);
  #else
  srb::async(srb::launch::async,[newTasks,l,p](){

	  std::vector<inncabs::future<void>> futures;
	  for(std::shared_ptr<Cell> c : *newTasks)
      futures.push_back(handleCell_async(l, c));
      //futures.push_back(inncabs::async(l, &handleCell, l, c));
	  for(auto& f : futures) f.wait();
    p->set_value();
  });
  #endif
  });
  });
  return p->get_future();
}


void compute(const inncabs::launch l, std::shared_ptr<Cell> net) {

	// step 1: get all cells in the net
	set<std::shared_ptr<Cell>> cells = getClosure(net);

	// step 2: get all connected principle ports
	std::vector<srb::future<void>> cuts;
	for(const std::shared_ptr<Cell> cur : cells) {
		const Port& port = cur->getPrinciplePort();
		if(cur < port.cell && isCut(cur, port.cell)) {
      #ifdef USE_SRB
        std::function<void()> f = std::bind(handleCell, l, cur);
			  cuts.push_back(srb::async(f));
      #else
			  //cuts.push_back(srb::async(l, handleCell, l, cur));
			  cuts.push_back(handleCell_async(l, cur));
      #endif
		}
	}

//std::cout << "Found " << cuts.size() << " cut(s).\n";

	// step 3: run processing
	for(auto& f: cuts) {
		f.wait();
	}

/*
	// debugging:
	stringstream file;
	file << "step" << counter++ << ".dot";
	plotGraph(net, file.str());
*/
}


int main(int argc, char** argv) {
	int N = 500;
	if(argc > 1) N = std::stoi(argv[1]);

	std::stringstream ss;
	ss << "Intersim (N = " << N << ")";
	std::shared_ptr<Cell> n;
	inncabs::run_all(
		[&](const inncabs::launch l) {
			compute(l, n);
			return n;
		},
		[&](std::shared_ptr<Cell> result) {
			bool ret = toValue(result->getPort(0)) == N*N;
			// clean up remaining network
			destroy(result);
			return ret;
		},
		ss.str(),
		[&]{ n = end(mul(toNet(N), toNet(N))); }
		);

	return 0;
}
