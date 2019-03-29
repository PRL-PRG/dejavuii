#pragma once

//#include <cstdlib>
#include <cassert>

class History;

template <class T>
class Node {
public:
    Node(T payload) : payload(payload) {assert(payload!= nullptr);};
    const Node<T> *next;
    const Node<T> *previous;
    const T payload;
};

class Version {
protected:
    const unsigned id;
    const unsigned path_id;
    const unsigned project_id;
    const unsigned timestamp;
public:
    const History *history();
    const History *before();
    const History *after();

    bool is(unsigned id){ return this->id == id; };
};

class History {
public:
    const Version *getFirst() {
        return start->payload;
    }

    const Version *getLast() {
        return end->payload;
    }

    const Version *getVersion (const unsigned id) const {
        const Node<Version *> *cursor = start;
        while(true) {
            assert(cursor != nullptr && cursor->payload != nullptr);
            if (cursor->payload->is(id)) {
                return cursor->payload;
            }
            if (cursor != end) {
                cursor = cursor->next;
            } else {
                assert(false && "Hash is absent from history.");
                return nullptr;
            }
        }
    }

protected:
    const Node<Version *> *start;
    const Node<Version *> *end;
};

class FullHistory: public History {
public:

};

class PartialHistory: public History {
public:
    PartialHistory(const FullHistory *parent,
                   const Node<Version *> *start,
                   const Node<Version *> *end)
            : parent(parent), start(start), end(end) {}

protected:
    const FullHistory *parent;
};