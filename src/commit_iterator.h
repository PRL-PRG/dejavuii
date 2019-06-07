#pragma once
#include <type_traits>


namespace dejavu {



    /** Commit:
            - numParentCommits function
            - childrenCommits() function returning container of the commits children

        Project
           - hasCommit()
           - commitsBegin(), commitsEnd()

        State :
            - default constructor
            - deep copy constructor
            - mergeWiTH(const &) method
     */

    template<typename PROJECT, typename COMMIT, typename STATE>
    class CommitForwardIterator {
    public:
        
        typedef std::function<bool(COMMIT * c, STATE & s)> Handler;

        CommitForwardIterator(PROJECT * p, Handler h):
            p_(p),
            handler_(h) {
            for (auto i = p->commitsBegin(), e = p->commitsEnd(); i != e; ++i) {
                if ((*i)->numParentCommits() == 0) {
                    QueueItem * qi = new QueueItem(*i, STATE());
                    q_.push_back(qi);
                }
            }
        }

        ~CommitForwardIterator() {
            for (auto i : q_)
                delete i;
            for (auto i : pending_)
                delete i.second;
        }

        /*
        void addInitialCommit(COMMIT * c) {
            QueueItem * qi = new QueueItem(c, STATE());
            assert(qi->merges == 0);
            q_.push_back(qi);
            } */

        void setLastCommitHandler(Handler h) {
            lastCommitHandler_ = h;
        }

        void process() {
            while (!q_.empty()) {
                QueueItem * current = q_.back();
                q_.pop_back();
                assert(current->merges == 0); // if merges is greater than 0 the commit is not ready to be processed
                // call the handler
                bool cont = handler_(current->c, current->s);
                // if we are not to continue, delete the queue item and do not schedule its offsprings, otherwise add all children of the current commit to the queue, or pending lists
                if (cont) {
                    addChildren(current);
                } else {
                    delete current;
                }
            }
            // not really testable if we don't continue analysis of all branches
            //assert(pending_.empty());
        }

    private:

        struct QueueItem {
            COMMIT * c;
            STATE s;
            unsigned merges;

            QueueItem(COMMIT * c, STATE const & s):
                c(c),
                s(s),
                merges(c->numParentCommits() == 0 ? 0 : c->numParentCommits() - 1) {
            }

            void replaceCommit(COMMIT * newC) {
                c = newC;
                merges = c->numParentCommits() - 1;
            }

            void  mergeState(STATE const & incomming) {
                s.mergeWith(incomming, c);
                --merges;
            }

            

        };

        void addChildren(QueueItem * i) {
            // get the children of current commit, if there are no current children, detete the queue item
            auto children = i->c->childrenCommits();
            bool shouldDelete = true;
            if (! children.empty()) {
                //otherwise for each child
                bool canReuse = true;
                for (COMMIT * child : i->c->childrenCommits()) {
                    if (!p_->hasCommit(child))
                        continue;
                    // first see if the child is staged in pending
                    auto j = pending_.find(child);
                    if (j != pending_.end()) {
                        QueueItem * qi = j->second;
                        qi->mergeState(i->s);
                        if (qi->merges == 0) {
                            pending_.erase(j);
                            schedule(qi);
                        }
                    // otherwise either reuse current item & state for the queued commit, or create a new one and schedule it
                    } else {
                        if (canReuse) {
                            canReuse = false;
                            i->replaceCommit(child);
                            schedule(i);
                            shouldDelete = false;
                        } else {
                            schedule(new QueueItem(child, i->s));
                        }
                    }
                }
            } else {
                if (lastCommitHandler_)
                    lastCommitHandler_(i->c, i->s);
            }
            if (shouldDelete)
                delete i;
        }

        void schedule(QueueItem * i) {
            if (i->merges == 0)
                q_.push_back(i);
            else
                pending_.insert(std::make_pair(i->c, i));
        }


        PROJECT * p_;
        Handler handler_;
        Handler lastCommitHandler_;
        
        std::vector<QueueItem *> q_;

        std::unordered_map<COMMIT *, QueueItem *> pending_;
        
    };

    
} // namespace dejavu
