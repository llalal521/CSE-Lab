#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>
#include <sys/time.h>
#include <fstream>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

template<typename state_machine, typename command>
class raft {

static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");


friend class thread_pool;

#define RAFT_LOG(fmt, args...) \
    do { \
        auto now = \
        std::chrono::duration_cast<std::chrono::milliseconds>(\
            std::chrono::system_clock::now().time_since_epoch()\
        ).count();\
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while(0);

public:
    raft(
        rpcs* rpc_server,
        std::vector<rpcc*> rpc_clients,
        int idx, 
        raft_storage<command>* storage,
        state_machine* state    
    );
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node. 
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped(). 
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false. 
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx;                     // A big lock to protect the whole data structure
    ThrPool* thread_pool;
    raft_storage<command>* storage;              // To persist the raft log
    std::vector<log_entry<command>> volatile_log;                 // log entry in mem
    state_machine* state;  // The state machine that applies the raft log, e.g. a kv store

    rpcs* rpc_server;               // RPC server to recieve and handle the RPC requests
    std::vector<rpcc*> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                     // The index of this node in rpc_clients, start from 0
    int vote_num;                  // remember the number of my votes
    int *nextIndex;                // next log index in each server
    int *mactchIndex;              // match log index in each server

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;

    std::thread* background_election;
    std::thread* background_ping;
    std::thread* background_commit;
    std::thread* background_apply;

    // Your code here:
    int voted_for; // candidate that received vote in current term (or null if none)
    int commit_id; // index of highest log entry known to be committed
    int lastapplied; //index of highest log entry applied to state machine
    timeval last_rpc_time;
    std::ofstream out;  //for debug(log is too large)


private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply& reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply& reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply& reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply);


private:
    bool is_stopped();
    int num_nodes() {return rpc_clients.size();}

    // background workers    
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:


};

template<typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs* server, std::vector<rpcc*> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    storage(storage),
    state(state),   
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    stopped(false),
    role(follower),
    current_term(0),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr)
{
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here: 
    // Do the initialization
    gettimeofday(&last_rpc_time, NULL); //initial timeout
    commit_id = 0;
    voted_for = -1; // -1 represents null
    vote_num = 0; // vote to no one
    lastapplied = 0;
    nextIndex = new int[rpc_clients.size()];
    mactchIndex = new int[rpc_clients.size()];
}

template<typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;    
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Your code here:
    log_entry<command> tmp;
    tmp.term = 0;
    command cmd;
    cmd.value = 20;
    tmp.cmd = cmd;
    volatile_log.push_back(tmp);
    storage->log_read(volatile_log);
    storage->meta_read(current_term, voted_for);
    RAFT_LOG("start log %d",current_term); 
    gettimeofday(&last_rpc_time, NULL);
    role = follower;

    RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Your code here:
    //std::this_thread::sleep_for(std::chrono::milliseconds(30)); //prevent two leaders
    std::unique_lock<std::mutex> lock(mtx);
    //RAFT_LOG("new_command get lock");
    if(role == leader){
        term = current_term;
        log_entry<command> new_log;
        new_log.term = current_term;
        new_log.cmd = cmd;
        volatile_log.push_back(new_log);
        storage->log_store(new_log);
        index = volatile_log.size() - 1;
        mactchIndex[my_id] = index;
        RAFT_LOG("new_command release %d", cmd.value);
        return true;
    }
    //RAFT_LOG("new_command release lock");
    return false;
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Your code here:
    return true;
}



/******************************************************************

                         RPC Related

*******************************************************************/
template<typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply& reply) { //voting function
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    //RAFT_LOG("voting get lock");
    int req_term = args.term;
    int old_term = current_term;
    // if(role == leader){
    //     if(req_term > commit_id && req_term < volatile_log.size() && volatile_log[req_term]->term == current_term){
    //         int match_num;
    //         for(int i = 0; i < rpc_clients.size(); ++i){
    //             if(mactchIndex[i] >= req_term)
    //                 match_num++;
    //         }
    //         if(match_num >= rpc_clients.size()/2 + 1)   commit_id = req_term;
    //     }
    // }
    if(req_term >= current_term){
        current_term = req_term;
        storage->meta_modify(current_term, voted_for);
        role = follower;
        gettimeofday(&last_rpc_time, NULL);
    }
    reply.current_term = current_term;
    reply.voteGranted = false;
    int log_size = volatile_log.size();
    if(old_term < args.term && (log_size == 0 || (volatile_log[log_size - 1].term < args.last_log_term || (volatile_log[log_size - 1].term == args.last_log_term && log_size <= (args.last_log_index + 1))))){
        voted_for = args.candidate_id;
        reply.voteGranted = true;
        storage->meta_modify(current_term, voted_for);
    }  //warning 
    //RAFT_LOG("voting release lock");
    RAFT_LOG("%d voting, %d: %d", my_id, (int)reply.voteGranted, args.candidate_id);
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    //RAFT_LOG("voting reply get lock");
    bool voted = reply.voteGranted;
    int reply_term = reply.current_term;
    if(reply_term > current_term){
        current_term = reply_term;
        role = follower;
        gettimeofday(&last_rpc_time, NULL);
    }
    //RAFT_LOG("%d and %d", current_term, arg.term);
    if(voted == true && current_term == arg.term){
        vote_num++;
        //RAFT_LOG("%d total", vote_num);
        if(vote_num > num_nodes() / 2 && role != leader){
            RAFT_LOG("%d is leader now", my_id);
            for(int i = 0; i < num_nodes(); ++i){
                nextIndex[i] = volatile_log.size();
                mactchIndex[i] = 0;
            }
            role = leader;
        }
    }
    //RAFT_LOG("voting reply release lock");
    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply& reply) {
    // Your code here:
    if(arg.type == 1 && arg.pre_index >= volatile_log.size()){
        //RAFT_LOG("arrive !");
        reply.refuse = true;
        reply.skip = true;
        reply.next_index = volatile_log.size();
        return 0;
    }
    std::unique_lock<std::mutex> lock(mtx);
    if(arg.type == 1 && (volatile_log.size() > arg.pre_index + 1 && volatile_log[arg.pre_index + 1].term == arg.entries.begin()->term)){
        reply.refuse = true;
        reply.skip = true;
        reply.next_index = volatile_log.size();
        return 0;
    }
    if(arg.leader_term >= current_term){
        reply.skip = false;
        role = follower;
        current_term = arg.leader_term;
        storage->meta_modify(current_term, voted_for);
        gettimeofday(&last_rpc_time, NULL);
        if(arg.type == 0){ //ping
            reply.reply_term = -1;
            int log_size = volatile_log.size();
            if(arg.pre_index >= log_size || volatile_log[arg.pre_index].term != arg.pre_term)
                return 0;
            if(arg.leaderCommit > commit_id){
                if(arg.leaderCommit >= log_size - 1)
                    commit_id = log_size - 1; 
                else    commit_id = arg.leaderCommit;
            }
            return 0;
        }
        if(arg.type == 1){ // append new command
            //RAFT_LOG("append get lock");
            int index; //next_index
            int log_size = volatile_log.size();
            if(volatile_log[arg.pre_index].term != arg.pre_term){
                //RAFT_LOG("refuse %d, %d", arg.pre_index, log_size);
                reply.refuse = true;
                int size = volatile_log.size();
                for(int i = 0; i < size - arg.pre_index; ++i){
                    volatile_log.pop_back();
                    storage->log_pop();
                }
                reply.reply_term = -1;
                reply.next_index = arg.pre_index;
                //RAFT_LOG("why %d, %d", arg.pre_index, volatile_log.size());
                return 0;
            }
            reply.refuse = false;
            int size = volatile_log.size();
            // if(size > arg.pre_index + 1 && volatile_log[arg.pre_index + 1].term == arg.entries.begin()->term){ //handle at least once
            //     RAFT_LOG("here, %d, %d", size, arg.pre_index);
            //     reply.reply_term = -1;
            //     reply.next_index = volatile_log.size();
            //     return 0;
            // }
            for(int i = arg.pre_index + 1; i < size; ++i){
                //RAFT_LOG("%d", arg.pre_index);
                volatile_log.pop_back();
                storage->log_pop();
            }
            for(auto it = arg.entries.begin(); it != arg.entries.end(); ++it){
                //RAFT_LOG("size of arg %d", arg.entries.size());
                volatile_log.push_back(*it);
                storage->log_store(*it);
            }
            index = volatile_log.size();
            //RAFT_LOG("size of arg %d", index);
            reply.reply_term = -1;
            reply.next_index = index;
            //RAFT_LOG("append entry release lock");
            return 0;
        }
    }   
    else{
        reply.skip = false;
        reply.reply_term = current_term;
        reply.refuse = true;
    }
    //RAFT_LOG("append entry release lock");
    return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply) {
    // Your code here:
    if(reply.skip){
        nextIndex[target] = reply.next_index;
        return;
    }
    std::unique_lock<std::mutex> lock(mtx);
    //RAFT_LOG("append entry reply get lock");
    if(arg.type == 0 && reply.reply_term != -1){
        current_term = reply.reply_term;
        storage->meta_modify(current_term, voted_for);
        role = follower;
        RAFT_LOG("my fault");
        gettimeofday(&last_rpc_time, NULL);
        //RAFT_LOG("append entry reply release lock");
        return;
    }
    if(arg.type == 1){
        if(!reply.refuse){
            if(rpc_server->reachable())
                mactchIndex[target] = reply.next_index - 1;
            int log_size = volatile_log.size();
            if(reply.next_index - 1 > commit_id && reply.next_index - 1 < log_size && volatile_log[reply.next_index - 1].term == current_term){
                int match_num = 0;
                for(int i = 0; i < num_nodes(); ++i){
                    if(mactchIndex[i] >= reply.next_index - 1){
                        //RAFT_LOG("%d", i);
                        match_num++;
                    }
                }
                //RAFT_LOG("\n");
                if(match_num >= num_nodes()/2 + 1)   { RAFT_LOG("update commit_id %d, %d", match_num, reply.next_index - 1); commit_id = reply.next_index - 1;}
            }
            nextIndex[target] = reply.next_index;
            //RAFT_LOG("update %d", reply.next_index);
            //RAFT_LOG("append entry reply release lock");
            return;
        }
        if(reply.reply_term != -1){
            role = follower;
            current_term = reply.reply_term;
            gettimeofday(&last_rpc_time, NULL);
            return;
        }
        //RAFT_LOG("%d, %d", reply.next_index, nextIndex[target]);
        if(reply.next_index < nextIndex[target])  
            nextIndex[target] = arg.pre_index;
        //RAFT_LOG("nextIndex %d : %d", target, nextIndex[target] - 1);
        //RAFT_LOG("append entry reply release lock");
        return;
    }
    //("append entry reply release lock");
    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply& reply) {
    // Your code here:
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply) {
    // Your code here:
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
        if(!rpc_server->reachable()){
            std::unique_lock<std::mutex> lock(mtx);
            role = follower;
            gettimeofday(&last_rpc_time, NULL);
        }
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
        //RAFT_LOG("RPC fails");
        if(!rpc_server->reachable()){
            std::unique_lock<std::mutex> lock(mtx);
            role = follower;
            gettimeofday(&last_rpc_time, NULL);
        }
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Check the liveness of the leader.
    // Work for followers and candidates.

    // Hints: You should record the time you received the last RPC.
    //        And in this function, you can compare the current time with it.
    //        For example:
    //        if (current_time - last_received_RPC_time > timeout) start_election();
    //        Actually, the timeout should be different between the follower (e.g. 300-500ms) and the candidate (e.g. 1s).

    while (true) {
        if (is_stopped()){
            return;
        }
        srand(my_id * 14);
        int randomSeed = rand() + rand() % 500;
        srand(randomSeed);
        unsigned int time_out = 0;
        if(role == follower){
            time_out = 300 + rand() % 200;
        }
        if(role == candidate)
            time_out = 1000;
        if(role != leader){
            timeval current_time;
            gettimeofday(&current_time, NULL);
            if(current_time.tv_sec * 1000 + current_time.tv_usec / 1000 - last_rpc_time.tv_sec * 1000 - last_rpc_time.tv_usec / 1000 >= time_out){
                //RAFT_LOG("begin %d %d", last_rpc_time.tv_sec * 1000 + last_rpc_time.tv_usec / 1000, time_out);
                //RAFT_LOG("begin %d %d", current_time.tv_sec * 1000 + current_time.tv_usec / 1000, time_out);
                std::unique_lock<std::mutex> lock(mtx);
                if(rpc_server->reachable()){
                    // RAFT_LOG("candidate get lock");
                    current_term = current_term + 1;
                    storage->meta_modify(current_term, voted_for);
                }
                vote_num = 0;
                voted_for = my_id;
                storage->meta_modify(current_term, voted_for);
                vote_num = 1;
                role = candidate; // vote for server himself

                gettimeofday(&last_rpc_time, NULL); //reset the timer

                request_vote_args args; //set the args
                args.candidate_id = my_id;
                args.last_log_index = volatile_log.size() - 1;
                if(args.last_log_index == -1)   args.last_log_term = 0;
                else
                    args.last_log_term = volatile_log[args.last_log_index].term;
                args.term = current_term;
                for(int i = 0; i < num_nodes(); ++i){
                    if(i == my_id)  continue;
                    thread_pool->addObjJob(this, &raft::send_request_vote, i, args);
                } //send vote rpc to all other servers and handle reply
                //RAFT_LOG("candidate releasing lock");
            }
        }
        // Your code here:
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    

    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Send logs/snapshots to the follower.
    // Only work for the leader.

    // Hints: You should check the leader's last log index and the follower's next log index.        
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        if(role == leader){
            std::unique_lock<std::mutex> lock(mtx);
            for(int i = 0; i < num_nodes(); ++i){
                if(i == my_id)  continue;
                int log_size = volatile_log.size();
                if(nextIndex[i] > log_size - 1) continue;
                append_entries_args<command> args;
                args.type = 1;
                args.leader_term = current_term;
                args.leaderCommit = commit_id;
                args.pre_index = nextIndex[i] - 1;
                args.pre_term = volatile_log[args.pre_index].term;
                for(int j = nextIndex[i]; j < log_size; ++j){
                    args.entries.push_back(volatile_log[j]);
                }
                //RAFT_LOG("next_index %d, %d, %d",i ,nextIndex[i], args.entries.size());
                thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Apply committed logs the state machine
    // Work for all the nodes.

    // Hints: You should check the commit index and the apply index.
    //        Update the apply index and apply the log if commit_index > apply_index
    
    while (true) {
        
        if (is_stopped()) return;
        // Your code here:
        if(commit_id > lastapplied){
            std::unique_lock<std::mutex> lock(mtx);
            //RAFT_LOG("applying %d with %d", lastapplied, volatile_log[lastapplied + 1].term);
            lastapplied++;
            RAFT_LOG("lastapplied %d", lastapplied);
            state->apply_log(volatile_log[lastapplied].cmd);
            //RAFT_LOG("apply release lock");
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Send empty append_entries RPC to the followers.

    // Only work for the leader.
    
    while (true) {
        if (is_stopped()) 
            return;
        //std::unique_lock<std::mutex> lock(mtx);
        // Your code here:
        if(role == leader){
            //RAFT_LOG("sending heartbeat %d", my_id);
            for(int i = 0; i < num_nodes(); ++i){
                if(i == my_id)  continue;
                append_entries_args<command> arg;
                arg.leader_term = current_term;
                arg.pre_index = nextIndex[i] - 1;
                arg.pre_term = volatile_log[arg.pre_index].term; //consistency
                arg.leaderCommit = commit_id;
                arg.type = 0;
                thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(140)); // Change the timeout here!
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); // Change the timeout here!
    }    
    return;
}


/******************************************************************

                        Other functions

*******************************************************************/



#endif // raft_h