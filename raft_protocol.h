#ifndef raft_protocol_h
#define raft_protocol_h

#include "rpc.h"
#include "raft_state_machine.h"

enum raft_rpc_opcodes {
    op_request_vote = 0x1212,
    op_append_entries = 0x3434,
    op_install_snapshot = 0x5656
};

enum raft_rpc_status {
   OK,
   RETRY,
   RPCERR,
   NOENT,
   IOERR
};

class request_vote_args {
public:
    // Your code here
    int term; //candidate's term
    int candidate_id; //candidate requesting vote
    int last_log_index; //index of the last log entry
    int last_log_term; //term num of the last log entry
};

marshall& operator<<(marshall &m, const request_vote_args& args);
unmarshall& operator>>(unmarshall &u, request_vote_args& args);


class request_vote_reply {
public:
    // Your code here
    int current_term; // if fail, tell candidate change back to follower
    bool voteGranted; // result
};

marshall& operator<<(marshall &m, const request_vote_reply& reply);
unmarshall& operator>>(unmarshall &u, request_vote_reply& reply);

template<typename command>
class log_entry {
public:
    // Your code here
    int term;
    command cmd;
};

template<typename command>
marshall& operator<<(marshall &m, const log_entry<command>& entry) {
    // Your code here
    m << entry.cmd;
    m << entry.term;
    return m;
}

template<typename command>
unmarshall& operator>>(unmarshall &u, log_entry<command>& entry) {
    // Your code here
    u >> entry.cmd;
    u >> entry.term;
    return u;
}

template<typename command>
class append_entries_args {
public:
    // Your code here
    int leader_term; // heartbeat
    int type;        // distinguish between entry and ping
    int pre_term;
    int pre_index;   // consistency check
    int leader_id;   // redirect
    int repair_index; 
    int repair_term;
    std::vector<log_entry<command>> repair_log; //leader change
    int leaderCommit;   
    std::vector<log_entry<command>> entries;   // log_entry to transfer
};

template<typename command>
marshall& operator<<(marshall &m, const append_entries_args<command>& args) {
    // Your code here
    m << args.leader_term;
    m << args.type;
    m << args.entries;
    m << args.pre_term;
    m << args.pre_index;
    m << args.repair_index;
    m << args.repair_log;
    m << args.leaderCommit;
    m << args.repair_term;
    m << args.leader_id;
    return m;
}

template<typename command>
unmarshall& operator>>(unmarshall &u, append_entries_args<command>& args) {
    // Your code here
    u >> args.leader_term;
    u >> args.type;
    u >> args.entries;
    u >> args.pre_term;
    u >> args.pre_index;
    u >> args.repair_index;
    u >> args.repair_log;
    u >> args.leaderCommit;
    u >> args.repair_term;
    u >> args.leader_id;
    return u;
}

class append_entries_reply {
public:
    // Your code here
    int reply_term;
    bool refuse;
    bool skip;
    int next_index;
};

marshall& operator<<(marshall &m, const append_entries_reply& reply);
unmarshall& operator>>(unmarshall &m, append_entries_reply& reply);


class install_snapshot_args {
public:
    // Your code here
};

marshall& operator<<(marshall &m, const install_snapshot_args& args);
unmarshall& operator>>(unmarshall &m, install_snapshot_args& args);


class install_snapshot_reply {
public:
    // Your code here
};

marshall& operator<<(marshall &m, const install_snapshot_reply& reply);
unmarshall& operator>>(unmarshall &m, install_snapshot_reply& reply);


#endif // raft_protocol_h