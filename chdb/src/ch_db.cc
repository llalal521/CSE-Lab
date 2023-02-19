#include "ch_db.h"

int view_server::execute(unsigned int query_key, unsigned int proc, const chdb_protocol::operation_var &var, int &r) {
    // TODO: Your code here
    int base_port = this->node->port();
    int shard_offset;
    if(proc == chdb_protocol::Put || proc == chdb_protocol::Get)
        shard_offset = this->dispatch(query_key, shard_num());
    else   shard_offset = query_key + 1;

    //printf("shard id %d, %d", shard_offset, query_key);
    
    // chdb_protocol::prepare_var pvar;
    // pvar.tx_id = var.tx_id;
    // int active = this->node->template call(base_port + shard_offset, chdb_protocol::Prepare, pvar, r);

    switch (proc)
    {
    case chdb_protocol::Put:
    case chdb_protocol::Get:
        return this->node->template call(base_port + shard_offset, proc, var, r);

    case chdb_protocol::Begin:
        return this->node->template call(base_port + shard_offset, proc, var, r);

    case chdb_protocol::Commit:
        chdb_protocol::commit_var commitVar;
        commitVar.tx_id = var.tx_id;
        return this->node->template call(base_port + shard_offset, proc, commitVar, r);

    default:
        chdb_protocol::commit_var rollbackVar;
        rollbackVar.tx_id = var.tx_id;
        return this->node->template call(base_port + shard_offset, proc, rollbackVar, r);
    }
}

view_server::~view_server() {
#if RAFT_GROUP
    delete this->raft_group;
#endif
    delete this->node;

}