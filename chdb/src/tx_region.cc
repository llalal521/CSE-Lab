#include "tx_region.h"

std::mutex tx_id_lock; //big lock
std::map<int, tx_lock *> twopls;
std::map<int, std::set<int> > tx_locknum;

int tx_region::put(const int key, const int val) {
    // TODO: Your code here
    int r;
    unsigned int query_key = key;
    // //if(query_key == 0)  query_key = db->shards.size();
    std::mutex *tmp = new std::mutex();
    tx_locknum[tx_id].insert(key);
    if(twopls.find(key) != twopls.end()){
        if(twopls[key]->tx_id == tx_id){
            printf("put same tx_id %d, %d\n", key, tx_id);
        } else{
            printf("put different tx_id %d, %d, %d\n", key, tx_id, twopls[key]->tx_id);
            // tx_lock *here = new tx_lock(tx_id, twopls[key]->txlock);
            twopls[key]->txlock->lock();
            twopls[key]->tx_id = tx_id;
        }
    }
    else{
        printf("put first time %d, %d\n", key, tx_id);
        twopls.insert(std::pair<int, tx_lock *>(key, new tx_lock(tx_id, tmp)));
        // twopls[key] = new tx_lock(key, tmp);
        twopls[key]->txlock->lock();
    }
    hasput = true;
    this->db->vserver->execute(query_key, chdb_protocol::Put, chdb_protocol::operation_var{.tx_id = tx_id, .key = key, .value = val}, r);
    return 0;
}

int tx_region::get(const int key) {
    // TODO: Your code here
    int r;
    unsigned int query_key = key;
    // //if(query_key == 0)  query_key = db->shards.size();
    std::mutex *tmp = new std::mutex();
    tx_locknum[tx_id].insert(key);
    if(twopls.find(key) != twopls.end()){
       if(twopls[key]->tx_id == tx_id){
           printf("get same tx_id %d, %d\n", key, tx_id);
           this->db->vserver->execute(query_key, chdb_protocol::Get, chdb_protocol::operation_var{.tx_id = tx_id, .key = key}, r);
       }
       else{
        //    tx_lock *here = new tx_lock(tx_id, twopls[key]->txlock);
           twopls[key]->txlock->lock();
           twopls[key]->tx_id = tx_id;
           printf("get different tx_id %d, %d, %d\n", key, tx_id, twopls[key]->tx_id);
           this->db->vserver->execute(query_key, chdb_protocol::Get, chdb_protocol::operation_var{.tx_id = tx_id, .key = key}, r);
       }
    }
    else{
        printf("get first time %d, %d\n", key, tx_id);
        twopls[key] = new tx_lock(tx_id, tmp);
        twopls[key]->txlock->lock();
        this->db->vserver->execute(query_key, chdb_protocol::Get, chdb_protocol::operation_var{.tx_id = tx_id, .key = key}, r);
    }
    return r;
}

int tx_region::tx_can_commit() {
    // TODO: Your code here
    int r = 0;
    unsigned int i = 0;
    int num = 0;
    for(; i < this->db->shards.size(); ++i){
        num ++;
        this->db->vserver->execute(i, chdb_protocol::CheckPrepareState, chdb_protocol::operation_var{.tx_id = tx_id}, r);
        printf("r: %d", r);
        if(r == 0) break;
        r = 0;
    }
    if(i != this->db->shards.size()) return chdb_protocol::prepare_not_ok;

    return chdb_protocol::prepare_ok;
}

int tx_region::tx_begin() {
    // TODO: Your code here
    printf("tx[%d] begin\n", tx_id);
    std::set<int> txset;
    tx_locknum.insert(std::pair<int, std::set<int> >(tx_id, txset));
    int r;
    unsigned int i = 0;
    for(; i < this->db->shards.size(); ++i){
        this->db->vserver->execute(i, chdb_protocol::Begin, chdb_protocol::operation_var{.tx_id = tx_id}, r);
    }
    return 0;
}

int tx_region::tx_commit() {
    // TODO: Your code here
    int r;
    unsigned int i = 0;
    for(; i < this->db->shards.size(); ++i){ 
        this->db->vserver->execute(i, chdb_protocol::Commit, chdb_protocol::operation_var{.tx_id = tx_id}, r);
    }
    printf("tx[%d] commit\n", tx_id);
    for(auto it = tx_locknum[tx_id].begin(); it != tx_locknum[tx_id].end(); ++it){
        twopls[*it]->txlock->unlock();
        printf("%d, %d release lock \n", tx_id, *it);
    }
    return 0;
}

int tx_region::tx_abort() {
    // TODO: Your code here
    int r;
    unsigned int i = 0;
    for(; i < this->db->shards.size(); ++i){
        this->db->vserver->execute(i, chdb_protocol::Rollback, chdb_protocol::operation_var{.tx_id = tx_id}, r);
    }
    printf("tx[%d] abort\n", tx_id);
    for(auto it = tx_locknum[tx_id].begin(); it != tx_locknum[tx_id].end(); ++it){
        twopls[*it]->txlock->unlock();
    }
    return 0;
}
