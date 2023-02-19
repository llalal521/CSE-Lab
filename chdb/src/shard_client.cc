#include "shard_client.h"


int shard_client::put(chdb_protocol::operation_var var, int &r) {
    // TODO: Your code here
    // if(!prepareSet[var.tx_id])  return 0; //handle middle abort

    // if(!this->active){
    //     prepareSet[var.tx_id] = false;
    // }
    // prepareSet[var.tx_id] = false;
    std::map<int, value_entry> primary = store[primary_replica]; //WAL undo
    std::map<int, value_entry*> *undo = &(undos[var.tx_id]);
    std::map<int, value_entry> *redo = &(redos[var.tx_id]);
    if(primary.find(var.key) != primary.end()){
        undo->insert(std::pair<int, value_entry *>(var.key, &primary[var.key]));
    }
    else    undo->insert(std::pair<int, value_entry *>(var.key, nullptr));

    for(auto it = store.begin(); it != store.end(); ++it){ //execute
        value_entry entry;
        entry.value = var.value;
        if((*it).find(var.key) != (*it).end()){
            (*it).erase(var.key);
        }
        (*it).insert(std::pair<int, value_entry>(var.key, entry));
    }
    value_entry tmp;
    tmp.value = var.value;
    if(redo->find(var.key) != redo->end()){
        printf("here insert %d \n", shard_id);
        redo->erase(var.key);
        redo->insert(std::pair<int, value_entry>(var.key, tmp));
    }
    else{
        printf("here insert %d \n", shard_id);
        redo->insert(std::pair<int, value_entry>(var.key, tmp));
    }

    r = var.tx_id; 
    // prepareSet[var.tx_id] = true; //prepare ok
    return 0;
}

int shard_client::get(chdb_protocol::operation_var var, int &r) {
    // TODO: Your code here
    // get has no side effect, no need to write log
    //if(!prepareSet[var.tx_id])  return 0; //handle middle abort

    std::map<int, value_entry> primary = store[primary_replica];
    if(primary.find(var.key) == primary.end()){
        return 0;
    }
    r = primary[var.key].value; // value of r is not sure
    // prepareSet[var.tx_id] = true;
    return 0;
}

int shard_client::commit(chdb_protocol::commit_var var, int &r) {
    // TODO: Your code here
    r = var.tx_id;
    for(auto it = store.begin(); it != store.end(); ++it){ //execute
        for(auto log_it = redos[var.tx_id].begin(); log_it != redos[var.tx_id].end(); ++log_it){
            int key = log_it->first;
            value_entry entry = log_it->second;
            if((*it).find(key) != (*it).end())
                (*it).erase(key);
            (*it).insert(std::pair<int, value_entry>(key, entry));
        }
    }
    commitSet[var.tx_id] = true;
    return 0;
}

int shard_client::rollback(chdb_protocol::rollback_var var, int &r) {
    // TODO: Your code here
    for(auto it = store.begin(); it != store.end(); ++it){ //execute
        for(auto log_it = undos[var.tx_id].begin(); log_it != undos[var.tx_id].end(); ++log_it){
            int key = log_it->first;
            value_entry *entry = log_it->second;
            if((*it).find(key) != (*it).end())
                (*it).erase(key);
            if(entry != nullptr)
                (*it).insert(std::pair<int, value_entry>(key, *entry));
        }
    }
    commitSet[var.tx_id] = false;
    return 0;
}

int shard_client::check_prepare_state(chdb_protocol::check_prepare_state_var var, int &r) {
    // TODO: Your code here
    if(this->active || redos[var.tx_id].size() == 0)
        r = 1;
    else{
        printf("here not prepare %d, %d \n", shard_id, redos[var.tx_id].size());
        r = 0;
    }
    return 0;
}

int shard_client::prepare(chdb_protocol::prepare_var var, int &r) {
    // TODO: Your code here
    if(!prepareSet[var.tx_id])  return 0;
    prepareSet[var.tx_id] = false;
    if(active){  
        prepareSet[var.tx_id] = true;
    }
    return 0;
}

int shard_client::begin(chdb_protocol::operation_var var, int &r){
    // prepareSet.insert(std::pair<int, bool>(var.tx_id, true));
    commitSet.insert(std::pair<int, bool>(var.tx_id, false)); //update prepareState
    std::map<int, value_entry> redo;
    std::map<int, value_entry *> undo;
    redos.insert(std::pair<int, std::map<int, value_entry> >(var.tx_id, redo));
    //printf("initial %d", redos[var.tx_id].size());
    undos.insert(std::pair<int, std::map<int, value_entry*> >(var.tx_id, undo)); //initialize log{tx: {key: value: ,...}}
    return 0; 
}

shard_client::~shard_client() {
    delete node;
}