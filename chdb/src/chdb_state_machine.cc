#include "chdb_state_machine.h"

chdb_command::chdb_command() : chdb_command(CMD_NONE, 0, 0, 0) {
    // TODO: Your code here
}

chdb_command::chdb_command(command_type tp, const int &key, const int &value, const int &tx_id)
        : cmd_tp(tp), key(key), value(value), tx_id(tx_id), res(std::make_shared<result>()){
    // TODO: Your code here
    res->start = std::chrono::system_clock::now();
    res->key = key;
    res->tx_id = tx_id;
    res->value = value;
    res->tp = tp;
}

chdb_command::chdb_command(const chdb_command &cmd) :
        cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), tx_id(cmd.tx_id), res(cmd.res) {
    // TODO: Your code here
}


void chdb_command::serialize(char *buf, int size) const {
    // TODO: Your code here
    std::string serget;
    char a = '0' + (int)cmd_tp;
    serget = serget + a;
    serget = serget + "tx:";
    serget = serget + std::to_string(tx_id);
    serget = serget + "key:";
    serget = serget + std::to_string(key);
    serget = serget + "value:";
    serget = serget + std::to_string(value);
    for(int i = 0; i < serget.size(); ++i){
        buf[i] = serget[i];
    }

}

void chdb_command::deserialize(const char *buf, int size) {
    // TODO: Your code here
    std::string tmp;
    tmp = buf;
    size_t txsign = tmp.find("tx:");
    size_t keysign = tmp.find("key:");
    size_t valuesign = tmp.find("value:");
    std::string tp = tmp.substr(0, txsign);
    cmd_tp = (command_type)std::atoi(tp.c_str());
    int txstart = txsign + 3;
    tx_id = std::atoi(tmp.substr(txstart, keysign - txstart).c_str());
    int keystart = keysign + 4;
    key = std::atoi(tmp.substr(keystart, valuesign - keystart).c_str());
    int valuestart = valuesign + 6;
    value = std::atoi(tmp.substr(valuestart).c_str());
}

marshall &operator<<(marshall &m, const chdb_command &cmd) {
    // TODO: Your code here
    return m << (int) cmd.cmd_tp << cmd.tx_id << cmd.key << cmd.value ;
}

unmarshall &operator>>(unmarshall &u, chdb_command &cmd) {
    // TODO: Your code here
    int cmd_tp;
    u >> cmd_tp >> cmd.tx_id >> cmd.key >> cmd.value;
    cmd.cmd_tp = (chdb_command::command_type) cmd_tp;
    return u;
}

void chdb_state_machine::apply_log(raft_command &cmd) {
    // TODO: Your code here
    chdb_command &chdb_cmd = dynamic_cast<chdb_command &>(cmd);
    std::unique_lock <std::mutex> lock(chdb_cmd.res->mtx);
    // Your code here:
    switch (chdb_cmd.cmd_tp) {
        case chdb_command::CMD_PUT: {
            //std::cout << kv_cmd.key << ' ' << kv_cmd.value << std::endl;
            put(chdb_cmd.tx_id, chdb_cmd.key, chdb_cmd.value);
            chdb_cmd.res->value = chdb_cmd.value;
            chdb_cmd.res->succ = true;
            break;
        }
        case chdb_command::CMD_GET: {
            int val = get(chdb_cmd.tx_id, chdb_cmd.key);
            chdb_cmd.res->value = val;
            chdb_cmd.res->succ = true;
            break;
        }
        case chdb_command::CMD_NONE: {
            chdb_cmd.res->value = chdb_cmd.value;
            chdb_cmd.res->succ = true;
            break;
        }
    }
    chdb_cmd.res->key = chdb_cmd.key;
    chdb_cmd.res->done = true;
    chdb_cmd.res->cv.notify_all();
    return;
}