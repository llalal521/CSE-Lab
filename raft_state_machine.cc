#include "raft_state_machine.h"


kv_command::kv_command() : kv_command(CMD_NONE, "", "") { }

kv_command::kv_command(command_type tp, const std::string &key, const std::string &value) : 
    cmd_tp(tp), key(key), value(value), res(std::make_shared<result>())
{
    res->start = std::chrono::system_clock::now();
    res->key = key;
}

kv_command::kv_command(const kv_command &cmd) :
    cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), res(cmd.res) {}

kv_command::~kv_command() { }

int kv_command::size() const {
    // Your code here:
    return key.size() + value.size() + 4 + 6 + 1;
}


void kv_command::serialize(char* buf, int size) const {
    // Your code here:
    std::string serget;
    char a = '0' + (int)cmd_tp;
    serget = serget + a;
    serget = serget + "key:";
    serget = serget + key;
    serget = serget + "value:";
    serget = serget + value;
    for(int i = 0; i < serget.size(); ++i){
        buf[i] = serget[i];
    }
}

void kv_command::deserialize(const char* buf, int size) {
    // Your code here:
    std::string tmp;
    tmp = buf;
    size_t keysign = tmp.find("key:");
    size_t valuesign = tmp.find("value:");
    std::string tp = tmp.substr(0, keysign);
    cmd_tp = (command_type)std::atoi(tp.c_str());
    int keystart = keysign + 4;
    key = tmp.substr(keystart, valuesign - keystart);
    int valuestart = valuesign + 6;
    value = tmp.substr(valuestart);
    //std::cout << key << ' ' << value << std::endl;
}

marshall& operator<<(marshall &m, const kv_command& cmd) {
    // Your code here:
    return m << (int) cmd.cmd_tp << cmd.key << cmd.value;
}

unmarshall& operator>>(unmarshall &u, kv_command& cmd) {
    // Your code here:
    int cmd_tp;
    u >> cmd_tp >> cmd.key >> cmd.value;
    cmd.cmd_tp = (kv_command::command_type) cmd_tp;
    return u;
}

kv_state_machine::~kv_state_machine() {

}

void kv_state_machine::apply_log(raft_command &cmd) {
    kv_command &kv_cmd = dynamic_cast<kv_command &>(cmd);
    std::unique_lock <std::mutex> lock(kv_cmd.res->mtx);
    // Your code here:
    switch (kv_cmd.cmd_tp) {
        case kv_command::CMD_PUT: {
            //std::cout << kv_cmd.key << ' ' << kv_cmd.value << std::endl;
            put(kv_cmd.key, kv_cmd.value);
            kv_cmd.res->value = kv_cmd.value;
            kv_cmd.res->succ = true;
            break;
        }
        case kv_command::CMD_DEL: {
            std::string val = del(kv_cmd.key);
            kv_cmd.res->value = val;
            kv_cmd.res->succ = !val.empty();
            break;
        }
        case kv_command::CMD_GET: {
            std::string val = get(kv_cmd.key);
            kv_cmd.res->value = val;
            //std::cout << kv_cmd.res->value << std::endl;
            kv_cmd.res->succ = !val.empty();
            break;
        }
        case kv_command::CMD_NONE: {
            kv_cmd.res->value = kv_cmd.value;
            kv_cmd.res->succ = true;
            break;
        }
    }
    kv_cmd.res->key = kv_cmd.key;
    kv_cmd.res->done = true;
    kv_cmd.res->cv.notify_all();
    return;
}

std::vector<char> kv_state_machine::snapshot() {
    // Your code here:
    return std::vector<char>();
}

void kv_state_machine::apply_snapshot(const std::vector<char>& snapshot) {
    // Your code here:
    return;    
}
