#include "rpc.h"
#include "raft_state_machine.h"


class chdb_command : public raft_command {
public:
    enum command_type {
        CMD_NONE = 0xdead,   // Do nothing
        CMD_GET,        // Get a key-value pair
        CMD_PUT,        // Put a key-value pair
    };

    // TODO: You may add more fields for implementation.
    struct result {
        std::chrono::system_clock::time_point start;
        int key, value, tx_id;
        command_type tp;

        bool succ;
        bool done;
        std::mutex mtx; // protect the struct
        std::condition_variable cv; // notify the caller
    };

    chdb_command();

    chdb_command(command_type tp, const int &key, const int &value, const int &tx_id);

    chdb_command(const chdb_command &cmd);

    virtual ~chdb_command() {}


    int key, value, tx_id;
    command_type cmd_tp;
    std::shared_ptr<result> res;


    virtual int size() const override {
        std::string keystring = std::to_string(key);
        std::string valuestring = std::to_string(value);
        std::string txstring = std::to_string(tx_id);
        return sizeof(int) * 3 + 1 + 3 + 4 + 6;
    }

    virtual void serialize(char *buf, int size) const override;

    virtual void deserialize(const char *buf, int size);
};

marshall &operator<<(marshall &m, const chdb_command &cmd);

unmarshall &operator>>(unmarshall &u, chdb_command &cmd);

class chdb_state_machine : public raft_state_machine {
public:
    virtual ~chdb_state_machine() {}

    // Apply a log to the state machine.
    // TODO: Implement this function.
    virtual void apply_log(raft_command &cmd) override;

    // Generate a snapshot of the current state.
    // In Chdb, you don't need to implement this function
    virtual std::vector<char> snapshot() {
        return std::vector<char>();
    }

    // Apply the snapshot to the state mahine.
    // In Chdb, you don't need to implement this function
    virtual void apply_snapshot(const std::vector<char> &) {}

    void put(int tx_id, int key, int value){
        std::map<int, int> *tmp;
        if(log.find(tx_id) != log.end()){
            tmp = &log[tx_id];
        }
        else{
            tmp = new std::map<int, int>();
            log.insert(std::pair<int, std::map<int, int> >(tx_id, *tmp));
        }
        if(tmp->find(key) != tmp->begin()){
            tmp->erase(key);
        }
        tmp->insert(std::pair<int, int>(key, value));
    }

    int get(int tx_id, int key){
        return log[tx_id][key];
    }

private:
    std::map<int, std::map<int, int> > log;
};