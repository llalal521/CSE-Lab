#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <fstream>

template<typename command>
class raft_storage {
public:
    raft_storage(const std::string& file_dir);
    ~raft_storage();
    
    void delimstr();
    // Your code here
    void log_store(log_entry<command> log);
    void meta_read(int &current_term, int &voted_for); //used for restore
    void log_read(std::vector<log_entry<command> > &log);
    void meta_modify(int current_term, int voted_for);
    void log_pop();
private:
    std::mutex log_mtx;
    std::mutex meta_mtx;
    std::ifstream meta_in;
    std::ifstream log_in;
    std::ofstream meta_out;
    std::ofstream log_out;
    std::string filename1;
    std::string filename2;
};

template<typename command>
raft_storage<command>::raft_storage(const std::string& dir){
    // Your code here
    //std::unique_lock<std::mutex> lock(log_mtx);
    filename1 = dir + "/metadata.txt";
    filename2 = dir + "/log.txt";
    log_out.open(filename2, std::ios::app); // log only support appending
//     if(!log_out.is_open())    
//         std::cout << "open file failed" << std::endl;
}

template<typename command>
raft_storage<command>::~raft_storage() {
   // Your code here
   log_out.close();
}

template<typename command>
void raft_storage<command>::log_store(log_entry<command> entry){ //store the log entry
    std::unique_lock<std::mutex> lock(log_mtx);
    // log_out.open(filename2, std::ios::app);
    int term = entry.term;
    char *cmd = new char[entry.cmd.size()];
    entry.cmd.serialize(cmd, entry.cmd.size());
    if(!log_out.is_open())    
        std::cout << "open file failed" << std::endl;
    log_out << "[APPEND] " << "term " << term << ' ' ;
    log_out << "content ";
    //std::cout << "log store" << std::endl;
    for(int i = 0; i < entry.cmd.size(); ++i){
        log_out << cmd[i] - '\0' << ' ';
    }
    log_out << '\n';
    log_out.flush();
    // log_out.close();
}

template<typename command>
void raft_storage<command>::log_read(std::vector<log_entry<command> > &log){
    std::unique_lock<std::mutex> lock(log_mtx);
    //std::cout << filename2 << "read" << std::endl;x
    log_in.open(filename2);
    std::string buf;
    std::string content;
    int term = 0;
    while(std::getline(log_in, buf)){
        //std::cout << buf << std::endl;
        size_t pos = buf.find("[APPEND]");
        if(pos != std::string::npos){
            size_t delimpos = buf.find(' ');
            int n = 1;
            while(delimpos != std::string::npos){
                std::string substring = buf.substr(0, delimpos);
                if(n == 3)  term = std::atoi(substring.c_str());
                if(n >= 5){
                    int tmp = std::atoi(substring.c_str());
                    char ascii = '\0' + tmp;
                    content.push_back(ascii);
                }
                buf = buf.substr(delimpos + 1, buf.size());
                delimpos = buf.find(' ');
                n = n + 1;
            }
            command cmd;
            //std::cout << "content: " << content << std::endl;
            cmd.deserialize(content.c_str(), cmd.size());
            log_entry<command> entry;
            entry.term = term;
            entry.cmd = cmd;
            log.push_back(entry);
            buf.clear();
            content.clear();
            continue;
        }
        pos = buf.find("[DELETE]");
        if(pos != std::string::npos)
            log.pop_back();
        buf.clear();
    }
    log_in.close();
}

template<typename command>
void raft_storage<command>::log_pop(){
    std::unique_lock<std::mutex> lock(log_mtx);
    // log_out.open(filename2, std::ios::app);
    log_out << "[DELETE]";
    log_out << '\n';
    log_out.flush();
    // log_out.close();
}

template<typename command>
void raft_storage<command>::meta_read(int &current_term, int &voted_for){
    std::unique_lock<std::mutex> lock(meta_mtx);
    meta_in.open(filename1);
    meta_in >> current_term >> voted_for;
    meta_in.close();
}

template<typename command>
void raft_storage<command>::meta_modify(int current_term, int voted_for){
    std::unique_lock<std::mutex> lock(meta_mtx);
    meta_out.open(filename1, std::ios::out|std::ios::trunc);
    meta_out << current_term << ' ' << voted_for << ' ' << std::endl;
    meta_out.close();
}

#endif // raft_storage_h