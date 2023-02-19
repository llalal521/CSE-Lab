#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>

#include <mutex>
#include <string>
#include <vector>
#include <map>
#include <algorithm>

#include "rpc.h"
#include "mr_protocol.h"
#include "extent_protocol.h"

using namespace std;

struct KeyVal {
    string key;
    string val;
};

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
vector<KeyVal> Map(const string &filename, const string &content)
{
    bool isWord = false;
    string key = "";
    vector<KeyVal> result;
    for(unsigned int i = 0; i < content.size(); ++i){
        if((content[i] <= 'z' && content[i] >= 'a') || (content[i] <='Z' && content[i] >= 'A')){
            isWord = true;
            key.push_back(content[i]);
        }
        else{
            if(isWord){
                KeyVal word;
                word.key = key;
                word.val = "1";
                result.push_back(word);
                key.clear();
            }
            isWord = false;
        }
    }
    return result;
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
string Reduce(const string &key, const vector < string > &values)
{
    int i = 0;
    for(int j = 0; j < values.size(); ++j){
        int value = atoi(values[j].c_str());
        i = i + value;
    }
    return std::to_string(i);
}


typedef vector<KeyVal> (*MAPF)(const string &key, const string &value);
typedef string (*REDUCEF)(const string &key, const vector<string> &values);

class Worker {
public:
	Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf);

	void doWork();

private:
	void doMap(int index, const vector<string> &filenames);
	void doReduce(int index, vector<int> map_id);
	void doSubmit(mr_tasktype taskType, int index);

	mutex mtx;
	int id;

	rpcc *cl;
	std::string basedir;
	MAPF mapf;
	REDUCEF reducef;
};


Worker::Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf)
{
	this->basedir = dir;
	this->mapf = mf;
	this->reducef = rf;

	sockaddr_in dstsock;
	make_sockaddr(dst.c_str(), &dstsock);
	this->cl = new rpcc(dstsock);
	if (this->cl->bind() < 0) {
		printf("mr worker: call bind error\n");
	}
}

void Worker::doMap(int index, const vector<string> &filenames)
{
	string content = "";
	vector<KeyVal> intermediate;
	for(int i = 0; i < filenames.size(); ++i){
		getline(ifstream(filenames[i]), content, '\0');
		intermediate = Map(filenames[i], content);
	}
	string result[4];
	cout << "begin part !!!!!!!!" << endl;
	for(const KeyVal &keyVal : intermediate){
		hash<string> hash_string;
		int index = hash_string(keyVal.key) % 4;
		result[index] += keyVal.key + "/" + keyVal.val + "/"; 
	}
	string outfile1 = this->basedir + "mr-" + std::to_string(index) + '-' + "0";
	string outfile2 = this->basedir + "mr-" + std::to_string(index) + '-' + "1";
	string outfile3 = this->basedir + "mr-" + std::to_string(index) + '-' + "2";
	string outfile4 = this->basedir + "mr-" + std::to_string(index) + '-' + "3";
	ofstream out(outfile1, ios::out);
	out << result[0];
	out.close();
	ofstream out1(outfile2, ios::out);
	out1 << result[1];
	out1.close();
	ofstream out2(outfile3, ios::out);
	out2 << result[2];
	out2.close();
	ofstream out3(outfile4, ios::out);
	out3 << result[3];
	out3.close();
}

void Worker::doReduce(int index, vector<int> map_id)
{
	vector<KeyVal> intermediate;
	KeyVal tmp;
	bool isKey = true;
	for(int i = 0; i < map_id.size(); ++i){
		string intermediate_name = this->basedir + "mr-" + std::to_string(map_id[i]) + '-' + std::to_string(index);
		cout << intermediate_name << endl;
		// getline(ifstream(intermediate_name), read_in, '\0');
		ifstream infile(intermediate_name);
		for (string line; std::getline(infile, line, '/'); ) {
        	if(isKey){
				tmp.key = line;
				isKey = false;
			} else{
				isKey = true;
				tmp.val = line;
				intermediate.push_back(tmp);
			}
    	}	
	} //read all files that to be reduced
	sort(intermediate.begin(), intermediate.end(),
    	[](KeyVal const & a, KeyVal const & b) {
		return a.key < b.key;
	});
	ofstream outfile(this->basedir + "mr-out-" + std::to_string(index));
	cout << this->basedir + "mr-out-" + std::to_string(index) << endl;
	for (unsigned int i = 0; i < intermediate.size();) {
        unsigned int j = i + 1;
        for (; j < intermediate.size() && intermediate[j].key == intermediate[i].key;)
            j++;

        vector < string > values;
        for (unsigned int k = i; k < j; k++) {
            values.push_back(intermediate[k].val);
        }

        string output = Reduce(intermediate[i].key, values);
		outfile << intermediate[i].key.data() << ' ';
		// cout << intermediate[i].key << ' ' << output << endl;
		outfile << output.data() << '\n';
        i = j;
    }
	outfile.close();
}

void Worker::doSubmit(mr_tasktype taskType, int index)
{
	bool b;
	mr_protocol::status ret = this->cl->call(mr_protocol::submittask, taskType, index, b);
	if (ret != mr_protocol::OK) {
		fprintf(stderr, "submit task failed\n");
		exit(-1);
	}
}

void Worker::doWork()
{
	for (;;) {
		cout << "ask work" << endl;
		mr_protocol::status ret = mr_protocol::OK;
		mr_protocol::AskTaskRequest req;
		req.id = this->id;
		mr_protocol::AskTaskResponse res;
		ret = cl->call(mr_protocol::asktask, req, res);
		if(res.taskType == NONE){
			sleep(1);
			continue;
		}
		if(res.taskType == MAP){
			cout << "begin doMap" << endl;
			doMap(res.task_id, res.filenames);
			doSubmit(res.taskType, res.task_id);
		}
		if(res.taskType == REDUCE){
			cout << "begin doReduce" << endl;
			doReduce(res.task_id, res.map_id);
			doSubmit(res.taskType, res.task_id);
		}
	}
}

int main(int argc, char **argv)
{
	if (argc != 3) {
		fprintf(stderr, "Usage: %s <coordinator_listen_port> <intermediate_file_dir> \n", argv[0]);
		exit(1);
	}

	MAPF mf = Map;
	REDUCEF rf = Reduce;
	
	Worker w(argv[1], argv[2], mf, rf);
	w.doWork();

	return 0;
}
