#ifndef mr_protocol_h_
#define mr_protocol_h_

#include <string>
#include <vector>

#include "rpc.h"

using namespace std;

#define REDUCER_COUNT 4

enum mr_tasktype {
	NONE = 0, // this flag means no task needs to be performed at this point
	MAP,
	REDUCE
};

class mr_protocol {
public:
	typedef int status;
	enum xxstatus { OK, RPCERR, NOENT, IOERR };
	enum rpc_numbers {
		asktask = 0xa001,
		submittask,
	};

	struct AskTaskResponse {
		vector<string> filenames;
		vector<int> map_id; //used to form the reduce file name
		mr_tasktype taskType;
		int task_id; //used to generate filename and sign finished tasks
	};

	struct AskTaskRequest {
		int id;
	};

	struct SubmitTaskResponse {
		// Lab2: Your definition here.
	};

	struct SubmitTaskRequest {
		// Lab2: Your definition here.
	};

};

inline marshall &
operator<<(marshall &m, mr_protocol::AskTaskResponse res){
	m << res.filenames;
	m << res.task_id;
	m << res.taskType;
	m << res.map_id;
	return m;
} 

inline unmarshall &
operator>>(unmarshall &um, mr_protocol::AskTaskResponse &res){
	int tmp;
	um >> res.filenames;
	um >> res.task_id;
	um >> tmp;
	um >> res.map_id;
	res.taskType = (mr_tasktype)tmp;
	return um;
} 

inline marshall &
operator<<(marshall &m, mr_protocol::AskTaskRequest req){
	m << req.id;
	return m;
}

inline unmarshall &
operator>>(unmarshall &um, mr_protocol::AskTaskRequest &req){
	um >> req.id;
	return um;
}

#endif

