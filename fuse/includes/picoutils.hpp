#ifndef PICOUTILS_HPP
#define PICOUTILS_HPP

#include <string>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <unordered_map>

using std::string;
using std::mutex;
using std::queue;
using std::condition_variable;
using std::unordered_map;
using std::lock_guard;
using std::unique_lock;

#include "webstor/wsconn.h"

using webstor::WsConnection;
using webstor::WsObject;
using webstor::WsGetResponse;
using webstor::WsPutResponse;
using webstor::WsListObjectsResponse;

////////////////////////////
// Internal class defines //
////////////////////////////

class ConnectionManager
{
private:
	string acc_key;
	string sec_key;
	string host;
	bool is_https;

	int count_all_conn;
	queue<WsConnection*> free_conns;
	mutex mtx;

	WsConnection *new_connection();

public:
	ConnectionManager(string acc_key, string sec_key, string host, bool is_https);
	WsConnection* get_connection();
	void release_connection(WsConnection* conn);

};

////////////////////////////
//        PartState       //
////////////////////////////

enum class PartStateEnum 
{
	init, failure, downloading, downloaded
};

class PartState
{
public:
	long int blk_num;
	PartStateEnum state;
	// condition_variable condition;
	mutex mtx;

	PartState(long int blk_num)
		: blk_num(blk_num)
		, state(PartStateEnum::init)
		// , condition()
		, mtx()
	{}
};

class PartFile
{
private:
	string path;
	unordered_map<long int, PartState*> parts;
	mutex mtx;

public:
	PartFile(string path) : path(path), parts(), mtx() {}

	~PartFile() 
	{
		for (auto it = parts.begin(); it != parts.end(); ++it) {
			delete it->second;
		}
		parts.clear();
	}

	PartState* get_or_create_part(long int blk_num)
	{
		lock_guard<mutex> lock(mtx);

		PartState* part = nullptr;
		auto it = parts.find(blk_num);
		if (it == parts.end()) {
			part = new PartState(blk_num);
			parts.insert({blk_num, part});
		} else {
			part = it->second;
		}
		return part;
	}
};

class PartFileCollection
{
private:
	unordered_map<string, PartFile*> files;
	mutex mtx;

public:
	PartFileCollection() : files() {}

	~PartFileCollection() 
	{
		for (auto it = files.begin(); it != files.end(); ++it) {
			delete it->second;
		}
		files.clear();
	}

	PartFile* get_or_create_file(string path)
	{
		lock_guard<mutex> lock(mtx);

		PartFile* file = nullptr;
		auto it = files.find(path);
		if (it == files.end()) {
			file = new PartFile(path);
			files.insert({path, file});
		} else {
			file = it->second;
		}
		return file;
	}
};

////////////////////////////
//       PicoCarrier      //
////////////////////////////

class PicoCarrier
{
private:
	int pico_id;
	string pico_cache_root; // e.g., /<fuse_mnt>/<pico_id>/
	string pico_prefix; // e.g., <pico_id>/
	string pico_page_log; // e.g., pico_cache_root + 
	string bucket;
	ConnectionManager *conn_man;
	int block_size;
	bool verbose;
	bool enable_page_log = true;
	bool enable_net_log = true;
	int num_retires; // Number of retries if network fails.
	// mutex mtx_ctrl;
	mutex mtx_log;
	PartFileCollection pfiles;

protected:
	bool download_file(WsConnection *conn, string path, string prefix);
	bool download_part(string path, long int blk_num);
	bool upload_file(WsConnection *conn, string path, string prefix);
	void log_page_read(string path, size_t size, off_t offset);

	bool try_download_file(WsConnection *conn, string path, string prefix);
	bool try_download_part(string s_part_path, string prefix, long int blk_num);
	bool try_upload_file(WsConnection *conn, string path, string prefix);

public:
	// cache_root is /<fuse_mnt>/ must include the last slash
	PicoCarrier(int pico_id, string cache_root, string bucket, 
		ConnectionManager *conn_man, int block_size, bool verbose=false);
	~PicoCarrier();
	void download_checkpoint(string checkpoint_dir);
	string create_checkpoint_meta_tarball(string checkpoint_dir);
	void upload_checkpoint(string checkpoint_dir);
	void ensure_cache_directory();
	int get_page_range(string path, char *buf, size_t size, off_t offset);
	void delete_cache();
	// void control(string cmd);

};

#endif