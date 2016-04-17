#include <unistd.h>
#include <unordered_map>
#include <mutex>
#include <cassert>

#include <sys/types.h>
#include <sys/syscall.h>
#include <pthread.h>

#include "picodefs.hpp"
#include "picostore.hpp"
#include "wsutils.hpp"
#include "picoutils.hpp"

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/trim.hpp>
namespace ba = boost::algorithm;

using std::unordered_map;
using std::mutex;
using std::cout;
using std::cerr;
using std::endl;

void fill_default_options(PicoStoreOptions *options)
{
	options->acc_key = "";
	options->sec_key = "";
	options->host = "";
	options->is_https = false;
	options->cache_root = "/tmp/pico_cache";
	options->max_conn_pico = 0;
	options->bucket = "picocenter";
	options->block_size = BLOCK_SIZE;
}

void print_options(PicoStoreOptions *options)
{
	cout<<"============== Pretty print PicoStoreOptions ================="<<endl;
	cout<<"      acc_key: "<<options->acc_key<<endl;
	cout<<"      sec_key: "<<"***hide***"<<endl;//options->sec_key<<endl;
	cout<<"         host: "<<options->host<<endl;
	cout<<"     is_https: "<<options->is_https<<endl;
	cout<<"   cache_root: "<<options->cache_root<<endl;
	cout<<"max_conn_pico: "<<options->max_conn_pico<<endl;
	cout<<"       bucket: "<<options->bucket<<endl;
	cout<<"   block_size: "<<options->block_size<<endl;
	cout<<"==================== End of printing  ========================"<<endl;
}

void load_options_env(PicoStoreOptions *options)
{
	fill_default_options(options);
    readEnvVar( "WS_ACCESS_KEY", &options->acc_key );
    readEnvVar( "WS_SECRET_KEY", &options->sec_key );
    readEnvVar( "WS_BUCKET_NAME", &options->bucket );
    readEnvVar( "WS_HOST", &options->host );
    readEnvVar( "PICO_BUCKET", &options->bucket );
    readEnvVar( "PICO_CACHE_ROOT", &options->cache_root );

    string tmp;
    readEnvVar( "WS_IS_HTTPS", &tmp);
    options->is_https = (tmp == "true");

    readEnvVar( "PICO_BLOCK_SIZE", &tmp);
    if (tmp.size() != 0)
    	options->block_size = std::stoi(tmp);

}

////////////////////////////
//        Defines         //
////////////////////////////

class PicoWrapper;

class PicoCollection
{
private:
	unordered_map<int, PicoWrapper*> container;
	mutex mtx;
	string cache_root;
	string bucket;
	ConnectionManager *conn_man;
	int block_size;

public:
	PicoCollection(string cache_root, string bucket, ConnectionManager *conn_man, int block_size);
	~PicoCollection();
	// ConnectionManager* get_connection_manager() {return conn_man;}
	PicoCarrier* create_carrier(int pico_id);
	PicoWrapper* get_or_create(int pico_id);
	void destroy(int pico_id);
};

class PicoWrapper
{
private:
	PicoCollection* picos;
	int pico_id;
	PicoCarrier* pico;
	pthread_rwlock_t rwlock;

public:
	PicoWrapper(PicoCollection* picos, int pico_id);
	~PicoWrapper();
	void control(string cmd);
	int get_page_range(string path, char *buf, size_t size, off_t offset);
};

////////////////////////////
//     PicoCollection     //
////////////////////////////

PicoCollection::PicoCollection(string cache_root, string bucket
	, ConnectionManager *conn_man, int block_size) 
	: container() 
	, mtx()
	, cache_root(cache_root)
	, bucket(bucket)
	, conn_man(conn_man)
	, block_size(block_size)
{

}

PicoCollection::~PicoCollection()
{
	mtx.lock();
	for (auto it = container.begin(); it != container.end(); ++it) {
		delete it->second;
	}
	mtx.unlock();
}

PicoWrapper* PicoCollection::get_or_create(int pico_id)
{
	assert(pico_id >= 0);

	PicoWrapper *pico = nullptr;
	mtx.lock();

	auto it = container.find(pico_id);
	if (it == container.end()) {
		// pico = create_carrier(pico_id);
		pico = new PicoWrapper(this, pico_id);
		container.insert({pico_id, pico});
	} else {
		pico = it->second;
	}

	mtx.unlock();
	return pico;
}

void PicoCollection::destroy(int pico_id)
{
	assert(0); // let picowrapper manage 
	mtx.lock();
	
	auto it = container.find(pico_id);
	if (it != container.end()) {
		delete it->second;
		container.erase(it);
	}
	mtx.unlock();
}

PicoCarrier *PicoCollection::create_carrier(int pico_id)
{
	return new PicoCarrier(pico_id, cache_root, bucket, conn_man, block_size);
}


////////////////////////////
//       PicoWrapper      //
////////////////////////////

PicoWrapper::PicoWrapper(PicoCollection* picos, int pico_id)
	: picos(picos)
	, pico_id(pico_id)
	// , pico(nullptr)
{
	pthread_rwlock_init(&rwlock, nullptr); // init with default policy
	pico = picos->create_carrier(pico_id);
}

PicoWrapper::~PicoWrapper()
{
	pthread_rwlock_destroy(&rwlock);
}

void PicoWrapper::control(string cmd)
{
	pthread_rwlock_wrlock(&rwlock);

	if (ba::starts_with(cmd, CMD_DOWNLOAD)) {
		string checkpoint_dir = cmd.substr(sizeof(CMD_DOWNLOAD));
		pico->download_checkpoint(checkpoint_dir);
	} else if (ba::starts_with(cmd, CMD_UPLOAD)) {
		string checkpoint_dir = cmd.substr(sizeof(CMD_UPLOAD));
		pico->upload_checkpoint(checkpoint_dir);
	} else if (ba::starts_with(cmd, CMD_DELETE)) {
		pico->delete_cache(); 
		delete pico;
		// create a new one for future uses. 
		// I don't want to create another lock for the creation of the pico object, 
		// so just always keep one.
		pico = picos->create_carrier(pico_id); 
	}

	pthread_rwlock_unlock(&rwlock);
}

int PicoWrapper::get_page_range(string path, char *buf, size_t size, off_t offset) 
{
	int result = -1;
	pthread_rwlock_rdlock(&rwlock);
	result = pico->get_page_range(path, buf, size, offset);
	pthread_rwlock_unlock(&rwlock);
	return result;
}


////////////////////////////
//        PicoStore       //
////////////////////////////

PicoStore::PicoStore(PicoStoreOptions options) 
	: options(options) 
{
	conn_man = new ConnectionManager(options.acc_key, options.sec_key
		, options.host, options.is_https);
	picos = new PicoCollection(options.cache_root, options.bucket
		, conn_man, options.block_size);
}

PicoStore::~PicoStore()
{
	delete conn_man;
	delete picos;
	conn_man = nullptr;
	picos = nullptr;
}

void PicoStore::pico_control(int pico_id, string cmd)
{
	string trim_cmd = ba::trim_copy(cmd);
// #ifdef PICO_TRACE
	cout<<"PicoStore::pico_control pico_id="<<pico_id<<" cmd="<<trim_cmd<<endl;
// #endif
	PicoWrapper* pico = picos->get_or_create(pico_id);
	pico->control(trim_cmd);
	// if (ba::starts_with(trim_cmd, CMD_DELETE)) {
	// 	pico->delete_cache();
	// 	// picos->destroy(pico_id); // PicoWrapper manages the pico lifecycle.
	// } else {
	// 	pico->control(trim_cmd);
	// }
}

int PicoStore::read_page_data(const char *path, char *buf, size_t size, off_t offset)
{
#ifdef PICO_TRACE
	cout<<syscall(SYS_gettid)<<" read_page_data: "<<path<<" size="<<size<<" offset="<<offset<<endl;
#endif
	string str_path = path;
	if (str_path.size() == 0) return -1;

	int start = path[0] == '/' ? 1 : 0;
	int end = str_path.find('/', start);
	string s_pico_id = str_path.substr(start, end-start);
	int pico_id = std::stoi(s_pico_id);
	PicoWrapper* pico = picos->get_or_create(pico_id);
	string page_path = str_path.substr(end+1);
// #ifdef PICO_TRACE
// 	cout<<"pico_id="<<s_pico_id<<" page_path="<<page_path<<endl;
// #endif
	return pico->get_page_range(page_path, buf, size, offset);
}
