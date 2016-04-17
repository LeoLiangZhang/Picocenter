#ifndef PICOSTORE_HPP
#define PICOSTORE_HPP

#include <string>

using std::string;

struct PicoStoreOptions
{
	string acc_key;
	string sec_key;
	string host;
	bool is_https;
	string cache_root;
	int max_conn_pico; // Max connections per PicoCarrier
	string bucket;
	int block_size;
};

class PicoCollection;
class ConnectionManager;

class PicoStore
{
private:
	PicoStoreOptions options;
	PicoCollection *picos;
	ConnectionManager *conn_man;

public:
	PicoStore(PicoStoreOptions options);
	~PicoStore();
	void pico_control(int pico_id, string cmd);
	int read_page_data(const char *path, char *buf, size_t size, off_t offset);

	int get_max_conn_pico() { return options.max_conn_pico; }
};

void load_options_env(PicoStoreOptions *options);
void print_options(PicoStoreOptions *options);
#endif
