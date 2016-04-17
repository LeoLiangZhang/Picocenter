#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cstring>
#include <iostream>
#include <cassert>

#include "webstor/wsconn.h"

#include "picodefs.hpp"
#include "picostore.hpp"
#include "picoutils.hpp"

using std::cout;
using std::endl;


void test_range_download(ConnectionManager *conn_man)
{
	char buf[4096*64];
	memset(buf, 0, sizeof(buf));
	string bucket = "picocenter";
	WsGetResponse response;
	WsConnection *conn = conn_man->get_connection();
	conn->getRange(bucket.c_str(), "pico/5/ckpt10/pages-1.img", buf, sizeof(buf), 0, 4096*32, &response);
	cout << "Downloaded: " << response.loadedContentLength << endl;
	// cout << "Result: " << buf <<endl;
	conn_man->release_connection(conn);
}

void test_download_page_range(ConnectionManager *conn_man)
{
	int pico_id = 5;
	string cache_root = "/tmp/pico_cache/";
	string bucket = "picocenter";
	bool verbose = true;
	PicoCarrier *carrier = new PicoCarrier(pico_id, cache_root, bucket, conn_man, verbose);
	char buf1[PAGE_SIZE];
	char buf2[PAGE_SIZE];
	int rc1 = carrier->get_page_range("ckpt10/pages-1.img", buf1, PAGE_SIZE, 0);
	int rc2 = carrier->get_page_range("ckpt11/pages-1.img", buf2, PAGE_SIZE, 0); // fetch from local
	cout << "rc1=" <<rc1<<", rc2="<<rc2<<", cmp="<<
		memcmp(buf1, buf2, PAGE_SIZE)<<endl;
	delete carrier;
}

void test_download(ConnectionManager *conn_man)
{
	int pico_id = 5;
	string cache_root = "/tmp/pico_cache/";
	string bucket = "picocenter";
	bool verbose = true;
	PicoCarrier *carrier = new PicoCarrier(pico_id, cache_root, bucket, conn_man, verbose);
	carrier->download_checkpoint("ckpt1");
	delete carrier;
}

void test_upload(ConnectionManager *conn_man)
{
	int pico_id = 5;
	string cache_root = "/tmp/pico_cache/";
	string bucket = "picocenter";
	bool verbose = true;
	PicoCarrier *carrier = new PicoCarrier(pico_id, cache_root, bucket, conn_man, verbose);
	carrier->upload_checkpoint("ckpt1");
	delete carrier;
}

ConnectionManager *create_connection_manager(const PicoStoreOptions *options)
{
	ConnectionManager *conn_man = new ConnectionManager(
		options->acc_key,
		options->sec_key,
		options->host,
		options->is_https
		);
	return conn_man;
}

int main(int argc, char const *argv[])
{
	int i;
	for (i = 0; i < argc; ++i) {
		printf("%d arg: %s\n", i, argv[i]);
	}

	PicoStoreOptions options;
	load_options_env(&options);
	ConnectionManager *conn_man = create_connection_manager(&options);

	string cmd = argv[1];

	cout<<"sizeof(upload)="<<sizeof("upload")<<endl;
	cout<<"sizeof(download_page_range)="<<sizeof("download_page_range")<<endl;
	
	if (cmd.compare("upload")==0)
		test_upload(conn_man);
	else if (cmd.compare("download")==0)
		test_download(conn_man);
	else if (cmd.compare("range_download")==0)
		test_range_download(conn_man);
	else if (cmd.compare("download_page_range")==0)
		test_download_page_range(conn_man);
	else 
		std::cerr<<"Unknown command."<<std::endl;

	return 0;
}
