#include <cstdio>
#include <fstream>
#include <iostream>
#include <sstream> 
#include <cassert>

#include <boost/filesystem.hpp>
namespace bf = boost::filesystem;

#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filtering_stream.hpp>
namespace io = boost::iostreams;

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/hex.hpp>
namespace ba = boost::algorithm;
#include <boost/uuid/sha1.hpp>

#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>

#include <exception>
using std::exception;

using std::cout;
using std::cerr;
using std::endl;
using std::ofstream;

using std::ios_base;
// using std::ios_base::failbit;
// using std::ios_base::badbit;

#include "wsutils.hpp"
#include "tarball.hpp"
#include "picodefs.hpp"
#include "picoutils.hpp"

// local helper function

static string get_relative_path(const string& root, const bf::path& path)
{
	string p = path.string();
	int pos = root.size();
	while (p[pos] == '/') pos++;
	return p.substr(pos);
}

static bool is_page_file(bf::path path)
{
	string filename = path.filename().string();
	// return filename.find(PAGE_PREFIX) == 0;
	return ba::starts_with(filename, PAGE_PREFIX);
}

static bool is_meta_file(bf::path path)
{
	string filename = path.filename().string();
	// return filename.rfind(META_SUFFIX) == filename.size()-5;
	return ba::ends_with(filename, META_SUFFIX);
}

static bool is_part_file(bf::path path)
{
	string filename = path.filename().string();
	return ba::ends_with(filename, PART_SUFFIX);
}

// More tools
string sha1_hex_digest(void const* buffer, size_t size)
{
	boost::uuids::detail::sha1 sha;
	char hash[20];
	sha.process_bytes(buffer, size);
	unsigned int digest[5];
	sha.get_digest(digest);
	for(int i = 0; i < 5; ++i)
	{
		const char* tmp = reinterpret_cast<char*>(digest);
		hash[i*4] = tmp[i*4+3];
		hash[i*4+1] = tmp[i*4+2];
		hash[i*4+2] = tmp[i*4+1];
		hash[i*4+3] = tmp[i*4];
	}
	string hex = boost::algorithm::hex(string(hash, sizeof(hash)));
	boost::algorithm::to_lower(hex);
	return hex;
}

long timevaldiff(struct timeval *starttime, struct timeval *finishtime)
{
  long usec;
  usec=(finishtime->tv_sec-starttime->tv_sec)*1000000;
  usec+=(finishtime->tv_usec-starttime->tv_usec);
  return usec;
}

#ifdef PICO_TRACE

// must use these functions as a pair
#define BEGIN_LOG_NET() \
	struct timeval _tv_log_net_start; \
	int _rc_log_net; \
	_rc_log_net = gettimeofday(&_tv_log_net_start, nullptr); \
	assert(_rc_log_net == 0); \

#define END_LOG_NET(op_msg, rangeStart, size, status, prefix, msg) \
	struct timeval _tv_log_net_end; \
	_rc_log_net = gettimeofday(&_tv_log_net_end, nullptr); \
	assert(_rc_log_net == 0); \
	long _td_log_net = timevaldiff(&_tv_log_net_start, &_tv_log_net_end); \
	char _buf_log_net[500]; \
	_rc_log_net = snprintf(_buf_log_net, sizeof(_buf_log_net), \
		"[%ld.%06ld] %s(rangeStart=%ld, total=%ld usec, size=%ld byte, status=%s): %s %s\n", \
		_tv_log_net_end.tv_sec, _tv_log_net_end.tv_usec, op_msg, rangeStart, _td_log_net, size, status, prefix, msg); \
	assert(_rc_log_net >= 0); \
	cout << _buf_log_net;

#else

#define BEGIN_LOG_NET()

#define END_LOG_NET(op_msg, rangeStart, size, status, prefix, msg)

#endif

////////////////////////////
//       PicoCarrier      //
////////////////////////////

PicoCarrier::PicoCarrier(int pico_id, string cache_root, string bucket, ConnectionManager *conn_man, int block_size, bool verbose)
	: pico_id(pico_id)
	, bucket(bucket)
	, conn_man(conn_man)
	, block_size(block_size)
	, verbose(verbose)
	, num_retires(3)
	// , mtx_ctrl()
	, mtx_log()
	, pfiles()
{
	assert(conn_man);

	bf::path root{cache_root};
	root /= std::to_string(pico_id);
	pico_cache_root = root.string() + "/";
	pico_prefix.reserve(20);
	pico_prefix += "pico/";
	pico_prefix += std::to_string(pico_id);
	pico_prefix += "/";
	pico_page_log = pico_cache_root + PAGE_READ_LOG_FILENAME;
	// cout<<"pico_cache_root="<<pico_cache_root<<endl;
	ensure_cache_directory();
}

PicoCarrier::~PicoCarrier()
{
	conn_man = nullptr;
}

void PicoCarrier::ensure_cache_directory()
{
	bf::create_directories(bf::path(pico_cache_root));
}

/* This function is moved to PicoWrapper
void PicoCarrier::control(string cmd)
{
//	mtx_ctrl.lock(); // I forgot why I commented it.
	if (ba::starts_with(cmd, CMD_DOWNLOAD)) {
		string checkpoint_dir = cmd.substr(sizeof(CMD_DOWNLOAD));
		download_checkpoint(checkpoint_dir);
	} else if (ba::starts_with(cmd, CMD_UPLOAD)) {
		string checkpoint_dir = cmd.substr(sizeof(CMD_UPLOAD));
		upload_checkpoint(checkpoint_dir);
	} else if (ba::starts_with(cmd, CMD_DELETE)) {
		delete_cache(); // this command is handled in PicoStore.
	}
//	mtx_ctrl.unlock();
}
*/

void PicoCarrier::download_checkpoint(string checkpoint_dir)
{
	bf::path tar_path = pico_cache_root;
	tar_path /= checkpoint_dir + META_TAR_GZ;
	string s_tar_path = tar_path.string();
	string prefix = pico_prefix + get_relative_path(pico_cache_root, s_tar_path);
// #ifdef PICO_TRACE
//     cout << "Trying to get connection when downloading " << checkpoint_dir << endl;
// #endif
	WsConnection *conn = conn_man->get_connection();
	bool result = download_file(conn, tar_path.string(), prefix);
 	conn_man->release_connection(conn);
 	if(!result){
 		return;
 	}

 	string dir_path = pico_cache_root+checkpoint_dir;
 	bf::create_directories(bf::path(dir_path));

 	std::ifstream in_file;
	in_file.open(s_tar_path);
	if (!in_file.is_open()) {
		perror(("error while opening file " + s_tar_path).c_str());
		return; 
	}
	in_file.exceptions(ios_base::failbit | ios_base::badbit);

	io::filtering_istream in_stream;
	in_stream.push(io::gzip_decompressor());
	in_stream.push(in_file);

	tar::Extractor tar(in_stream);
	tar.extract(dir_path.c_str());
	io::close(in_stream);
	in_file.close();
}

static void write_to_file(string &path, const char *buf, size_t size)
{
	ofstream myfile;
	myfile.open(path);
	if (myfile.is_open()) {
		myfile.write(buf, size);
		myfile.close();
	} else {
		cerr<<__func__<<": cannot write to "<<path<<endl;
	}
}

string PicoCarrier::create_checkpoint_meta_tarball(string checkpoint_dir)
{
	// checkpoint_dir = "checkpoint1" etc.
	// the function return string(path_to_tarball)

	bf::path path = pico_cache_root + checkpoint_dir;
	bf::path tar_path = pico_cache_root;
	tar_path /= checkpoint_dir + META_TAR_GZ;

	io::filtering_ostream out_stream;
	out_stream.push(io::gzip_compressor());
	ofstream out_file;
	out_file.open(tar_path.string(), std::ios::out | std::ios::binary);
	if (!out_file.is_open()) {
		perror(("error while opening file " + tar_path.string()).c_str());
		return ""; // should throw exception? 
	}
	out_file.exceptions(ios_base::failbit | ios_base::badbit);

	out_stream.push(out_file);
	tar::Archive tar(out_stream);

#ifdef PICO_TRACE
	cout << "tar(" << tar_path << ")" << endl;
	string type;
#endif
	bf::recursive_directory_iterator it(path);
	bf::recursive_directory_iterator end;
	for (; it != end; ++it) {
		string rname = get_relative_path(path.string(), it->path());

		if (bf::is_symlink(it->path())) { 
		// check link first, or a symlink is also true as a regular file
#ifdef PICO_TRACE
			type = "symlink";
#endif
			bf::path target = bf::read_symlink(it->path());
			tar.putSymlink(rname.c_str(), target.string().c_str());
		} else if (bf::is_regular_file(it->path())) {
#ifdef PICO_TRACE
			type = "file";
#endif
			// string filename = it->path().filename().string();
			// if (filename.find("pages-") == 0) {
			if (is_meta_file(it->path())) {
#ifdef PICO_TRACE
				type = "file-meta";
#endif
			} else if (is_part_file(it->path())) {
#ifdef PICO_TRACE
				type = "file-part";
#endif
			} else if (is_page_file(it->path())) {
				// Save page size to pages-<pid>.img.meta file.
				int sz = bf::file_size(it->path());
#ifdef PICO_TRACE
				type = "file-page";
				// cout<<"sizeof "<<it->path()<<" "<<sz<<endl;
#endif
				// string content = std::to_string(sz);
				UNUSED(sz);
				struct stat content;
				int rc = stat(it->path().c_str(), &content);
				if (rc != 0) {
					cerr<<"error stat "<<it->path()<<endl;
				} else {
					string meta_path = rname + ".meta";
					tar.put(meta_path.c_str(), (const char*)&content, sizeof(content));
					string meta_full_path = it->path().string() + ".meta";
					write_to_file(meta_full_path, (const char*)&content, sizeof(content));
				}

#ifdef PICO_TRACE
				// create shasum file
				std::stringstream ss;
				char buf[block_size];
				FILE* fp = NULL;
				fp = fopen(it->path().c_str(), "r");
				if (fp == NULL) {
					cerr<<"could not open page file for shasum: "<<it->path()<<endl;
				} else {
					while (true) {
						if (feof(fp)) break;
						long pos = ftell(fp);
						assert(pos%block_size == 0); 
						size_t size = fread(buf, 1, sizeof(buf), fp);
						string sha_hex = sha1_hex_digest(buf, sizeof(buf));
						ss<<(pos/block_size)<<" "<<sha_hex<<endl;
						if (size != sizeof(buf)) {
							if (ferror(fp)){
								cerr<<"could not read page file for shasum: "<<it->path()<<endl;
							} // else eof
							break;
						}
					}
					fclose(fp);
					string s_shasum = ss.str();
					int s_shasum_size = s_shasum.size();
					string shasum_path = rname + ".shasum";
					tar.put(shasum_path.c_str(), (const char*)s_shasum.c_str(), s_shasum_size);
					string shasum_full_path = it->path().string() + ".shasum";
					write_to_file(shasum_full_path, (const char*)s_shasum.c_str(), s_shasum_size);
					cout << it->path() << " -> "<< rname << " is a shasum" << endl;
				}
#endif
			} else {
				tar.putFile(it->path().string().c_str(), rname.c_str());
			}
		} else if (bf::is_directory(it->path())) {
#ifdef PICO_TRACE
			type = "directory";
#endif
			tar.putDir(rname.c_str());
		} 
#ifdef PICO_TRACE
		cout << it->path() << " -> "<< rname << " is a " << type << endl;
#endif
	}
	tar.finish();
	// myfile.close();
	io::close(out_stream);
	out_file.close();
	return tar_path.string();
}

void PicoCarrier::upload_checkpoint(string checkpoint_dir)
{
	string tar_path = create_checkpoint_meta_tarball(checkpoint_dir);
	if (tar_path.size() == 0) {
		cerr << "Cannot create meta tarball: " << checkpoint_dir << endl;
		return; 
	}
	string prefix = pico_prefix + get_relative_path(pico_cache_root, tar_path);

	WsConnection *conn = conn_man->get_connection();
	upload_file(conn, tar_path, prefix);

	bf::path path = pico_cache_root + checkpoint_dir;

	bf::recursive_directory_iterator it(path);
	bf::recursive_directory_iterator end;
	for (; it != end; ++it) {
		if (bf::is_regular_file(it->path())) {
			if (is_page_file(it->path())) {
				prefix = pico_prefix + get_relative_path(pico_cache_root, it->path().string());
				// string prefix = pico_prefix + it->path().filename().string();
				upload_file(conn, it->path().string(), prefix);
			}
		}
	}

	conn_man->release_connection(conn);
}

int PicoCarrier::get_page_range(string path, char *buf, size_t size, off_t offset) {
	FILE *fp_page;
	int rc = -1;

	if (enable_page_log) {
		log_page_read(path, size, offset);
	}

	// if origin file exists, we just use it.
	bf::path path_(pico_cache_root+path);
	if (bf::exists(path_)) {
		fp_page = fopen(path_.c_str(), "r");
		if (fp_page) {
			fseek(fp_page, offset, SEEK_SET);
			rc = fread(buf, 1, size, fp_page);
			fclose(fp_page);
			return rc;
		}
		return rc;
	}

	// Download all parts for requested range first
	long int blk_start = offset / block_size;
	long int blk_end = (offset+size-1) / block_size;
	// cout<<"blk_start="<<blk_start<<" blk_end="<<blk_end<<endl;
	for (long int blk_num = blk_start; blk_num <= blk_end; ++blk_num) {
		if(!download_part(path, blk_num)) break;
	}
	
	// Fill buffer
	long int written = 0;
	char *ptr_buf = buf;
	off_t p_offset = offset;
	while (written < (long int)size) {
		int blk_offset = p_offset % block_size;
		long int blk_num = p_offset / block_size;
		string s_part_path = pico_cache_root + path + "." + std::to_string(blk_num) + ".part";
		size_t blk_read_size = block_size - blk_offset;
		if (blk_read_size > size - written) {
			blk_read_size = size - written;
		}
		fp_page = fopen(s_part_path.c_str(), "r");
		if (fp_page) {
			errno = 0;
			rc = fseek(fp_page, blk_offset, SEEK_SET);
			if (rc != 0) {
				cerr<<"get_page_range seek error: "<<s_part_path<<endl;
				fclose(fp_page);
				break;
			}
			errno = 0;
			rc = fread(ptr_buf, 1, blk_read_size, fp_page);
			// return rc;
			if (rc > 0) {
				written += rc;
				p_offset += rc;
				ptr_buf += rc;
			} else if (rc == 0 && errno == 0) {
				// reach the end of the file
				fclose(fp_page);
				break;
			}
			if (errno != 0) {
				char buf_err[500];
				snprintf(buf_err, sizeof(buf_err), 
					"get_page_range error=%d:%s, path=%s, written=%ld, blk_offset=%d, blk_read_size=%ld\n",
					errno, strerror(errno), s_part_path.c_str(), written, blk_offset, blk_read_size);
				cerr<<buf_err;
				// cerr<<"get_page_range part file reading error: "<<errno<<" "<<s_part_path<<endl;
				written = -errno;
				fclose(fp_page);
				break;
			}
			fclose(fp_page);
		} else {
			// return -1;
			cerr<<"get_page_range cannot open: "<<errno<<" "<<s_part_path<<endl;
			written = -errno;
			break;
		}
	}
	return written;
}

void PicoCarrier::log_page_read(string path, size_t size, off_t offset)
{
	struct timeval tv;
	int rc = gettimeofday(&tv, nullptr);
	assert(rc == 0);
	long int microsec = tv.tv_sec * 1000000L + tv.tv_usec;

	mtx_log.lock();
	std::ofstream slog;
	slog.open(pico_page_log, std::ofstream::out | std::ofstream::binary | std::ofstream::app);
	if (!slog.is_open()) {
		perror(("error while opening file " + pico_page_log).c_str());
		mtx_log.unlock();
		return;
	}
	// slog.exceptions(ios_base::failbit | ios_base::badbit);

	slog<<microsec<<" "<<path<<" "<<size<<" "<<offset<<endl;
	slog.close();
	mtx_log.unlock();
}

bool PicoCarrier::download_file(WsConnection *conn, string path, string prefix)
{
	int i;
	for (i = 0; i < num_retires; ++i) {
		bool success = try_download_file(conn, path, prefix);
		if(success) return true;
		cerr<<"try_download_file("<<prefix<<") fail, retry"<<endl;
	}
	return false;
}

bool PicoCarrier::try_download_file(WsConnection *conn, string path, string prefix)
{
	std::fstream stream;
	stream.open( path.c_str(), 
		std::ofstream::trunc | std::ofstream::out | std::ifstream::binary );
	if (!stream.is_open()) {
		perror(("error while opening file " + path).c_str());
		return false;
	}
	stream.exceptions(ios_base::failbit | ios_base::badbit);
	
	WsGetResponse response;
	StreamLoader loader( &stream, verbose );

	BEGIN_LOG_NET();
	try {
		conn->get(bucket.c_str(), prefix.c_str(), &loader, &response);
		END_LOG_NET("get", 0l, response.loadedContentLength, "OK", prefix.c_str(), path.c_str());
	} catch (const exception &e) {
		cerr << "try_download_file error: " << e.what() << '\n';
		END_LOG_NET("get", 0l, response.loadedContentLength, "FAIL", prefix.c_str(), path.c_str());
		return false;
	}

#ifdef PICO_TRACE
	if (response.loadedContentLength != (size_t)-1) {
		std::cout << "Downloaded: " << response.loadedContentLength << 
			" to " << path << endl;
	} 
#endif
	if (response.loadedContentLength == (size_t)-1) {
		cerr<<"Cloud storage object not found: "<<prefix<<endl;
	}
	loader.flush();
	stream.close();
	return response.loadedContentLength != (size_t)-1;
}

bool PicoCarrier::download_part(string path, long int blk_num)
{
	PartFile* pfile = pfiles.get_or_create_file(path);
	PartState* part = pfile->get_or_create_part(blk_num);

	bool success = false;
	lock_guard<mutex> lock(part->mtx);

	if (part->state == PartStateEnum::init) {
		part->state = PartStateEnum::downloading;
		// downloading
		string s_part_path = pico_cache_root + path + "." + std::to_string(blk_num) + ".part";
		string prefix = pico_prefix + path;
		int i;
		for (i = 0; i < num_retires; ++i) {
			success = try_download_part(s_part_path, prefix, blk_num);
			if(success) break;
			cerr<<"try_download_part("<<prefix<<") fail, retry"<<endl;
		}
		// update state
		part->state = success ? PartStateEnum::downloaded : PartStateEnum::failure;
	} else if (part->state == PartStateEnum::downloaded) {
		success = true;
	} else if (part->state == PartStateEnum::downloading) {
		// I'm not using condition_variable, should not in this state
		assert(0);
	} else { // failure
		success = false;
	}
	return success;
}

bool PicoCarrier::try_download_part(string s_part_path, string prefix, long int blk_num)
{

	bf::path path_(s_part_path);
	if (bf::exists(path_)) {
#ifdef PICO_TRACE
		cout << "try_download_part found AS file "<< s_part_path<<" for "<<prefix<< " start at "<<blk_num*block_size<<endl;
#endif
		return true;
	}

	std::fstream stream;
	stream.open( s_part_path.c_str(), 
		std::ofstream::trunc | std::ofstream::out | std::ifstream::binary );
	if (!stream.is_open()) {
		perror(("error while opening file " + s_part_path).c_str());
		// return -errno;
		return false;
	}
	stream.exceptions(ios_base::failbit | ios_base::badbit);
	
	WsGetResponse response;
	StreamLoader loader( &stream, verbose );
	WsConnection *conn = conn_man->get_connection();
	size_t rangeStart = blk_num * block_size;
	BEGIN_LOG_NET();
	try{
		conn->getRange(bucket.c_str(), prefix.c_str(), rangeStart, block_size, 
			&loader, &response);
		END_LOG_NET("getRange", rangeStart, response.loadedContentLength, "OK", prefix.c_str(), s_part_path.c_str());
	} catch (const exception &e) {
		cerr << "try_download_part error: " << e.what() << '\n';
		END_LOG_NET("getRange", rangeStart, response.loadedContentLength, "FAIL", prefix.c_str(), s_part_path.c_str());
		// return -1;
		return false;
	}
	conn_man->release_connection(conn);
	loader.flush();
	stream.close();

	if (response.loadedContentLength == (size_t)-1) {
		cerr<<"Cloud storage object not found: "<<prefix<<endl;
		// return rc;
		return false;
	} else if (response.loadedContentLength == (size_t)0) {
		// end of file.
		return true;
	}

#ifdef PICO_TRACE
	char buf[block_size];
	FILE* fp = NULL;
	fp = fopen(s_part_path.c_str(), "r");
	if (fp == NULL) {
		cerr<<"could not open part file for verification: "<<s_part_path<<endl;
		return false;
	}
	size_t size = fread(buf, 1, sizeof(buf), fp);
	if (size != sizeof(buf)) {
		cerr<<"could not read part file for verification: "<<s_part_path<<endl;
		return false;
	}
	string sha_hex = sha1_hex_digest(buf, sizeof(buf));
	cout<<"sha1_hex_digest "<<s_part_path<<" "<<blk_num<<" "<<sha_hex<<endl;
	fclose(fp);
#endif
	
	return true;
}

bool PicoCarrier::upload_file(WsConnection *conn, string path, string prefix)
{
	int i;
	for (i = 0; i < num_retires; ++i) {
		bool success = try_upload_file(conn, path, prefix);
		if(success) return true;
		cerr<<"try_upload_file("<<prefix<<") fail, retry"<<endl;
	}
	return false;
}

bool PicoCarrier::try_upload_file(WsConnection *conn, string path, string prefix)
{
	bool result = true;
	std::fstream stream;
	stream.open(path.c_str(), std::ifstream::in | std::ifstream::binary );
	if (!stream.is_open()) {
		cerr << "upload_file fail: cannot open file. " << path << endl;
		return false;
	}
	// stream.exceptions(ios_base::failbit );//| ios_base::badbit);

	bool makePublic = false;
	StreamUploader uploader( &stream, verbose );
	WsPutResponse response;
	
	BEGIN_LOG_NET();
	try
	{
		conn->put( bucket.c_str(), prefix.c_str(), 
			&uploader, uploader.totalSize(),
			NULL /* contentType */, conn->c_noCacheControl,
			makePublic, false /* useSrvEncrypt */,
			&response );
#ifdef PICO_TRACE
		std::cout << "Uploaded: " << uploader.totalSize() << endl;
#endif
		END_LOG_NET("put", 0l, uploader.totalSize(), "OK", prefix.c_str(), path.c_str());
	}
	catch (const exception &ex) {
		cerr<<"try_upload_file exception("<<path<<",prefix="<<prefix<<",type="<<typeid(ex).name()<<"): "<<ex.what()<<endl;
		result = false;
		END_LOG_NET("put", 0l, uploader.totalSize(), "FAIL", prefix.c_str(), path.c_str());
	}

	stream.close();
	return result;
}

void PicoCarrier::delete_cache()
{
	bf::path path = pico_cache_root;
	bf::remove_all(path);
}


////////////////////////////
//     implementations    //
////////////////////////////

ConnectionManager::ConnectionManager(string acc_key, string sec_key, string host, bool is_https)
	: acc_key(acc_key)
	, sec_key(sec_key)
	, host(host)
	, is_https(is_https)
	, count_all_conn(0)
	, free_conns()
	, mtx()
{

}

WsConnection* ConnectionManager::new_connection()
{
	webstor::WsStorType storType = webstor::WST_S3;
	bool ishttps = storType != webstor::WST_WALRUS && is_https;

	webstor::WsConfig config = {
		.accKey = acc_key.c_str(),
		.secKey = sec_key.c_str(),
		.host = host.c_str(),
		.port = NULL,
		.isHttps = ishttps,
		.storType = storType,
		.proxy = NULL,
		.sslCertFile = NULL
	};
	
	return new WsConnection(config);
}

WsConnection* ConnectionManager::get_connection()
{
	WsConnection *conn = nullptr;
	mtx.lock();

	if (free_conns.size() == 0) { // run out of free connection
		conn = new_connection();
		count_all_conn += 1;
	} else {
		conn = free_conns.front();
		free_conns.pop();
	}
#ifdef PICO_TRACE
	cout<<"Using connection "<< conn << endl;
#endif
	mtx.unlock();
	return conn;
}

void ConnectionManager::release_connection(WsConnection *conn)
{
	mtx.lock();
	free_conns.push(conn);
	mtx.unlock();
}


