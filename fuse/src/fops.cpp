#include "fops.hpp"

#include <cstring>
#include <cerrno>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

// #define HAVE_SETXATTR

#ifdef HAVE_SETXATTR
#include <attr/xattr.h>
#endif

#include <boost/algorithm/string.hpp>
namespace ba = boost::algorithm;

#include <boost/filesystem.hpp>
namespace bf = boost::filesystem;

#include "picodefs.hpp"
#include "picostore.hpp"

using std::string;
using std::cout;
using std::cerr;
using std::endl;


#define RETURN_IF_VIRTUAL_IPC(val) \
	if (ba::starts_with(path, VIRTUAL_FILE_PATH_PREFIX)) { \
		return (val); \
	}

#define RETURN_IF_PAGE(val) \
	{ \
	bf::_path = path; \
	string filename = _path.filename().string(); \
	if (ba::starts_with(filename, "pages-")) { \
		return (val); \
	} \
	}

int get_pico_id(const char *vpath)
{
	return atoi(vpath+sizeof(VIRTUAL_FILE_PATH_PREFIX)-1);
}

static PicoStore *store;
static int uid;
static int gid;

/* put S3 stuff in here */
struct private_data {
	bf::path root;
	private_data() : root(ROOT_DIR) {
		bf::create_directories(root);
	}
};

#define GET_CTX() fuse_get_context()
#define GET_PD() ((struct private_data*) (GET_CTX()->private_data))


static inline bf::path to_actual(const char *path) {
	struct private_data *pd = GET_PD();
	bf::path actual(pd->root);
	actual += path;
	return actual;
}


void * pico_init(struct fuse_conn_info *conn) {
	(void) conn;
	/* set up/verify capabilities and everything that you want here */
	private_data *priv = new private_data();

	PicoStoreOptions options;
	load_options_env(&options);
	print_options(&options);
	// priv->root = bf::path(options.cache_root);
	bf::create_directories(bf::path(options.cache_root));
	store = new PicoStore(options);

	uid = getuid();
	gid = getgid();

	return priv;
}

void pico_destroy(void * priv) {
	delete((private_data*)priv);
}

int pico_access(const char *path, int mode) {
	bf::path actual(to_actual(path));
	return access(actual.c_str(), mode) < 0 ? -errno : 0;
}

int pico_truncate(const char *path, off_t offset) {
	RETURN_IF_VIRTUAL_IPC(0);
	bf::path actual(to_actual(path));
	return truncate(actual.c_str(), offset) < 0 ? -errno : 0;
}

int pico_mkdir(const char *path, mode_t mode) {
	bf::path actual(to_actual(path));
	return mkdir(actual.c_str(), mode) < 0 ? -errno : 0;
}

int pico_unlink(const char *path) {
	bf::path actual(to_actual(path));
	return unlink(actual.c_str()) < 0 ? -errno : 0;
}

int pico_symlink(const char *target, const char *linkpath) {
	bf::path actual(to_actual(linkpath));
	return symlink(target, actual.c_str());
}

int pico_readlink(const char *path, char *buf, size_t size) {
	bf::path actual(to_actual(path));
	int res = readlink(actual.c_str(), buf, size);
	if (res > 0)
		buf[res] = 0;
#ifdef PICO_TRACE
	cerr<<"[DEBUG] readline("<<path<<") = "<<res<<":"<<buf<<endl;
#endif
	if (res < 0) return -errno;
	return 0;
}

int pico_flush(const char *path, struct fuse_file_info *finfo) {
	UNUSED(path);
	UNUSED(finfo);
	return 0;
}

int pico_rmdir(const char *path) {
	bf::path actual(to_actual(path));
	return rmdir(actual.c_str()) < 0 ? -errno : 0;
}

#ifdef HAVE_SETXATTR
int pico_getxattr(const char *path, const char *name, char *value, size_t size) {
	bf::path actual(to_actual(path));
	ssize_t got = getxattr(actual.c_str(), name, value, size);
	if (got < 0) {
		return -errno;
	}
	return got;
}

int pico_setxattr(const char *path, const char *name, const char *value, size_t size, int flags) {
	bf::path actual(to_actual(path));
	int ret = setxattr(actual.c_str(), name, value, size, flags);
	if (ret < 0) {
		return -errno;
	}
	return ret;
}
#endif

int pico_getattr_v(const char *path, struct stat *stbuf)
{
	assert(path);
	memset(stbuf, 0, sizeof(struct stat));
	stbuf->st_mode = S_IFREG | 0220;
	stbuf->st_nlink = 1;
	stbuf->st_uid = uid;
	stbuf->st_gid = gid;
	return 0;
}

int pico_getattr_internal(bf::path actual, struct stat *stbuf)
{
	int res = 0;
	memset(stbuf, 0, sizeof(struct stat));
	res = stat(actual.c_str(), stbuf);
#ifdef PICO_TRACE
	cerr<<"[DEBUG] stat("<<actual.c_str()<<") = "<<res<<endl;
#endif
	if (res != 0) {
		res = -errno;

		string filename = actual.filename().string();
		if (ba::starts_with(filename, PAGE_PREFIX)) {
			bf::path metapath = actual;
			metapath += META_SUFFIX;
			FILE *fp = fopen(metapath.c_str(), "r");
			if (fp == nullptr) {
				stbuf->st_size = 0;
				// My intention was trying to fake some pages file
				// without download the meta files, but criu don't
				// like it.
				return -errno; 
			} else {
				// char buf[100];
				// int rc = fread(buf, 1, sizeof(buf), fp);
				// string str_num = string(buf, rc);
				// long int sz = std::stol(str_num);
				// // cout<<metapath<<" has "<<sz<<endl;
				// pico_getattr_v(path, stbuf);
				// stbuf->st_size = sz;
				int rc = fread(stbuf, sizeof(struct stat), 1, fp);
				if (rc == 1) {
					res = 0;
				} else {
					res = -errno;
				}
				fclose(fp);
			}
			return 0;
		}
	}

	return res;
}

int pico_getattr(const char *path, struct stat *stbuf)
{
	RETURN_IF_VIRTUAL_IPC(pico_getattr_v(path, stbuf));

	bf::path actual(to_actual(path));

	return pico_getattr_internal(actual, stbuf);
}

int pico_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                 off_t offset, struct fuse_file_info *fi) {

	(void) offset; /* silence unused warnings */
	(void) fi;

	bf::path actual(to_actual(path));
	if (!bf::is_directory(actual)) {
		return -ENOENT;
	}

	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

	bf::directory_iterator it(actual);

	bf::directory_iterator end;
	
	for(; it != end; ++it) {
		string filename = it->path().filename().string();
		if (ba::starts_with(filename, "pages-") && ba::ends_with(filename, ".meta")) {
			filename = filename.substr(0, filename.size() - string(".meta").size());
		}

		struct stat stbuf;
		struct stat *ptr_stbuf = &stbuf;
		int rc = pico_getattr_internal(it->path(), &stbuf);
		if (rc != 0) ptr_stbuf = NULL;

		if (filler(buf, filename.c_str(), ptr_stbuf, 0)) {
			cerr << "[ERROR] Buffer full\n";
			return -ENOMEM;
		}
	}
	return 0;
}

int pico_create(const char *path, mode_t mode, struct fuse_file_info *finfo) {
	(void) finfo;
	bf::path actual(to_actual(path));
	int fd = creat(actual.c_str(), mode);
	if (fd >= 0) {
		close(fd);
	} else {
		return -errno;
	}
	return 0;
	
}

int pico_open(const char *path, struct fuse_file_info *fi) {
	RETURN_IF_VIRTUAL_IPC(0);

	/* just test we can open it by open/closing it. */
	int ret(0);
	bf::path actual(to_actual(path));
#ifdef PICO_TRACE
	cerr << "[DEBUG] Opening " << actual << " path " << path << endl;
#endif
	int fd = open(actual.c_str(), fi->flags);
	if (fd < 0) {
		ret = -errno;
		if (ba::starts_with(actual.filename().string(), PAGE_PREFIX)) {
			// check meta suffix later
			return 0;
		}
	} else {
		close(fd);
	}

	return ret;
}

int pico_release(const char *path, struct fuse_file_info *finfo) {
	(void) finfo;
	(void )path;
	return 0;
}

int pico_write_v(const char *path, const char *buf, size_t count, off_t offset, struct fuse_file_info *finfo) {
	assert(finfo);
	int pico_id = get_pico_id(path);
	// cout<<"pico_write_v: "<<path<<" count="<<count<<
	// 	" offset="<<offset<<" pico_id="<<pico_id<<" data:";
	// cout.write(buf, count);
	// cout<<endl;
	UNUSED(finfo);
	UNUSED(offset);
	string cmd(buf, count);
	store->pico_control(pico_id, cmd);
	return count;
}

#define HAS_PREAD_PWRITE 1

int pico_write(const char *path, const char *buf, size_t count, off_t offset, struct fuse_file_info *finfo) {
	bf::path actual(to_actual(path));

	RETURN_IF_VIRTUAL_IPC(pico_write_v(path, buf, count, offset, finfo));

	if (offset > (off_t)bf::file_size(actual)) {
		return 0;
	}

	int fd = open(actual.c_str(), finfo->flags);
	if (fd < 0) {
		return -errno;
	}

	int ret;
	ssize_t got;

#if HAS_PREAD_PWRITE
	got = pwrite(fd, buf, count, offset);
#else
	lseek(fd, offset, SEEK_SET);
	got = write(fd, buf, count);
#endif

	if (got < 0) {
		ret = -errno;
	} else {
		ret = got;
	}

	close(fd);
	return ret;
}

int pico_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
	bf::path actual(to_actual(path));

	// if (!bf::exists(actual)) {
	string filename = actual.filename().string();
	if (ba::starts_with(filename, PAGE_PREFIX)) {
		return store->read_page_data(path, buf, size, offset);
	}
	// }

	if (offset >= (off_t)bf::file_size(actual)) {
		return 0;
	}

	int fd = open(actual.c_str(), fi->flags);
	if (fd < 0) {
		return -errno;
	}

	int ret;
	ssize_t got;

#if HAS_PREAD_PWRITE
	got = pread(fd, buf, size, offset);
#else
	lseek(fd, offset, SEEK_SET);
	got = read(fd, buf, size);
#endif

	if (got < 0) {
		ret = -errno;
	} else {
		ret = got;
		if ((size_t)got < size) {
			bzero(buf + got, size - got);
		}
	}

	close(fd);
	return ret;
}
