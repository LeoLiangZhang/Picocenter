#ifndef FOPS_HPP
#define FOPS_HPP

#define FUSE_USE_VERSION 26
#include <fuse.h>

#ifdef __cplusplus
extern "C" {
#endif
	void * pico_init(struct fuse_conn_info *conn);
	void pico_destroy(void * priv);

	int pico_access(const char *path, int mask);
	int pico_bmap(const char *path, size_t blocksize, uint64_t *idx);
	int pico_chmod(const char *path, mode_t mode);
	int pico_chown(const char *path, uid_t uid, gid_t gid);
	int pico_create(const char *path, mode_t mode, struct fuse_file_info *finfo);
	int pico_fallocate(const char *path, int mode, off_t offset, off_t len, struct fuse_file_info *finfo);
	int pico_fgetattr(const char *path, struct stat *statbuf, struct fuse_file_info *finfo);
	int pico_flock(const char *path, struct fuse_file_info *finfo, int op);
	int pico_flush(const char *path, struct fuse_file_info *finfo);
	int pico_fsync(const char *path, int isdatasync , struct fuse_file_info *finfo);
	int pico_fsyncdir(const char *path, int isdatasync, struct fuse_file_info *finfo);
	int pico_ftruncate(const char *path, off_t offset, struct fuse_file_info *finfo);
	int pico_getattr(const char *path, struct stat *statbuf);
	int pico_ioctl(const char *path, int cmd, void *arg, struct fuse_file_info *finfo, unsigned int flags, void *data);
	int pico_link(const char *oldpath, const char *newpath);
	int pico_lock(const char *path, struct fuse_file_info *finfo, int cmd, struct flock *locks);
	int pico_mkdir(const char *path, mode_t mode);
	int pico_mknod(const char *path, mode_t mode, dev_t dev);
	int pico_open(const char *path, struct fuse_file_info *finfo);
	int pico_opendir(const char *path, struct fuse_file_info *finfo);
	int pico_poll(const char *path, struct fuse_file_info *finfo, struct fuse_pollhandle *ph, unsigned *reventsp);
	int pico_read(const char *path, char *buf, size_t count, off_t offset, struct fuse_file_info *finfo);
	int pico_read_buf(const char *path, struct fuse_bufvec **bufp, size_t size, off_t off, struct fuse_file_info *finfo);
	int pico_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *finfo);
	int pico_readlink(const char *path, char *buf, size_t size);
	int pico_release(const char *path, struct fuse_file_info *finfo);
	int pico_releasedir(const char *path, struct fuse_file_info *finfo);
	int pico_rename(const char *oldpath, const char *newpath);
	int pico_rmdir(const char *path);
	int pico_statfs(const char *path, struct statvfs *buf);
	int pico_symlink(const char *target, const char *linkpath);
	int pico_truncate(const char *path, off_t offset);
	int pico_unlink(const char *path);
	int pico_utimens(const char *path, const struct timespec tv[2]);
	int pico_write(const char *path, const char *buf, size_t count, off_t offset, struct fuse_file_info *finfo);
	int pico_write_buf(const char *path, struct fuse_bufvec *buf, off_t off, struct fuse_file_info *finfo);

	int pico_getxattr(const char *path, const char *name, char *value, size_t size);
	int pico_listxattr(const char *path, char *list, size_t size);
	int pico_removexattr(const char *path, const char *name);
	int pico_setxattr(const char *path, const char *name, const char *value, size_t size, int flags);

#ifdef __cplusplus
};
#else

/* Put anything actually defined in fops.cpp here */

// #define HAVE_SETXATTR 1

#ifdef HAVE_SETXATTR
#define PICO_ATTR .getxattr = pico_getxattr, .setxattr = pico_setxattr
#else
#define PICO_ATTR
#endif

/* C++ struct init differs, so can't use this */
#define FUSE_OP_ASSIGN(var_name) struct fuse_operations var_name = {	  \
		.getattr = pico_getattr, \
		.access = pico_access, \
		.open = pico_open, \
		.read = pico_read, \
		.write = pico_write, \
		.readdir = pico_readdir, \
		.init = pico_init, \
		.destroy = pico_destroy, \
		.release = pico_release, \
		.create = pico_create, \
		.truncate = pico_truncate, \
		.mkdir = pico_mkdir, \
		.unlink = pico_unlink, \
		.symlink = pico_symlink, \
		.readlink = pico_readlink, \
		.flush = pico_flush, \
		.rmdir = pico_rmdir, \
		PICO_ATTR \
	};

#endif



#endif
