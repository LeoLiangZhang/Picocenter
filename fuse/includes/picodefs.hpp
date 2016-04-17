#ifndef PICODEFS_HPP
#define PICODEFS_HPP

#define UNUSED(x) (void)(x)

#define ROOT_DIR "/tmp/pico_cache"
#define VIRTUAL_FILE_PATH_PREFIX "/.ipc."
#define PAGE_PREFIX "pages-"
#define META_SUFFIX ".meta"
#define PART_SUFFIX ".part"
#define META_TAR_GZ ".meta.tar.gz"

#define PAGE_SIZE 4096
#define BLOCK_SIZE (PAGE_SIZE*32)

#define CMD_DOWNLOAD "download"
#define CMD_UPLOAD "upload"
#define CMD_DELETE "delete"

#define PAGE_READ_LOG_FILENAME "pages.log"

#endif
