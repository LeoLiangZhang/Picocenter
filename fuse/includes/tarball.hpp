/*
* tarball.hpp
*
*  Created on: Jul 28, 2010
*      Author: Pierre Lindenbaum PhD
*              plindenbaum@yahoo.fr
*              http://plindenbaum.blogspot.com
*              
*              
*		Modified by OJ in order to make cross platform
*
*  Updated by Liang Zhang, Sep 18, 2015
*  
*    Upgraded to c++11
*    Added features:
*      + add directory to tar
*      + add symlink to tar
*      + extract tar created by this module
*              
*/

#ifndef TARBALL_HPP
#define TARBALL_HPP

#include <iostream>
#include <fstream>
#include <exception>
#include <stdexcept>
#include <cstdio>
#include <cstring>
#include <cerrno>
#include <ctime>
#include <cstdio>
#include <sstream>
#include <algorithm>

#include <sys/stat.h>
#include <unistd.h>

using std::ios;

namespace tar {

#define TARHEADER static_cast<PosixTarHeader*>(header)
	/* Values used in typeflag field.  */
#define REGTYPE  '0'            /* regular file */
#define AREGTYPE '\0'           /* regular file */
#define LNKTYPE  '1'            /* link */
#define SYMTYPE  '2'            /* reserved */
#define CHRTYPE  '3'            /* character special */
#define BLKTYPE  '4'            /* block special */
#define DIRTYPE  '5'            /* directory */
#define FIFOTYPE '6'            /* FIFO special */
#define CONTTYPE '7'            /* reserved */

	struct PosixTarHeader
	{
		char name[100];
		char mode[8];
		char uid[8];
		char gid[8];
		char size[12];
		char mtime[12];
		char checksum[8];
		char typeflag[1];
		char linkname[100];
		char magic[6];
		char version[2];
		char uname[32];
		char gname[32];
		char devmajor[8];
		char devminor[8];
		char prefix[155];
		char pad[12];
	};

#define	PosixTarHeaderSize 512

	/**
	*  A Tar Archive
	*/

	class Archive
	{
	private:
		bool _finished;
		std::string _user_name;
		void checkSizeOfTagHeader()
		{
			if (sizeof(PosixTarHeader) != PosixTarHeaderSize)
			{
				std::stringstream err;
				err << sizeof(PosixTarHeader);
				throw std::runtime_error(err.str());
			}
		}

	protected:
		std::ostream& out;
		void _init(void* header)
		{
			std::memset(header, 0, sizeof(PosixTarHeader));
			std::snprintf(TARHEADER->magic, sizeof(TARHEADER->magic), "ustar  ");
			std::snprintf(TARHEADER->mtime, sizeof(TARHEADER->mtime), "%011lo", time(NULL));
			// std::snprintf(TARHEADER->mode,sizeof(TARHEADER->mode),"%07o",0644);
			_mode(header, 0644);
			if (!_user_name.empty()) {
				std::snprintf(TARHEADER->uname, sizeof(TARHEADER->uname), "%s", _user_name.c_str());
				// std::snprintf(TARHEADER->gname,sizeof(TARHEADER->gname),"%s","users");
				std::snprintf(TARHEADER->gname, sizeof(TARHEADER->gname), "%s", _user_name.c_str());
			} else {
				// liang: leave uname and gname empty.
			}
		}

		void _checksum(void* header)
		{
			unsigned int sum = 0;
			char *p = (char *) header;
			char *q = p + sizeof(PosixTarHeader);
			while (p < TARHEADER->checksum) sum += *p++ & 0xff;
			for (int i = 0; i < 8; ++i)  {
				sum += ' ';
				++p;
			}
			while (p < q) sum += *p++ & 0xff;

			std::snprintf(TARHEADER->checksum, sizeof(TARHEADER->checksum), "%06o", sum);
		}

		void _size(void* header, unsigned long fileSize)
		{
			std::snprintf(TARHEADER->size, sizeof(TARHEADER->size), "%011llo", (long long unsigned int)fileSize);
		}

		void _mode(void* header, int mode)
		{
			std::snprintf(TARHEADER->mode, sizeof(TARHEADER->mode), "%07o", mode);
		}

		void _filename(void* header, const char* filename)
		{
			if (filename == NULL || filename[0] == 0 || std::strlen(filename) >= 100)
			{
				std::stringstream err;
				err << "invalid archive name \"" << filename << "\"";
				throw std::runtime_error(err.str());
			}
			strcpy(TARHEADER->name, filename);
		}

		void _linkname(void *header, const char *linkname)
		{
			strcpy(TARHEADER->linkname, linkname);
		}

		void _endRecord(std::size_t len)
		{
			char c = '\0';
			while ((len % sizeof(PosixTarHeader)) != 0)
			{
				out.write(&c, sizeof(char));
				++len;
			}
		}

	public:
		Archive(std::ostream & out_, const std::string & user_name) : out(out_)
		{
			checkSizeOfTagHeader();
			if (user_name.length() <= 32)
			{
				_user_name = user_name;
			}
		}

		Archive(std::ostream & out_) : out(out_)
		{
			checkSizeOfTagHeader();
		}

		virtual ~Archive()
		{
			if (!_finished)
			{
				std::cerr << "[warning]tar file was not finished." << std::endl;
			}
		}

		/** writes 2 empty blocks. Should be always called before closing the Tar file */
		void finish()
		{
			_finished = true;
			//The end of the archive is indicated by two blocks filled with binary zeros
			PosixTarHeader header;
			std::memset((void*)&header, 0, sizeof(PosixTarHeader));
			out.write((const char*)&header, sizeof(PosixTarHeader));
			out.write((const char*)&header, sizeof(PosixTarHeader));
			out.flush();
		}

		void put(const char* filename, const std::string & s)
		{
			put(filename, s.c_str(), s.size());
		}

		void put(const char* filename, const char* content)
		{
			put(filename, content, std::strlen(content));
		}

		void put(const char* filename, const char* content, std::size_t len)
		{
			PosixTarHeader header;
			_init((void*)&header);
			_filename((void*)&header, filename);
			header.typeflag[0] = 0;
			_size((void*)&header, len);
			_checksum((void*)&header);
			out.write((const char*)&header, sizeof(PosixTarHeader));
			out.write(content, len);
			_endRecord(len);
		}

		void putFile(const char* filename, const char* nameInArchive)
		{
			char buff[BUFSIZ];
			std::FILE* in = std::fopen(filename, "rb");
			if (in == NULL)
			{
				std::stringstream err;
				err << "Cannot open " << filename << " " << std::strerror(errno);
				throw std::runtime_error(err.str());
			}
			std::fseek(in, 0L, SEEK_END);
			long int len = std::ftell(in);
			std::fseek(in, 0L, SEEK_SET);

			PosixTarHeader header;
			_init((void*)&header);
			_filename((void*)&header, nameInArchive);
			header.typeflag[0] = 0;
			_size((void*)&header, len);
			_checksum((void*)&header);
			out.write((const char*)&header, sizeof(PosixTarHeader));

			std::size_t nRead = 0;
			while ((nRead = std::fread(buff, sizeof(char), BUFSIZ, in)) > 0)
			{
				out.write(buff, nRead);
			}
			std::fclose(in);

			_endRecord(len);
		}

		void putDir(const char* nameInArchive)
		{
			PosixTarHeader header;
			_init((void*)&header);
			_filename((void*)&header, nameInArchive);
			_mode(&header, 0755);
			header.typeflag[0] = DIRTYPE;
			_size((void*)&header, 0);
			_checksum((void*)&header);
			out.write((const char*)&header, sizeof(PosixTarHeader));
		}

		void putSymlink(const char *nameInArchive, const char *linkname)
		{
			PosixTarHeader header;
			_init((void*)&header);
			_filename((void*)&header, nameInArchive);
			_mode(&header, 0777);
			header.typeflag[0] = SYMTYPE; // got this by running gnu tar
			// e.g., tar -cf pico.tar --format=ustar sym
			_linkname(&header, linkname);
			_size((void*)&header, 0);
			_checksum((void*)&header);
			out.write((const char*)&header, sizeof(PosixTarHeader));
		}

		// impl. putSymlink
	};

	class Extractor
	{
	private:
		std::istream& in;

	protected:
		void readBlock(void *block)
		{
			in.read((char*)block, PosixTarHeaderSize); // should be 512
		}

		long int getSize(struct PosixTarHeader *header)
		{
			long int size;
			std::sscanf(header->size, "%011lo", &size);
			return size;
		}

		mode_t getMode(struct PosixTarHeader *header)
		{
			mode_t mode;
			std::sscanf(header->mode, "%07o", &mode);
			return mode;
		}

	public:
		Extractor(std::istream& in) : in(in) {}

		void extract(const char *dest)
		{
			// do not need trailing "/" in dest, e.g., "/path/to/dir"
			
			char block[PosixTarHeaderSize];
			if (dest == nullptr || dest[0] == 0)
			{
				throw std::runtime_error("invalid destination");
			}

			int numBlankBlock = 0;
			while (!in.eof()) {
				readBlock(block);
				if (block[0] == 0) {
					numBlankBlock ++;
					if (numBlankBlock == 2) {
						return;
					}
					continue;
				}
				struct PosixTarHeader *header = (struct PosixTarHeader *)block;
				char path[100];
				mode_t mode = getMode(header);
				std::snprintf(path, sizeof(path), "%s/%s", dest, header->name);
#ifdef PICO_TRACE
				std::cout<<"Extract "<<header->name<<" to "<<path<<std::endl;
#endif
				long int size = getSize(header);
				switch (header->typeflag[0]) 
				{
				case AREGTYPE:
				{
					std::ofstream myfile;
					myfile.open (path, ios::out | ios::binary); 
					while (size > 0) {
						readBlock(block);
						int len = size > PosixTarHeaderSize ? PosixTarHeaderSize : size;
						myfile.write(block, len);
						size -= PosixTarHeaderSize;
					};
					myfile.close();
					break;
				}
				case DIRTYPE:
				{
					if (mkdir(path, mode) != 0) {
						// ignore if dir exist
						// throw std::runtime_error("fail to make directory");
					}
					break;
				}
				case SYMTYPE:
				{
					if (symlink(header->linkname, path) != 0) { 
						// ignore error
					}
					break;
				}
				default:
					char t = header->typeflag[0];
					std::cerr << "Unknown type " << t << " of " << path << std::endl;
				}
				

			}

		}

	};

}

/* liang: original demo code, see my test_tar.cpp for more.

 * Boost IO streams GZIP Tutorials
 * How to Gzip encode entire folder with Boost IO streams
 * https://code.google.com/p/cloudobserver/wiki/TutorialsBoostIOstreams

#include <iostream>
#include <fstream>
#include <cstdlib>
#include <sstream>
#include <string>
#include <algorithm>

#include <boost/filesystem.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/stream.hpp>

#include "tarball.hpp"

void is_file( boost::filesystem::path p, lindenb::io::Tar & archive )
{
	std::string file_name_in_archive = p.relative_path().normalize().string();
	std::replace(file_name_in_archive.begin(), file_name_in_archive.end(), '\\', '/');
	file_name_in_archive = file_name_in_archive.substr(2, file_name_in_archive.size());
	archive.putFile(p.string().c_str(), file_name_in_archive.c_str());
}

void is_dir( boost::filesystem::path dir, lindenb::io::Tar & archive )
{
	if (!boost::filesystem::exists(dir))
	{
		return;
	}
	//create_file(dir, old_fs, new_fs);
	if (boost::filesystem::is_directory(dir))
	{
		boost::filesystem::directory_iterator dirIter( dir );
		boost::filesystem::directory_iterator dirIterEnd;

		while ( dirIter != dirIterEnd )
		{
			if ( boost::filesystem::exists( *dirIter ) && !boost::filesystem::is_directory( *dirIter ) )
			{
				try
				{
					is_file((*dirIter), archive);
				}
				catch (std::exception) {}
			}
			else
			{
				try
				{
					is_dir((*dirIter), archive);
				}
				catch (std::exception) {}
			}
			++dirIter;
		}
	}
}

int main(int argc, char** argv)
{
	std::stringstream out("pseudofile");
	lindenb::io::Tar tarball(out);
	is_dir("./myfiles", tarball);
	tarball.finish();
	{
		std::ofstream file( std::string( "hello folder.tar.gz" ).c_str(),  std::ofstream::binary);
		boost::iostreams::filtering_streambuf< boost::iostreams::input> in;
		in.push( boost::iostreams::gzip_compressor());
		in.push(out);
		boost::iostreams::copy(in, file);
	}
	std::cin.get();
	return 0;
}

*/

#endif // TARBALL_HPP