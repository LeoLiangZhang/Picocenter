#include <iostream>
#include <fstream>
#include <string>
#include <boost/filesystem.hpp>

#include "tarball.hpp"

#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filtering_stream.hpp>
namespace io = boost::iostreams;

using std::cout;
using std::endl;
using std::string;
using std::ofstream;
namespace bf = boost::filesystem;

string get_relative_path(const string& root, const bf::path& path)
{
	string p = path.string();
	int pos = root.size();
	while (p[pos] == '/') pos++;
	return p.substr(pos);
}

void test1(const char* path_)
{
	bf::path path(path_);
	string root = path_;


	io::filtering_ostream out_stream;
	out_stream.push(io::gzip_compressor());
	ofstream out_file;
	out_file.open("/tmp/test.tar.gz", std::ios::out | std::ios::binary);
	out_stream.push(out_file);
	tar::Archive tar(out_stream);

	// ofstream myfile;
	// myfile.open("/tmp/test.tar", std::ios::out | std::ios::binary);
	// tar::Archive tar(myfile);

	cout << "tar(\"" << path_ << "\")" << endl;
	string type;
	bf::recursive_directory_iterator it(path);
	bf::recursive_directory_iterator end;
	for (; it != end; ++it) {
		string rname = get_relative_path(root, it->path());

		if (bf::is_symlink(it->path())) { 
		// check link first, or a symlink is also true as a regular file
			type = "symlink";
			bf::path target = bf::read_symlink(it->path());
			tar.putSymlink(rname.c_str(), target.string().c_str());
		} else if (bf::is_regular_file(it->path())) {
			type = "file";
			tar.putFile(it->path().string().c_str(), rname.c_str());
		} else if (bf::is_directory(it->path())) {
			type = "directory";
			tar.putDir(rname.c_str());
		} 

		cout << it->path() << " -> "<< rname << " is a " << type << endl;
	}
	tar.finish();
	io::close(out_stream);
	out_file.close();
	cout<< "file is_open: " << out_file.is_open() << endl;

}

void test2()
{
	std::ifstream in_file;
	in_file.open("/tmp/test.tar.gz");

	io::filtering_istream in_stream;
	in_stream.push(io::gzip_decompressor());
	in_stream.push(in_file);

	tar::Extractor tar(in_stream);
	tar.extract("/tmp/tar");
}

int main(int argc, char const *argv[])
{
	if (argc < 2) {
		test1("/tmp/dir");
	} else {
		test1(argv[1]);
	}
	test2();
}
