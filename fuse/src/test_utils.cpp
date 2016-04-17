#include <boost/uuid/sha1.hpp>
#include <boost/algorithm/hex.hpp>
#include <boost/algorithm/string.hpp>
#include <iostream>
#include <cstdio>
using std::string;
using std::cout;
using std::endl;

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

int main(int argc, char const *argv[])
{
	(void)(argc);
	(void)(argv);
	char buf[4096];
	int size = fread(buf, 1, sizeof(buf), stdin);
	cout<<size<<endl;
	cout<<sha1_hex_digest(buf, size)<<endl;
}
