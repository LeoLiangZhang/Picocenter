#ifndef WSUTILS_HPP
#define WSUTILS_HPP

////////////////////////////
// Borrow from wscmd.cpp  //
////////////////////////////

#include <iostream>
#include "webstor/wsconn.h"
#include "webstor/sysutils.h"
using webstor::internal::Stopwatch;

using webstor::WsGetResponseLoader;
using webstor::WsPutRequestUploader;

static const webstor::internal::UInt64 s_verboseInterval = 3000; // in msecs.

class StreamLoader : public WsGetResponseLoader
{
public:
					StreamLoader( std::ostream *stream, bool verbose = false );
					~StreamLoader();

					// liang: call flush function when loading is done.
	void            flush();  // nofail

	virtual size_t  onLoad( const void *chunkData, size_t chunkSize, size_t totalSizeHint ); 
	
private:

	std::ostream   *m_stream;
	std::vector< char > m_buf;
	char           *m_p;
	size_t          m_size;
	size_t          m_totalReceived;

	bool            m_verbose;
	Stopwatch       m_stopwatch;
};

class StreamUploader : public WsPutRequestUploader
{
public:
					StreamUploader( std::istream *stream, bool verbose = false );
	size_t          totalSize() const { return m_totalSize; }
	virtual size_t  onUpload( void *chunkBuf, size_t chunkSize );
	
private:
	bool            fill();

	std::istream   *m_stream;
	std::vector< char > m_buf;
	char           *m_p;
	size_t          m_size;

	size_t          m_totalSize;
	size_t          m_totalSent;

	bool            m_verbose;
	Stopwatch       m_stopwatch;
};

void readEnvVar( const char *var, std::string *field );

#endif