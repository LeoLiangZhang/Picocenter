////////////////////////////
// Borrow from wscmd.cpp  //
////////////////////////////

#include <string.h>
#include "wsutils.hpp"

void readEnvVar( const char *var, std::string *field )
{
    // dbgAssert( var );
    // dbgAssert( field );

    const char *value = getenv( var );

    if( value && *value )
    {
        field->assign( value );
    }
}

StreamLoader::StreamLoader( std::ostream *stream, bool verbose )
	: m_stream( stream )
	, m_p( NULL )
	, m_size( 0 )
	, m_totalReceived( 0 )
	, m_verbose( verbose )
	, m_stopwatch( true )
{
	dbgAssert( stream );
	dbgAssert( *stream );
}

StreamLoader::~StreamLoader()
{
	flush(); // nofail
}

void            
StreamLoader::flush() // nofail
{
	// liang: call this function when loading is done.
	if( !m_size )
	{
		// Nothing to flush.

		return;
	}

	// Write the buffer.

	// std::cout<<"flush "<<m_size<<std::endl;
	m_stream->write( &m_buf[ 0 ], m_size );

	// Reset the position.

	m_p = &m_buf[ 0 ];
	m_size = 0;
}

size_t
StreamLoader::onLoad( const void *chunkData, size_t chunkSize, size_t totalSizeHint )
{
	// std::cout<<"chunkSize "<<chunkSize<<" totalSizeHint "<<totalSizeHint<<std::endl;
	if( m_buf.size() == 0 )
	{
		// Allocate a buffer.

		size_t bufferSize = 64 * 1024;

		if( totalSizeHint != 0 && totalSizeHint < bufferSize )
		{
			bufferSize = totalSizeHint;
		}

		m_buf.resize( bufferSize );
		m_p = &m_buf[ 0 ];
	}

	size_t copied = 0;

	while( true )
	{
		// Copy min.

		size_t toCopy = std::min( m_buf.size() - m_size, chunkSize );
		memcpy( m_p, chunkData, toCopy );

		chunkSize -= toCopy;
		m_size += toCopy;
		m_p += toCopy;
		copied += toCopy;

		if( !chunkSize )
		{
			// Copied everything from the provided buffer.

			break;
		}

		// Need to flush to the file, there is more to read from the provided 
		// buffer.

		chunkData = reinterpret_cast< const char *>( chunkData ) + toCopy;
		flush();
	}

	// Print progress.

	m_totalReceived += copied;

	if( m_verbose && m_stopwatch.elapsed() > s_verboseInterval )
	{
		std::cout << "Received: " << m_totalReceived << std::endl;
		m_stopwatch.start();
	}

	return copied;
}

StreamUploader::StreamUploader( std::istream *stream, bool verbose )
	: m_stream( stream )
	, m_p( NULL )
	, m_size( 0 )
	, m_totalSize( 0 )
	, m_totalSent( 0 )
	, m_verbose( verbose )
	, m_stopwatch( true )
{
	dbgAssert( stream );
	dbgAssert( *stream );

	// Get file size.

	stream->seekg( 0, std::ios::end );
	m_totalSize = stream->tellg();
	stream->seekg( 0, std::ios::beg );

	// Resize buffer and fill it.

	m_buf.resize( std::min( m_totalSize + !m_totalSize, ( size_t )64 * 1024 ) );
	fill();
}

bool            
StreamUploader::fill()
{
	dbgAssert( !m_size );

	if( m_stream->eof() )
	{
		// Nothing to read.

		return false;
	}

	// Read.

	m_stream->read( &m_buf[ 0 ], m_buf.size() );

	// Reset position.

	m_p = &m_buf[ 0 ];
	m_size = m_stream->gcount();

	return true;
}

size_t
StreamUploader::onUpload( void *chunkBuf, size_t chunkSize )
{
	size_t copied = 0;

	while( true )
	{
		if( !m_size && !fill() )
		{
			// End of file reached.

			break;
		}

		// Copy min.

		size_t toCopy = std::min( m_size, chunkSize );
		memcpy( chunkBuf, m_p, toCopy );

		chunkSize -= toCopy;
		m_size -= toCopy;
		m_p += toCopy;
		copied += toCopy;

		if( !chunkSize )
		{
			// The provided buffer is full.

			break;
		}

		// Need to read more from the file, there is some space left in the provided 
		// buffer.

		chunkBuf = reinterpret_cast< char *>( chunkBuf ) + toCopy;
	}


	// Print progress.

	m_totalSent += copied;

	if( m_verbose && m_stopwatch.elapsed() > s_verboseInterval )
	{
		std::cout << "Sent: " << m_totalSent << std::endl;
		m_stopwatch.start();
	}

	return copied;
}
