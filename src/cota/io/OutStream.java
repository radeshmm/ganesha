package cota.io;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.Socket;

import cota.crypto.Murmur;
import cota.util.Hashtable_io;
import cota.util.Util;


// Note that writeLine and writeString are not unicode safe!
// They assume that chars are one byte
//Combines a small buffer with the BufferedOutputStream class for increased performance
public class OutStream
	{
	int bufferSize = 1024 * 4;
	public byte[] buffer = null;

	// Where we should start writing data into the buffer
	public int start = 0;

	Socket socket = null;
	OutputStream out = null;

	boolean closed = false;

	static int numOpen = 0;

	public boolean debug = false;

	static int nextIndex = 0;
	int hashCode0 = 0;

	String stackTrace = "";

	static Hashtable_io f = new Hashtable_io();


	public int hashCode()
		{
		return hashCode0;
		}


	public OutStream()
		{
		stackTrace = cota.util.Util.currentStackTrace();

		buffer = new byte[bufferSize];

		hashCode0 = nextIndex++;
		}


	public OutStream( OutputStream out )
		{
		stackTrace = cota.util.Util.currentStackTrace();

		numOpen++;

		this.out = new BufferedOutputStream( out );

		buffer = new byte[bufferSize];

		hashCode0 = nextIndex++;
		}


	public OutStream( String fileName ) throws Throwable
		{
		stackTrace = cota.util.Util.currentStackTrace();
		closed = true;

		out = new BufferedOutputStream( new FileOutputStream( fileName ) );

		numOpen++;

		buffer = new byte[bufferSize];

		hashCode0 = nextIndex++;

		closed = false;
		}


	public OutStream( File f ) throws Throwable
		{
		stackTrace = cota.util.Util.currentStackTrace();
		closed = true;

		out = new BufferedOutputStream( new FileOutputStream( f ) );

		numOpen++;

		buffer = new byte[bufferSize];

		hashCode0 = nextIndex++;
		closed = false;
		}


	public OutStream( String ip, int port ) throws Throwable
		{
		stackTrace = cota.util.Util.currentStackTrace();
		closed = true;

		numOpen++;

		socket = new Socket( ip, port );
		socket.setSoTimeout( 1000 * 30 );

		out = new BufferedOutputStream( socket.getOutputStream() );

		buffer = new byte[bufferSize];

		hashCode0 = nextIndex++;
		closed = false;
		}


	public void close() throws Throwable
		{
		//		System.out.println( "CLOSING OutStream: " + out );

		if ( !closed )
			{
			numOpen--;
			closed = true;

			// It's really important to have the sockets clean up as much as
			// possible
			// Allow the steps of closure to happen within separate try blocks
			try
				{
				flush();
				}
			catch ( Throwable theX )
				{
				//				Util.printX( theX );
				}

			try
				{
				if ( out != null )
					out.close();

				out = null;
				}
			catch ( Throwable theX )
				{
				//				Util.printX( theX );
				}

			try
				{
				if ( socket != null )
					socket.close();
				}
			catch ( Throwable theX )
				{
				//				Util.printX( theX );
				}

			}
		}


	public void reassign( OutputStream out )
		{
		this.out = out;
		numOpen++;
		closed = false;
		start = 0;

		hashCode0 = nextIndex++;
		}


	// Flush the buffer
	public void flush() throws Throwable
		{
		if ( start == 0 )
			return;

		out.write( buffer, 0, start );
		out.flush();

		start = 0;
		}


	/**
	 * Write fixed number of bytes to the stream
	 * 
	 * @param bytes
	 *            the source array of data
	 * @param offset
	 *            where in the array we begin reading the data from
	 * @param length
	 *            the amount of data to be sent
	 */

	/*OLD
	 * 	public void writeBytes( byte[] bytes, int offset, int length ) throws Throwable
		{
		int spaceLeft = bufferSize - start;

		// see if there's enough space in the buffer
		if ( spaceLeft >= length )
			{
			System.arraycopy( bytes, offset, buffer, start, length );
			start = start + length;

			return;
			}

		// Write what we can to the buffer, and then flush it
		int numWritten = spaceLeft;
		System.arraycopy( bytes, offset, buffer, start, numWritten );
		start = start + numWritten;
		flush();

		// Write bufferSize chunks directly from the byte array (rather than
		// copying them to the buffer first)
		while ( ( length - numWritten ) > bufferSize )
			{
			out.write( bytes, offset + numWritten, bufferSize );
			out.flush();

			numWritten = numWritten + bufferSize;
			}

		// What's left to output from the inputed byte array is smaller than our
		// buffer, so just put rest in there
		// to be outputed later (once it is added to or flushed)
		System.arraycopy( bytes, offset + numWritten, buffer, 0, ( length - numWritten ) );
		start = ( length - numWritten );
		}
	*/

	public void writeBytes( byte[] bytes, int offset, int length ) throws Throwable
		{
		int spaceLeft = bufferSize - start;

		// see if there's enough space in the buffer
		if ( spaceLeft >= length )
			{
			System.arraycopy( bytes, offset, buffer, start, length );
			start = start + length;

			return;
			}

		// Write what was in the buffer
		if ( start != 0 )
			{
			out.write( buffer, 0, start ); // write all the bytes from the buffer
			start = 0;
			}

		// Write the entire byte array
		out.write( bytes, offset, length );

		// Need to flush the array as start has been set to 0 which will disable flushing
		out.flush();
		}


	/**
	 * Write a full byte array to the stream
	 */
	public void writeBytes( byte[] bytes ) throws Throwable
		{
		writeBytes( bytes, 0, bytes.length );
		}


	// 0..255
	public void writeByte( int n ) throws Throwable
		{
		int numLeft = bufferSize - start;

		if ( numLeft < 1 )
			flush();

		buffer[ start++ ] = (byte) ( n & 0xFF );
		}


	// 0..65536
	public void write2Bytes( int n ) throws Throwable
		{
		int numLeft = bufferSize - start;

		if ( numLeft < 2 )
			flush();

		buffer[ start++ ] = (byte) ( ( n >> 8 ) & 0xFF );
		buffer[ start++ ] = (byte) ( ( n ) & 0xFF );
		}


	// 0..16777215
	public void write3Bytes( int n ) throws Throwable
		{
		int numLeft = bufferSize - start;

		if ( numLeft < 3 )
			flush();

		buffer[ start++ ] = (byte) ( ( n >> 16 ) & 0xFF );
		buffer[ start++ ] = (byte) ( ( n >> 8 ) & 0xFF );
		buffer[ start++ ] = (byte) ( ( n ) & 0xFF );
		}


	// 0..2,147,483,647
	public void write4Bytes( int n ) throws Throwable
		{
		int numLeft = bufferSize - start;

		if ( numLeft < 4 )
			flush();

		buffer[ start++ ] = (byte) ( ( n >> 24 ) & 0xFF );
		buffer[ start++ ] = (byte) ( ( n >> 16 ) & 0xFF );
		buffer[ start++ ] = (byte) ( ( n >> 8 ) & 0xFF );
		buffer[ start++ ] = (byte) ( ( n ) & 0xFF );
		}


	// 0..2,147,483,647
	public void write8Bytes( long n ) throws Throwable
		{
		int numLeft = bufferSize - start;

		if ( numLeft < 8 )
			flush();

		buffer[ start++ ] = (byte) ( ( n >> 56 ) & 0xFF );
		buffer[ start++ ] = (byte) ( ( n >> 48 ) & 0xFF );
		buffer[ start++ ] = (byte) ( ( n >> 40 ) & 0xFF );
		buffer[ start++ ] = (byte) ( ( n >> 32 ) & 0xFF );
		buffer[ start++ ] = (byte) ( ( n >> 24 ) & 0xFF );
		buffer[ start++ ] = (byte) ( ( n >> 16 ) & 0xFF );
		buffer[ start++ ] = (byte) ( ( n >> 8 ) & 0xFF );
		buffer[ start++ ] = (byte) ( ( n ) & 0xFF );
		}


	public void writeDouble( double d ) throws Throwable
		{
		write8Bytes( Double.doubleToLongBits( d ) );
		}


	// Write a String to the stream
	// WARNING - not unicode!!!!!!!!!!!!!!!
	@SuppressWarnings( "deprecation" )
	public void writeString0( String s, boolean withLength ) throws Throwable
		{
		// if ( s == null )
		// throw new Throwable( "ATTEMPT TO WRITE NULL STRING TO THE STREAM" );

		int length = s.length();

		if ( withLength )
			write3Bytes( length );

		// see if there's enough space in the buffer
		int numLeft = bufferSize - start;
		if ( numLeft >= length )
			{
			s.getBytes( 0, length, buffer, start );
			start = start + length;

			return;
			}

		int numWritten = numLeft;
		s.getBytes( 0, numLeft, buffer, start );
		start = start + numWritten;

		flush();

		while ( ( length - numWritten ) > bufferSize )
			{
			s.getBytes( numWritten, numWritten + bufferSize, buffer, 0 );
			start = bufferSize;
			flush();

			numWritten = numWritten + bufferSize;
			}

		s.getBytes( numWritten, length, buffer, 0 );
		start = ( length - numWritten );
		}


	// Write a String to the stream
	// WARNING - not unicode!!!!!!!!!!!!!!!
	public void writeString( String s ) throws Throwable
		{
		writeString0( s, true );
		}


	// Write a plain String to the stream
	// WARNING - not unicode!!!!!!!!!!!!!!!
	public void writePlainString( String s ) throws Throwable
		{
		if ( debug )
			System.out.println( Thread.currentThread() + "O:::" + s );

		// System.out.print( "OUT: " + s );
		writeString0( s, false );
		}


	public void writeLine( String line ) throws Throwable
		{
		if ( debug )
			System.out.println( Thread.currentThread() + "O:::" + line + "#" );

		writePlainString( line );

		writeByte( '\r' );
		writeByte( '\n' );
		}


	// write fixed number of bytes to the stream
	public void writeStream( InStream in, long length ) throws Throwable
		{
		long spaceLeft = bufferSize - start;

		// See if there's enough space in the buffer
		if ( spaceLeft >= length )
			{
			in.readBytes( buffer, start, (int) length );
			start = start + (int) length;

			return;
			}

		// Write what we can to the buffer, and then flush it
		long numWritten = spaceLeft;
		in.readBytes( buffer, start, (int) numWritten );
		start = start + (int) numWritten;
		flush();

		// Flush chunks of the full bufferSize as long as we can
		while ( ( length - numWritten ) > bufferSize )
			{
			in.readBytes( buffer, 0, bufferSize );
			start = bufferSize;
			flush();

			numWritten = numWritten + bufferSize;
			}

		// What's left to output from the inStream is smaller than our buffer,
		// so just put rest in there
		// to be outputed later
		in.readBytes( buffer, 0, (int) ( length - numWritten ) );
		start = (int) ( length - numWritten );
		}


	public void fullyTransfer( InStream in ) throws Throwable
		{
		try
			{
			while ( true )
				{
				int avail = in.bytesInBuffer;
				if ( avail > 0 )
					writeStream( in, avail );

				in.fillBuffer();
				}
			}
		catch ( StreamClosedX ignored )
			{
			in.close();
			}

		flush();
		}


	public char convertToHex( int h )
		{
		if ( h <= 9 )
			return (char) ( '0' + h );

		return (char) ( 'A' + ( h - 10 ) );
		}


	public void writeUnicodeEscape( char c ) throws Throwable
		{
		int unicode = c;

		int h1 = ( unicode >> 12 ) & 0xF;
		int h2 = ( unicode >> 8 ) & 0xF;
		int h3 = ( unicode >> 4 ) & 0xF;
		int h4 = unicode & 0xF;

		writeByte( '\\' );
		writeByte( 'u' );
		writeByte( convertToHex( h1 ) );
		writeByte( convertToHex( h2 ) );
		writeByte( convertToHex( h3 ) );
		writeByte( convertToHex( h4 ) );
		}


	public void writeDBUnicodeEscape( char c ) throws Throwable
		{
		int unicode = c;

		int h1 = ( unicode >> 12 ) & 0xF;
		int h2 = ( unicode >> 8 ) & 0xF;
		int h3 = ( unicode >> 4 ) & 0xF;
		int h4 = unicode & 0xF;

		writeByte( '\\' );
		writeByte( '\\' );
		writeByte( 'u' );
		writeByte( convertToHex( h1 ) );
		writeByte( convertToHex( h2 ) );
		writeByte( convertToHex( h3 ) );
		writeByte( convertToHex( h4 ) );
		}


	public void writeUTF8Char( char c ) throws Throwable
		{
		if ( c <= 0x7F )
			{
			writeByte( c );

			return;
			}

		if ( c <= 0x7FF )
			{
			int ys = c >> 6;
			int zs = c & 0x3F;

			writeByte( 0xC0 | ys );
			writeByte( 0x80 | zs );

			return;
			}

		if ( c <= 0xFFFF )
			{
			int xs = c >> 12;
			int ys = ( c >> 6 ) & 0x3F;
			int zs = c & 0x3F;

			writeByte( 0xE0 | xs );
			writeByte( 0x80 | ys );
			writeByte( 0x80 | zs );

			return;
			}

		int ws = c >> 18;
		int xs = ( c >> 12 ) & 0x3F;
		int ys = ( c >> 6 ) & 0x3F;
		int zs = c & 0x3F;

		writeByte( 0xF0 | ws );
		writeByte( 0x80 | xs );
		writeByte( 0x80 | ys );
		writeByte( 0x80 | zs );
		}


	// Write a given string in JSON format and encode it in UTF-8
	// Note that the "'s are not written by this method
	public void writeJSONStringUTF8( String s ) throws Throwable
		{
		char[] chars = s.toCharArray();
		int length = chars.length;

		for ( int i = 0; i < length; i++ )
			{
			char c = chars[ i ];

			//	System.out.println( i + " " + (int) c + " " + c );

			boolean handled = false;

			// Check for characters that we will need to escape
			if ( c == '"' )
				{
				writeByte( '\\' );
				writeByte( '"' );

				handled = true;
				}

			if ( !handled )
				if ( c == '\\' )
					{
					writeByte( '\\' );
					writeByte( '\\' );

					handled = true;
					}

			if ( !handled )
				if ( c == '/' )
					{
					writeByte( '\\' );
					writeByte( '/' );

					handled = true;
					}

			if ( !handled )
				if ( c == '\b' )
					{
					writeByte( '\\' );
					writeByte( 'b' );

					handled = true;
					}

			if ( !handled )
				if ( c == '\n' )
					{
					writeByte( '\\' );
					writeByte( 'n' );

					handled = true;
					}

			if ( !handled )
				if ( c == '\r' )
					{
					writeByte( '\\' );
					writeByte( 'r' );

					handled = true;
					}

			if ( !handled )
				if ( c == '\t' )
					{
					writeByte( '\\' );
					writeByte( 't' );

					handled = true;
					}

			// See if we're dealing with a control character
			// If so, JSON specifies that it needs to be unicode escaped
			if ( !handled )
				if ( ( c <= 0x1F ) || ( ( c >= 0x7F ) && ( c <= 0x9F ) ) )
					{
					writeUnicodeEscape( c );

					handled = true;
					}

			// See if we're just dealing with normal ASCII
			if ( !handled )
				if ( c <= 0x7F )
					{
					writeByte( c );

					handled = true;
					}

			// If things haven't been handled yet, assume we're dealing with
			// some other unicode character
			// Write it in UTF8 form
			if ( !handled )
				writeUTF8Char( c );
			}
		}


	// This is a special case that is utilized when objects have to be written to the database
	// and the dat values need to be escaped correctly
	// Write a given string in JSON format and encode it in UTF-8
	// Note that the "'s are not written by this method
	public void writeJSONDBStringUTF8( String s ) throws Throwable
		{
		char[] chars = s.toCharArray();

		int length = chars.length;
		for ( int i = 0; i < length; i++ )
			{
			char c = chars[ i ];

			//	System.out.println( i + " " + (int) c + " " + c );

			boolean handled = false;

			// Check for characters that we will need to escape
			if ( c == '\'' )
				{
				writeByte( '\'' );
				writeByte( '\'' );

				handled = true;
				}

			if ( c == '"' )
				{
				writeByte( '\\' );
				writeByte( '\\' );
				writeByte( '\\' );
				writeByte( '"' );

				handled = true;
				}

			if ( !handled )
				if ( c == '\\' )
					{
					writeByte( '\\' );
					writeByte( '\\' );
					writeByte( '\\' );
					writeByte( '\\' );

					handled = true;
					}

			if ( !handled )
				if ( c == '/' )
					{
					writeByte( '\\' );
					writeByte( '\\' );
					writeByte( '/' );

					handled = true;
					}

			if ( !handled )
				if ( c == '\b' )
					{
					writeByte( '\\' );
					writeByte( '\\' );
					writeByte( 'b' );

					handled = true;
					}

			if ( !handled )
				if ( c == '\n' )
					{
					writeByte( '\\' );
					writeByte( '\\' );
					writeByte( 'n' );

					handled = true;
					}

			if ( !handled )
				if ( c == '\r' )
					{
					writeByte( '\\' );
					writeByte( '\\' );
					writeByte( 'r' );

					handled = true;
					}

			if ( !handled )
				if ( c == '\t' )
					{
					writeByte( '\\' );
					writeByte( '\\' );
					writeByte( 't' );

					handled = true;
					}

			// See if we're dealing with a control character
			// If so, JSON specifies that it needs to be unicode escaped
			if ( !handled )
				if ( ( c <= 0x1F ) || ( ( c >= 0x7F ) && ( c <= 0x9F ) ) )
					{
					writeDBUnicodeEscape( c );

					handled = true;
					}

			// See if we're just dealing with normal ASCII
			if ( !handled )
				if ( c <= 0x7F )
					{
					writeByte( c );

					handled = true;
					}

			// If things haven't been handled yet, assume we're dealing with
			// some other unicode character
			// Write it in UTF8 form
			if ( !handled )
				writeUTF8Char( c );
			}
		}


	public void writeDataObject( DataObject d ) throws Throwable
		{
		d.writeToStream( this );
		}


	public void writeMessage( Message m ) throws Throwable
		{
		if ( m.numBytesWritten() != 0 )
			{
			write4Bytes( m.numBytesWritten() );
			writeBytes( m.returnBytes(), 0, m.numBytesWritten() );
			}
		}


	protected void finalize() throws Throwable
		{
		if ( !closed )
			{
			int hash = Murmur.hash( stackTrace, 0 );

			if ( f.get( hash ) == null )
				{
				f.put( hash, "" );

				System.out.println( "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!The OutStream was never closed: " + stackTrace );
				}
			}

		super.finalize();
		}


	public static void main( String[] args )
		{
		try
			{
			Socket s = new Socket( "127.0.0.1", 9999 );

			OutStream out = new OutStream( s.getOutputStream() );

			byte[] bytes = new byte[Integer.parseInt( args[ 1 ] )];

			long start = System.currentTimeMillis();
			for ( int i = 0; i < Integer.parseInt( args[ 0 ] ); i++ )
				{
				out.writeByte( 0 );
				out.write4Bytes( 0 );
				out.write4Bytes( bytes.length );
				out.writeBytes( bytes );
				out.flush();
				}

			long end = System.currentTimeMillis();
			System.out.println( end - start );

			bytes = new byte[Integer.parseInt( args[ 1 ] )];

			start = System.currentTimeMillis();
			for ( int i = 0; i < Integer.parseInt( args[ 0 ] ); i++ )
				{
				out.writeByte( 0 );
				out.write4Bytes( 0 );
				out.write4Bytes( bytes.length );
				out.writeBytes( bytes );
				out.flush();
				}

			end = System.currentTimeMillis();
			System.out.println( end - start );
			}
		catch ( Throwable theX )
			{
			Util.printX( theX );
			}
		}
	}
