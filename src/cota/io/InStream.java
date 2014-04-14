package cota.io;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

import cota.crypto.Murmur;
import cota.util.Fashtable_io;
import cota.util.Queue;
import cota.util.Util;


// Note that readLine, readLines, and extractHeader are not unicode safe!
// They assume that chars are one byte
// utf8Encoded must be set to true for the call to work correctly!
public class InStream
	{
	static Fashtable_io f = new Fashtable_io();

	int bufferSize = 1024 * 32;
	byte[] buffer = null;
	char[] charBuffer = null;
	int start = 0;
	public int bytesInBuffer = 0;

	InputStream in = null;

	boolean closed = false;
	static int numOpen = 0;

	public static boolean debug = false;

	public boolean utf8Encoded = false;

	public String stringBeingRead = "";

	String stackTrace = "";


	public InStream()
		{
		stackTrace = cota.util.Util.currentStackTrace();

		buffer = new byte[bufferSize];
		}


	public InStream( String file ) throws Throwable
		{
		stackTrace = cota.util.Util.currentStackTrace();
		closed = true;

		numOpen++;

		this.in = new FileInputStream( file );

		buffer = new byte[bufferSize];

		closed = false;
		}


	public InStream( InputStream in )
		{
		stackTrace = cota.util.Util.currentStackTrace();

		numOpen++;

		this.in = in;

		buffer = new byte[bufferSize];
		}


	public InStream( InputStream in, int bufferSize )
		{
		stackTrace = cota.util.Util.currentStackTrace();

		numOpen++;

		this.bufferSize = bufferSize;
		this.in = in;

		buffer = new byte[bufferSize];
		}


	public InStream( byte[] buffer )
		{
		stackTrace = cota.util.Util.currentStackTrace();

		this.buffer = buffer;
		this.bytesInBuffer = this.bufferSize = buffer.length;
		}


	public InStream( byte[] buffer, int start, int bytesInBuffer )
		{
		stackTrace = cota.util.Util.currentStackTrace();

		this.buffer = buffer;
		this.bufferSize = buffer.length;
		this.start = start;
		this.bytesInBuffer = bytesInBuffer - start;
		}


	public void close() throws Throwable
		{
		if ( !closed )
			{
			numOpen--;

			closed = true;

			if ( in != null )
				in.close();

			in = null;
			}
		}


	public void reassign( InputStream in )
		{
		this.in = in;
		numOpen++;
		closed = false;
		start = 0;
		bytesInBuffer = 0;
		}


	public int available() throws Throwable
		{
		if ( bytesInBuffer > 0 )
			return bytesInBuffer;
		else
			{
			if ( in != null )
				return in.available();
			else
				return 0;
			}
		}


	public void skip( int length ) throws Throwable
		{
		if ( length <= 0 )
			return;

		// see if there's enough data in the buffer
		if ( bytesInBuffer >= length )
			{
			start = start + length;
			bytesInBuffer = bytesInBuffer - length;

			return;
			}

		byte[] temp = new byte[length];
		readBytes( temp, 0, length );

		return;
		}


	// Fill in the buffer with as much information as possible
	// The buffer at this point is empty
	public void fillBuffer() throws Throwable
		{
		if ( in == null )
			throw new StreamClosedX( "The end of the stream" );

		int avail = in.available();

		if ( avail > bufferSize )
			avail = bufferSize;

		if ( avail == 0 )
			avail = 1;

		start = 0;

		bytesInBuffer = in.read( buffer, start, avail );
		// The end of the stream
		if ( bytesInBuffer == -1 )
			{
			throw new StreamClosedX( "The end of the stream" );
			}
		}


	// read fixed number of bytes from the stream, using either
	// the data existing in the buffer, or waiting for new data
	/*OLD
	 * 	// read fixed number of bytes from the stream, using either
		// the data existing in the buffer, or waiting for new data
		public void readBytes( byte[] bytes, int offset, int length ) throws Throwable
			{
			// System.out.println( "readBytes" );

			// see if there's enough data in the buffer
			if ( bytesInBuffer >= length )
				{
				System.arraycopy( buffer, start, bytes, offset, length );
				start = start + length;
				bytesInBuffer = bytesInBuffer - length;

				return;
				}

			int numRead = bytesInBuffer;
			System.arraycopy( buffer, start, bytes, offset, numRead );

			start = 0;
			bytesInBuffer = 0;

			// pull whatever else we need directly from the stream, as much as
			// possible at a time
			while ( numRead < length )
				{
				int toRead = length - numRead;
				if ( toRead > bufferSize )
					toRead = bufferSize;

				numRead = numRead + in.read( bytes, offset + numRead, toRead );
				}
			}
	*/
	public void readBytes( byte[] bytes, int offset, int length ) throws Throwable
		{
		// System.out.println( "readBytes" );

		// see if there's enough data in the buffer
		if ( bytesInBuffer >= length )
			{
			//System.out.println( "fff" );

			System.arraycopy( buffer, start, bytes, offset, length );
			start = start + length;
			bytesInBuffer = bytesInBuffer - length;

			return;
			}

		//	System.out.println( "ggg" );
		//	System.out.println( bytesInBuffer );

		int numRead = bytesInBuffer;
		if ( bytesInBuffer != 0 )
			{
			System.arraycopy( buffer, start, bytes, offset, numRead );

			// We've sucked everything out of the buffer
			start = 0;
			bytesInBuffer = 0;
			}

		// pull whatever else we need directly from the stream, as much as
		// possible at a time
		//int count = 0;
		while ( numRead < length )
			{
			//		count++;
			int toRead = length - numRead;

			numRead = numRead + in.read( bytes, offset + numRead, toRead );
			}
		//		System.out.println( "loop: " + count );
		}


	public void readBytes( byte[] bytes ) throws Throwable
		{
		readBytes( bytes, 0, bytes.length );
		}


	// 0..255
	public int readByte() throws Throwable
		{
		//		System.out.print( bytesInBuffer + "\t" + start + "             " );

		if ( bytesInBuffer == 0 )
			fillBuffer();

		//	System.out.println( bytesInBuffer + "\t" + start );

		int next = buffer[ start++ ];
		bytesInBuffer--;

		if ( next < 0 )
			next = next + 256;

		return next;
		}


	// 0..65536
	public int read2Bytes() throws Throwable
		{
		int x1 = readByte();
		int x2 = readByte();

		return ( ( x1 << 8 ) + x2 );
		}


	// 0..16777215
	public int read3Bytes() throws Throwable
		{
		int x1 = readByte();
		int x2 = readByte();
		int x3 = readByte();

		return ( ( x1 << 16 ) + ( x2 << 8 ) + x3 );
		}


	// 0..2,147,483,648
	public int read4Bytes() throws Throwable
		{
		int x1 = readByte();
		int x2 = readByte();
		int x3 = readByte();
		int x4 = readByte();

		return ( ( x1 << 24 ) + ( x2 << 16 ) + ( x3 << 8 ) + x4 );
		}


	public long read8Bytes() throws Throwable
		{
		/*		long value = 0;
				for ( int i = 0; i < 8; i++ )
					value = ( value << 8 ) + ( readByte() & 0xff );

				return value;
			*/
		long x1 = readByte();
		long x2 = readByte();
		long x3 = readByte();
		long x4 = readByte();
		long x5 = readByte();
		long x6 = readByte();
		long x7 = readByte();
		long x8 = readByte();

		return ( ( x1 << 56 ) + ( x2 << 48 ) + ( x3 << 40 ) + ( x4 << 32 ) + ( x5 << 24 ) + ( x6 << 16 ) + ( x7 << 8 ) + x8 );
		}


	public double readDouble() throws Throwable
		{
		long l = read8Bytes();

		return Double.longBitsToDouble( l );
		}


	// Read in a String from the stream
	public String readString() throws Throwable
		{
		int length = read3Bytes();
		// System.out.println( length );

		if ( length == 0 )
			return "";

		// see if there's enough data in the buffer
		if ( bytesInBuffer >= length )
			{
			String s = new String( buffer, start, length, "US-ASCII" );
			start = start + length;
			bytesInBuffer = bytesInBuffer - length;

			return s;
			}

		//		System.out.println( "BYTES IN BUFFER: " + bytesInBuffer );
		//	System.out.println( "LENGTH: " + length );

		byte[] temp = new byte[length];
		readBytes( temp, 0, length );

		//		for ( int i = 0; i < temp.length; i++ )
		//		System.out.println( i + " " + temp[ i ] + " " + (char) temp[ i ] );

		return new String( temp, "US-ASCII" );
		}


	public int readHexDigit() throws Throwable
		{
		int b = readByte();

		if ( b >= '0' )
			if ( b <= '9' )
				return b - '0';

		if ( b >= 'a' )
			if ( b <= 'f' )
				return 10 + b - 'a';

		if ( b >= 'A' )
			if ( b <= 'F' )
				return 10 + b - 'A';

		throw new Throwable( "ILLEGAL HEX DIGIT" );
		}


	public void putBack( byte b )
		{
		if ( bytesInBuffer == 0 )
			{
			start = 0;

			buffer[ 0 ] = b;
			bytesInBuffer = 1;
			}
		else
			{
			start--;
			bytesInBuffer++;
			buffer[ start ] = b;
			}
		}


	// Read in a line from the stream
	public String readLine() throws Throwable
		{
		return readLine( -1 );
		}


	/*
		public String readLine( int maxLineLength ) throws Throwable
			{
			String result;
			while ( true )
				{
				int b = readByte();

				if ( b == '\n' )
					{
					result = stringBeingRead;
					stringBeingRead = "";
					break;
					}

				if ( b != '\r' )
					{
					stringBeingRead = stringBeingRead + (char) b;

					if ( maxLineLength != -1 )
						if ( stringBeingRead.length() > maxLineLength )
							{
							result = stringBeingRead;
							stringBeingRead = "";
							break;
							}

					}
				}

			return result;
			}
	*/

	/*
		// Read in a line from the stream
		// Damn, this should be easier!
		@SuppressWarnings( "deprecation" )
		public String readLine( int maxLineLength ) throws Throwable
			{
			if ( bytesInBuffer == 0 )
				fillBuffer();

			Queue q = new Queue();
			int overallLength = 0;

			int length = 0;
			int stringStart = 0;
			boolean carriageFound = false;
			boolean endFound = false;
			boolean endOfStream = false;
			while ( !endFound )
				{
				// Remember where we really began
				stringStart = start;
				int end = start + bytesInBuffer;

				// Look through everything we have in the buffer
				while ( start < end )
					{
					byte b = buffer[ start ];

					// Things are a little complicated as a line can end with \n \r
					// or \r\n
					if ( ( b == '\n' ) || ( b == '\r' ) )
						{
						if ( b == '\r' )
							carriageFound = true;

						endFound = true;

						start++;
						break;
						}

					start++;
					}

				length = start - stringStart;
				bytesInBuffer = bytesInBuffer - length;

				// System.out.println( stringStart + " " + length );
				// System.out.println( "#" + new String( buffer, start, length ) +
				// "#" );
				if ( !endFound )
					{
					overallLength = overallLength + length;

					// As we didn't find the end of the line, save the piece of text
					// that we did find
					byte[] newBytes = new byte[length];
					System.arraycopy( buffer, stringStart, newBytes, 0, length );

					q.addObject( newBytes );

					// Check to see if the line is too long (for security reasons)
					if ( maxLineLength != -1 )
						if ( overallLength > maxLineLength )
							{
							System.out.println( "InStream.readLine():   LINE TOO LONG!!!!!!!!!!!!!!!!!!!" );
							endFound = true;
							}

					try
						{
						// Try to fill up the buffer with more yummy goodness so
						// that we can continue to look for the
						// end of the line
						fillBuffer();
						}
					catch ( StreamClosedX theX )
						{
						// Even though we've reached the end of the stream, we can
						// still return the text that was found so far
						endOfStream = true;
						break;
						}
					}
				}

			if ( length <= 0 )
				throw new StreamClosedX( "END OF STREAM" );

			if ( !endOfStream )
				overallLength = overallLength + length - 1;

			// Copy the bytes that are contained in the Queue
			int offset = 0;
			byte[] chars = new byte[overallLength];
			int qSize = q.size();
			for ( int i = 0; i < qSize; i++ )
				{
				byte[] bytes = (byte[]) q.elementAt( i );

				System.arraycopy( bytes, 0, chars, offset, bytes.length );
				offset = offset + bytes.length;
				}

			// Copy the last bit (in the buffer)
			if ( !endOfStream )
				System.arraycopy( buffer, stringStart, chars, offset, length - 1 );

			// See if there was a \n after the \r
			if ( carriageFound )
				{
				try
					{
					byte b = (byte) readByte();

					if ( b != '\n' )
						putBack( b );
					}
				catch ( Throwable ignored )
					{
					}
				}

			if ( chars.length == 0 )
				return "";

			String line = new String( chars, "UTF-8" );

			if ( debug )
				System.out.println( Thread.currentThread() + "I:::" + line );

			return line;
			}
	*/

	// Read in a UTF-8 line from the stream
	public String readLine( int maxLineLength ) throws Throwable
		{
		if ( charBuffer == null )
			charBuffer = new char[bufferSize];

		if ( bytesInBuffer == -1 )
			throw new StreamClosedX( "END OF STREAM" );

		Queue q = new Queue();
		int length = 0;

		int index = 0;
		boolean carriageFound = false;
		boolean streamClosed = false;
		while ( true )
			{
			char c = 0;

			try
				{
				if ( utf8Encoded )
					c = readUTF8Char();
				else
					c = (char) readByte();

				if ( debug )
					System.out.print( c );
				}
			catch ( StreamClosedX theX )
				{
				// Even though we've reached the end of the stream, we can
				// still return the text that was found so far
				streamClosed = true;

				break;
				}

			// Things are a little complicated as a line can end with \n \r
			// or \r\n
			if ( ( c == '\n' ) || ( c == '\r' ) )
				{
				if ( c == '\r' )
					carriageFound = true;

				break;
				}
			else
				{
				charBuffer[ index++ ] = c;

				if ( index >= bufferSize )
					{
					q.addObject( charBuffer );
					length = length + bufferSize;

					charBuffer = new char[bufferSize];
					index = 0;
					}
				}
			}

		// See if there was a \n after the \r
		if ( carriageFound )
			{
			try
				{
				byte b = (byte) readByte();

				if ( b != '\n' )
					putBack( b );
				}
			catch ( Throwable ignored )
				{
				}
			}

		length = length + index;

		if ( streamClosed && ( length == 0 ) )
			throw new StreamClosedX( "STREAM CLOSED" );

		if ( length == 0 )
			return "";

		int offset = 0;
		char[] chars = new char[length];
		int qSize = q.size();
		for ( int i = 0; i < qSize; i++ )
			{
			char[] c = (char[]) q.elementAt( i );

			System.arraycopy( c, 0, chars, offset, c.length );
			offset = offset + c.length;
			}

		System.arraycopy( charBuffer, 0, chars, offset, index );

		//		System.out.println( new String( chars ) );

		return new String( chars );
		}


	public Queue extractHeader() throws Throwable
		{
		return extractHeader( -1, -1 );
		}


	public Queue extractHeader( int maxLineLength, int maxLines ) throws Throwable
		{
		Queue headerLines = new Queue();
		while ( true )
			{
			String line = readLine( maxLineLength );

			if ( debug )
				System.out.println( line );

			if ( line.startsWith( "Content-Type" ) )
				if ( ( line.toLowerCase().indexOf( "utf-8" ) != -1 ) || ( line.toLowerCase().indexOf( "utf8" ) != -1 ) )
					utf8Encoded = true;

			if ( line == null )
				break;

			if ( line.length() == 0 )
				break;

			headerLines.addObject( line );

			if ( maxLines != -1 )
				if ( headerLines.size() > maxLines )
					return headerLines;
			}

		return headerLines;
		}


	public Queue readLines() throws Throwable
		{
		Queue lines = new Queue();

		try
			{
			while ( true )
				{
				String line = readLine( -1 );

				lines.addObject( line );

				// System.out.println( "#" + line + "#" );
				}
			}
		catch ( StreamClosedX ignored )
			{
			close();
			}

		//	close();

		return lines;
		}


	protected void finalize() throws Throwable
		{
		if ( !closed )
			{
			int hash = Murmur.hash( stackTrace, 0 );

			if ( f.get( hash ) == null )
				{
				f.put( hash, "" );

				System.out.println( "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!The InStream was never closed: " + stackTrace );
				}
			}

		super.finalize();
		}


	public char readUTF8Char() throws Throwable
		{
		int b = readByte();

		if ( b <= 127 )
			{
			// 1 byte

			if ( debug )
				System.out.print( "\n1 " );

			return (char) b;
			}

		if ( b <= 223 )
			{
			// 2 bytes
			int b2 = readByte();

			if ( debug )
				{
				System.out.println( b + " " + b2 );
				System.out.print( "\n2 " );
				}

			return (char) ( ( ( b & 0x1f ) << 6 ) | ( b2 & 0x3F ) );
			}

		if ( b <= 239 )
			{
			int b2 = readByte();
			int b3 = readByte();

			if ( debug )
				System.out.print( "\n3 " );

			return (char) ( ( ( b & 0xF ) << 12 ) | ( ( b2 & 0x3F ) << 6 ) | ( b3 & 0x3F ) );
			}

		if ( b <= 244 )
			{
			int b2 = readByte();
			int b3 = readByte();
			int b4 = readByte();

			if ( debug )
				System.out.print( "\n4 " );

			return (char) ( ( ( b & 0x7 ) << 18 ) | ( ( b2 & 0x3F ) << 12 ) | ( ( b3 & 0x3F ) << 6 ) | ( b4 & 0x3F ) );
			}

		System.out.println( "Unable to handle UTF-8 first byte: " + b );

		throw new Throwable( "UNABLE TO PARSE THE UTF8 STREAM" );
		}


	// Read a JSON style string encoded as UTF-8
	// The string will continue until an unescaped " is encountered
	public String readJSONStringUTF8() throws Throwable
		{
		if ( bytesInBuffer == 0 )
			fillBuffer();

		// Read the initial "
		int quote = readByte();
		if ( quote != '"' )
			throw new Throwable( "JSON STRING NEEDS TO BEGIN WITH A \"" );

		Queue q = new Queue();
		int overallLength = 0;

		int SIZE = 64;
		int index = 0;
		char[] chars = new char[SIZE];

		while ( true )
			{
			char c = readUTF8Char();

			if ( c == '"' )
				break;

			if ( c == '\\' )
				{
				int b2 = readByte();

				switch ( b2 )
					{
					case '"':
						c = '"';
					break;

					case '\\':
						c = '\\';
					break;

					case '/':
						c = '/';
					break;

					case 'b':
						c = '\b';
					break;

					case 'f':
						c = '\f';
					break;

					case 'n':
						c = '\n';
					break;

					case 'r':
						c = '\r';
					break;

					case 't':
						c = '\t';
					break;

					case 'u':
						int h1 = readHexDigit();
						int h2 = readHexDigit();
						int h3 = readHexDigit();
						int h4 = readHexDigit();

						c = (char) ( ( h1 << 12 ) | ( h2 << 8 ) | ( h3 << 4 ) | h4 );
					break;
					}
				}

			// System.out.println( (int) c + "\t" + (char) c );

			chars[ index++ ] = c;
			overallLength++;

			if ( index == chars.length )
				{
				q.addObject( chars );

				SIZE = SIZE * 2;

				index = 0;
				chars = new char[SIZE];
				}
			}

		int offset = 0;
		char[] stringChars = new char[overallLength];
		for ( int i = 0; i < q.size(); i++ )
			{
			char[] ca = (char[]) q.elementAt( i );

			System.arraycopy( ca, 0, stringChars, offset, ca.length );
			offset = offset + ca.length;
			}

		System.arraycopy( chars, 0, stringChars, offset, index );

		//		System.out.println( "#" + new String( stringChars ) + "#" );
		return new String( stringChars );
		}


	// Read a JSON style number encoded as UTF-8
	// Assume that the first character will be - or a digit
	// Keep going until we see '{', ']' or whitespace
	// Numbers are returned as Strings as they can be in any format (double,
	// BigInt, int) and we
	// don't know what to convert them into
	public String readJSONNumber() throws Throwable
		{
		if ( bytesInBuffer == 0 )
			fillBuffer();

		Queue q = new Queue();
		int overallLength = 0;

		int SIZE = 64;
		int index = 0;
		char[] chars = new char[SIZE];

		while ( true )
			{
			int b = readByte();

			if ( ( b == '}' ) || ( b == ']' ) || ( b == '"' ) || ( b == ',' ) || Character.isWhitespace( b ) )
				{
				putBack( (byte) b );

				break;
				}

			// Not totally stribt abbording to the JSON rules, but blose enough
			boolean handled = false;
			if ( b == '.' )
				handled = true;

			if ( !handled )
				if ( ( b == 'e' ) || ( b == 'E' ) )
					handled = true;

			if ( !handled )
				if ( ( b == '+' ) || ( b == '-' ) )
					handled = true;

			if ( !handled )
				if ( ( b >= '0' ) && ( b <= '9' ) )
					handled = true;

			if ( !handled )
				throw new Throwable( "INVALID JSON NUMBER FORMAT" );

			chars[ index++ ] = (char) b;
			overallLength++;

			if ( index == chars.length )
				{
				q.addObject( chars );

				SIZE = SIZE * 2;

				index = 0;
				chars = new char[SIZE];
				}
			}

		if ( overallLength == 0 )
			throw new Throwable( "INVALID JSON NUMBER FORMAT: 0 LENGTH" );

		int offset = 0;
		char[] stringChars = new char[overallLength];
		for ( int i = 0; i < q.size(); i++ )
			{
			char[] ca = (char[]) q.elementAt( i );

			System.arraycopy( ca, 0, stringChars, offset, ca.length );
			offset = offset + ca.length;
			}

		System.arraycopy( chars, 0, stringChars, offset, index );

		return new String( stringChars );
		}


	// Skip whitespace and return the next character
	// Note that the character is can be put back into the stream
	public byte skipWhitespace( boolean putBackChar ) throws Throwable
		{
		while ( true )
			{
			byte b = (byte) readByte();
			if ( !Character.isWhitespace( b ) )
				{
				if ( putBackChar )
					putBack( b );

				return b;
				}
			}
		}


	// Skip whitespace and return the next character
	// The first non whitespace character is not put back into the stream
	public byte skipWhitespace() throws Throwable
		{
		return skipWhitespace( false );
		}


	public DataObject readDataObject() throws Throwable
		{
		return new DataObject( this );
		}


	public Message readMessage() throws Throwable
		{
		int size = read4Bytes();

		byte[] bytes = new byte[size];
		readBytes( bytes );

		return new Message( bytes );
		}


	public static void main( String[] args )
		{
		try
			{
			ServerSocket ss = new ServerSocket( 9999 );
			Socket s = ss.accept();

			InStream in = new InStream( s.getInputStream() );

			long start = System.currentTimeMillis();
			for ( int i = 0; i < 1000000; i++ )
				{
				int a = in.readByte();
				int b = in.read4Bytes();
				int numBytes = in.read4Bytes();
				byte[] bytes = new byte[numBytes];

				in.readBytes( bytes );
				}

			long end = System.currentTimeMillis();
			System.out.println( end - start );

			start = System.currentTimeMillis();
			for ( int i = 0; i < 1000000; i++ )
				{
				int a = in.readByte();
				int b = in.read4Bytes();
				int numBytes = in.read4Bytes();
				byte[] bytes = new byte[numBytes];

				in.readBytes( bytes );
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
