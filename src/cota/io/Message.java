package cota.io;

import java.util.zip.CRC32;

import cota.util.Queue;


// Messages are essentially blocks of bytes
// They are lighter than DataObjects, but provide more functionality than raw streams

// Note that when writing to the message, that the message automatically
// grows in size to accomodate all data
// For this reason, Messages should not be used for excessively long streams of data

public class Message
	{
	// For reading and writing
	public int index = 0;
	public int maxIndex = 0;
	public byte[] buffer;


	public Message()
		{
		buffer = new byte[1024];
		maxIndex = index = 0;
		}


	public Message( byte[] buffer )
		{
		this.buffer = buffer;

		maxIndex = buffer.length;
		}


	public int checksum()
		{
		CRC32 crc = new CRC32();
		crc.update( buffer, 0, maxIndex );

		return (int) crc.getValue();
		}


	public void init( byte[] buffer )
		{
		this.buffer = buffer;

		maxIndex = buffer.length;
		}


	public byte[] returnBytes()
		{
		byte[] bytes = new byte[maxIndex];

		System.arraycopy( buffer, 0, bytes, 0, bytes.length );

		return bytes;
		}


	public int numBytesRead()
		{
		return index;
		}


	public int numBytesWritten()
		{
		return maxIndex;
		}


	public void checkSpace( int size )
		{
		if ( ( buffer.length - index ) > size )
			return;

		int neededSize = buffer.length + size;
		int newSize = buffer.length;
		while ( newSize < neededSize )
			newSize = newSize * 2;

		byte[] newBuffer = new byte[newSize];
		System.arraycopy( buffer, 0, newBuffer, 0, buffer.length );

		buffer = newBuffer;
		}


	public void writeByte( int b )
		{
		checkSpace( 1 );

		buffer[ index++ ] = (byte) ( b & 0xFF );

		if ( index > maxIndex )
			maxIndex = index;
		}


	// 0..65536
	public void write2Bytes( int n )
		{
		checkSpace( 2 );

		buffer[ index++ ] = (byte) ( ( n >> 8 ) & 0xFF );
		buffer[ index++ ] = (byte) ( ( n ) & 0xFF );

		if ( index > maxIndex )
			maxIndex = index;
		}


	// 0..16777215
	public void write3Bytes( int n )
		{
		checkSpace( 3 );

		buffer[ index++ ] = (byte) ( ( n >> 16 ) & 0xFF );
		buffer[ index++ ] = (byte) ( ( n >> 8 ) & 0xFF );
		buffer[ index++ ] = (byte) ( ( n ) & 0xFF );

		if ( index > maxIndex )
			maxIndex = index;
		}


	// 0..2,147,483,647
	public void write4Bytes( int n )
		{
		checkSpace( 4 );

		buffer[ index++ ] = (byte) ( ( n >> 24 ) & 0xFF );
		buffer[ index++ ] = (byte) ( ( n >> 16 ) & 0xFF );
		buffer[ index++ ] = (byte) ( ( n >> 8 ) & 0xFF );
		buffer[ index++ ] = (byte) ( ( n ) & 0xFF );

		if ( index > maxIndex )
			maxIndex = index;
		}


	public void write8Bytes( long n )
		{
		checkSpace( 8 );

		buffer[ index++ ] = (byte) ( ( n >> 56 ) & 0xFF );
		buffer[ index++ ] = (byte) ( ( n >> 48 ) & 0xFF );
		buffer[ index++ ] = (byte) ( ( n >> 40 ) & 0xFF );
		buffer[ index++ ] = (byte) ( ( n >> 32 ) & 0xFF );
		buffer[ index++ ] = (byte) ( ( n >> 24 ) & 0xFF );
		buffer[ index++ ] = (byte) ( ( n >> 16 ) & 0xFF );
		buffer[ index++ ] = (byte) ( ( n >> 8 ) & 0xFF );
		buffer[ index++ ] = (byte) ( ( n ) & 0xFF );

		if ( index > maxIndex )
			maxIndex = index;
		}


	public void writeByte( int offset, int b )
		{
		index = offset;
		writeByte( b );
		}


	public void write2Bytes( int offset, int n )
		{
		index = offset;
		write2Bytes( n );
		}


	public void write3Bytes( int offset, int n )
		{
		index = offset;
		write3Bytes( n );
		}


	public void write4Bytes( int offset, int n )
		{
		index = offset;
		write4Bytes( n );
		}


	public void write8Bytes( int offset, long n )
		{
		index = offset;
		write8Bytes( n );
		}


	public void writeString( int offset, String s )
		{
		index = offset;
		writeString( s );
		}


	public void writeBytes( int offset, byte[] buffer ) throws Throwable
		{
		index = offset;
		writeBytes( buffer );
		}


	public void writeBytesWithoutLength( int offset, byte[] buffer )
		{
		index = offset;
		writeBytesWithoutLength( buffer );
		}


	/*broken


		public void writeDouble( double n ) 
			{
			write8Bytes( Double.doubleToLongBits( n ) );
			}

	*/
	public void writeUTF8Char( char c )
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


	// All strings are written as UTF 8 strings
	public void writeString( String s )
		{
		write3Bytes( s.length() );

		for ( int i = 0; i < s.length(); i++ )
			{
			char c = s.charAt( i );

			writeUTF8Char( c );
			}
		}


	// Write an array of bytes
	public void writeBytes( byte[] bytes ) throws Throwable
		{
		//		System.out.println( "WRITING BYTES OF LENGTH: " + bytes.length );

		if ( bytes.length > 16000000 )
			throw new Throwable( "TRYING TO WRITE MORE THAN 16 MILLION BYTES TO A MESSAGE!!!!" );

		write3Bytes( bytes.length );

		checkSpace( bytes.length );

		System.arraycopy( bytes, 0, buffer, index, bytes.length );
		index = index + bytes.length;

		if ( index > maxIndex )
			maxIndex = index;
		}


	// Write an array of bytes without writig the length
	public void writeBytesWithoutLength( byte[] bytes )
		{
		checkSpace( bytes.length );

		System.arraycopy( bytes, 0, buffer, index, bytes.length );
		index = index + bytes.length;

		if ( index > maxIndex )
			maxIndex = index;
		}


	public void writeQueueOfStrings( Queue q )
		{
		write4Bytes( q.size() );
		for ( int i = 0; i < q.size(); i++ )
			writeString( (String) q.elementAt( i ) );
		}


	public char readUTF8Char() throws Throwable
		{
		int b = readByte();

		if ( b <= 127 )
			{
			// 1 byte
			return (char) b;
			}

		if ( b <= 223 )
			{
			// 2 bytes
			int b2 = readByte();

			return (char) ( ( ( b & 0x1f ) << 6 ) | ( b2 & 0x3F ) );
			}

		if ( b <= 239 )
			{
			int b2 = readByte();
			int b3 = readByte();

			return (char) ( ( ( b & 0xF ) << 12 ) | ( ( b2 & 0x3F ) << 6 ) | ( b3 & 0x3F ) );
			}

		if ( b <= 244 )
			{
			int b2 = readByte();
			int b3 = readByte();
			int b4 = readByte();

			return (char) ( ( ( b & 0x7 ) << 18 ) | ( ( b2 & 0x3F ) << 12 ) | ( ( b3 & 0x3F ) << 6 ) | ( b4 & 0x3F ) );
			}

		System.out.println( "Unable to handle UTF-8 first byte: " + b );

		throw new Throwable( "UNABLE TO PARSE THE UTF8 STREAM" );
		}


	// All strings are read as UTF 8 strings
	public String readString() throws Throwable
		{
		int length = read3Bytes();
		//		System.out.println( "STRING LENGTH: " + length );

		char[] c = new char[length];
		for ( int i = 0; i < length; i++ )
			c[ i ] = readUTF8Char();

		return new String( c );
		}


	// Write an array of bytes
	public byte[] readBytes()
		{
		int length = read3Bytes();

		/*		System.out.println( "$$$!!!$$$" );
				System.out.println( "READ BYTES LENGTH: " + length );
				System.out.println( "BUFFER LENGTH: " + buffer.length );
				System.out.println( "INDEX: " + index );
		*/
		byte[] bytes = new byte[length];

		System.arraycopy( buffer, index, bytes, 0, length );
		index = index + length;

		return bytes;
		}


	public int readByte()
		{
		int i = buffer[ index++ ];

		if ( i < 0 )
			i = i + 256;

		return i;
		}


	// 0..65536
	public int read2Bytes()
		{
		int x1 = readByte();
		int x2 = readByte();

		return ( ( x1 << 8 ) + x2 );
		}


	// 0..16777215
	public int read3Bytes()
		{
		int x1 = readByte();
		int x2 = readByte();
		int x3 = readByte();

		return ( ( x1 << 16 ) + ( x2 << 8 ) + x3 );
		}


	// 0..2,147,483,648
	public int read4Bytes()
		{
		int x1 = readByte();
		int x2 = readByte();
		int x3 = readByte();
		int x4 = readByte();

		return ( ( x1 << 24 ) + ( x2 << 16 ) + ( x3 << 8 ) + x4 );
		}


	public long read8Bytes()
		{
		long x1 = readByte();
		long x2 = readByte();
		long x3 = readByte();
		long x4 = readByte();
		long x5 = readByte();
		long x6 = readByte();
		long x7 = readByte();
		long x8 = readByte();

		return ( ( x1 << 56 ) + ( x2 << 48 ) + ( x3 << 40 ) + ( x4 << 32 ) + ( x5 << 24 ) + +( x6 << 16 ) + ( x7 << 8 ) + x8 );
		}


	public Queue readQueueOfStrings() throws Throwable
		{
		Queue q = new Queue();

		int size = read4Bytes();
		for ( int i = 0; i < size; i++ )
			q.addObject( readString() );

		return q;
		}


	public String readString( int offset ) throws Throwable
		{
		index = offset;

		return readString();
		}


	public byte[] readBytes( int offset )
		{
		index = offset;

		return readBytes();
		}


	public int readByte( int offset )
		{
		index = offset;

		return readByte();
		}


	public int read2Bytes( int offset )
		{
		index = offset;

		return read2Bytes();
		}


	public int read3Bytes( int offset )
		{
		index = offset;

		return read3Bytes();
		}


	public int read4Bytes( int offset )
		{
		index = offset;

		return read4Bytes();
		}


	public long read8Bytes( int offset )
		{
		index = offset;

		return read8Bytes();
		}


	/*broken

	public long read8Bytes() 
		{
		int x1 = readByte();
		int x2 = readByte();
		int x3 = readByte();
		int x4 = readByte();
		int x5 = readByte();
		int x6 = readByte();
		int x7 = readByte();
		int x8 = readByte();

		return ( ( x1 << 56 ) + ( x2 << 48 ) + ( x3 << 40 ) + ( x4 << 32 ) + ( x5 << 24 ) + +( x6 << 16 ) + ( x7 << 8 ) + x8 );
		}


	public double readDouble() 
		{
		long n = read8Bytes();

		return Double.longBitsToDouble( n );
		}
	*/

	public static void main( String[] args )
		{
		try
			{
			long start = System.currentTimeMillis();
			for ( int i = 0; i < 5000000; i++ )
				{
				if ( ( i % 10000 ) == 0 )
					System.out.println( i );

				Message mb = new Message();

				int total = 0;
				int[] sizes = new int[20];
				for ( int k = 0; k < 20; k++ )
					{
					int size = (int) ( 500 * Math.random() );
					sizes[ k ] = size;

					total = total + size;
					}

				byte[] source = new byte[total];
				for ( int k = 0; k < total; k++ )
					source[ k ] = (byte) k;

				int index = 0;
				for ( int k = 0; k < 20; k++ )
					{
					byte[] bytes2 = new byte[sizes[ k ]];
					System.arraycopy( source, index, bytes2, 0, sizes[ k ] );

					mb.writeBytes( bytes2 );

					index = index + sizes[ k ];
					}

				for ( int z = 0; z < 50; z++ )
					mb.write2Bytes( 0 );

				byte[] d = mb.returnBytes();
				Message mb2 = new Message( d );

				index = 0;
				for ( int k = 0; k < 20; k++ )
					{
					byte[] d2 = mb2.readBytes();

					if ( sizes[ k ] != d2.length )
						{
						System.out.println( "ERRRRRRORRRR 2!!!!!!!!!!!!!" );
						System.out.println( "LENGTH0: " + sizes[ k ] );
						System.out.println( "LENGTH: " + d2.length );

						return;
						}

					for ( int j = 0; j < sizes[ k ]; j++ )
						{
						if ( source[ index + j ] != d2[ j ] )
							{
							System.out.println( "ERRRRRRORRRR 3!!!!!!!!!!!!!: " + index + " " + j );

							return;
							}

						}

					index = index + sizes[ k ];
					}
				}

			System.out.println( System.currentTimeMillis() - start );
			}
		catch ( Throwable theX )
			{
			cota.util.Util.printX( theX );
			}
		}
	}
