package cota.crypto;

import cota.util.Util;


public final class SHA
	{
	static int[] H0 =
		{ 0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0 };
	static int[] kSubT = null;

	// The H values
	int[] H =
		{ 0, 0, 0, 0, 0 };

	long totalLength = 0;

	// An 80 32bit word buffer
	int[] W = new int[80];
	int bytesInW = 0;

	// Buffer varaibles
	int A, B, C, D, E, TEMP;

	boolean messageEnded = false;

	byte[] digest = null;

	static
		{
		kSubT = new int[80];
		for ( int t = 0; t < 20; t++ )
			kSubT[ t ] = 0x5A827999;

		for ( int t = 20; t < 40; t++ )
			kSubT[ t ] = 0x6ED9EBA1;

		for ( int t = 40; t < 60; t++ )
			kSubT[ t ] = 0x8F1BBCDC;

		for ( int t = 60; t < 80; t++ )
			kSubT[ t ] = 0xCA62C1D6;
		}


	// Constructor
	public SHA()
		{
		reset();
		}


	public void reset()
		{
		totalLength = 0;
		bytesInW = 0;
		messageEnded = false;

		for ( int i = 0; i < 5; i++ )
			H[ i ] = H0[ i ];
		}


	// Circular shift left an integer
	public static int circularLeftShift( int theNumber, int bitsToShift )
		{
		return ( ( theNumber << bitsToShift ) | ( theNumber >>> ( 32 - bitsToShift ) ) );
		}


	// The function fSubT
	public int fSubT( int B, int C, int D, int t )
		{
		if ( t < 20 )
			return D ^ ( B & ( C ^ D ) ); //( ( B & C ) | ( ( B ^ -1 ) & D ) );

		if ( t < 40 )
			return ( ( B ^ C ) ^ D );

		if ( t < 60 )
			return ( B & C ) | ( D & ( B | C ) ); //( ( ( B & C ) | ( B & D ) ) | ( C & D ) );

		return ( ( B ^ C ) ^ D );
		}


	// Process a chunk
	public void processChunk()
		{
		for ( int t = 16; t < 80; t++ )
			W[ t ] = circularLeftShift( ( ( W[ t - 3 ] ^ W[ t - 8 ] ) ^ W[ t - 14 ] ) ^ W[ t - 16 ], 1 );

		A = H[ 0 ];
		B = H[ 1 ];
		C = H[ 2 ];
		D = H[ 3 ];
		E = H[ 4 ];

		for ( int t = 0; t < 80; t++ )
			{
			TEMP = circularLeftShift( A, 5 ) + fSubT( B, C, D, t ) + E + W[ t ] + kSubT[ t ];

			E = D;
			D = C;
			C = ( B << 30 ) | ( B >>> 2 );
			B = A;
			A = TEMP;
			}

		H[ 0 ] = H[ 0 ] + A;
		H[ 1 ] = H[ 1 ] + B;
		H[ 2 ] = H[ 2 ] + C;
		H[ 3 ] = H[ 3 ] + D;
		H[ 4 ] = H[ 4 ] + E;

		// Reset the W array
		bytesInW = 0;
		for ( int i = 0; i < 16; i++ )
			W[ i ] = 0;
		}


	// Encode the bytes
	public void encode( byte[] bytes, int offset, int length )
		{
		// Try to fill in the 16 32bit Ws with 64 8bit bytes
		int end = offset + length;
		for ( ; offset < end; offset++ )
			{
			byte b = bytes[ offset ];

			W[ bytesInW >> 2 ] = W[ bytesInW >> 2 ] | ( b << ( ( 3 - bytesInW % 4 ) << 3 ) );

			bytesInW++;
			if ( bytesInW == 64 )
				processChunk();
			}

		totalLength = totalLength + ( length << 3 );
		}


	// Encode the bytes
	public void endMessage()
		{
		// If already ended then return
		if ( messageEnded )
			return;

		messageEnded = true;

		// Append a 1
		W[ bytesInW >> 2 ] = W[ bytesInW >> 2 ] | ( 0x80 << ( ( 3 - bytesInW % 4 ) << 3 ) );
		bytesInW++;
		if ( bytesInW == 64 )
			processChunk();

		// We need to have the last chunk end with 64 length bits
		// If they wont fit, then end this chunk, and create a new one
		if ( bytesInW > 56 )
			processChunk();

		W[ 14 ] = (int) ( ( totalLength >> 32 ) & 0xFFFFFFFF );
		W[ 15 ] = (int) ( totalLength & 0xFFFFFFFF );

		processChunk();
		}


	// Encode the bytes
	public void encode( byte[] bytes )
		{
		encode( bytes, 0, bytes.length );
		}


	// Given an integer, return the 8 character HEX representation
	public String returnHexString( int n )
		{
		String pad = "00000000";
		String theString = Integer.toHexString( n );
		theString = theString.toLowerCase();

		return ( pad.substring( 0, 8 - theString.length() ) + theString );
		}


	public String returnHexDigest()
		{
		endMessage();

		return returnHexString( H[ 0 ] ) + returnHexString( H[ 1 ] ) + returnHexString( H[ 2 ] ) + returnHexString( H[ 3 ] ) + returnHexString( H[ 4 ] );
		}


	// Encode the string, and return the Message Digest
	public String returnHexDigest( String theString )
		{
		byte[] bytes = new byte[theString.length()];
		theString.getBytes( 0, bytes.length, bytes, 0 );

		encode( bytes, 0, bytes.length );

		return returnHexDigest();
		}


	public void encodeString( String s )
		{
		byte[] bytes = new byte[s.length()];
		s.getBytes( 0, bytes.length, bytes, 0 );

		encode( bytes, 0, bytes.length );
		}


	// Copy the digest into the array
	public void copyDigest( byte[] a, int offset )
		{
		endMessage();

		for ( int i = 0; i < 5; i++ )
			{
			a[ offset++ ] = (byte) ( ( H[ i ] >> 24 ) & 0xFF );
			a[ offset++ ] = (byte) ( ( H[ i ] >> 16 ) & 0xFF );
			a[ offset++ ] = (byte) ( ( H[ i ] >> 8 ) & 0xFF );
			a[ offset++ ] = (byte) ( ( H[ i ] ) & 0xFF );
			}
		}


	// Copy the digest into the array
	public byte[] returnDigest()
		{
		if ( digest == null )
			digest = new byte[20];

		copyDigest( digest, 0 );

		return digest;
		}


	// Main entry point
	static public void main( String[] args )
		{
		try
			{
			SHA se = new SHA();

			System.out.println( se.returnHexDigest( args[ 0 ] ) );
			/*			System.out.println( se.returnHexDigest( "abc" ) );
						se.reset();
						System.out.println( se.returnHexDigest( "The quick brown fox jumps over the lazy dog" ) );
						se.reset();
						System.out.println( se.returnHexDigest( "The quick brown fox jumps over the lazy cog" ) );
						se.reset();
						byte[] b = new byte[1000000];
						for ( int i = 0; i < b.length; i++ )
							{
							b[ i ] = 'a';
							}

						se.reset();
						se.encode( b );
						System.out.println( se.returnHexDigest() );
				*/}
		catch ( Throwable theX )
			{
			Util.printX( "", theX );
			}
		}
	}
