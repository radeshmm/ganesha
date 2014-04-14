package cota.crypto;

import java.nio.charset.Charset;


public class Murmur
	{
	private static final Charset UTF8_CHARSET = Charset.forName( "UTF-8" );

	public static final String salt = "%!%$";


	public static int hash( String s, int seed )
		{
		s = s + salt;

		return hash( s.getBytes( UTF8_CHARSET ), seed );
		}


	public static int hash( byte[] data, int seed )
		{
		// 'm' and 'r' are mixing constants generated offline.
		// They're not really 'magic', they just happen to work well.
		int m = 0x5bd1e995;
		int r = 24;

		// Initialize the hash to a 'random' value
		int len = data.length;
		int h = seed ^ len;

		int i = 0;
		while ( len >= 4 )
			{
			int k = data[ i + 0 ] & 0xFF;
			k |= ( data[ i + 1 ] & 0xFF ) << 8;
			k |= ( data[ i + 2 ] & 0xFF ) << 16;
			k |= ( data[ i + 3 ] & 0xFF ) << 24;

			k *= m;
			k ^= k >>> r;
			k *= m;

			h *= m;
			h ^= k;

			i += 4;
			len -= 4;
			}

		switch ( len )
			{
			case 3:
				h ^= ( data[ i + 2 ] & 0xFF ) << 16;
			case 2:
				h ^= ( data[ i + 1 ] & 0xFF ) << 8;
			case 1:
				h ^= ( data[ i + 0 ] & 0xFF );
				h *= m;
			}

		h ^= h >>> 13;
		h *= m;
		h ^= h >>> 15;

		if ( h < 0 )
			h = h + Integer.MAX_VALUE;

		return h;
		}


	public static int hash( long data, int seed )
		{
		// 'm' and 'r' are mixing constants generated offline.
		// They're not really 'magic', they just happen to work well.
		int m = 0x5bd1e995;
		int r = 24;

		// Initialize the hash to a 'random' value
		int h = seed ^ 8;

		int b8 = (int) ( ( data >>> 56 ) & 0xff );
		int b7 = (int) ( ( data >>> 48 ) & 0xff );
		int b6 = (int) ( ( data >>> 40 ) & 0xff );
		int b5 = (int) ( ( data >>> 32 ) & 0xff );
		int b4 = (int) ( ( data >>> 24 ) & 0xff );
		int b3 = (int) ( ( data >>> 16 ) & 0xff );
		int b2 = (int) ( ( data >>> 8 ) & 0xff );
		int b1 = (int) ( data & 0xff );

		int k = b8;
		k |= b7 << 8;
		k |= b6 << 16;
		k |= b5 << 24;

		k *= m;
		k ^= k >>> r;
		k *= m;

		h *= m;
		h ^= k;

		k = b4 & 0xFF;
		k |= b3 << 8;
		k |= b2 << 16;
		k |= b1 << 24;

		k *= m;
		k ^= k >>> r;
		k *= m;

		h *= m;
		h ^= k;

		h ^= h >>> 13;
		h *= m;
		h ^= h >>> 15;

		if ( h < 0 )
			h = h + Integer.MAX_VALUE;

		return h;
		}


	public static int hash( long data )
		{
		return hash( data, 0 );
		}


	public static int hash( int data, int seed )
		{
		// 'm' and 'r' are mixing constants generated offline.
		// They're not really 'magic', they just happen to work well.
		int m = 0x5bd1e995;
		int r = 24;

		// Initialize the hash to a 'random' value
		int h = seed ^ 4;

		int b4 = ( data >>> 24 ) & 0xff;
		int b3 = ( data >>> 16 ) & 0xff;
		int b2 = ( data >>> 8 ) & 0xff;
		int b1 = data & 0xff;

		int k = b4;
		k |= b3 << 8;
		k |= b2 << 16;
		k |= b1 << 24;

		k *= m;
		k ^= k >>> r;
		k *= m;

		h *= m;
		h ^= k;

		h ^= h >>> 13;
		h *= m;
		h ^= h >>> 15;

		return h;
		}
	}
