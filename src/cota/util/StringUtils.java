package cota.util;

import java.math.BigInteger;
import java.text.Normalizer;
import java.util.Calendar;
import java.util.regex.Pattern;


public class StringUtils
	{
	public static Queue opticallyEquivalentStrings = new Queue();
	public static Queue opticallyEquivalentStringsForeignOnly = new Queue();

	static
		{
		// Roman
		addPair( "l", "I" );
		addPair( "O", "0" );

		opticallyEquivalentStringsForeignOnly = new Queue();

		// Cyrillic
		addPair( "Ѕ", "S" );
		addPair( "І", "I" );
		addPair( "Ј", "J" );
		addPair( "А", "A" );
		addPair( "В", "B" );
		addPair( "Е", "E" );
		addPair( "К", "K" );
		addPair( "М", "M" );
		addPair( "Н", "H" );
		addPair( "О", "O" );
		addPair( "Р", "P" );
		addPair( "С", "C" );
		addPair( "Т", "T" );
		addPair( "У", "y" );
		addPair( "Х", "X" );
		addPair( "а", "a" );
		addPair( "е", "e" );
		addPair( "о", "o" );
		addPair( "р", "p" );
		addPair( "с", "c" );
		addPair( "у", "y" );
		addPair( "х", "x" );
		addPair( "ѕ", "s" );
		addPair( "і", "i" );
		addPair( "ј", "j" );
		addPair( "ѡ", "w" );
		addPair( "ԁ", "d" );
		addPair( "ԝ", "w" );
		addPair( "Ԛ", "Q" );
		addPair( "ԛ", "q" );
		addPair( "Ԝ", "w" );

		// Greek
		addPair( "Α", "A" );
		addPair( "Β", "B" );
		addPair( "Ε", "E" );
		addPair( "Z", "Z" );
		addPair( "H", "H" );
		addPair( "I", "I" );
		addPair( "K", "K" );
		addPair( "M", "M" );
		addPair( "Ν", "N" );
		addPair( "Ο", "O" );
		addPair( "Ρ", "P" );
		addPair( "Τ", "T" );
		addPair( "Υ", "Y" );
		addPair( "Χ", "X" );
		addPair( "ϲ", "c" );

		// Latin
		addPair( "ᴀ", "A" );
		addPair( "ᴅ", "D" );
		addPair( "ᴄ", "c" );
		addPair( "ᴇ", "E" );
		addPair( "ᴊ", "J" );
		addPair( "ᴋ", "K" );
		addPair( "ᴍ", "M" );
		addPair( "ᴏ", "o" );
		addPair( "ᴏ", "O" );
		addPair( "ᴘ", "P" );
		addPair( "ᴛ", "T" );
		addPair( "ᴜ", "U" );
		addPair( "ᴜ", "u" );
		addPair( "ᴠ", "v" );
		addPair( "ᴡ", "w" );
		addPair( "ᴢ", "z" );
		}


	public static void addPair( String x, String y )
		{
		//		System.out.println( x + " " + (int) x.charAt( 0 ) + ":" + (int) y.charAt( 0 ) );

		opticallyEquivalentStrings.addObject( new PairSS( x, y ) );
		opticallyEquivalentStrings.addObject( new PairSS( y, x ) );

		opticallyEquivalentStringsForeignOnly.addObject( new PairSS( x, y ) );
		}


	public static String removeDiacritics( String input )
		{
		String nrml = Normalizer.normalize( input, Normalizer.Form.NFD );
		StringBuilder stripped = new StringBuilder();
		for ( int i = 0; i < nrml.length(); ++i )
			{
			if ( Character.getType( nrml.charAt( i ) ) != Character.NON_SPACING_MARK )
				{
				stripped.append( nrml.charAt( i ) );
				}
			}
		return stripped.toString();
		}


	public static String deAccent( String str )
		{
		String nfdNormalizedString = Normalizer.normalize( str, Normalizer.Form.NFD );
		Pattern pattern = Pattern.compile( "\\p{InCombiningDiacriticalMarks}+" );
		return pattern.matcher( nfdNormalizedString ).replaceAll( "" );
		}


	public static String bytesToHexString( byte[] bytes )
		{
		BigInteger bi = new BigInteger( 1, bytes );
		return String.format( "%0" + ( bytes.length << 1 ) + "x", bi );
		}


	public static byte[] hexStringToBytes( String s )
		{
		int sLength = s.length();

		byte[] buffer = new byte[sLength >> 1];
		for ( int i = 0; i < ( sLength - 1 ); i = i + 2 )
			{
			int t = Integer.parseInt( s.substring( i, i + 2 ), 16 );

			buffer[ i >> 1 ] = (byte) t;
			}

		return buffer;
		}


	// find a String that starts with one String
	public static String findString( String s, String prefix )
		{
		return findString( s, prefix, null );
		}


	// find a String that lies between two other Strings
	public static String findString( String s, String prefix, String suffix )
		{
		int index = s.indexOf( prefix );
		if ( index == -1 )
			return null;

		int index2 = 0;
		if ( suffix != null )
			{
			index2 = s.indexOf( suffix, index + prefix.length() );

			if ( index2 == -1 )
				return null;
			}
		else
			index2 = s.length();

		return s.substring( index + prefix.length(), index2 );
		}


	// find the location of a given String backwards from a certain point
	public static String findBackwards( String s, String s1, String s2, int index )
		{
		while ( !s.startsWith( s1, index ) )
			{
			index--;
			}

		int index2 = s.indexOf( s2, index + s1.length() );
		if ( index2 == -1 )
			{
			return null;
			}

		return s.substring( index + s1.length(), index2 );
		}


	// perform an efficient search for a given String without case
	public static int findWithoutCase( char[] chars, String x, int start )
		{
		char[] low = x.toLowerCase().toCharArray();
		char[] high = x.toUpperCase().toCharArray();

		int end = ( chars.length - low.length );
		for ( int i = start; i < end; i++ )
			{
			boolean match = true;
			for ( int j = 0; j < low.length; j++ )
				{
				boolean upperMatch = chars[ i + j ] == high[ j ];
				boolean lowerMatch = chars[ i + j ] == low[ j ];
				if ( !( upperMatch || lowerMatch ) )
					{
					match = false;
					break;
					}
				}

			if ( match )
				{
				return i;
				}
			}

		return -1;
		}


	// perform an efficient search for a given String without case
	public static int findWithoutCase( byte[] chars, String x, int start )
		{
		char[] low = x.toLowerCase().toCharArray();
		char[] high = x.toUpperCase().toCharArray();

		int end = ( chars.length - low.length );
		for ( int i = start; i < end; i++ )
			{
			boolean match = true;
			for ( int j = 0; j < low.length; j++ )
				{
				boolean upperMatch = chars[ i + j ] == high[ j ];
				boolean lowerMatch = chars[ i + j ] == low[ j ];
				if ( !( upperMatch || lowerMatch ) )
					{
					match = false;
					break;
					}
				}

			if ( match )
				{
				return i;
				}
			}

		return -1;
		}


	// perform an efficient search for a given String
	public static int findWithCase( char[] chars, String x, int start )
		{
		char[] low = x.toCharArray();

		int end = ( chars.length - low.length );
		for ( int i = start; i < end; i++ )
			{
			boolean match = true;
			for ( int j = 0; j < low.length; j++ )
				{
				boolean lowerMatch = chars[ i + j ] == low[ j ];
				if ( !lowerMatch )
					{
					match = false;
					break;
					}
				}

			if ( match )
				{
				return i;
				}
			}

		return -1;
		}


	// perform an efficient search for a given String
	public static int findWithCase( byte[] chars, String x, int start )
		{
		char[] low = x.toCharArray();

		int end = ( chars.length - low.length );
		for ( int i = start; i < end; i++ )
			{
			boolean match = true;
			for ( int j = 0; j < low.length; j++ )
				{
				boolean lowerMatch = chars[ i + j ] == low[ j ];
				if ( !lowerMatch )
					{
					match = false;
					break;
					}
				}

			if ( match )
				{
				return i;
				}
			}

		return -1;
		}


	// perform an efficient search for a given String
	public static int findWithCase( byte[] chars, char[] toFind, int start )
		{
		int end = ( chars.length - toFind.length );
		for ( int i = start; i < end; i++ )
			{
			boolean match = true;
			for ( int j = 0; j < toFind.length; j++ )
				{
				boolean lowerMatch = chars[ i + j ] == toFind[ j ];
				if ( !lowerMatch )
					{
					match = false;
					break;
					}
				}

			if ( match )
				{
				return i;
				}
			}

		return -1;
		}


	// perform an efficient search for a given String without case
	public static int findWithoutCase( String s, String x, int start )
		{
		return findWithoutCase( s.toCharArray(), x, start );
		}


	// perform an efficient search for a given String without case
	public static int findWithoutCaseBackwards( char[] chars, String x, int start )
		{
		char[] low = x.toLowerCase().toCharArray();
		char[] high = x.toUpperCase().toCharArray();

		if ( ( start + low.length ) >= chars.length )
			{
			start = chars.length - low.length - 1;
			}

		for ( int i = start; i >= 0; i-- )
			{
			boolean match = true;
			for ( int j = 0; j < low.length; j++ )
				{
				boolean upperMatch = chars[ i + j ] == high[ j ];
				boolean lowerMatch = chars[ i + j ] == low[ j ];
				if ( !( upperMatch || lowerMatch ) )
					{
					match = false;
					break;
					}
				}

			if ( match )
				{
				return i;
				}
			}

		return -1;
		}


	// perform an efficient search for a given String without case
	public static int findWithoutCaseBackwards( String s, String x, int start )
		{
		return findWithoutCaseBackwards( s.toCharArray(), x, start );
		}


	// return a string array substring
	public static char[] substring( char[] s, int start, int end )
		{
		char[] result = new char[end - start];
		for ( int i = 0; i < result.length; i++ )
			{
			result[ i ] = s[ start + i ];

			}

		return result;
		}


	// return a string array substring
	public static byte[] substring( byte[] s, int start, int end )
		{
		byte[] result = new byte[end - start];
		for ( int i = 0; i < result.length; i++ )
			result[ i ] = s[ start + i ];

		return result;
		}


	public static char[] substring( String s, int start, int end )
		{
		return substring( s.toCharArray(), start, end );
		}


	// Decode a String
	public static String decodeURLString( String theString ) throws Throwable
		{
		if ( theString == null )
			return null;

		int count = 0;
		byte[] buffer = new byte[theString.length()];
		for ( int i = 0; i < buffer.length; i++ )
			{
			char c = theString.charAt( i );

			if ( c == '+' )
				buffer[ count++ ] = ' ';
			else
				{
				if ( c == '%' )
					{
					String s = theString.substring( i + 1, i + 3 );

					Integer num = Integer.valueOf( s, 16 );
					c = (char) num.intValue();

					i = i + 2;
					}

				// System.out.print( c );
				buffer[ count++ ] = (byte) c;
				}
			}

		return new String( buffer, 0, count, "UTF-8" );
		}


	/*
	 * This is the pre UTF8 way of doing it
		// Decode a String
		public static String decodeURLString( String theString )
			{
			if ( theString == null )
				return null;

			// System.out.println( theString );
			char[] buffer = new char[theString.length()];
			int offset = 0;

			for ( int i = 0; i < theString.length(); i++ )
				{
				char c = theString.charAt( i );

				if ( c == '+' )
					buffer[ offset++ ] = ' ';
				else
					{
					if ( c == '%' )
						{
						String s = theString.substring( i + 1, i + 3 );

						Integer num = Integer.valueOf( s, 16 );
						c = (char) num.intValue();

						i = i + 2;
						}

					// System.out.print( c );
					buffer[ offset++ ] = c;
					}
				}

			return new String( buffer, 0, offset );
			}
	*/

	// Split a string into its component words
	public static Queue split( String s, char div )
		{
		Queue results = new Queue();
		String nextWord = "";

		char[] chars = s.toCharArray();
		for ( int i = 0; i < chars.length; i++ )
			{
			if ( chars[ i ] == div )
				{
				if ( nextWord.length() > 0 )
					results.addObject( nextWord );

				nextWord = "";
				}
			else
				nextWord = nextWord + chars[ i ];
			}

		if ( nextWord.length() > 0 )
			results.addObject( nextWord );

		return results;
		}


	//  within a given string, replace one string with another
	public static String replace( String s, String x, String y ) throws Throwable
		{
		if ( y.indexOf( x ) != -1 )
			{
			String foo = replace( s, x, "@@@###@@@" );

			return replace( foo, "@@@###@@@", y );
			}

		while ( true )
			{
			int index = s.indexOf( x );

			if ( index == -1 )
				{
				return s;
				}

			String firstPart = s.substring( 0, index );
			String secondPart = s.substring( index + x.length(), s.length() );

			s = firstPart + y + secondPart;
			}
		}


	public static String formatPrice( String price )
		{
		return formatPrice( Double.valueOf( price ).doubleValue() );
		}


	public static String formatPrice( double price )
		{
		price = price + .000001;
		String neg = "";
		if ( price < 0 )
			{
			neg = "-";
			price = -price;
			}

		int wp = (int) price;
		int cents = (int) ( 100 * ( price - wp ) );
		if ( cents == 0 )
			return neg + wp;

		String priceS = neg + wp + ".";

		if ( cents < 10 )
			priceS = priceS + "0";

		priceS = priceS + cents;

		return priceS;
		}


	// remove certain character from the String
	public static String removeChar( String s, char x )
		{
		int count = 0;
		char[] chars = new char[s.length()];
		for ( int i = 0; i < s.length(); i++ )
			{
			char c = s.charAt( i );

			if ( c != x )
				chars[ count++ ] = c;
			}

		return new String( chars, 0, count );
		}


	// extract a numeric price from the given String
	// Returns -1 if there was a problem
	public static double extractPrice( char[] chars, int start )
		{
		try
			{
			int index = 0;
			for ( int i = start; i < chars.length; i++ )
				{
				char c = chars[ i ];
				if ( ( ( c >= '0' ) && ( c <= '9' ) ) || ( c == '.' ) )
					{
					index = i;
					break;
					}
				}

			int index2 = index;
			boolean decimalSeen = false;
			for ( index2 = index + 1; index2 < chars.length; index2++ )
				{
				char c = chars[ index2 ];

				if ( decimalSeen )
					{
					if ( !( ( ( c >= '0' ) && ( c <= '9' ) ) || ( c == ',' ) ) )
						{
						break;
						}
					}
				else
					{
					if ( !( ( ( c >= '0' ) && ( c <= '9' ) ) || ( c == '.' ) || ( c == ',' ) ) )
						{
						break;
						}
					}

				if ( c == '.' )
					{
					decimalSeen = true;
					}
				}

			String priceS = new String( chars, index, index2 - index );
			priceS = removeChar( priceS, ',' );

			return Double.valueOf( priceS ).doubleValue();
			}
		catch ( Throwable theX )
			{
			// Util.printX( "FOOOOO", theX );
			}

		return -1;
		}


	// extract a numeric price from the given String
	// Returns -1 if there was a problem
	public static double extractPrice( String s )
		{
		return extractPrice( s.toCharArray(), 0 );
		}


	public static String getYYMMDD()
		{
		Calendar c = Calendar.getInstance();

		int year = c.get( Calendar.YEAR ) - 2000;
		String yearS = "" + year;
		if ( yearS.length() == 1 )
			yearS = "0" + yearS;

		int month = c.get( Calendar.MONTH ) + 1;
		String monthS = "" + month;
		if ( monthS.length() == 1 )
			monthS = "0" + monthS;

		int day = c.get( Calendar.DAY_OF_MONTH );// + 1;
		String dayS = "" + day;
		if ( dayS.length() == 1 )
			dayS = "0" + dayS;

		return yearS + monthS + dayS;
		}


	// remove starting and ending white space
	public static String cleanup( String line )
		{
		char[] c = line.toCharArray();

		int index = 0;
		for ( int i = 0; i < c.length; i++ )
			{
			char d = c[ i ];
			if ( ( d != ' ' ) && ( d != '\t' ) )
				{
				index = i;
				break;
				}
			}

		boolean spaceSeen = false;
		int count = 0;
		for ( int i = index; i < c.length; i++ )
			{
			char d = c[ i ];
			if ( ( d == ' ' ) || ( d == '\t' ) )
				{
				spaceSeen = true;
				}
			else
				{
				if ( spaceSeen )
					{
					c[ count++ ] = ' ';
					}
				c[ count++ ] = d;

				spaceSeen = false;
				}
			}

		return new String( c, 0, count );
		}


	public static String numbersOnly( String s )
		{
		int count = 0;
		char[] chars = new char[s.length()];
		for ( int i = 0; i < chars.length; i++ )
			{
			char c = s.charAt( i );

			if ( ( c >= '0' ) && ( c <= '9' ) )
				chars[ count++ ] = c;
			}

		return new String( chars, 0, count );
		}


	public static String lettersNumbersSpaceOnly( String s )
		{
		int count = 0;
		char[] chars = new char[s.length()];
		for ( int i = 0; i < chars.length; i++ )
			{
			char c = s.charAt( i );

			if ( ( c >= '0' ) && ( c <= '9' ) )
				chars[ count++ ] = c;

			if ( ( c >= 'a' ) && ( c <= 'z' ) )
				chars[ count++ ] = c;

			if ( ( c >= 'A' ) && ( c <= 'Z' ) )
				chars[ count++ ] = c;

			if ( c == ' ' )
				chars[ count++ ] = c;
			}

		return new String( chars, 0, count );
		}


	public static String cleanName( String name )
		{
		name = name.toLowerCase();

		char[] chars = name.toCharArray();
		char[] newChars = new char[chars.length];

		boolean lastWasSpace = false;
		for ( int i = 0; i < chars.length; i++ )
			{
			char c = chars[ i ];

			boolean good = false;

			if ( ( c >= 'a' ) && ( c <= 'z' ) )
				good = true;
			if ( ( c >= 'A' ) && ( c <= 'Z' ) )
				good = true;
			if ( ( c >= '0' ) && ( c <= '9' ) )
				good = true;

			if ( ( lastWasSpace ) && ( c == ' ' ) )
				{

				}
			else
				{
				if ( !good )
					c = '_';

				newChars[ i ] = c;
				}

			lastWasSpace = c == ' ';
			}

		return new String( newChars );
		}


	public static void main( String[] args )
		{
		System.out.println( removeDiacritics( args[ 0 ] ) );
		}
	}