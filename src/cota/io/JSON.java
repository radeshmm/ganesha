package cota.io;

import cota.util.Queue;


public class JSON
	{
	// Print out the error, print out the rest of the stream, and then throw an
	// error
	public static void jsonError( String error, Throwable theX0, InStream in ) throws Throwable
		{
		System.out.println( "ERROR PARSING JSON OBJECT: " + error );
		theX0.printStackTrace( System.out );
		System.out.println( "The first byte in the following json byte stream triggered the error" );
		System.out.println( "################ STREAM BELOW #######################" );

		try
			{
			while ( true )
				{
				char c = in.readUTF8Char();

				System.out.print( c );
				// System.out.println( c + "\t" + (char) c );
				}
			}
		catch ( Throwable theX )
			{
			}

		System.out.println( "\n################ STREAM ABOVE #######################" );

		throw new JSONException( "JSON PARSE ERROR" );
		}


	public static void jsonError( String error, InStream in ) throws Throwable
		{
		System.out.println( "ERROR PARSING JSON OBJECT: " + error );
		System.out.println( "The first byte in the following json byte stream triggered the error" );
		System.out.println( "################ STREAM BELOW #######################" );

		try
			{
			while ( true )
				{
				char c = in.readUTF8Char();

				System.out.print( c );
				// System.out.println( c + "\t" + (char) c );
				}
			}
		catch ( Throwable theX )
			{
			}

		System.out.println( "\n################ STREAM ABOVE #######################" );

		throw new JSONException( "JSON PARSE ERROR" );
		}


	public static void parseBytes( InStream in ) throws Throwable
		{
		in.readByte();
		if ( in.readByte() != 'y' )
			jsonError( "ERROR PARSING BYTES VALUE", in );
		if ( in.readByte() != 't' )
			jsonError( "ERROR PARSING BYTES VALUE", in );
		if ( in.readByte() != 'e' )
			jsonError( "ERROR PARSING BYTES VALUE", in );
		if ( in.readByte() != 's' )
			jsonError( "ERROR PARSING BYTES VALUE", in );
		}


	public static void parseTrue( InStream in ) throws Throwable
		{
		in.readByte();
		if ( in.readByte() != 'r' )
			jsonError( "ERROR PARSING TRUE VALUE", in );
		if ( in.readByte() != 'u' )
			jsonError( "ERROR PARSING TRUE VALUE", in );
		if ( in.readByte() != 'e' )
			jsonError( "ERROR PARSING TRUE VALUE", in );
		}


	public static void parseFalse( InStream in ) throws Throwable
		{
		in.readByte();
		if ( in.readByte() != 'a' )
			jsonError( "ERROR PARSING FALSE VALUE", in );
		if ( in.readByte() != 'l' )
			jsonError( "ERROR PARSING FALSE VALUE", in );
		if ( in.readByte() != 's' )
			jsonError( "ERROR PARSING FALSE VALUE", in );
		if ( in.readByte() != 'e' )
			jsonError( "ERROR PARSING FALSE VALUE", in );
		}


	public static void parseNull( InStream in ) throws Throwable
		{
		in.readByte();
		if ( in.readByte() != 'u' )
			jsonError( "ERROR PARSING NULL VALUE", in );
		if ( in.readByte() != 'l' )
			jsonError( "ERROR PARSING NULL VALUE", in );
		if ( in.readByte() != 'l' )
			jsonError( "ERROR PARSING NULL VALUE", in );
		}


	public static DataValue[] parseArray( InStream in ) throws Throwable
		{
		Queue q = new Queue();

		try
			{
			// Keep reading from the stream until we encounter non white space
			byte c = in.skipWhitespace( false );

			if ( c == '[' )
				{
				while ( true )
					{
					c = in.skipWhitespace( true );

					// End of array?
					if ( c == ']' )
						{
						// Take off the ']' as it was put back in the stream
						in.readByte();

						break;
						}

					// Read in the value
					DataValue dv = DataValue.readValue( c, in );

					q.addObject( dv );

					// See if we have a comma
					c = in.skipWhitespace( true );
					if ( c == ',' )
						in.readByte();
					}
				}
			}
		catch ( JSONException jsonX )
			{
			throw jsonX;
			}
		catch ( Throwable theX )
			{
			jsonError( "GENERAL PARSE ERROR", theX, in );
			}

		DataValue[] results = new DataValue[q.size()];
		int size = q.size();
		for ( int i = 0; i < size; i++ )
			results[ i ] = (DataValue) q.elementAt( i );

		return results;
		}


	// The root DataObject is the one the is at the top of the hierarchy
	public static DataObject parseObject( InStream in ) throws Throwable
		{
		DataObject root = new DataObject();

		try
			{
			// Keep reading from the stream until we encounter non white space
			byte c = in.skipWhitespace( false );
			if ( c == '{' )
				{
				// We're inside of an object now
				while ( true )
					{
					c = in.skipWhitespace( true );

					// End of object?
					if ( c == '}' )
						{
						// Take off the '}' as it was put back in the stream
						in.readByte();

						return root;
						}

					// Immediate sub object object?
					if ( c == '{' )
						jsonError( "IMMEDIATE SUB OBJECT WITHOUT IDENTIFIER", in );

					// String identifier?
					if ( c == '\"' )
						{
						String identifier = in.readJSONStringUTF8();

						c = in.skipWhitespace( true );
						if ( c != ':' )
							jsonError( "EXPECTING ':', '" + c + "' instead", in );
						else
							in.readByte();

						c = in.skipWhitespace( true );

						// Read in the value
						DataValue value = DataValue.readValue( c, in );

						root.parts.put( identifier, value );

						// See if we have a comma
						c = in.skipWhitespace( true );
						if ( c == ',' )
							in.readByte();
						}
					else
						jsonError( "EXPECTING '\"' FOR STRING IDENTIFIER, '" + c + "' instead", in );
					}
				}

			jsonError( "INVALID JSON OBJECT: EXPECTING '{', '" + c + "' instead", in );
			}
		catch ( JSONException jsonX )
			{
			throw jsonX;
			}
		catch ( Throwable theX )
			{
			jsonError( "GENERAL PARSE ERROR", theX, in );
			}

		return root;
		}
	}