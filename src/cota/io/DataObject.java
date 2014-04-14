package cota.io;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.math.BigInteger;
import java.net.URLEncoder;
import java.util.Date;

import cota.util.FashEntry_so;
import cota.util.Fashtable_so;
import cota.util.PairSO;
import cota.util.Queue;
import cota.util.Util;


// The DataObject is really the basis of everything in the Othernet
// It is used as the argument to WebServices
// It is returned as the results from WebServices
// It is returned as when grabbing objects from an ObjectPool
// It is created and then placed into ObjectPools
// Note that numbers (integers, floats and doubles) are stored within memory as Strings
// When they are taken from memory, they are converted to whatever numeric type the call suggests
// Very useful indeed!
//
// Arrays can only be created out of the following classes, and each element of an array must be the same type
// DataObject
// String
// byte
// short
// int
// long
// char
// float
// double
// BigInteger

// Every piece of data is stored internally within the DataObject as a DataValue
// This allows us to keep track of the data type so that it can be formatted correctly within JSON
public class DataObject
	{
	private static final String OTHERNET__TYPE_SUFFIX = "__@!!@@!";

	Fashtable_so parts = new Fashtable_so();

	boolean outputBytes = true;


	public DataObject()
		{
		}


	public DataObject( DataObject o ) throws Throwable
		{
		String s = o.toString();

		byte[] bytes = s.getBytes( "UTF-8" );

		InStream in = new InStream( bytes );
		DataObject o2 = JSON.parseObject( in );

		parts = o2.parts;

		outputBytes = outputBytes || o.outputBytes;
		}


	// Create a DataObject from a String
	public DataObject( String s ) throws Throwable
		{
		byte[] bytes = s.getBytes( "UTF-8" );

		InStream in = new InStream( bytes );
		DataObject o = JSON.parseObject( in );

		parts = o.parts;
		}


	// Create a DataObject from the given InStream
	public DataObject( InStream in ) throws Throwable
		{
		// Use a modified JSON style encoding to create the DataObject
		DataObject o = JSON.parseObject( in );

		// Check to see if we have any raw bytes to fill in within the
		// DataObject
		// If not, then we are done
		boolean rawBytesExist = o.containsRawBytes();
		if ( !rawBytesExist )
			{
			parts = o.parts;

			return;
			}

		// We've read in the usual JSON object
		// Now read the actualy byte[]s that currently are associated with
		// identifiers with byte[] stubs
		Fashtable_so bytesF = readBytes( in );

		o.fillInByteStubs( bytesF );

		parts = o.parts;
		}


	public void reset()
		{
		parts = new Fashtable_so();

		outputBytes = false;
		}


	public void makeAttributeNamesLowerCase()
		{
		FashEntry_so[] entries = parts.returnArrayOfEntries();

		for ( int i = 0; i < entries.length; i++ )
			{
			FashEntry_so e = entries[ i ];

			String key = e.key;
			Object value = e.value;

			String keyL = key.toLowerCase();
			if ( !key.equals( keyL ) )
				{
				parts.remove( key );
				parts.put( keyL, value );
				}
			}
		}


	// Returns a null object if the file is not found
	public static DataObject loadFromFile( String fileName ) throws Throwable
		{
		try
			{
			InStream in = new InStream( new FileInputStream( fileName ) );

			DataObject d = in.readDataObject();
			in.close();

			return d;
			}
		catch ( Throwable theX )
			{
			Util.printX( theX );
			}

		return null;
		}


	public void setContents( String s ) throws Throwable
		{
		DataObject o = new DataObject( s );

		copyContentsFrom( o );
		}


	public void copyContentsFrom( DataObject o )
		{
		parts = o.parts;
		outputBytes = o.outputBytes;
		}


	public void addContentsFrom( DataObject o )
		{
		parts.addTable( o.parts );
		outputBytes = outputBytes || o.outputBytes;
		}


	// Fill in the byte stubs that were reported in the JSON object itself
	// This is sort of a hack
	// Rather then have the actual bytes of the byte[] with the JSON object
	// itself, they follow after the actual JSON object
	public void fillInByteStubs( Fashtable_so bytesF ) throws Throwable
		{
		FashEntry_so[] entries = parts.returnArrayOfEntries();

		for ( int i = 0; i < entries.length; i++ )
			{
			String identifier = entries[ i ].key;

			DataValue dv = (DataValue) entries[ i ].value;

			if ( dv.type == DataValue.BYTES )
				dv.fillInByteStubs( identifier, bytesF );

			if ( dv.type == DataValue.ARRAY )
				dv.fillInByteStubs( identifier, bytesF );

			if ( dv.type == DataValue.DATA_OBJECT )
				dv.fillInByteStubs( identifier + ".", bytesF );
			}
		}


	// Fill in the byte stubs that were reported in the JSON object itself
	// This is sort of a hack
	// Rather then have the actual bytes of the byte[] with the JSON object
	// itself, they follow after the actual JSON object
	public void fillInByteStubs( String identifier0, Fashtable_so bytesF ) throws Throwable
		{
		FashEntry_so[] entries = parts.returnArrayOfEntries();

		for ( int i = 0; i < entries.length; i++ )
			{
			String identifier = identifier0 + entries[ i ].key;
			DataValue dv = (DataValue) entries[ i ].value;

			if ( dv.type == DataValue.BYTES )
				dv.fillInByteStubs( identifier, bytesF );

			if ( dv.type == DataValue.ARRAY )
				dv.fillInByteStubs( identifier, bytesF );

			if ( dv.type == DataValue.DATA_OBJECT )
				dv.fillInByteStubs( identifier + ".", bytesF );
			}
		}


	// This is where the bytes located after the JSON object are actually read
	public static Fashtable_so readBytes( InStream in ) throws Throwable
		{
		Fashtable_so f = new Fashtable_so();

		in.skipWhitespace( true );
		String num0 = in.readLine();
		int num = Integer.parseInt( num0 );

		for ( int i = 0; i < num; i++ )
			{
			in.skipWhitespace( true );

			String identifier = in.readJSONStringUTF8();

			in.skipWhitespace( true );
			String length0 = in.readLine();
			int length = Integer.parseInt( length0 );

			//			System.out.println( "BYTE[] #" + identifier + "#" + length );

			byte[] b = new byte[length];
			in.readBytes( b );

			if ( f.get( identifier ) != null )
				throw new Throwable( "CANNOT HAVE TWO BYTE[]s WITH THE SAME IDENTIFIER: " + identifier );

			f.put( identifier, b );
			}

		return f;
		}


	public void remove( String id )
		{
		parts.remove( id );
		}


	public static void writeIndent( int indent, OutStream out ) throws Throwable
		{
		for ( int i = 0; i < indent; i++ )
			out.writeByte( '\t' );
		}


	public void writeToStream( String path, int indent, OutStream out, Queue bytesToWrite ) throws Throwable
		{
		writeIndent( indent, out );
		out.writeByte( '{' );
		out.writeByte( '\n' );

		FashEntry_so[] entries = parts.returnArrayOfEntries();

		for ( int i = 0; i < entries.length; i++ )
			{
			String key = entries[ i ].key;

			writeIndent( indent, out );
			out.writeByte( '\"' );
			out.writeJSONStringUTF8( key );
			out.writeByte( '\"' );
			out.writeByte( ':' );

			DataValue dv = (DataValue) entries[ i ].value;
			dv.writeToStream( path, key, indent + 1, out, false, bytesToWrite );

			if ( i != ( entries.length - 1 ) )
				{
				out.writeByte( ',' );
				out.writeByte( '\n' );
				}
			else
				out.writeByte( '\n' );
			}

		writeIndent( indent, out );
		out.writeByte( '}' );
		}


	public void writeToStream( OutStream out ) throws Throwable
		{
		Queue bytesToWrite = new Queue();

		writeToStream( "", 0, out, bytesToWrite );
		out.writeByte( '\n' );

		if ( !outputBytes )
			return;

		// Write the actual bytes that are defined within the JSON object as
		// merely byte stubs
		int size = bytesToWrite.size();

		if ( size != 0 )
			{
			out.writePlainString( "" + size );
			out.writeByte( '\n' );
			}

		for ( int i = 0; i < size; i++ )
			{
			PairSO dv = (PairSO) bytesToWrite.elementAt( i );

			String identifier = dv.x;
			//			System.out.println( i + "\t" + identifier );

			byte[] b = (byte[]) dv.y;

			out.writeByte( '\n' );
			out.writeByte( '\"' );
			out.writeJSONStringUTF8( identifier );
			out.writeByte( '\"' );
			out.writeByte( '\n' );

			out.writePlainString( "" + b.length );
			out.writeByte( '\n' );

			out.writeBytes( b );
			out.writeByte( '\n' );
			}
		}


	// Check for " or \ within the string
	public static String nonNull( String s )
		{
		if ( s == null )
			return "";

		return s;
		}


	// //////////////////////////////////////////////////////////////////////////////////
	// These calls are used to insert data directly into the DataObject
	public void put( String id, String s )
		{
		s = nonNull( s );
		DataValue dv = new DataValue( s, DataValue.STRING );

		parts.put( id, dv );
		}


	public void put( String id, byte n )
		{
		DataValue dv = new DataValue( "" + n, DataValue.NUMBER );

		parts.put( id, dv );
		}


	public void put( String id, short n )
		{
		DataValue dv = new DataValue( "" + n, DataValue.NUMBER );

		parts.put( id, dv );
		}


	public void put( String id, int n )
		{
		DataValue dv = new DataValue( "" + n, DataValue.NUMBER );

		parts.put( id, dv );
		}


	public void put( String id, long n )
		{
		DataValue dv = new DataValue( "" + n, DataValue.NUMBER );

		parts.put( id, dv );
		}


	public void put( String id, char n )
		{
		DataValue dv = new DataValue( "" + n, DataValue.NUMBER );

		parts.put( id, dv );
		}


	public void put( String id, float n )
		{
		DataValue dv = new DataValue( "" + n, DataValue.NUMBER );

		parts.put( id, dv );
		}


	public void put( String id, double n )
		{
		DataValue dv = new DataValue( "" + n, DataValue.NUMBER );

		parts.put( id, dv );
		}


	public void put( String id, BigInteger n )
		{
		DataValue dv = new DataValue( n.toString(), DataValue.NUMBER );

		parts.put( id, dv );
		}


	public void put( String id, DataObject o )
		{
		DataValue dv = new DataValue( o, DataValue.DATA_OBJECT );

		parts.put( id, dv );
		}


	// One dimensional arrays
	public void put( String id, DataObject[] a )
		{
		DataValue[] dva = new DataValue[a.length];
		for ( int i = 0; i < a.length; i++ )
			dva[ i ] = new DataValue( a[ i ], DataValue.DATA_OBJECT );

		DataValue dv = new DataValue( dva, DataValue.ARRAY );

		parts.put( id, dv );
		}


	public void put( String id, String[] a )
		{
		DataValue[] dva = new DataValue[a.length];
		for ( int i = 0; i < a.length; i++ )
			{
			a[ i ] = nonNull( a[ i ] );

			dva[ i ] = new DataValue( a[ i ], DataValue.STRING );
			}

		DataValue dv = new DataValue( dva, DataValue.ARRAY );

		parts.put( id, dv );
		}


	// This is used for variable size args which can be (in the Othernet) string or numbers
	public void put( String id, Object[] a ) throws Throwable
		{
		String[] types = new String[a.length];

		DataValue[] dva = new DataValue[a.length];
		for ( int i = 0; i < a.length; i++ )
			{
			Object o = a[ i ];
			String type = null;

			boolean handled = false;
			if ( o instanceof java.lang.String )
				{
				type = "String";

				handled = true;
				}

			if ( !handled )
				if ( o instanceof java.lang.Short )
					{
					type = "Short";

					handled = true;
					}

			if ( !handled )
				if ( o instanceof java.lang.Integer )
					{
					type = "Integer";

					handled = true;
					}

			if ( !handled )
				if ( o instanceof java.lang.Long )
					{
					type = "Long";

					handled = true;
					}

			if ( !handled )
				if ( o instanceof java.lang.Float )
					{
					type = "Float";

					handled = true;
					}

			if ( !handled )
				if ( o instanceof java.lang.Double )
					{
					type = "Double";

					handled = true;
					}

			if ( !handled )
				if ( o instanceof java.lang.Boolean )
					{
					type = "Boolean";

					handled = true;
					}

			if ( !handled )
				if ( o instanceof java.util.Date )
					type = "Date";

			if ( type == null )
				throw new Throwable( "UNHANDLED OBJECT TYPE ENCOUTERED: " + a[ i ].toString() );

			types[ i ] = type;

			dva[ i ] = new DataValue( a[ i ].toString(), DataValue.STRING );
			}

		DataValue dv = new DataValue( dva, DataValue.ARRAY );

		parts.put( id, dv );

		// Add the types as well
		put( id + OTHERNET__TYPE_SUFFIX, types );
		}


	public void put( String id, byte[] a )
		{
		if ( a.length > 1024 )
			{
			System.out.println( "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" );
			System.out.println( "You may wish to use the putRawBytes method for large arrays of bytes as it greatly improves performance" );

			Throwable x = new Throwable();
			Util.printX( x );

			System.out.println( "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" );
			}

		DataValue[] dva = new DataValue[a.length];
		for ( int i = 0; i < a.length; i++ )
			dva[ i ] = new DataValue( "" + a[ i ], DataValue.NUMBER );

		DataValue dv = new DataValue( dva, DataValue.ARRAY );

		parts.put( id, dv );
		}


	public void put( String id, short[] a )
		{
		DataValue[] dva = new DataValue[a.length];
		for ( int i = 0; i < a.length; i++ )
			dva[ i ] = new DataValue( "" + a[ i ], DataValue.NUMBER );

		DataValue dv = new DataValue( dva, DataValue.ARRAY );

		parts.put( id, dv );
		}


	public void put( String id, int[] a )
		{
		DataValue[] dva = new DataValue[a.length];
		for ( int i = 0; i < a.length; i++ )
			dva[ i ] = new DataValue( "" + a[ i ], DataValue.NUMBER );

		DataValue dv = new DataValue( dva, DataValue.ARRAY );

		parts.put( id, dv );
		}


	public void put( String id, long[] a )
		{
		DataValue[] dva = new DataValue[a.length];
		for ( int i = 0; i < a.length; i++ )
			dva[ i ] = new DataValue( "" + a[ i ], DataValue.NUMBER );

		DataValue dv = new DataValue( dva, DataValue.ARRAY );

		parts.put( id, dv );
		}


	public void put( String id, char[] a )
		{
		DataValue[] dva = new DataValue[a.length];
		for ( int i = 0; i < a.length; i++ )
			dva[ i ] = new DataValue( "" + a[ i ], DataValue.NUMBER );

		DataValue dv = new DataValue( dva, DataValue.ARRAY );

		parts.put( id, dv );
		}


	public void put( String id, float[] a )
		{
		DataValue[] dva = new DataValue[a.length];
		for ( int i = 0; i < a.length; i++ )
			dva[ i ] = new DataValue( "" + a[ i ], DataValue.NUMBER );

		DataValue dv = new DataValue( dva, DataValue.ARRAY );

		parts.put( id, dv );
		}


	public void put( String id, double[] a )
		{
		DataValue[] dva = new DataValue[a.length];
		for ( int i = 0; i < a.length; i++ )
			dva[ i ] = new DataValue( "" + a[ i ], DataValue.NUMBER );

		DataValue dv = new DataValue( dva, DataValue.ARRAY );

		parts.put( id, dv );
		}


	public void put( String id, BigInteger[] a )
		{
		DataValue[] dva = new DataValue[a.length];
		for ( int i = 0; i < a.length; i++ )
			dva[ i ] = new DataValue( "" + a[ i ], DataValue.NUMBER );

		DataValue dv = new DataValue( dva, DataValue.ARRAY );

		parts.put( id, dv );
		}


	// Two dimensional arrays
	public void put( String id, DataObject[][] a )
		{
		int dim1 = a.length;
		int dim2 = a[ 0 ].length;

		DataValue[] dva = new DataValue[dim1];
		for ( int i = 0; i < dim1; i++ )
			{
			DataValue[] dva2 = new DataValue[dim2];
			for ( int j = 0; j < dim2; j++ )
				dva2[ j ] = new DataValue( a[ i ][ j ], DataValue.DATA_OBJECT );

			dva[ i ] = new DataValue( dva2, DataValue.ARRAY );
			}

		DataValue dv = new DataValue( dva, DataValue.ARRAY );

		parts.put( id, dv );
		}


	public void put( String id, String[][] a )
		{
		int dim1 = a.length;
		int dim2 = a[ 0 ].length;

		DataValue[] dva = new DataValue[dim1];
		for ( int i = 0; i < dim1; i++ )
			{
			DataValue[] dva2 = new DataValue[dim2];
			for ( int j = 0; j < dim2; j++ )
				{
				a[ i ][ j ] = nonNull( a[ i ][ j ] );

				dva2[ j ] = new DataValue( a[ i ][ j ], DataValue.STRING );
				}

			dva[ i ] = new DataValue( dva2, DataValue.ARRAY );
			}

		DataValue dv = new DataValue( dva, DataValue.ARRAY );

		parts.put( id, dv );
		}


	public void put( String id, byte[][] a )
		{
		int dim1 = a.length;
		int dim2 = a[ 0 ].length;

		DataValue[] dva = new DataValue[dim1];
		for ( int i = 0; i < dim1; i++ )
			{
			DataValue[] dva2 = new DataValue[dim2];
			for ( int j = 0; j < dim2; j++ )
				dva2[ j ] = new DataValue( "" + a[ i ][ j ], DataValue.NUMBER );

			dva[ i ] = new DataValue( dva2, DataValue.ARRAY );
			}

		DataValue dv = new DataValue( dva, DataValue.ARRAY );

		parts.put( id, dv );
		}


	public void put( String id, short[][] a )
		{
		int dim1 = a.length;
		int dim2 = a[ 0 ].length;

		DataValue[] dva = new DataValue[dim1];
		for ( int i = 0; i < dim1; i++ )
			{
			DataValue[] dva2 = new DataValue[dim2];
			for ( int j = 0; j < dim2; j++ )
				dva2[ j ] = new DataValue( "" + a[ i ][ j ], DataValue.NUMBER );

			dva[ i ] = new DataValue( dva2, DataValue.ARRAY );
			}

		DataValue dv = new DataValue( dva, DataValue.ARRAY );

		parts.put( id, dv );
		}


	public void put( String id, int[][] a )
		{
		int dim1 = a.length;
		int dim2 = a[ 0 ].length;

		DataValue[] dva = new DataValue[dim1];
		for ( int i = 0; i < dim1; i++ )
			{
			DataValue[] dva2 = new DataValue[dim2];
			for ( int j = 0; j < dim2; j++ )
				dva2[ j ] = new DataValue( "" + a[ i ][ j ], DataValue.NUMBER );

			dva[ i ] = new DataValue( dva2, DataValue.ARRAY );
			}

		DataValue dv = new DataValue( dva, DataValue.ARRAY );

		parts.put( id, dv );
		}


	public void put( String id, long[][] a )
		{
		int dim1 = a.length;
		int dim2 = a[ 0 ].length;

		DataValue[] dva = new DataValue[dim1];
		for ( int i = 0; i < dim1; i++ )
			{
			DataValue[] dva2 = new DataValue[dim2];
			for ( int j = 0; j < dim2; j++ )
				dva2[ j ] = new DataValue( "" + a[ i ][ j ], DataValue.NUMBER );

			dva[ i ] = new DataValue( dva2, DataValue.ARRAY );
			}

		DataValue dv = new DataValue( dva, DataValue.ARRAY );

		parts.put( id, dv );
		}


	public void put( String id, char[][] a )
		{
		int dim1 = a.length;
		int dim2 = a[ 0 ].length;

		DataValue[] dva = new DataValue[dim1];
		for ( int i = 0; i < dim1; i++ )
			{
			DataValue[] dva2 = new DataValue[dim2];
			for ( int j = 0; j < dim2; j++ )
				dva2[ j ] = new DataValue( "" + a[ i ][ j ], DataValue.NUMBER );

			dva[ i ] = new DataValue( dva2, DataValue.ARRAY );
			}

		DataValue dv = new DataValue( dva, DataValue.ARRAY );

		parts.put( id, dv );
		}


	public void put( String id, float[][] a )
		{
		int dim1 = a.length;
		int dim2 = a[ 0 ].length;

		DataValue[] dva = new DataValue[dim1];
		for ( int i = 0; i < dim1; i++ )
			{
			DataValue[] dva2 = new DataValue[dim2];
			for ( int j = 0; j < dim2; j++ )
				dva2[ j ] = new DataValue( "" + a[ i ][ j ], DataValue.NUMBER );

			dva[ i ] = new DataValue( dva2, DataValue.ARRAY );
			}

		DataValue dv = new DataValue( dva, DataValue.ARRAY );

		parts.put( id, dv );
		}


	public void put( String id, double[][] a )
		{
		int dim1 = a.length;
		int dim2 = a[ 0 ].length;

		DataValue[] dva = new DataValue[dim1];
		for ( int i = 0; i < dim1; i++ )
			{
			DataValue[] dva2 = new DataValue[dim2];
			for ( int j = 0; j < dim2; j++ )
				dva2[ j ] = new DataValue( "" + a[ i ][ j ], DataValue.NUMBER );

			dva[ i ] = new DataValue( dva2, DataValue.ARRAY );
			}

		DataValue dv = new DataValue( dva, DataValue.ARRAY );

		parts.put( id, dv );
		}


	public void put( String id, BigInteger[][] a )
		{
		int dim1 = a.length;
		int dim2 = a[ 0 ].length;

		DataValue[] dva = new DataValue[dim1];
		for ( int i = 0; i < dim1; i++ )
			{
			DataValue[] dva2 = new DataValue[dim2];
			for ( int j = 0; j < dim2; j++ )
				dva2[ j ] = new DataValue( a[ i ][ j ].toString(), DataValue.NUMBER );

			dva[ i ] = new DataValue( dva2, DataValue.ARRAY );
			}

		DataValue dv = new DataValue( dva, DataValue.ARRAY );

		parts.put( id, dv );
		}


	// Boolean values
	public void put( String id, boolean b )
		{
		DataValue dv = null;
		if ( b )
			dv = new DataValue( "", DataValue.TRUE );
		else
			dv = new DataValue( "", DataValue.FALSE );

		parts.put( id, dv );
		}


	// Dates
	public void put( String id, Date d )
		{
		put( id, d.getTime() );
		}


	public void putNull( String id )
		{
		DataValue dv = new DataValue( "", DataValue.NULL );

		parts.put( id, dv );
		}


	public void putRawBytes( String id, byte[] b )
		{
		DataValue dv = new DataValue( b, DataValue.BYTES );

		parts.put( id, dv );
		}


	// //////////////////////////////////////////////////////////////////////////////////
	// Accessors to the data inside the object
	// We have so many of them because the calls themselves specify how the
	// programmer wants to interpret the data that is stored in
	// the object (especially for numbers)
	public DataObject getDataObject( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			{
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );
			}

		return (DataObject) dv.value;
		}


	public DataObject getOptionalDataObject( String id ) throws DataObjectException
		{
		try
			{
			DataObject o = getDataObject( id );

			return o;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	public String getString( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		if ( dv.type == DataValue.TRUE )
			return "true";

		if ( dv.type == DataValue.FALSE )
			return "false";

		return (String) dv.value;
		}


	public String getOptionalString( String id ) throws DataObjectException
		{
		try
			{
			String s = getString( id );

			return s;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	public String getNonNullString( String key ) throws Throwable
		{
		String s = getOptionalString( key );
		if ( s == null )
			return "";

		return s;
		}


	public byte getByte( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		return (byte) Integer.parseInt( (String) dv.value );
		}


	public byte getByte( String id, byte b ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			return b;

		return (byte) Integer.parseInt( (String) dv.value );
		}


	public short getShort( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		return (short) Integer.parseInt( (String) dv.value );
		}


	public short getShort( String id, short s ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			return s;

		return (short) Integer.parseInt( (String) dv.value );
		}


	public int getInt( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		return Integer.parseInt( (String) dv.value );
		}


	public int getInt( String id, int defaultValue )
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			return defaultValue;

		return Integer.parseInt( (String) dv.value );
		}


	public boolean getBoolean( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		return ( dv.type == DataValue.TRUE );
		}


	public boolean getOptionalBoolean( String id, boolean defaultBoolean ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			return defaultBoolean;

		return ( dv.type == DataValue.TRUE );
		}


	public int getOptionalInt( String id, int optionalValue ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			return optionalValue;

		return Integer.parseInt( (String) dv.value );
		}


	public long getLong( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		return Long.parseLong( (String) dv.value );
		}


	public long getOptionalLong( String id, long optionalValue ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			return optionalValue;

		return Long.parseLong( (String) dv.value );
		}


	public char getChar( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		return ( (String) dv.value ).charAt( 0 );
		}


	public char getChar( String id, char c ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			return c;

		return ( (String) dv.value ).charAt( 0 );
		}


	public float getFloat( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		return Float.valueOf( (String) dv.value ).floatValue();
		}


	public float getOptionalFloat( String id, float f ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			return f;

		return Float.valueOf( (String) dv.value ).floatValue();
		}


	public double getDouble( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		return Double.valueOf( (String) dv.value ).doubleValue();
		}


	public double getOptionalDouble( String id, double d ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			return d;

		return Double.valueOf( (String) dv.value ).doubleValue();
		}


	public BigInteger getBigInteger( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		return new BigInteger( (String) dv.value );
		}


	public Date getDate( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		return new Date( Long.parseLong( (String) dv.value ) );
		}


	public BigInteger getOptionalBigInteger( String id ) throws DataObjectException
		{
		try
			{
			BigInteger i = getBigInteger( id );

			return i;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	public byte[] getRawBytes( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		try
			{
			return (byte[]) dv.value;
			}
		catch ( Throwable theX )
			{
			throw new DataObjectException( "REMEMBER, THAT WHEN USING DataObject.getRawBytes( String ), YOU MUST USE THE CORREPONDING DataObject.putRawBytes( String, byte[] ) function.  OTHERWISE, YOU CAN USE DataObject.getByteArray( String ) and DataObject.put( String, byte[] ) which are less efficient." );
			}
		}


	public byte[] getOptionalRawBytes( String id ) throws DataObjectException
		{
		try
			{
			byte[] ba = getRawBytes( id );

			return ba;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	// Arrays
	public DataObject[] getDataObjectArray( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		DataValue[] a = (DataValue[]) dv.value;

		DataObject[] a2 = new DataObject[a.length];
		for ( int i = 0; i < a.length; i++ )
			a2[ i ] = (DataObject) a[ i ].value;

		return a2;
		}


	public DataObject[] getOptionalDataObjectArray( String id ) throws DataObjectException
		{
		try
			{
			DataObject[] oa = getDataObjectArray( id );

			return oa;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	public DataObject[][] get2DDataObjectArray( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		DataValue[] a = (DataValue[]) dv.value;

		int dim1 = a.length;
		int dim2 = ( (DataValue[]) ( a[ 0 ].value ) ).length;

		DataObject[][] a2 = new DataObject[dim1][dim2];
		for ( int i = 0; i < a.length; i++ )
			{
			DataValue[] a1 = (DataValue[]) ( a[ i ].value );

			for ( int j = 0; j < dim2; j++ )
				a2[ i ][ j ] = (DataObject) a1[ j ].value;
			}

		return a2;
		}


	public DataObject[][] getOptional2DDataObjectArray( String id ) throws DataObjectException
		{
		try
			{
			DataObject[][] oa = get2DDataObjectArray( id );

			return oa;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	public String[] getStringArray( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		DataValue[] a = (DataValue[]) dv.value;

		String[] a2 = new String[a.length];
		for ( int i = 0; i < a.length; i++ )
			a2[ i ] = (String) a[ i ].value;

		return a2;
		}


	public Object[] getOptionalObjectArray( String id ) throws Throwable
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			return null;

		String[] dvTypes = getStringArray( id + OTHERNET__TYPE_SUFFIX );

		DataValue[] a = (DataValue[]) dv.value;

		Object[] a2 = new Object[a.length];
		for ( int i = 0; i < a.length; i++ )
			{
			String s = (String) a[ i ].value;
			String type = dvTypes[ i ];

			boolean handled = false;

			if ( type.equals( "String" ) )
				{
				a2[ i ] = s;
				handled = true;
				}

			if ( !handled )
				if ( type.equals( "Short" ) )
					{
					a2[ i ] = new Short( s );
					handled = true;
					}

			if ( !handled )
				if ( type.equals( "Integer" ) )
					{
					a2[ i ] = new Integer( s );
					handled = true;
					}

			if ( !handled )
				if ( type.equals( "Long" ) )
					{
					a2[ i ] = new Long( s );
					handled = true;
					}

			if ( !handled )
				if ( type.equals( "Float" ) )
					{
					a2[ i ] = new Float( s );
					handled = true;
					}

			if ( !handled )
				if ( type.equals( "Double" ) )
					{
					a2[ i ] = new Double( s );
					handled = true;
					}

			if ( !handled )
				if ( type.equals( "Boolean" ) )
					{
					a2[ i ] = new Boolean( s );
					handled = true;
					}

			if ( !handled )
				if ( type.equals( "Date" ) )
					{
					a2[ i ] = new Date( s );
					handled = true;
					}

			if ( !handled )
				throw new Throwable( "UNKNOWN TYPE ENCOUNTERED: " + type );
			}

		return a2;
		}


	public Object[] getObjectArray( String id ) throws Throwable
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		String[] dvTypes = getStringArray( id + OTHERNET__TYPE_SUFFIX );

		DataValue[] a = (DataValue[]) dv.value;

		Object[] a2 = new Object[a.length];
		for ( int i = 0; i < a.length; i++ )
			{
			String s = (String) a[ i ].value;
			String type = dvTypes[ i ];

			boolean handled = false;

			if ( type.equals( "String" ) )
				{
				a2[ i ] = s;
				handled = true;
				}

			if ( !handled )
				if ( type.equals( "Short" ) )
					{
					a2[ i ] = new Short( s );
					handled = true;
					}

			if ( !handled )
				if ( type.equals( "Integer" ) )
					{
					a2[ i ] = new Integer( s );
					handled = true;
					}

			if ( !handled )
				if ( type.equals( "Long" ) )
					{
					a2[ i ] = new Long( s );
					handled = true;
					}

			if ( !handled )
				if ( type.equals( "Float" ) )
					{
					a2[ i ] = new Float( s );
					handled = true;
					}

			if ( !handled )
				if ( type.equals( "Double" ) )
					{
					a2[ i ] = new Double( s );
					handled = true;
					}

			if ( !handled )
				if ( type.equals( "Boolean" ) )
					{
					a2[ i ] = new Boolean( s );
					handled = true;
					}

			if ( !handled )
				if ( type.equals( "Date" ) )
					{
					a2[ i ] = new Date( s );
					handled = true;
					}

			if ( !handled )
				throw new Throwable( "UNKNOWN TYPE ENCOUNTERED: " + type );
			}

		return a2;
		}


	public String[] getOptionalStringArray( String id ) throws DataObjectException
		{
		try
			{
			String[] sa = getStringArray( id );

			return sa;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	public String[][] get2DStringArray( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		DataValue[] a = (DataValue[]) dv.value;

		int dim1 = a.length;
		int dim2 = ( (DataValue[]) ( a[ 0 ].value ) ).length;

		String[][] a2 = new String[dim1][dim2];
		for ( int i = 0; i < a.length; i++ )
			{
			DataValue[] a1 = (DataValue[]) ( a[ i ].value );

			for ( int j = 0; j < dim2; j++ )
				a2[ i ][ j ] = (String) a1[ j ].value;
			}

		return a2;
		}


	public String[][] getOptional2DStringArray( String id ) throws DataObjectException
		{
		try
			{
			String[][] sa = get2DStringArray( id );

			return sa;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	public byte[] getByteArray( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		DataValue[] a = (DataValue[]) dv.value;

		byte[] a2 = new byte[a.length];
		for ( int i = 0; i < a.length; i++ )
			a2[ i ] = (byte) Integer.parseInt( (String) a[ i ].value );

		return a2;
		}


	public byte[] getOptionalByteArray( String id ) throws DataObjectException
		{
		try
			{
			byte[] ba = getByteArray( id );

			return ba;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	public byte[][] get2DByteArray( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		DataValue[] a = (DataValue[]) dv.value;

		int dim1 = a.length;
		int dim2 = ( (DataValue[]) ( a[ 0 ].value ) ).length;

		byte[][] a2 = new byte[dim1][dim2];
		for ( int i = 0; i < a.length; i++ )
			{
			DataValue[] a1 = (DataValue[]) ( a[ i ].value );

			for ( int j = 0; j < dim2; j++ )
				a2[ i ][ j ] = (byte) Integer.parseInt( (String) a1[ j ].value );
			}

		return a2;
		}


	public byte[][] getOptional2DByteArray( String id ) throws DataObjectException
		{
		try
			{
			byte[][] ba = get2DByteArray( id );

			return ba;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	public short[] getShortArray( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		DataValue[] a = (DataValue[]) dv.value;

		short[] a2 = new short[a.length];
		for ( int i = 0; i < a.length; i++ )
			a2[ i ] = (short) Integer.parseInt( (String) a[ i ].value );

		return a2;
		}


	public short[] getOptionalShortArray( String id ) throws DataObjectException
		{
		try
			{
			short[] sa = getShortArray( id );

			return sa;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	public short[][] get2DShortArray( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		DataValue[] a = (DataValue[]) dv.value;

		int dim1 = a.length;
		int dim2 = ( (DataValue[]) ( a[ 0 ].value ) ).length;

		short[][] a2 = new short[dim1][dim2];
		for ( int i = 0; i < a.length; i++ )
			{
			DataValue[] a1 = (DataValue[]) ( a[ i ].value );

			for ( int j = 0; j < dim2; j++ )
				a2[ i ][ j ] = (short) Integer.parseInt( (String) a1[ j ].value );
			}

		return a2;
		}


	public short[][] getOptional2DShortArray( String id ) throws DataObjectException
		{
		try
			{
			short[][] sa = get2DShortArray( id );

			return sa;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	public int[] getIntArray( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		DataValue[] a = (DataValue[]) dv.value;

		int[] a2 = new int[a.length];
		for ( int i = 0; i < a.length; i++ )
			a2[ i ] = Integer.parseInt( (String) a[ i ].value );

		return a2;
		}


	public int[] getOptionalIntArray( String id ) throws DataObjectException
		{
		try
			{
			int[] ia = getIntArray( id );

			return ia;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	public int[][] get2DIntArray( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		DataValue[] a = (DataValue[]) dv.value;

		int dim1 = a.length;
		int dim2 = ( (DataValue[]) ( a[ 0 ].value ) ).length;

		int[][] a2 = new int[dim1][dim2];
		for ( int i = 0; i < a.length; i++ )
			{
			DataValue[] a1 = (DataValue[]) ( a[ i ].value );

			for ( int j = 0; j < dim2; j++ )
				a2[ i ][ j ] = Integer.parseInt( (String) a1[ j ].value );
			}

		return a2;
		}


	public int[][] getOptional2DIntArray( String id ) throws DataObjectException
		{
		try
			{
			int[][] ia = get2DIntArray( id );

			return ia;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	public long[] getLongArray( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		DataValue[] a = (DataValue[]) dv.value;

		long[] a2 = new long[a.length];
		for ( int i = 0; i < a.length; i++ )
			a2[ i ] = Long.parseLong( (String) a[ i ].value );

		return a2;
		}


	public long[] getOptionalLongArray( String id ) throws DataObjectException
		{
		try
			{
			long[] la = getLongArray( id );

			return la;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	public long[][] get2DLongArray( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		DataValue[] a = (DataValue[]) dv.value;

		int dim1 = a.length;
		int dim2 = ( (DataValue[]) ( a[ 0 ].value ) ).length;

		long[][] a2 = new long[dim1][dim2];
		for ( int i = 0; i < a.length; i++ )
			{
			DataValue[] a1 = (DataValue[]) ( a[ i ].value );

			for ( int j = 0; j < dim2; j++ )
				a2[ i ][ j ] = Long.parseLong( (String) a1[ j ].value );
			}

		return a2;
		}


	public long[][] getOptional2DLongArray( String id ) throws DataObjectException
		{
		try
			{
			long[][] la = get2DLongArray( id );

			return la;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	public char[] getCharArray( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		DataValue[] a = (DataValue[]) dv.value;

		char[] a2 = new char[a.length];
		for ( int i = 0; i < a.length; i++ )
			a2[ i ] = ( (String) a[ i ].value ).charAt( 0 );

		return a2;
		}


	public char[] getOptionalCharArray( String id ) throws DataObjectException
		{
		try
			{
			char[] ca = getCharArray( id );

			return ca;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	public char[][] get2DCharArray( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		DataValue[] a = (DataValue[]) dv.value;

		int dim1 = a.length;
		int dim2 = ( (DataValue[]) ( a[ 0 ].value ) ).length;

		char[][] a2 = new char[dim1][dim2];
		for ( int i = 0; i < a.length; i++ )
			{
			DataValue[] a1 = (DataValue[]) ( a[ i ].value );

			for ( int j = 0; j < dim2; j++ )
				a2[ i ][ j ] = ( (String) a1[ j ].value ).charAt( 0 );
			}

		return a2;
		}


	public char[][] getOptional2DCharArray( String id ) throws DataObjectException
		{
		try
			{
			char[][] ca = get2DCharArray( id );

			return ca;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	public float[] getFloatArray( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		DataValue[] a = (DataValue[]) dv.value;

		float[] a2 = new float[a.length];
		for ( int i = 0; i < a.length; i++ )
			a2[ i ] = Float.valueOf( (String) a[ i ].value ).floatValue();

		return a2;
		}


	public float[] getOptionalFloatArray( String id ) throws DataObjectException
		{
		try
			{
			float[] fa = getFloatArray( id );

			return fa;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	public float[][] get2DFloatArray( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		DataValue[] a = (DataValue[]) dv.value;

		int dim1 = a.length;
		int dim2 = ( (DataValue[]) ( a[ 0 ].value ) ).length;

		float[][] a2 = new float[dim1][dim2];
		for ( int i = 0; i < a.length; i++ )
			{
			DataValue[] a1 = (DataValue[]) ( a[ i ].value );

			for ( int j = 0; j < dim2; j++ )
				a2[ i ][ j ] = Float.valueOf( (String) a1[ j ].value ).floatValue();
			}

		return a2;
		}


	public float[][] getOptional2DFloatArray( String id ) throws DataObjectException
		{
		try
			{
			float[][] fa = get2DFloatArray( id );

			return fa;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	public double[] getDoubleArray( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		DataValue[] a = (DataValue[]) dv.value;

		double[] a2 = new double[a.length];
		for ( int i = 0; i < a.length; i++ )
			a2[ i ] = Double.valueOf( (String) a[ i ].value ).doubleValue();

		return a2;
		}


	public double[] getOptionalDoubleArray( String id ) throws DataObjectException
		{
		try
			{
			double[] da = getDoubleArray( id );

			return da;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	public double[][] get2DDoubleArray( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		DataValue[] a = (DataValue[]) dv.value;

		int dim1 = a.length;
		int dim2 = ( (DataValue[]) ( a[ 0 ].value ) ).length;

		double[][] a2 = new double[dim1][dim2];
		for ( int i = 0; i < a.length; i++ )
			{
			DataValue[] a1 = (DataValue[]) ( a[ i ].value );

			for ( int j = 0; j < dim2; j++ )
				a2[ i ][ j ] = Double.valueOf( (String) a1[ j ].value ).doubleValue();
			}

		return a2;
		}


	public double[][] getOptional2DDoubleArray( String id ) throws DataObjectException
		{
		try
			{
			double[][] da = get2DDoubleArray( id );

			return da;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	public BigInteger[] getBigIntegerArray( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		DataValue[] a = (DataValue[]) dv.value;

		BigInteger[] a2 = new BigInteger[a.length];
		for ( int i = 0; i < a.length; i++ )
			a2[ i ] = new BigInteger( (String) a[ i ].value );

		return a2;
		}


	public BigInteger[] getOptionalBigIntegerArray( String id ) throws DataObjectException
		{
		try
			{
			BigInteger[] bia = getBigIntegerArray( id );

			return bia;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	public BigInteger[][] get2DBigIntegerArray( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		if ( dv == null )
			throw new DataObjectException( "NO VALUE FOUND WITHIN DATA OBJECT: " + id );

		DataValue[] a = (DataValue[]) dv.value;

		int dim1 = a.length;
		int dim2 = ( (DataValue[]) ( a[ 0 ].value ) ).length;

		BigInteger[][] a2 = new BigInteger[dim1][dim2];
		for ( int i = 0; i < a.length; i++ )
			{
			DataValue[] a1 = (DataValue[]) ( a[ i ].value );

			for ( int j = 0; j < dim2; j++ )
				a2[ i ][ j ] = new BigInteger( (String) a1[ j ].value );
			}

		return a2;
		}


	public BigInteger[][] getOptional2DBigIntegerArray( String id ) throws DataObjectException
		{
		try
			{
			BigInteger[][] bia = get2DBigIntegerArray( id );

			return bia;
			}
		catch ( DataObjectException ignored )
			{
			}

		return null;
		}


	// Returns the representation of the DataObject as UTF8 bytes
	public byte[] getUTF8Bytes()
		{
		try
			{
			ByteArrayOutputStream out0 = new ByteArrayOutputStream();

			OutStream out = new OutStream( out0 );

			outputBytes = false;
			writeToStream( out );
			outputBytes = true;

			out.flush();
			out.close();

			return out0.toByteArray();
			}
		catch ( Throwable theX )
			{
			outputBytes = true;

			Util.printX( theX );
			}

		return null;
		}


	public String toString()
		{
		try
			{
			ByteArrayOutputStream out0 = new ByteArrayOutputStream();

			OutStream out = new OutStream( out0 );

			outputBytes = false;
			writeToStream( out );
			outputBytes = true;

			out.flush();
			out.close();

			byte[] bytes = out0.toByteArray();

			return new String( bytes, "UTF-8" );
			}
		catch ( Throwable theX )
			{
			outputBytes = true;

			Util.printX( theX );
			}

		return null;
		}


	public boolean containsRawBytes()
		{
		FashEntry_so[] entries = parts.returnArrayOfEntries();
		for ( int i = 0; i < entries.length; i++ )
			{
			DataValue dv = (DataValue) entries[ i ].value;

			if ( dv.containsRawBytes() )
				return true;
			}

		return false;
		}


	public Fashtable_so returnRawBytes()
		{
		Fashtable_so f = new Fashtable_so();

		addRawBytes( "", f );

		return f;
		}


	public void addRawBytes( String path, Fashtable_so f )
		{
		FashEntry_so[] entries = parts.returnArrayOfEntries();
		for ( int i = 0; i < entries.length; i++ )
			{
			DataValue dv = (DataValue) entries[ i ].value;

			if ( dv.type == DataValue.BYTES )
				f.put( path + entries[ i ].key, dv.value );

			if ( dv.type == DataValue.DATA_OBJECT )
				{
				DataObject o = (DataObject) dv.value;

				o.addRawBytes( entries[ i ].key + ".", f );
				}
			}
		}


	// Return URL style args using only the first level of the DataObject
	public String returnURLStyleArgs()
		{
		StringBuffer sb = new StringBuffer();

		FashEntry_so[] entries = parts.returnArrayOfEntries();
		for ( int i = 0; i < entries.length; i++ )
			{
			FashEntry_so fe = entries[ i ];

			if ( i != 0 )
				sb.append( "&" );

			sb.append( fe.key + "=" + fe.value );
			}

		return sb.toString();
		}


	// Return URL style args using only the first level of the DataObject
	public String createCookieString()
		{
		StringBuffer sb = new StringBuffer();
		sb.append( "Cookie: " );

		FashEntry_so[] entries = parts.returnArrayOfEntries();
		for ( int i = 0; i < entries.length; i++ )
			{
			FashEntry_so fe = entries[ i ];

			if ( i > 0 )
				sb.append( "; " );

			sb.append( fe.key + "=" + fe.value );
			}

		sb.append( "\r\n" );

		return sb.toString();
		}


	// Return URL style args using only the first level of the DataObject
	public String returnURLStyleArgsEncoded()
		{
		StringBuffer sb = new StringBuffer();

		FashEntry_so[] entries = parts.returnArrayOfEntries();
		for ( int i = 0; i < entries.length; i++ )
			{
			FashEntry_so fe = entries[ i ];

			if ( i != 0 )
				sb.append( "&" );

			sb.append( fe.key + "=" + URLEncoder.encode( (String) ( (DataValue) fe.value ).value ) );
			}

		return sb.toString();
		}


	public Fashtable_so returnParts()
		{
		return parts;
		}


	public boolean exists( String id ) throws DataObjectException
		{
		DataValue dv = (DataValue) parts.get( id );

		return ( dv != null );
		}


	public static void main( String[] args )
		{
		try
			{

			DataObject o = new DataObject();

			System.out.println( o.toString() );
			if ( 1 == 1 )
				return;

			File f = new File( "t" );

			InStream in = new InStream( new FileInputStream( f ) );

			DataObject d = new DataObject( in );

			d.put( "greeting", "hello" );

			String[][] foo = new String[2][2];
			foo[ 0 ][ 0 ] = "c";
			foo[ 0 ][ 1 ] = "a";
			foo[ 1 ][ 0 ] = "t";
			foo[ 1 ][ 1 ] = "s";

			d.put( "cats", foo );

			System.out.println( d.toString() );

			System.out.println( "" );
			System.out.println( "" );

			DataObject p = d.getDataObject( "person" );
			float[][] nao = p.get2DFloatArray( "nao" );

			System.out.println( nao[ 0 ][ 0 ] );
			System.out.println( nao[ 0 ][ 1 ] );
			System.out.println( nao[ 1 ][ 0 ] );
			System.out.println( nao[ 1 ][ 1 ] );
			}
		catch ( Throwable theX )
			{
			Util.printX( theX );
			}
		}
	}
