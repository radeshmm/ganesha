package cota.io;

import cota.util.Hashtable_so;
import cota.util.PairSO;
import cota.util.Queue;


public class DataValue
	{
	// Note that numbers are stored internally as Strings and are only converted to something else when accessed
	public static final int STRING = 0;
	public static final int NUMBER = 1;
	public static final int DATA_OBJECT = 2;
	public static final int ARRAY = 3;
	public static final int TRUE = 4;
	public static final int FALSE = 5;
	public static final int NULL = 6;
	public static final int BYTES = 7;

	int type = 0;

	public Object value = null;


	public DataValue( Object value, int type )
		{
		this.type = type;
		this.value = value;
		}


	public static DataValue readValue( byte c, InStream in ) throws Throwable
		{
		if ( c == '\"' )
			{
			String value = in.readJSONStringUTF8();

			return new DataValue( value, STRING );
			}

		if ( ( c == '-' ) || ( ( c >= '0' ) && ( c <= '9' ) ) )
			{
			String value = in.readJSONNumber();

			return new DataValue( value, NUMBER );
			}

		if ( c == 'b' )
			{
			JSON.parseBytes( in );

			// The actual bytes for the the DataValue will be provided after the usual JSON
			return new DataValue( null, BYTES );
			}

		if ( c == 't' )
			{
			JSON.parseTrue( in );

			return new DataValue( null, TRUE );
			}

		if ( c == 'f' )
			{
			JSON.parseFalse( in );

			return new DataValue( null, FALSE );
			}

		if ( c == 'n' )
			{
			JSON.parseNull( in );

			return new DataValue( null, NULL );
			}

		if ( c == '{' )
			{
			DataObject newObject = JSON.parseObject( in );

			return new DataValue( newObject, DATA_OBJECT );
			}

		if ( c == '[' )
			{
			DataValue[] newArray = JSON.parseArray( in );

			return new DataValue( newArray, ARRAY );
			}

		throw new Throwable( "ERROR PARSING THE STREAM" );
		}


	public void writeToStream( String path, String identifier, int indent, OutStream out, boolean array, Queue bytesToWrite ) throws Throwable
		{
		switch ( type )
			{
			case STRING:
				if ( array )
					DataObject.writeIndent( indent, out );
				else
					out.writeByte( ' ' );

				out.writeByte( '\"' );
				out.writeJSONStringUTF8( (String) value );
				out.writeByte( '\"' );
			break;

			case NUMBER:
				if ( array )
					DataObject.writeIndent( indent, out );
				else
					out.writeByte( ' ' );

				out.writeJSONStringUTF8( (String) value );
			break;

			case BYTES:
				if ( array )
					DataObject.writeIndent( indent, out );
				else
					out.writeByte( ' ' );

				out.writePlainString( "bytes" );

				byte[] b = (byte[]) value;
				bytesToWrite.addObject( new PairSO( path + identifier, b ) );
			break;

			case TRUE:
				if ( array )
					DataObject.writeIndent( indent, out );
				else
					out.writeByte( ' ' );

				out.writePlainString( "true" );
			break;

			case FALSE:
				if ( array )
					DataObject.writeIndent( indent, out );
				else
					out.writeByte( ' ' );

				out.writePlainString( "false" );
			break;

			case NULL:
				if ( array )
					DataObject.writeIndent( indent, out );
				else
					out.writeByte( ' ' );

				out.writePlainString( "null" );
			break;

			case DATA_OBJECT:
				if ( !array )
					out.writeByte( '\n' );

				DataObject o = (DataObject) value;

				path = path + identifier + ".";

				o.writeToStream( path, indent, out, bytesToWrite );
			break;

			case ARRAY:
				if ( !array )
					out.writeByte( '\n' );
				else
					indent++;

				DataObject.writeIndent( indent, out );
				out.writeByte( '[' );
				out.writeByte( '\n' );

				Object[] a = (Object[]) value;
				for ( int i = 0; i < a.length; i++ )
					{
					DataValue dv = (DataValue) a[ i ];

					dv.writeToStream( path, identifier + "_" + i, indent, out, true, bytesToWrite );

					if ( i != ( a.length - 1 ) )
						{
						out.writeByte( ',' );
						out.writeByte( '\n' );
						}
					else
						out.writeByte( '\n' );
					}

				DataObject.writeIndent( indent, out );
				out.writeByte( ']' );
			break;
			}
		}


	// Check to see if the DataValue needs to have some bytes added, or if any of its children do (for sub DataObjects)
	// Arrays can't currently have byte[] children directly, but must pack them into DataObjects
	public void fillInByteStubs( String identifier, Hashtable_so bytesF ) throws Throwable
		{
		switch ( type )
			{
			case BYTES:
				// A byte[] was specified within the JSON object
				// Make sure the actual bytes where specified
				byte[] bytes = (byte[]) bytesF.get( identifier );
				if ( bytes == null )
					throw new Throwable( "BYTE[] NOT PROVIDED FOR: " + identifier );

				value = bytes;
			break;

			case ARRAY:
				Object[] a = (Object[]) value;
				for ( int i = 0; i < a.length; i++ )
					{
					DataValue dv = (DataValue) a[ i ];

					if ( dv.type == DATA_OBJECT )
						dv.fillInByteStubs( identifier + "_" + i + ".", bytesF );
					else
						dv.fillInByteStubs( identifier + "_" + i, bytesF );
					}
			break;

			case DATA_OBJECT:
				// Looks like we're sitting inside of a DataObject whose parent called this method
				// Check to see if we need to fill in any bytes stubs within it
				DataObject o = (DataObject) value;

				o.fillInByteStubs( identifier, bytesF );
			break;
			}
		}


	public boolean isString()
		{
		return type == STRING;
		}


	public boolean containsRawBytes()
		{
		switch ( type )
			{
			case BYTES:
				return true;

			case ARRAY:
				Object[] a = (Object[]) value;
				for ( int i = 0; i < a.length; i++ )
					{
					DataValue dv = (DataValue) a[ i ];

					return dv.containsRawBytes();
					}
			break;

			case DATA_OBJECT:
				// Looks like we're sitting inside of a DataObject whose parent called this method
				// Check to see if we need to fill in any bytes stubs within it
				DataObject o = (DataObject) value;

				return o.containsRawBytes();
			}

		return false;
		}
	}