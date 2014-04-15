package cota.ganesha;

import cota.io.Message;
import cota.util.HashEntry_io;
import cota.util.Hashtable_io;
import cota.util.Hashtable_so;
import cota.util.Queue;


// NOTE - This class provides strict attribute typing and requires extra work to be used
// For most cases it is recommended to use the Gob

// Gob: Ganesha-Object
// A Gob is a structured data object that can contain attributes of the types seen below

public class StrictGob
	{
	// Used to specify the types of the attributes
	public static final int STRING = ( 1 << 16 );
	public static final int INT = ( 2 << 16 );
	public static final int LONG = ( 3 << 16 );
	public static final int BYTES = ( 4 << 16 );
	public static final int LIST = ( 5 << 16 );
	public static final int ORDERED_LIST = ( 6 << 16 );
	public static final int STRING_LIST = ( 7 << 16 );
	public static final int MEMORY_BYTES = ( 8 << 16 );
	public static final int MEMORY_INT = ( 9 << 16 );

	public long id = -1;

	// Used to verify that called attributes are from the type of Gob that they should be
	public int gobType = -1;

	// Used by classes that extend  Gob and provide attributes as specially constructed ints
	// This is more work, but provides an extra level of error checking
	// GobType also must be provided
	public Hashtable_io f = new Hashtable_io();


	// For creating new objects of a specific GobType
	public StrictGob( int gobType )
		{
		super();

		this.gobType = gobType;
		}


	// Can throw a NotFoundException if something goes wrong
	public StrictGob( int gobType, byte[] bytes ) throws Throwable
		{
		this.gobType = gobType;

		init( bytes );
		}


	// Can throw a NotFoundException if something goes wrong
	public StrictGob( int gobType, long id ) throws Throwable
		{
		this.gobType = gobType;

		byte[] bytes = Ganesha.getObjectBytes( id );

		init( bytes );

		this.id = id;
		}


	// Can throw a NotFoundException if something goes wrong
	public StrictGob( int gobType, String workspace, String table, String name ) throws Throwable
		{
		this.gobType = gobType;

		// Could just call Ganesha.get here but we need to know what the id is
		long id = Translator.translate( workspace, table, name, false );
		byte[] bytes = Ganesha.getObjectBytes( id );

		init( bytes );

		this.id = id;
		}


	public void init( byte[] bytes ) throws Throwable
		{
		//		System.out.println( "init bytes.length: " + bytes.length );

		f = new Hashtable_io();

		Message m = new Message( bytes );

		int numAttributes = m.read2Bytes();
		for ( int i = 0; i < numAttributes; i++ )
			{
			try
				{
				int attribute = m.read4Bytes();

				int attributeType = attribute & 0x00FF0000;
				switch ( attributeType )
					{
					case STRING:
						{
						String s = m.readString();
						f.put( attribute, s );
						}
					break;

					case INT:
						{
						int n = m.read4Bytes();
						f.put( attribute, n );
						}
					break;

					case LONG:
					case BYTES:
					case LIST:
					case STRING_LIST:
					case ORDERED_LIST:
					case MEMORY_BYTES:
					case MEMORY_INT:
						{
						long n = m.read8Bytes();
						f.put( attribute, n );
						}
					break;
					}
				}
			catch ( Throwable theX )
				{
				cota.util.Util.printX( theX );
				}
			}
		}


	public byte[] returnBytes() throws Throwable
		{
		HashEntry_io[] entries = f.returnArrayOfEntries();

		Message m = new Message();
		m.write2Bytes( entries.length );

		for ( int i = 0; i < entries.length; i++ )
			{
			int attribute = 0;
			try
				{
				attribute = entries[ i ].key;
				Object o = entries[ i ].value;

				m.write4Bytes( attribute );

				int attributeType = attribute & 0x00FF0000;
				switch ( attributeType )
					{
					case STRING:
						m.writeString( (String) o );
					break;

					case INT:
						m.write4Bytes( (Integer) o );
					break;

					case LONG:
					case BYTES:
					case LIST:
					case STRING_LIST:
					case ORDERED_LIST:
					case MEMORY_BYTES:
					case MEMORY_INT:
						m.write8Bytes( (Long) o );
					break;
					}
				}
			catch ( Throwable theX )
				{
				System.out.println( "ERROR SERIALIZING GobObject: " + ( attribute & 0xFFFF ) );
				System.out.println( "Make sure you're using Ganesha.add(bytes) in GobObject constructor!" );

				throw theX;
				}
			}

		byte[] bytes = m.returnBytes();

		return bytes;
		}


	// ================================================
	void ____GET_API________()
		{
		}


	public int getInt( int attribute ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != INT )
			throw new Throwable( "Calling getInt with non int attribute" );

		Integer i = (Integer) f.get( attribute );
		if ( i == null )
			return 0;

		return i;
		}


	public long getLong( int attribute ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != LONG )
			if ( attributeType != BYTES )
				throw new Throwable( "Calling getLong with non long attribute" );

		Long l = (Long) f.get( attribute );
		if ( l == null )
			return 0;

		return l;
		}


	public String getString( int attribute ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != STRING )
			throw new Throwable( "Calling getString with non string attribute" );

		return (String) f.get( attribute );
		}


	public byte[] getBytes( int attribute ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != BYTES )
			throw new Throwable( "Calling getBytes with non bytes attribute" );

		Long id = (Long) f.get( attribute );
		if ( id == null )
			return null;

		return Ganesha.getBytes( id );
		}


	public Queue getIDs( int attribute ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != LIST )
			throw new Throwable( "Calling getList with non list attribute" );

		Long id = (Long) f.get( attribute );
		if ( id == null )
			return null;

		byte[] bytes = Ganesha.getBytes( id );
		if ( bytes == null )
			return null;

		return Ganesha.returnIDs( bytes );
		}


	public Queue getStrings( int attribute ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != STRING_LIST )
			throw new Throwable( "Calling getStrings with non list attribute" );

		Long id = (Long) f.get( attribute );
		if ( id == null )
			return null;

		byte[] bytes = Ganesha.getBytes( id );
		if ( bytes == null )
			return null;

		return Ganesha.returnStrings( bytes );
		}


	public Queue getOrderedIDs( int attribute ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != ORDERED_LIST )
			throw new Throwable( "Calling getList with non ordered list attribute" );

		Long id = (Long) f.get( attribute );
		if ( id == null )
			return null;

		byte[] bytes = Ganesha.getBytes( id );
		if ( bytes == null )
			return null;

		return Ganesha.returnOrderedIDs( bytes );
		}


	public Queue getDeletedIDs( int attribute ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != LIST )
			throw new Throwable( "Calling getDeletedIDs with non list attribute" );

		Long id = (Long) f.get( attribute );
		if ( id == null )
			return null;

		byte[] bytes = Ganesha.getBytes( id );
		if ( bytes == null )
			return null;

		return Ganesha.returnDeletedIDs( bytes );
		}


	// Note that after modification, the object is no longer valid for reading
	// This is because modifications are done on the server itself and we don't know
	// exactly how they are going to be done
	// ================================================
	void ____PUT_API________()
		{
		}


	public void putInt( int attribute, int v ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != INT )
			throw new Throwable( "Calling putInt with non int attribute" );

		byte[] newBytes = Ganesha.putInt( id, attribute, v );

		init( newBytes );
		}


	public void putList( int attribute, long listID ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != LIST )
			throw new Throwable( "Calling putList with non list attribute" );

		byte[] newBytes = Ganesha.putLong( id, attribute, listID );

		init( newBytes );
		}


	public void putLong( int attribute, long v ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != LONG )
			if ( attributeType != MEMORY_BYTES )
				if ( attributeType != MEMORY_INT )
					throw new Throwable( "Calling putLong with non long attribute" );

		byte[] newBytes = Ganesha.putLong( id, attribute, v );

		//		System.out.println( "ORIGINAL BYTES.LENGTH: " + bytes.length );
		//	System.out.println( "NEW BYTES.LENGTH: " + newBytes.length );

		init( newBytes );
		}


	public void putString( int attribute, String s ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != STRING )
			throw new Throwable( "Calling putString with non string attribute" );

		byte[] newBytes = Ganesha.putString( id, attribute, s );

		init( newBytes );
		}


	public void putBytes( int attribute, byte[] bytes ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != BYTES )
			throw new Throwable( "Calling putBytes with non bytes attribute" );

		long id = getLong( attribute );

		Ganesha.putBytes( id, bytes );
		}


	public int increment( int attribute ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != INT )
			throw new Throwable( "Calling increment with non int attribute" );

		byte[] newBytes = Ganesha.increment( id, attribute );

		//		System.out.println( "newBytes.length: " + newBytes.length );
		init( newBytes );

		return (Integer) f.get( attribute );
		}


	public int decrement( int attribute ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != INT )
			throw new Throwable( "Calling decrement with non int attribute" );

		byte[] newBytes = Ganesha.decrement( id, attribute );

		init( newBytes );

		return (Integer) f.get( attribute );
		}


	// ================================================
	void ____MEMORY_API________()
		{
		}


	public void appendMemoryBytes( int attribute, byte[] bytes, int maxCount ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != MEMORY_BYTES )
			throw new Throwable( "Calling appendMemoryBytes with non memory bytes attribute" );

		Long id0 = (Long) f.get( attribute );
		if ( id0 == null )
			{
			id0 = Ganesha.newID();
			System.out.println( "Creating new memory bytes: " + id0 );

			putLong( attribute, id0 );
			}

		Ganesha.appendMemoryBytes( id0, bytes, maxCount );
		}


	public Queue getMemoryBytes( int attribute ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != MEMORY_BYTES )
			throw new Throwable( "Calling getMemoryBytes with non memory bytes attribute" );

		Long id0 = (Long) f.get( attribute );
		if ( id0 == null )
			{
			id0 = Ganesha.newID();
			System.out.println( "Creating new memory bytes: " + id0 );

			putLong( attribute, id0 );
			}

		return Ganesha.getMemoryBytes( id0 );
		}


	public int incrementMemoryInt( int attribute ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != MEMORY_INT )
			throw new Throwable( "Calling incrementMemoryInt with non memory int attribute" );

		Long id0 = (Long) f.get( attribute );
		if ( id0 == null )
			{
			id0 = Ganesha.newID();
			System.out.println( "Creating new memory int: " + id0 );

			putLong( attribute, id0 );
			}

		return Ganesha.incrementMemoryInt( id0 );
		}


	public int getMemoryInt( int attribute ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != MEMORY_INT )
			throw new Throwable( "Calling getMemoryInt with non memory int attribute" );

		Long id0 = (Long) f.get( attribute );
		if ( id0 == null )
			return 0;

		return Ganesha.getMemoryInt( id0 );
		}


	public void putMemoryInt( int attribute, int value ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != MEMORY_INT )
			throw new Throwable( "Calling putMemoryInt with non memory int attribute" );

		Long id0 = (Long) f.get( attribute );
		if ( id0 == null )
			{
			id0 = Ganesha.newID();
			System.out.println( "Creating new memory int: " + id0 );

			putLong( attribute, id0 );
			}

		if ( id0 != null )
			Ganesha.putMemoryInt( id0, value );
		}


	// ================================================
	void ____LIST_API________()
		{
		}


	public int listSize( int attribute ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != LIST )
			throw new Throwable( "Calling listSize with non list attribute" );

		Long id = (Long) f.get( attribute );
		if ( id == null )
			return 0;

		return Ganesha.listSize( id );
		}


	public boolean exists( int attribute, long id ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != LIST )
			throw new Throwable( "Calling exists with non list attribute" );

		Queue ids = getIDs( attribute );
		for ( int i = 0; i < ids.size(); i++ )
			{
			Long id0 = (Long) ids.elementAt( i );

			if ( id0 == id )
				return true;
			}

		return false;
		}


	public void insertID( int attribute, long newObjectID, int metric, int max ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != ORDERED_LIST )
			throw new Throwable( "Calling appendID with non ordered list attribute" );

		long id = (Long) f.get( attribute );

		Ganesha.insertID( id, newObjectID, metric, max );
		}


	public void appendID( int attribute, long newObjectID ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != LIST )
			throw new Throwable( "Calling appendID with non list attribute" );

		//		System.out.println( "f: " + f );
		//	System.out.println( "id: " + f.get( attribute ) );
		long id = (Long) f.get( attribute );

		Ganesha.appendID( id, newObjectID );

		//		System.out.println( "f: " + f );
		//	System.out.println( "id: " + f.get( attribute ) );
		}


	public void prependID( int attribute, long newObjectID ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != LIST )
			throw new Throwable( "Calling appendID with non list attribute" );

		//		System.out.println( "f: " + f );
		//	System.out.println( "id: " + f.get( attribute ) );
		long id = (Long) f.get( attribute );

		Ganesha.prependID( id, newObjectID );

		//		System.out.println( "f: " + f );
		//	System.out.println( "id: " + f.get( attribute ) );
		}


	public void appendIDs( int attribute, Queue ids ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != LIST )
			throw new Throwable( "Calling appendID with non list attribute" );

		//		System.out.println( "f: " + f );
		//	System.out.println( "id: " + f.get( attribute ) );
		long id = (Long) f.get( attribute );

		Ganesha.appendIDs( id, ids );

		//		System.out.println( "f: " + f );
		//	System.out.println( "id: " + f.get( attribute ) );
		}


	public void appendIDWithMax( int attribute, long newObjectID, int max ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != LIST )
			throw new Throwable( "Calling appendIDWithMax with non list attribute" );

		long id = (Long) f.get( attribute );
		//		System.out.println( "GobObject.appendIDWithMax: " + attribute + "\t" + id );

		Ganesha.appendIDWithMax( id, newObjectID, max );
		}


	public void removeID( int attribute, long newObjectID ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != LIST )
			throw new Throwable( "Calling removeID with non list attribute" );

		long id = (Long) f.get( attribute );

		Ganesha.removeID( id, newObjectID );
		}


	public void undeleteID( int attribute, long newObjectID ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != LIST )
			throw new Throwable( "Calling removeID with non list attribute" );

		long id = (Long) f.get( attribute );

		Ganesha.undeleteID( id, newObjectID );
		}


	public void removeOrderedID( int attribute, long idToRemove ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != ORDERED_LIST )
			throw new Throwable( "Calling removeID with non list attribute" );

		long id = (Long) f.get( attribute );

		Ganesha.removeOrderedID( id, idToRemove );
		}


	public static void printIDs( StrictGob gob, int attribute ) throws Throwable
		{
		Queue ids = gob.getIDs( attribute );
		//System.out.println( "# ids : " + ids.size() );

		System.out.println( "" );
		for ( int i = 0; i < ids.size(); i++ )
			{
			System.out.println( ids.elementAt( i ) );
			}
		}


	// Can't delete from a string list
	// but can have a maximum number of string sthat appear in the list (older discarded)
	public void appendStringWithMax( int attribute, String s, int max ) throws Throwable
		{
		int type = attribute & 0xFF000000;
		if ( type != gobType )
			throw new Throwable( "Using an attribute from the wrong Gob type" );

		int attributeType = attribute & 0x00FF0000;
		if ( attributeType != STRING_LIST )
			throw new Throwable( "Calling appendString with non string list attribute" );

		//		System.out.println( "f: " + f );
		//	System.out.println( "id: " + f.get( attribute ) );
		long id = (Long) f.get( attribute );

		Ganesha.appendStringWithMax( id, s, max );

		//		System.out.println( "f: " + f );
		//	System.out.println( "id: " + f.get( attribute ) );
		}
	}
