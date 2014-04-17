package cota.ganesha;

import cota.io.Message;
import cota.util.HashEntry_so;
import cota.util.Hashtable_so;
import cota.util.Queue;


// Gob: Ganesha-Object
// A Gob is a structured data object that can contain attributes references by a name
// It is essentially a Hashtable with String keys and values of different types

public class Gob
	{
	public long id = -1;

	// This Hashtable contains the attribute values for the object
	// All values are represented by bytes
	public Hashtable_so f = new Hashtable_so();


	// Construct a new object
	public Gob() throws Throwable
		{
		id = Ganesha.addObject( this );
		}


	// Return a named object
	// Or construct a new one if it doesn't exist yet
	public Gob( String workspace, String table, String name ) throws Throwable
		{
		// Call on the translator to determine what the id of the object is
		id = Translator.translate( workspace, table, name, false );

		if ( id != 0 )
			{
			// Object already exists
			byte[] bytes = Ganesha.getObjectBytes( id );

			init( bytes );
			}
		else
			{
			// Create a new object
			id = Ganesha.addObject( workspace, table, name, this );

			put( "name", name );
			}
		}


	// Can throw a NotFoundException if something goes wrong
	public Gob( byte[] bytes ) throws Throwable
		{
		init( bytes );
		}


	// Can throw a NotFoundException if something goes wrong
	public Gob( long id ) throws Throwable
		{
		byte[] bytes = Ganesha.getObjectBytes( id );

		init( bytes );

		this.id = id;
		}


	public void init( byte[] bytes0 ) throws Throwable
		{
		//		System.out.println( "init bytes.length: " + bytes.length );

		f = new Hashtable_so();

		Message m = new Message( bytes0 );

		int numAttributes = m.read2Bytes();
		for ( int i = 0; i < numAttributes; i++ )
			{
			String attribute = m.readString();
			byte[] bytes = m.readBytes();

			f.put( attribute, bytes );
			}
		}


	public byte[] returnBytes() throws Throwable
		{
		HashEntry_so[] entries = f.returnArrayOfEntries();

		Message m = new Message();
		m.write2Bytes( entries.length );

		for ( int i = 0; i < entries.length; i++ )
			{
			String attribute = entries[ i ].key;
			byte[] bytes = (byte[]) entries[ i ].value;

			m.writeString( attribute );
			m.writeBytes( bytes );
			}

		return m.returnBytes();
		}


	// ================================================
	void ____GET_API________()
		{
		}


	public int getInt( String attribute ) throws Throwable
		{
		byte[] bytes = (byte[]) f.get( attribute );
		if ( bytes == null )
			return 0;

		int x1 = bytes[ 0 ];
		if ( x1 < 0 )
			x1 = x1 + 256;

		int x2 = bytes[ 1 ];
		if ( x2 < 0 )
			x2 = x2 + 256;

		int x3 = bytes[ 2 ];
		if ( x3 < 0 )
			x3 = x3 + 256;

		int x4 = bytes[ 3 ];
		if ( x4 < 0 )
			x4 = x4 + 256;

		return ( ( x1 << 24 ) + ( x2 << 16 ) + ( x3 << 8 ) + x4 );
		}


	public long getLong( String attribute ) throws Throwable
		{
		byte[] bytes = (byte[]) f.get( attribute );
		if ( bytes == null )
			return 0;

		long x1 = bytes[ 0 ];
		if ( x1 < 0 )
			x1 = x1 + 256;

		long x2 = bytes[ 1 ];
		if ( x2 < 0 )
			x2 = x2 + 256;

		long x3 = bytes[ 2 ];
		if ( x3 < 0 )
			x3 = x3 + 256;

		long x4 = bytes[ 3 ];
		if ( x4 < 0 )
			x4 = x4 + 256;

		long x5 = bytes[ 4 ];
		if ( x5 < 0 )
			x5 = x5 + 256;

		long x6 = bytes[ 5 ];
		if ( x6 < 0 )
			x6 = x6 + 256;

		long x7 = bytes[ 6 ];
		if ( x7 < 0 )
			x7 = x7 + 256;

		long x8 = bytes[ 7 ];
		if ( x8 < 0 )
			x8 = x8 + 256;

		return ( ( x1 << 56 ) + ( x2 << 48 ) + ( x3 << 40 ) + ( x4 << 32 ) + ( x5 << 24 ) + ( x6 << 16 ) + ( x7 << 8 ) + x8 );
		}


	public String getString( String attribute ) throws Throwable
		{
		byte[] bytes = (byte[]) f.get( attribute );

		return new String( bytes, "UTF-8" );
		}


	public byte[] getBytes( String attribute ) throws Throwable
		{
		long id = getLong( attribute );
		if ( id == 0 )
			return null;

		return Ganesha.getBytes( id );
		}


	public Gob getGob( String attribute ) throws Throwable
		{
		long id = getLong( attribute );
		if ( id == 0 )
			return null;

		return new Gob( id );
		}


	public Queue getGobs( String attribute ) throws Throwable
		{
		long id = getLong( attribute );
		if ( id == 0 )
			return new Queue();

		byte[] bytes = Ganesha.getBytes( id );
		if ( bytes == null )
			return null;

		Queue ids = Ganesha.returnIDs( bytes );

		Queue gobs = new Queue();
		for ( int i = 0; i < ids.size(); i++ )
			{
			long gobID = (Long) ids.elementAt( i );

			gobs.addObject( new Gob( gobID ) );
			}

		return gobs;
		}


	public Queue getIDs( String attribute ) throws Throwable
		{
		long id = getLong( attribute );
		if ( id == 0 )
			return new Queue();

		byte[] bytes = Ganesha.getBytes( id );
		if ( bytes == null )
			return null;

		return Ganesha.returnIDs( bytes );
		}


	public Queue getStrings( String attribute ) throws Throwable
		{
		long id = getLong( attribute );
		if ( id == 0 )
			return new Queue();

		byte[] bytes = Ganesha.getBytes( id );
		if ( bytes == null )
			return null;

		return Ganesha.returnStrings( bytes );
		}


	public Queue getOrderedIDs( String attribute ) throws Throwable
		{
		long id = getLong( attribute );
		if ( id == 0 )
			return new Queue();

		byte[] bytes = Ganesha.getBytes( id );
		if ( bytes == null )
			return null;

		return Ganesha.returnOrderedIDs( bytes );
		}


	public Queue getDeletedIDs( String attribute ) throws Throwable
		{
		long id = getLong( attribute );
		if ( id == 0 )
			return new Queue();

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


	public void put( String attribute, int v ) throws Throwable
		{
		byte[] newBytes = Ganesha.putInt( id, attribute, v );

		init( newBytes );
		}


	public void put( String attribute, long v ) throws Throwable
		{
		byte[] newBytes = Ganesha.putLong( id, attribute, v );

		init( newBytes );
		}


	public void put( String attribute, Gob gob ) throws Throwable
		{
		byte[] newBytes = Ganesha.putLong( id, attribute, gob.id );

		init( newBytes );
		}


	public void put( String attribute, String s ) throws Throwable
		{
		byte[] newBytes = Ganesha.putString( id, attribute, s );

		init( newBytes );
		}


	public void put( String attribute, byte[] bytes ) throws Throwable
		{
		long id = getLong( attribute );

		Ganesha.putBytes( id, bytes );
		}


	public int increment( String attribute ) throws Throwable
		{
		byte[] newBytes = Ganesha.increment( id, attribute );

		init( newBytes );

		return getInt( attribute );
		}


	public int decrement( String attribute ) throws Throwable
		{
		byte[] newBytes = Ganesha.decrement( id, attribute );

		init( newBytes );

		return getInt( attribute );
		}


	// ================================================
	void ____MEMORY_API________()
		{
		}


	public void appendMemoryBytes( String attribute, byte[] bytes, int maxCount ) throws Throwable
		{
		long id = getLong( attribute );
		if ( id == 0 )
			{
			id = Ganesha.newID();
			//		System.out.println( "Creating new memory bytes: " + id );

			put( attribute, id );
			}

		Ganesha.appendMemoryBytes( id, bytes, maxCount );
		}


	public Queue getMemoryBytes( String attribute ) throws Throwable
		{
		long id = getLong( attribute );
		if ( id == 0 )
			{
			id = Ganesha.newID();
			//		System.out.println( "Creating new memory bytes: " + id );

			put( attribute, id );
			}

		return Ganesha.getMemoryBytes( id );
		}


	public int incrementMemoryInt( String attribute ) throws Throwable
		{
		long id = getLong( attribute );
		if ( id == 0 )
			{
			id = Ganesha.newID();
			//		System.out.println( "Creating new memory int: " + id );

			put( attribute, id );
			}

		return Ganesha.incrementMemoryInt( id );
		}


	public int getMemoryInt( String attribute ) throws Throwable
		{
		long id = getLong( attribute );
		if ( id == 0 )
			return 0;

		return Ganesha.getMemoryInt( id );
		}


	public void putMemoryInt( String attribute, int value ) throws Throwable
		{
		long id = getLong( attribute );
		if ( id == 0 )
			{
			id = Ganesha.newID();
			//			System.out.println( "Creating new memory int: " + id );

			put( attribute, id );
			}

		Ganesha.putMemoryInt( id, value );
		}


	// ================================================
	void ____LIST_API________()
		{
		}


	public int listSize( String attribute ) throws Throwable
		{
		long id = getLong( attribute );
		if ( id == 0 )
			return 0;

		return Ganesha.listSize( id );
		}


	public boolean exists( String attribute, long id ) throws Throwable
		{
		Queue ids = getIDs( attribute );
		for ( int i = 0; i < ids.size(); i++ )
			{
			Long id0 = (Long) ids.elementAt( i );

			if ( id0 == id )
				return true;
			}

		return false;
		}


	public void insertID( String attribute, long newObjectID, int metric, int max ) throws Throwable
		{
		long id = getLong( attribute );
		if ( id == 0 )
			{
			id = Ganesha.createEmptyList();

			put( attribute, id );
			}

		Ganesha.insertID( id, newObjectID, metric, max );
		}


	public void appendID( String attribute, long newObjectID ) throws Throwable
		{
		long id = getLong( attribute );
		if ( id == 0 )
			{
			id = Ganesha.createEmptyList();

			put( attribute, id );
			}

		Ganesha.appendID( id, newObjectID );
		}


	public void prependID( String attribute, long newObjectID ) throws Throwable
		{
		long id = getLong( attribute );
		if ( id == 0 )
			{
			id = Ganesha.createEmptyList();

			put( attribute, id );
			}

		Ganesha.prependID( id, newObjectID );
		}


	public void appendIDs( String attribute, Queue ids ) throws Throwable
		{
		long id = getLong( attribute );
		if ( id == 0 )
			{
			id = Ganesha.createEmptyList();

			put( attribute, id );
			}

		Ganesha.appendIDs( id, ids );
		}


	public void appendIDWithMax( String attribute, long newObjectID, int max ) throws Throwable
		{
		long id = getLong( attribute );
		if ( id == 0 )
			{
			id = Ganesha.createEmptyList();

			put( attribute, id );
			}

		Ganesha.appendIDWithMax( id, newObjectID, max );
		}


	public void removeID( String attribute, long newObjectID ) throws Throwable
		{
		long id = getLong( attribute );
		if ( id == 0 )
			return;

		Ganesha.removeID( id, newObjectID );
		}


	public void undeleteID( String attribute, long newObjectID ) throws Throwable
		{
		long id = getLong( attribute );
		if ( id == 0 )
			return;

		Ganesha.undeleteID( id, newObjectID );
		}


	public void removeOrderedID( String attribute, long idToRemove ) throws Throwable
		{
		long id = getLong( attribute );
		if ( id == 0 )
			return;

		Ganesha.removeOrderedID( id, idToRemove );
		}


	public static void printIDs( Gob gob, String attribute ) throws Throwable
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
	public void appendStringWithMax( String attribute, String s, int max ) throws Throwable
		{
		long id = getLong( attribute );
		if ( id == 0 )
			{
			id = Ganesha.createEmptyList();

			put( attribute, id );
			}

		Ganesha.appendStringWithMax( id, s, max );
		}


	// ================================================
	void ____LIST_API_GOBS________()
		{
		}


	public boolean exists( String attribute, Gob gob ) throws Throwable
		{
		return exists( attribute, gob.id );
		}


	public void insert( String attribute, Gob gob, int metric, int max ) throws Throwable
		{
		insert( attribute, gob, metric, max );
		}


	public void append( String attribute, Gob gob ) throws Throwable
		{
		appendID( attribute, gob.id );
		}


	public void prepend( String attribute, Gob gob ) throws Throwable
		{
		prependID( attribute, gob.id );
		}


	public void appendWithMax( String attribute, Gob gob, int max ) throws Throwable
		{
		appendIDWithMax( attribute, gob.id, max );
		}


	public void remove( String attribute, Gob gob ) throws Throwable
		{
		removeID( attribute, gob.id );
		}


	public void undelete( String attribute, Gob gob ) throws Throwable
		{
		undeleteID( attribute, gob.id );
		}


	public void removeOrdered( String attribute, Gob gob ) throws Throwable
		{
		removeOrderedID( attribute, gob.id );
		}

	}
