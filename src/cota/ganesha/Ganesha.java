package cota.ganesha;

import java.util.Date;

import cota.io.Message;
import cota.objectstore.HashStoreServer;
import cota.objectstore.MemoryStoreServer;
import cota.util.Queue;


// 6GB delegated minimum
// moreThan6Delegated = delegatedGB - 6

// HashStoreServer.data_bytes.cache		100MB + moreThan6Delegated / 5
// BytesServer.cache							100MB + moreThan6Delegated / 10
// MemoryServer.cache							100MB + moreThan6Delegated / 10
// MemoryStoreServer.cache					100MB + moreThan6Delegated / 10
// ObjectServer.cache							100MB + moreThan6Delegated / 20 (max 1GB)
// HashStoreServer.data_objects.cache	100MB + moreThan6Delegated / 20 (max 1GB)
// ObjectStore[objects].indices 				1GB
// ObjectStore[objects].sizes	 				1GB
// ObjectStore[bytes].indices 				1GB
// ObjectStore[bytes].sizes	 				1GB
// WriteManager.recentlyMarkedDirty		100MB
// TranslationServer.translationLocks		100MB
// ObjectStore[translations].sizes	 		100MB
// ObjectStore[translations].sizes	 		100MB
// ObjectServer.locks							100MB
// Translator.cache								100MB
// data_translations.cache						100MB
// WriteManager.dirtyFileLocks				100MB
// HashStore.objectLocks						100MB

// Provides the public API and handles all necessary translation
public class Ganesha
	{
	static long lastTimeInSeconds = 0;
	static int indexInSecond = 0;

	static Object lock = new Object();


	public static void startGanesha() throws Throwable
		{
		// Normally, only DirtyManager.debug is true
		//			DirtyManager.debug = true;

		//TranslationServer.debug = true;
		//HashStoreServer.debug = true;
		//		GobServer.debug = true;
		//			MemoryStoreServer.debug = true;
		//WriteManager.debug = true;
		//Connection.debug = true;

		// Used to determine which calls are trigger ObjectServer mod calls
		//	ObjectServer.debug = true;
		//BytesServer.debug = true;

		MapServer.init( true );
		DirtyManager.init();

		// HashStoreServer and its analogy MemoryStoreServer
		HashStoreServer hashStoreServer = new HashStoreServer();
		hashStoreServer.start();

		MemoryStoreServer memoryStoreServer = new MemoryStoreServer();
		memoryStoreServer.start();

		ObjectServer objectServer = new ObjectServer();
		objectServer.start();

		BytesServer bytesServer = new BytesServer();
		bytesServer.start();

		TranslationServer translationServer = new TranslationServer();
		translationServer.start();

		MemoryServer memoryServer = new MemoryServer();
		memoryServer.start();

		Thread.sleep( 5000 );

		// Memory server needs to be rebuilt every time at startup
		// It will obtain current values from the other servers
		System.out.print( "Rebuilding MemoryServer..." );
		MemoryServer.rebuildServer();
		System.out.println( "done" );

		MapServer.shouldMarkServersAsDown = true;
		}


	void ______INTERNAL________()
		{
		}


	// put/update
	protected static void putObjectBytes( long id, byte[] bytes ) throws Throwable
		{
		Message m = new Message();
		m.write8Bytes( id );
		m.writeBytes( bytes );

		ObjectServer.sendRequest( ObjectServer.PUT_OBJECT, m );
		}


	protected static byte[] getObjectBytes( long id ) throws Throwable
		{
		if ( id == 0 )
			{
			//			System.out.println( "REQUESTING 0 ID" );

			throw new NotFoundException();
			}

		Message m = new Message();
		m.write8Bytes( id );

		Message result = ObjectServer.sendRequest( ObjectServer.GET_OBJECT, m );

		return result.readBytes();
		}


	private static void rebuildServer( String ip ) throws Throwable
		{
		HashStoreServer.rebuildIP( ip );
		}


	void ______INTERNAL_ATTRIBUTE_API________()
		{
		}


	public static byte[] putLong( long id, int attribute, long n ) throws Throwable
		{
		Message m = new Message();
		m.write8Bytes( id );
		m.write4Bytes( attribute );
		m.write8Bytes( n );

		Message r = ObjectServer.sendRequest( ObjectServer.SET_LONG, m );

		return r.readBytes();
		}


	protected static byte[] putInt( long id, int attribute, int n ) throws Throwable
		{
		Message m = new Message();
		m.write8Bytes( id );
		m.write4Bytes( attribute );
		m.write4Bytes( n );

		Message r = ObjectServer.sendRequest( ObjectServer.SET_INTEGER, m );

		return r.readBytes();
		}


	protected static byte[] putString( long id, int attribute, String s ) throws Throwable
		{
		Message m = new Message();
		m.write8Bytes( id );
		m.write4Bytes( attribute );
		m.writeString( s );

		Message r = ObjectServer.sendRequest( ObjectServer.SET_STRING, m );

		return r.readBytes();
		}


	protected static byte[] increment( long id, int attribute ) throws Throwable
		{
		Message m = new Message();
		m.write8Bytes( id );
		m.write4Bytes( attribute );

		Message r = ObjectServer.sendRequest( ObjectServer.INCREMENT, m );

		return r.readBytes();
		}


	protected static byte[] decrement( long id, int attribute ) throws Throwable
		{
		Message m = new Message();
		m.write8Bytes( id );
		m.write4Bytes( attribute );

		Message r = ObjectServer.sendRequest( ObjectServer.DECREMENT, m );

		return r.readBytes();
		}


	void ____LISTS_INTERNAL________()
		{
		}


	protected static Queue returnStrings( byte[] bytes ) throws Throwable
		{
		Message m = new Message( bytes );

		int num = m.read4Bytes( 0 );

		Queue q = new Queue();
		for ( int i = 0; i < num; i++ )
			{
			String s = m.readString();

			q.addObject( s );
			}

		return q;
		}


	protected static Queue returnIDs( byte[] bytes ) throws Throwable
		{
		Message m = new Message( bytes );

		int num = m.read4Bytes();

		Queue q = new Queue();
		for ( int i = 0; i < num; i++ )
			{
			long id = m.read8Bytes();
			int deleted = m.readByte();

			// Only include undeleted objects
			if ( deleted == 0 )
				q.addObject( id );
			}

		return q;
		}


	protected static Queue returnOrderedIDs( byte[] bytes ) throws Throwable
		{
		Message m = new Message( bytes );

		int num = m.read4Bytes();

		Queue q = new Queue();
		for ( int i = 0; i < num; i++ )
			{
			long id = m.read8Bytes();
			int metric = m.read4Bytes();

			q.addObject( id );
			}

		return q;
		}


	protected static Queue returnDeletedIDs( byte[] bytes ) throws Throwable
		{
		Message m = new Message( bytes );

		int num = m.read4Bytes();

		Queue q = new Queue();
		for ( int i = 0; i < num; i++ )
			{
			long id = m.read8Bytes();
			int deleted = m.readByte();

			// Only include deleted objects
			if ( deleted != 0 )
				q.addObject( id );
			}

		return q;
		}


	void ______ID_UTILS________()
		{
		}


	// Note that generatingServerNum has a max value of 128 but only refers to the 
	// generating servers, not the data servers
	// There could be 5000000 data servers (unlimited as a hash is used) 
	// The generating servers will essentially be those servers that accept client connections
	// But they could be a group of servers dedicated to only generating IDs as well

	// 7 bits - generatingServerNum    128 max 
	// 24 bits - indexInSecond 			16 million max objects created per second per generating server
	// 33 bits - timeInSeconds				272 years of values
	public static long newID()
		{
		synchronized ( lock )
			{
			long timeInSeconds = System.currentTimeMillis() / 1000L;

			if ( lastTimeInSeconds != timeInSeconds )
				indexInSecond = 0;

			long id = ( ( (long) MapServer.returnServerNum() ) << 57 ) | ( ( (long) indexInSecond ) << 33 ) | timeInSeconds;

			lastTimeInSeconds = timeInSeconds;
			indexInSecond++;

			//	System.out.println( "Ganesha.newID: " + id );
			return id;
			}
		}


	// Returns the time in seconds at which the given id was created
	public static long timestampFromID( long id )
		{
		return id & 0x1FFFFFFFFL;
		}


	public static void printIDInfo( long id ) throws Throwable
		{
		long time = timestampFromID( id );
		System.out.println( "TIMESTAMP: " + time );

		Date d = new Date( time * 1000L );
		System.out.println( "CREATED: " + d.toString() );

		System.out.println( "GENERATED BY SERVER: " + ( id >> 57 ) );
		System.out.println( "INDEX IN SECOND: " + ( ( id >> 33 ) & 0xFFFFFFL ) );

		System.out.println( "\nOwning servers:" );

		Queue set = MapServer.returnOwningSet( id );
		for ( int i = 0; i < set.size(); i++ )
			System.out.println( "\t" + (String) set.elementAt( i ) );

		System.out.println( "" );
		}


	// Note that the GobServer methods using String keys are not used here
	// That is left to the Translator
	// There are three types of objects used
	// objects - GobObjects
	// bytes - lists and raw binary arrays
	// translations
	void ______FULL_OBJECT_API________()
		{
		}


	public static boolean exists( String workspace, String table, String name ) throws Throwable
		{
		long translation = Translator.translate( workspace, table, name, false );

		System.out.println( "Ganesha.exist: " + workspace + "\t" + table + "\t" + name + ": " + translation );

		return ( translation != 0 );
		}


	public static long addBytes( byte[] bytes ) throws Throwable
		{
		long id = Ganesha.newID();

		Ganesha.putBytes( id, bytes );

		return id;
		}


	public static long addObject( Gob gob ) throws Throwable
		{
		long id = Ganesha.newID();
		gob.id = id;

		Ganesha.putObjectBytes( id, gob.returnBytes() );

		return id;
		}


	public static long addObject( String workspace, String table, String name, Gob gob ) throws Throwable
		{
		long id = Translator.translate( workspace, table, name, true );
		gob.id = id;

		Ganesha.putObjectBytes( id, gob.returnBytes() );

		return id;
		}


	// put/update explicit bytes
	public static void putBytes( long id, byte[] bytes ) throws Throwable
		{
		Message m = new Message();
		m.write8Bytes( id );
		m.writeBytes( bytes );

		BytesServer.sendRequest( BytesServer.PUT_BYTES, m );
		}


	public static byte[] getBytes( long id ) throws Throwable
		{
		if ( id == 0 )
			{
			//			System.out.println( "REQUESTING 0 ID" );

			throw new NotFoundException();
			}

		Message m = new Message();
		m.write8Bytes( id );

		Message result = BytesServer.sendRequest( BytesServer.GET_BYTES, m );

		return result.readBytes();
		}


	// These calls are for storing receiving data from memory caches that are never written to disk
	void ______MEMORY_API________()
		{
		}


	public static void appendMemoryBytes( long id, byte[] bytes, int maxCount ) throws Throwable
		{
		Message m = new Message();
		m.write8Bytes( id );
		m.writeBytes( bytes );
		m.write4Bytes( maxCount );

		MemoryServer.sendRequest( MemoryServer.APPEND_BYTES, m );
		}


	public static Queue getMemoryBytes( long id ) throws Throwable
		{
		Message m = new Message();
		m.write8Bytes( id );

		Message result = MemoryServer.sendRequest( MemoryServer.GET_BYTES, m );
		Message result2 = new Message( result.readBytes() );

		Queue q = new Queue();

		int numEntries = result2.read4Bytes();
		for ( int i = 0; i < numEntries; i++ )
			{
			byte[] bytes = result2.readBytes();

			q.addObject( bytes );
			}

		return q;
		}


	public static int incrementMemoryInt( long id ) throws Throwable
		{
		Message m = new Message();
		m.write8Bytes( id );

		Message result = MemoryServer.sendRequest( MemoryServer.INCREMENT_INT, m );

		return result.read4Bytes();
		}


	public static int getMemoryInt( long id ) throws Throwable
		{
		Message m = new Message();
		m.write8Bytes( id );

		Message result = MemoryServer.sendRequest( MemoryServer.GET_INT, m );

		return result.read4Bytes();
		}


	public static void putMemoryInt( long id, int value ) throws Throwable
		{
		Message m = new Message();
		m.write8Bytes( id );
		m.write4Bytes( value );

		MemoryServer.sendRequest( MemoryServer.SET_INT, m );
		}


	void ______LIST_API________()
		{
		}


	public static long createEmptyList() throws Throwable
		{
		// Create a new object which can be used to create lists
		Message m = new Message();
		m.write4Bytes( 0 );

		long id = Ganesha.addBytes( m.returnBytes() );

		//		System.out.println( "Ganesha.createEmptyList id: " + id );

		return id;
		}


	public static int listSize( long id ) throws Throwable
		{
		Message m = new Message();
		m.write8Bytes( id );

		Message result = BytesServer.sendRequest( BytesServer.LIST_SIZE, m );

		return result.read4Bytes();
		}


	public static void appendID( long id, long newObjectID ) throws Throwable
		{
		Message m = new Message();
		m.write8Bytes( id );
		m.write8Bytes( newObjectID );

		BytesServer.sendRequest( BytesServer.APPEND_ID, m );
		}


	public static void prependID( long id, long newObjectID ) throws Throwable
		{
		Message m = new Message();
		m.write8Bytes( id );
		m.write8Bytes( newObjectID );

		BytesServer.sendRequest( BytesServer.PREPEND_ID, m );
		}


	public static void appendStringWithMax( long id, String s, int max ) throws Throwable
		{
		Message m = new Message();
		m.write8Bytes( id );
		m.writeString( s );
		m.write4Bytes( max );

		BytesServer.sendRequest( BytesServer.APPEND_STRING, m );
		}


	public static void appendIDs( long id, Queue ids ) throws Throwable
		{
		Message m = new Message();
		m.write8Bytes( id );

		m.write4Bytes( ids.size() );
		for ( int i = 0; i < ids.size(); i++ )
			m.write8Bytes( (Long) ids.elementAt( i ) );

		BytesServer.sendRequest( BytesServer.APPEND_IDS, m );
		}


	public static void appendIDWithMax( long id, long newObjectID, int max ) throws Throwable
		{
		Message m = new Message();
		m.write8Bytes( id );
		m.write8Bytes( newObjectID );
		m.write4Bytes( max );

		BytesServer.sendRequest( BytesServer.APPEND_ID_WITH_MAX, m );
		}


	public static void insertID( long id, long newObjectID, int metric, int max ) throws Throwable
		{
		Message m = new Message();
		m.write8Bytes( id );
		m.write8Bytes( newObjectID );
		m.write4Bytes( metric );
		m.write4Bytes( max );

		BytesServer.sendRequest( BytesServer.INSERT_ID, m );
		}


	public static void appendID( Gob gob, long newObjectID ) throws Throwable
		{
		appendID( gob.id, newObjectID );
		}


	public static void removeID( long id, long idToRemove ) throws Throwable
		{
		Message m = new Message();
		m.write8Bytes( id );
		m.write8Bytes( idToRemove );

		BytesServer.sendRequest( BytesServer.REMOVE_ID, m );
		}


	public static void removeOrderedID( long id, long idToRemove ) throws Throwable
		{
		Message m = new Message();
		m.write8Bytes( id );
		m.write8Bytes( idToRemove );

		BytesServer.sendRequest( BytesServer.REMOVE_ORDERED_ID, m );
		}


	public static void undeleteID( long id, long idToUndelete ) throws Throwable
		{
		Message m = new Message();
		m.write8Bytes( id );
		m.write8Bytes( idToUndelete );

		BytesServer.sendRequest( BytesServer.UNDELETE_ID, m );
		}


	public static void undeleteID( Gob gob, long idToUndelete ) throws Throwable
		{
		undeleteID( gob.id, idToUndelete );
		}


	public static Queue getIDs( long id ) throws Throwable
		{
		byte[] bytes = Ganesha.getBytes( id );
		if ( bytes == null )
			return null;

		return Ganesha.returnIDs( bytes );
		}


	public static Queue getStrings( long id ) throws Throwable
		{
		byte[] bytes = Ganesha.getBytes( id );
		if ( bytes == null )
			return null;

		return Ganesha.returnStrings( bytes );
		}


	public static Queue getOrderedIDs( long id ) throws Throwable
		{
		byte[] bytes = Ganesha.getBytes( id );
		if ( bytes == null )
			return null;

		return Ganesha.returnOrderedIDs( bytes );
		}


	public static Queue getDeletedIDs( long id ) throws Throwable
		{
		byte[] bytes = Ganesha.getBytes( id );
		if ( bytes == null )
			return null;

		return Ganesha.returnDeletedIDs( bytes );
		}


	public static void main( String[] args )
		{
		try
			{
			if ( args.length > 0 )
				{
				MapServer.init( true );

				// Servers can be told to send appropriate data to an external ip
				// This is necessary when rebuilding a new server or after a server as been down for an
				// extended period
				if ( args[ 0 ].equals( "rebuild_external_ip" ) )
					{
					String ip = args[ 1 ];

					rebuildServer( ip );
					}

				if ( args[ 0 ].equals( "new_id" ) )
					{
					System.out.println( newID() );
					}

				if ( args[ 0 ].equals( "id" ) )
					{
					long id = Long.parseLong( args[ 1 ] );

					printIDInfo( id );
					}

				return;
				}

			startGanesha();
			}
		catch ( Throwable theX )
			{
			cota.util.Util.printX( theX );
			}
		}
	}
