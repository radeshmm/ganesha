package cota.ganesha;

import java.net.InetAddress;
import java.net.Socket;

import cota.io.InStream;
import cota.io.Message;
import cota.io.OutStream;
import cota.networking.Connection;
import cota.networking.ConnectionPool;
import cota.networking.TCPServer;
import cota.objectstore.HashStoreServer;
import cota.util.ErrorHandler;
import cota.util.Hashtable_lo;
import cota.util.Hashtable_so;
import cota.util.LRU_lbt;
import cota.util.LRU_lo;
import cota.util.PairBL;
import cota.util.PairLI;
import cota.util.Queue;
import cota.util.TriOLI;
import cota.util.Util;


// Caching here (on the default authority server) means that
// lookups on the other servers in the set can be avoided
public class BytesServer extends TCPServer
	{
	static final int PORT = 20003;

	public static boolean debug = false;

	static long startTime = System.currentTimeMillis();
	static int modCount = 0;
	static int readCount = 0;

	// Object requests
	static final int APPEND_ID = 11;
	static final int APPEND_ID_WITH_MAX = 12;
	static final int INSERT_ID = 13;
	static final int REMOVE_ID = 14;
	static final int REMOVE_ORDERED_ID = 15;
	static final int UNDELETE_ID = 16;
	static final int APPEND_IDS = 17;
	static final int APPEND_STRING = 19;
	static final int PREPEND_ID = 20;
	static final int PUT_BYTES = 21;
	static final int GET_BYTES = 22;
	static final int GET_BYTES_WITH_TIMESTAMP_AND_NO_BACKUP = 24;
	static final int LIST_SIZE = 25;

	static final int NOT_FOUND = 50;
	static final int SUCCESS = 99;

	private LRU_lo locks = null;

	private static LRU_lbt cache = null;

	static int count = 0;


	public BytesServer() throws Throwable
		{
		super( "BytesServer", PORT );

		// No socket timeout
		socketTimeout = 0;

		// Used for locking the objects based on objectNum
		// 200MB will allow about 2 million locks (the limit of objects 
		// being modified simultaneously on one synced machine)
		locks = new LRU_lo( 1024L * 1024L * 200L );
		locks.name = "BytesServer.locks";

		long heapSize = Runtime.getRuntime().totalMemory();
		long extra = ( heapSize - 1024L * 1024L * 1024L * 6L ) / 10;

		cache = new LRU_lbt( 1024L * 1024L * 100L + extra );
		cache.name = "BytesServer.cache";
		}


	private Object returnLock( long id )
		{
		Object lock = null;

		synchronized ( locks )
			{
			lock = locks.get( id );

			if ( lock == null )
				{
				lock = new Object();
				locks.put( id, lock );
				}
			}

		return lock;
		}


	void ______LISTS________()
		{
		}


	private static byte[] appendID( long id, long objectIDToAppend ) throws Throwable
		{
		byte[] bytes = getBytes( id );

		Message m = new Message( bytes );

		int numObjects = m.read4Bytes( 0 );

		// Check first to make sure the ID is not already there
		for ( int i = 0; i < numObjects; i++ )
			{
			long objectID0 = m.read8Bytes();
			int deleted = m.readByte();

			if ( objectID0 == objectIDToAppend )
				{
				if ( deleted == 1 )
					{
					// Looks like the object was here but deleted
					// Undelete it
					m.writeByte( m.index - 1, 0 );

					return m.returnBytes();
					}

				return null;
				}
			}

		// Append the id
		m.write4Bytes( 0, numObjects + 1 );

		m.write8Bytes( m.maxIndex, objectIDToAppend );
		m.writeByte( 0 );

		return m.returnBytes();
		}


	private static byte[] prependID( long id, long objectIDToPrepend ) throws Throwable
		{
		byte[] bytes = getBytes( id );

		Message m = new Message( bytes );

		Queue q = new Queue();
		q.addObject( new PairLI( objectIDToPrepend, 0 ) );

		int numObjects = m.read4Bytes( 0 );
		for ( int i = 0; i < numObjects; i++ )
			{
			long objectID0 = m.read8Bytes();
			int deleted = m.readByte();

			// Don't add multiple copies of the objectID
			if ( objectID0 != objectIDToPrepend )
				q.addObject( new PairLI( objectID0, deleted ) );
			}

		Message newM = new Message();
		newM.write4Bytes( q.size() );
		for ( int i = 0; i < q.size(); i++ )
			{
			PairLI p = (PairLI) q.elementAt( i );

			newM.write8Bytes( p.x );
			newM.writeByte( p.y );
			}

		return newM.returnBytes();
		}


	private static byte[] appendStringWithMax( long id, String stringToAppend, int max ) throws Throwable
		{
		byte[] bytes = getBytes( id );

		Message m = new Message( bytes );

		int numStrings = m.read4Bytes( 0 );

		// Check first to make sure the string is not already there
		// Duplicates are not allowed
		for ( int i = 0; i < numStrings; i++ )
			{
			String s = m.readString();
			if ( s.equals( stringToAppend ) )
				return null;
			}

		if ( max == 0 )
			max = Integer.MAX_VALUE;

		if ( ( numStrings + 1 ) < max )
			{
			// Append the id
			m.write4Bytes( 0, numStrings + 1 );

			m.writeString( m.maxIndex, stringToAppend );

			return m.returnBytes();
			}
		else
			{
			// Too many strings!
			Message m2 = new Message();
			m2.write4Bytes( max );

			m.index = 4;
			int stringsToSkip = ( numStrings + 1 ) - max;
			for ( int i = 0; i < stringsToSkip; i++ )
				m.readString();

			for ( int i = stringsToSkip; i < numStrings; i++ )
				{
				String s = m.readString();

				m2.writeString( s );
				}

			m2.writeString( stringToAppend );

			return m2.returnBytes();
			}
		}


	private static byte[] appendIDs( long id, Queue q ) throws Throwable
		{
		Hashtable_lo f = new Hashtable_lo();
		for ( int j = 0; j < q.size(); j++ )
			{
			long objectIDToAppend = (Long) q.elementAt( j );

			f.put( objectIDToAppend, "" );
			}

		byte[] bytes = getBytes( id );

		Message m = new Message( bytes );

		int numObjects = m.read4Bytes( 0 );

		Hashtable_lo undeletedF = new Hashtable_lo();

		// First undelete any ids that were deleted
		for ( int i = 0; i < numObjects; i++ )
			{
			long objectID0 = m.read8Bytes();
			int deleted = m.readByte();

			if ( deleted == 1 )
				if ( f.get( objectID0 ) != null )
					{
					m.writeByte( m.index - 1, 0 );

					undeletedF.put( objectID0, "" );
					}
			}

		// Append new ids
		for ( int j = 0; j < q.size(); j++ )
			{
			long objectIDToAppend = (Long) q.elementAt( j );

			if ( undeletedF.get( objectIDToAppend ) == null )
				{
				numObjects++;

				m.write8Bytes( m.maxIndex, objectIDToAppend );
				m.writeByte( 0 );
				}
			}

		m.write4Bytes( 0, numObjects );

		return m.returnBytes();
		}


	// Here, the bytes array is treated as a normal list (not as a binary tree of leaf nodes)
	// The objectID is appended to the end
	// If the list now exceeds the max size, the first objectID is removed
	private static byte[] appendIDWithMax( long id, long objectIDToAppend, int max ) throws Throwable
		{
		byte[] bytes = getBytes( id );

		//		System.out.println( "APPENDING ID WITH MAX: " + bytes );
		//	System.out.println( "APPENDING ID WITH MAX: " + bytes.length );

		Message m = new Message( bytes );

		Queue q = new Queue();
		int numObjects = m.read4Bytes( 0 );
		//	System.out.println( "CURRENT NUM OBJECTS: " + numObjects );

		for ( int i = 0; i < numObjects; i++ )
			{
			long objectID0 = m.read8Bytes();
			int deleted = m.readByte();

			// Don't add multiple copies of the objectID
			if ( objectID0 != objectIDToAppend )
				q.addObject( new PairLI( objectID0, deleted ) );
			}

		//		System.out.println( "A: " + q.size() );

		q.addObject( new PairLI( objectIDToAppend, 0 ) );

		//		System.out.println( "A: " + q.size() );
		while ( q.size() > max )
			q.removeFirst();
		//		System.out.println( "A: " + q.size() );

		Message newM = new Message();
		newM.write4Bytes( q.size() );

		for ( int i = 0; i < q.size(); i++ )
			{
			PairLI p = (PairLI) q.elementAt( i );

			newM.write8Bytes( p.x );
			newM.writeByte( p.y );
			}

		return newM.returnBytes();
		}


	// Here, the bytes array is treated as a special kind of ordered list
	// In the list, each objectID is followed by an integer
	// The integer represent the metric
	// ObjectIDs with higher valued metrics are placed at the beginning
	// Note that ordered lists are just stored in one object, not in a binary tree
	// Note also that deleted forms of ids can't exist in the list, they are simply removed
	private static byte[] insertID( long id, long objectIDToInsert, int metric, int max ) throws Throwable
		{
		byte[] bytes = getBytes( id );

		Message m = new Message( bytes );

		// Note that this includes deleted objects as well!
		int numObjects = m.read4Bytes( 0 );

		Queue q = new Queue();
		boolean addedToQueue = false;
		boolean existed = false;
		for ( int i = 0; i < numObjects; i++ )
			{
			long objectID0 = m.read8Bytes();
			int metric0 = m.read4Bytes();

			if ( !addedToQueue )
				if ( metric > metric0 )
					{
					q.addObject( new PairLI( objectIDToInsert, metric ) );
					addedToQueue = true;

					//						System.out.println( "insertID0 " + objectID + " INSERTED IN POSITION: " + q.size() );
					}

			if ( objectID0 == objectIDToInsert )
				existed = true;

			// Don't add previous copies of the objectID 
			if ( objectID0 != objectIDToInsert )
				q.addObject( new PairLI( objectID0, metric0 ) );
			}

		if ( !addedToQueue ) // not any better than any of what we've seen
			{
			if ( numObjects < max )
				q.addObject( new PairLI( objectIDToInsert, metric ) ); // space to add it to the end
			else
				{
				// Only bail if the objectID was never in the list in the first place
				if ( !existed )
					return null; // no room for low performers, no modification needs to be made
				}
			}

		while ( q.size() > max )
			q.removeLast();

		// Reconstruct the list
		Message newM = new Message();
		newM.write4Bytes( q.size() );

		for ( int i = 0; i < q.size(); i++ )
			{
			PairLI p = (PairLI) q.elementAt( i );

			newM.write8Bytes( p.x );
			newM.write4Bytes( p.y );
			}

		return newM.returnBytes();
		}


	// Traverse the binary tree and mark the id as deleted if it is found
	private static byte[] removeID( long id, long objectIDToRemove ) throws Throwable
		{
		byte[] bytes = getBytes( id );

		Message m = new Message( bytes );

		int numObjects = m.read4Bytes( 0 );
		// Dealing with a node with less than kMaxObjectsInBinaryList ids in the list
		// Just search the ids present in this node and remove the one we're looking for if found
		for ( int i = 0; i < numObjects; i++ )
			{
			long objectID0 = m.read8Bytes();
			int deleted = m.readByte();

			if ( objectID0 == objectIDToRemove )
				{
				m.writeByte( m.index - 1, 1 ); // deleted

				return m.returnBytes();
				}
			}

		return null;
		}


	// Traverse the binary tree and remove the object when it is found
	// Note that ordered lists are just stored in one object, not in a binary tree
	private static byte[] removeOrderedID( long id, long objectIDToRemove ) throws Throwable
		{
		byte[] bytes = getBytes( id );

		Message m = new Message( bytes );

		int numObjects = m.read4Bytes( 0 );

		// Create a queue containing all the ids (except for the one we are deleting)
		// Then rewrite the message
		Queue q = new Queue();

		// Just search the ids present in this node and remove the one we're looking for if found
		for ( int i = 0; i < numObjects; i++ )
			{
			long objectID0 = m.read8Bytes();
			int metric0 = m.read4Bytes();

			if ( objectID0 != objectIDToRemove )
				q.addObject( new PairLI( objectID0, metric0 ) );
			}

		Message newM = new Message();
		newM.write4Bytes( q.size() );

		for ( int i = 0; i < q.size(); i++ )
			{
			PairLI p = (PairLI) q.elementAt( i );

			newM.write8Bytes( p.x );
			newM.write4Bytes( p.y );
			}

		return newM.returnBytes();
		}


	private static byte[] undeleteID( long id, long objectIDToUndelete ) throws Throwable
		{
		byte[] bytes = getBytes( id );

		Message m = new Message( bytes );

		int numObjects = m.read4Bytes( 0 );
		// Try to undeleted the objectID
		for ( int i = 0; i < numObjects; i++ )
			{
			long objectID0 = m.read8Bytes();
			int deleted = m.readByte();

			if ( objectID0 == objectIDToUndelete )
				if ( deleted == 1 )
					{
					// Looks like the object was deleted
					// Undelete it
					m.writeByte( m.index - 1, 0 );

					return m.returnBytes();
					}
			}

		return null;
		}


	// Note that these caches only work when the server is the default owning server
	// for the object in question
	// This is to prevent an out of date cache taking precedent on a server
	// which is only occasionally the current owning server and which might not have
	// the newest version of the object
	// ================================================
	void ____CACHE________()
		{
		}


	private static void cacheBytes( long id, byte[] bytes, long timestamp )
		{
		count++;

		if ( ( count % 100000 ) == 0 )
			{
			Runtime r = Runtime.getRuntime();

			double free = r.freeMemory() / 1024 / 1024;
			double total = r.totalMemory() / 1024 / 1024;
			double used = total - free;

			System.out.println( "" );
			System.out.println( "\t\tMemory: " + (int) used + "M / " + total + "M" );
			}

		synchronized ( cache )
			{
			cache.put( id, bytes, timestamp );
			}
		}


	private static PairBL getBytesFromCache( long id )
		{
		synchronized ( cache )
			{
			return cache.get( id );
			}
		}


	void ______SERVER________()
		{
		}


	// The Message uses an argument is crafted by Ganesha
	// Note that synchronization is not required in an explicit get object message
	// Lack of synchronization may result in some occasional unnecessary updating
	// of objects (as it's possible that the get happens when only some of the objects
	// have the current value and updates happen when data is not unanimously similar)
	// This will be okay though as the timestamp is used to determine the most
	// recent object to be returned
	private static PairBL getBytesWithTimestamp( long objectID, boolean sendUpdates ) throws Throwable
		{
		Queue set = MapServer.returnOwningSet( objectID );

		// Only use the cache if the set is the default owning server for the object
		boolean isDefaultOwningServer = ( (String) set.elementAt( 0 ) ).equals( MapServer.myIP );

		if ( isDefaultOwningServer )
			{
			// Note that when an object is in the cache, it may prevent other
			// servers from being updated even if they do not include the most recent object
			PairBL p = getBytesFromCache( objectID );
			if ( p != null )
				{
				//				System.out.println( "bytes in cache" );
				return new PairBL( p.x, p.y );
				}
			}
		else
			System.out.println( "BytesServer:  Not default server!!!!!!!!!!!!!!!" );

		//		System.out.println( "Not using the cache!!!!: " + objectID );

		Hashtable_so f = new Hashtable_so();
		long newestTimestamp = 0;
		TriOLI newestT = null;

		// Pull the current objects from the given set
		Queue objects = new Queue();
		for ( int i = 0; i < set.size(); i++ )
			{
			String ip = (String) set.elementAt( i );

			if ( !MapServer.serverIsDown( ip ) )
				{
				TriOLI t = HashStoreServer.getBytes( ip, objectID );

				if ( t != null )
					{
					long timestamp = t.y;

					// Used later to determine if the server needs updating
					f.put( ip, t );

					if ( timestamp > newestTimestamp )
						{
						newestTimestamp = timestamp;
						newestT = t;
						}

					objects.addObject( t );
					}
				}
			}

		// See how many objects hold the newest object value
		int numWithNewest = 0;
		if ( objects.size() == set.size() ) // only possible success if all servers have the same
			{
			for ( int i = 0; i < objects.size(); i++ )
				{
				TriOLI t = (TriOLI) objects.elementAt( i );

				// Compare the checksum
				if ( t.z == newestT.z )
					numWithNewest++;
				}

			// If all servers have the newest object, then we're done
			if ( numWithNewest == set.size() )
				{
				if ( isDefaultOwningServer )
					cacheBytes( objectID, (byte[]) newestT.x, newestT.y );

				return new PairBL( (byte[]) newestT.x, newestT.y );
				}
			}

		if ( newestT == null )
			throw new NotFoundException();

		// Didn't have unanimous consistency
		// Will need to update those servers that don't have the newest object value
		if ( sendUpdates )
			for ( int i = 0; i < set.size(); i++ )
				{
				String ip = (String) set.elementAt( i );
				TriOLI t = (TriOLI) f.get( ip );

				// Non-existant or a different checksum?
				if ( ( t == null ) || ( t.z != newestT.z ) )
					WriteManager.putObjectOrBytes( ip, objectID, (byte[]) newestT.x, true );
				}

		if ( isDefaultOwningServer )
			cacheBytes( objectID, (byte[]) newestT.x, newestT.y );

		return new PairBL( (byte[]) newestT.x, newestT.y );
		}


	private static byte[] getBytes( long objectID ) throws Throwable
		{
		PairBL p = getBytesWithTimestamp( objectID, true );

		return p.x;
		}


	// The Message uses an argument is crafted by Ganesha
	private Message handleGenericObjectRequest( int requestType, Message m ) throws Throwable
		{
		long objectID = m.read8Bytes();

		// Update the relevant set of servers
		Queue set = MapServer.returnOwningSet( objectID );

		Message r = new Message();

		// All requests need to be synchronized so that we're not modifying
		// the same object within two different threads
		Object lock = returnLock( objectID );
		byte[] bytes = null;

		// Dealing with a binary array or a GobObject?
		synchronized ( lock )
			{
			// After this switch statement, bytes should contain the actual bytes
			// that need to but attached to the given objectID
			// or null if no update needs to be made (as with LEAF_NODES)
			switch ( requestType )
				{
				case PUT_BYTES:
					{
					bytes = m.readBytes();
					}
				break;

				case APPEND_ID:
					{
					long objectIDToAppend = m.read8Bytes();

					bytes = appendID( objectID, objectIDToAppend );
					}
				break;

				case PREPEND_ID:
					{
					long objectIDToPrepend = m.read8Bytes();

					bytes = prependID( objectID, objectIDToPrepend );
					}
				break;

				case APPEND_STRING:
					{
					String stringToAppend = m.readString();
					int max = m.read4Bytes();

					bytes = appendStringWithMax( objectID, stringToAppend, max );
					}
				break;

				case APPEND_IDS:
					{
					Queue q = new Queue();

					int numIDs = m.read4Bytes();
					for ( int i = 0; i < numIDs; i++ )
						{
						long objectIDToAppend = m.read8Bytes();

						q.addObject( objectIDToAppend );
						}

					bytes = appendIDs( objectID, q );
					}
				break;

				case APPEND_ID_WITH_MAX:
					{
					long objectIDToAppend = m.read8Bytes();
					int max = m.read4Bytes();

					bytes = appendIDWithMax( objectID, objectIDToAppend, max );
					}
				break;

				case INSERT_ID:
					{
					long objectIDToInsert = m.read8Bytes();
					int metric = m.read4Bytes();
					int max = m.read4Bytes();

					bytes = insertID( objectID, objectIDToInsert, metric, max );
					}
				break;

				case REMOVE_ID:
					{
					long objectIDToRemove = m.read8Bytes();

					bytes = removeID( objectID, objectIDToRemove );
					}
				break;

				case REMOVE_ORDERED_ID:
					{
					long objectIDToRemove = m.read8Bytes();

					bytes = removeOrderedID( objectID, objectIDToRemove );
					}
				break;

				case UNDELETE_ID:
					{
					long objectIDToUndelete = m.read8Bytes();

					bytes = undeleteID( objectID, objectIDToUndelete );
					}
				break;
				}

			if ( bytes != null )
				{
				// Only use the cache if the set is the default owning server for the object
				boolean isDefaultOwningServer = ( (String) set.elementAt( 0 ) ).equals( MapServer.myIP );
				if ( isDefaultOwningServer )
					{
					cacheBytes( objectID, bytes, System.currentTimeMillis() );
					}
				}

			// This server has only been used for syncing so far
			// It also needs to write any updated values to other servers

			// WriteManager will queue writes and write the most recent version the server
			// Note that as this loop is synchronized on the object, but that for an extra
			// measure of protection:
			// HashStore includes functionality to disallowed updates older than the versions
			// that currently exist in the store
			//		System.out.println( bytes + "\t" + set.size() );

			if ( bytes != null )
				{
				for ( int i = 0; i < set.size(); i++ )
					{
					String ip = (String) set.elementAt( i );

					WriteManager.putObjectOrBytes( ip, objectID, bytes, true );
					}
				}
			}

		return r;
		}


	public void handleRequest( Socket s, InStream in, OutStream out ) throws Throwable
		{
		InetAddress address = s.getInetAddress();
		String ip0 = address.getHostAddress();

		if ( !MapServer.isValidIP( ip0 ) )
			{
			System.out.println( "!!!!!!!  ATTEMPT TO ACCESS OBJECT SERVER FROM INVALID IP: " + ip0 );
			return;
			}

		// Allow connection to be reused
		long objectID = 0;

		while ( true )
			{
			try
				{
				int requestType = in.read4Bytes();
				Message m = in.readMessage();

				Message r = new Message();

				switch ( requestType )
					{
					case GET_BYTES:
						{
						objectID = m.read8Bytes();

						byte[] bytes = getBytes( objectID );
						r.writeBytes( bytes );
						}
					break;

					case LIST_SIZE:
						{
						objectID = m.read8Bytes();

						byte[] bytes = getBytes( objectID );

						Message m2 = new Message( bytes );

						int count = 0;

						int numObjects = m2.read4Bytes( 0 );
						for ( int i = 0; i < numObjects; i++ )
							{
							long objectID0 = m2.read8Bytes();
							int deleted = m2.readByte();

							if ( deleted == 0 )
								count++;
							}

						r.write4Bytes( count );
						}
					break;

					case GET_BYTES_WITH_TIMESTAMP_AND_NO_BACKUP:
						{
						objectID = m.read8Bytes();

						PairBL p = getBytesWithTimestamp( objectID, true );

						r.writeBytes( p.x );
						r.write8Bytes( p.y );
						}
					break;

					default:
						r = handleGenericObjectRequest( requestType, m );
					break;
					}

				out.writeByte( SUCCESS );

				// Don't write empty messages
				if ( r.index == 0 )
					r.writeByte( 0 );

				out.writeMessage( r );
				out.flush();
				}
			catch ( cota.io.StreamClosedX theX )
				{
				// Sometimes connections to the Server will break
				// Just exit the connection loop which will close the connection in the client
				// without returning it to the ConnectionPool
				// Really?
				break;
				}
			catch ( NotFoundException theX )
				{
				//				cota.util.Util.printX( theX );
				out.writeByte( NOT_FOUND );
				out.flush();
				}
			catch ( Throwable theX )
				{
				cota.util.Util.printX( "ERROR IN BytesServer.handleRequest objectID: " + objectID, theX );
				out.writeByte( 0 );
				out.flush();

				//				throw theX;
				}
			}
		}


	// Needs to have variable args and results
	public static Message sendRequest( int requestType, Message args ) throws Throwable
		{
		if ( ( requestType == GET_BYTES ) || ( requestType == GET_BYTES_WITH_TIMESTAMP_AND_NO_BACKUP ) )
			readCount++;
		else
			modCount++;

		if ( ( ( readCount + modCount ) % 1000 ) == 0 )
			{
			long elapsed = System.currentTimeMillis() - startTime;
			double avgRead = readCount * 1.0 / elapsed * 1000.0 * 60 * 60;
			double avgMods = modCount * 1.0 / elapsed * 1000.0 * 60 * 60;

			System.out.println( "BytesServer: " + (int) avgRead + " read/h\t: " + (int) avgMods + " mods/h" );
			}

		// Combine this with Tools analyze to determine which calls are yielding the most writes
		if ( debug )
			if ( requestType != GET_BYTES )
				if ( requestType != LIST_SIZE )
					Util.printStackTrace( "\nBytesServer.sendRequest " + requestType );

		// Determine the active set for the given id
		// Takes into account the time that the object was created
		// This simply allows us to determine which server will take ownership for syncing

		long objectID = args.read8Bytes( 0 );
		Queue setForSync = MapServer.returnOwningSet( objectID );

		// Whatever server in the set that responds first will be the one that is used for synchronization
		for ( int i = 0; i < setForSync.size(); i++ )
			{
			String ip = null;

			Connection c = null;

			boolean requireNewConnection = false;
			for ( int j = 0; j < 2; j++ )
				{
				try
					{
					ip = (String) setForSync.elementAt( i );

					if ( debug )
						System.out.println( "\tsyncing on " + ip );

					c = ConnectionPool.returnConnection( ip, PORT, requireNewConnection );
					c.out.write4Bytes( requestType );
					c.out.writeMessage( args );

					c.out.flush();

					int status = c.in.readByte();
					if ( status == NOT_FOUND )
						throw new NotFoundException();

					if ( status != SUCCESS )
						throw new Throwable( "INVALID STATUS RECEIVED" );

					Message r = c.in.readMessage();

					ConnectionPool.returnConnectionToPool( c );

					return r;
					}
				catch ( cota.io.StreamClosedX theX )
					{
					// Looks like the cached connection we were using is no longer valid
					// Try again with a new connection
					requireNewConnection = true;

					System.out.println( "Previous BytesServer connection no longer valid: " + ip );
					}
				catch ( NotFoundException theX )
					{
					System.out.println( "BytesServer.NotFoundException: " + objectID );
					c.close();

					throw theX;
					}
				catch ( Throwable theX )
					{
					cota.util.Util.printX( theX );

					if ( c != null )
						c.close();

					ErrorHandler.error( "ERROR CONNECTING TO IP: " + ip, theX );
					break;
					}
				}
			}

		throw new Throwable( "Couldn't sync correctly on any of the servers" );
		}
	}
