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
import cota.util.Fashtable_so;
import cota.util.LRU_lbt;
import cota.util.LRU_lo;
import cota.util.PairBL;
import cota.util.Queue;
import cota.util.TriOLI;
import cota.util.Util;


// Caching here (on the default authority server only) means that
// lookups on the other servers in the set can be avoided
public class ObjectServer extends TCPServer
	{
	static final int PORT = 20002;

	public static boolean debug = false;

	static long startTime = System.currentTimeMillis();
	static int modCount = 0;
	static int readCount = 0;

	// Object requests
	static final int PUT_OBJECT = 1;
	static final int GET_OBJECT = 2;
	static final int SET_INTEGER = 5;
	static final int SET_LONG = 6;
	static final int SET_STRING = 7;
	static final int INCREMENT = 8;
	static final int DECREMENT = 9;

	static final int NOT_FOUND = 50;
	static final int SUCCESS = 99;

	// Constants
	private LRU_lo locks = null;

	private static LRU_lbt cache = null;

	static int count = 0;


	public ObjectServer() throws Throwable
		{
		super( "ObjectServer", PORT );

		// No socket timeout
		socketTimeout = 0;

		// Used for locking the objects based on objectNum
		// 100MB will allow about 2 million locks (the limit of objects 
		// being modified simultaneously on one synced machine)
		locks = new LRU_lo( 1024L * 1024L * 100L );
		locks.name = "ObjectServer.locks";

		long heapSize = Runtime.getRuntime().totalMemory();
		long extra = ( heapSize - 1024L * 1024L * 1024L * 6L ) / 20;

		long cacheSize = 1024L * 1024L * 100L + extra;
		if ( cacheSize > 1024L * 1024L * 1024L * 1L )
			cacheSize = 1024L * 1024L * 1024L * 1L;

		cache = new LRU_lbt( cacheSize );
		cache.name = "ObjectServer.cache";
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


	// Note that these caches only work when the server is the default owning server
	// for the object in question
	// This is to prevent an out of date cache taking precedent on a server
	// which is only occasionally the current owning server and which might not have
	// the newest version of the object
	// ================================================
	void ____CACHE________()
		{
		}


	private static void cacheObject( long id, byte[] bytes, long timestamp )
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


	private static PairBL getObjectFromCache( long id )
		{
		synchronized ( cache )
			{
			return cache.get( id );
			}
		}


	void ______SERVER________()
		{
		}


	// The Message used as an argument is crafted by Ganesha
	// Note that synchronization is not required in an explicit get object message
	// Lack of synchronization may result in some occasional unnecessary updating
	// of objects (as it's possible that the get happens when only some of the objects
	// have the current value and updates happen when data is not unanimously similar)
	// This will be okay though as the timestamp is used to determine the most
	// recent object to be returned
	private static PairBL getObjectWithTimestamp( long objectID, boolean sendUpdates ) throws Throwable
		{
		Queue set = MapServer.returnOwningSet( objectID );

		// Only use the cache if the set is the default owning server for the object
		// As the default owning server it will be the first in line for provding the data
		boolean isDefaultOwningServer = ( (String) set.elementAt( 0 ) ).equals( MapServer.myIP );

		// If this server is the authority for the object, then we can just use the cache and ignore
		// checking the other machines for what they currently have
		// Luckily, this server should generally be the default server as the first  one in the list
		// is used for synchronization (which means this method will get called)
		// If the default owning server is down, then this server my be a secondary or tertiary in which
		// case it has no cache for the given object
		if ( isDefaultOwningServer )
			{
			// Note that when an object is in the cache, it may prevent the auto-healing checking in which
			// servers are updated when they do not have the most recent copy of the object
			// 
			PairBL p = getObjectFromCache( objectID );
			if ( p != null )
				{
				//				System.out.println( "bytes in cache" );
				return new PairBL( p.x, p.y );
				}
			}

		// This should not be called unless the default server is down
		//		System.out.println( "Not using the cache!!!!: " + objectID );

		Fashtable_so f = new Fashtable_so();
		long newestTimestamp = 0;
		TriOLI newestT = null;

		// Pull the current objects from the given set
		Queue objects = new Queue();
		for ( int i = 0; i < set.size(); i++ )
			{
			String ip = (String) set.elementAt( i );

			if ( !MapServer.serverIsDown( ip ) )
				{
				TriOLI t = HashStoreServer.getObject( ip, objectID );

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
					cacheObject( objectID, (byte[]) newestT.x, newestT.y );

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

				// If the object didn't exist on the server or the checksum is difrerent than the most recent one then 
				// write it to the server
				if ( ( t == null ) || ( t.z != newestT.z ) )
					{
					System.out.println( "SELF-HEALING OBJECT: " + ip + "\t" + objectID );
					WriteManager.putObjectOrBytes( ip, objectID, (byte[]) newestT.x, false );
					}
				}

		if ( isDefaultOwningServer )
			cacheObject( objectID, (byte[]) newestT.x, newestT.y );

		return new PairBL( (byte[]) newestT.x, newestT.y );
		}


	private static byte[] getObject( long objectID ) throws Throwable
		{
		PairBL p = getObjectWithTimestamp( objectID, true );

		return p.x;
		}


	private void modifyObjectAttribute( int requestType, Gob gob, Message m ) throws Throwable
		{
		switch ( requestType )
			{
			case SET_INTEGER:
				{
				int attribute = m.read4Bytes();
				int n = m.read4Bytes();

				gob.f.put( attribute, n );
				}
			break;

			case SET_LONG:
				{
				int attribute = m.read4Bytes();
				long n = m.read8Bytes();

				gob.f.put( attribute, n );
				}
			break;

			case SET_STRING:
				{
				int attribute = m.read4Bytes();
				String s = m.readString();

				gob.f.put( attribute, s );
				}
			break;

			case INCREMENT:
				{
				int attribute = m.read4Bytes();

				int v = 0;
				try
					{
					v = (Integer) gob.f.get( attribute );
					}
				catch ( Throwable theX )
					{
					}

				v++;
				gob.f.put( attribute, v );
				}
			break;

			case DECREMENT:
				{
				int attribute = m.read4Bytes();

				int v = (Integer) gob.f.get( attribute );
				v--;
				gob.f.put( attribute, v );
				}
			break;
			}
		}


	// The Message used as an argument is crafted by Ganesha
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

		synchronized ( lock )
			{
			// After this switch statement, bytes should contain the actual bytes
			// that need to but attached to the given objectID
			// or null if no update needs to be made (as with LEAF_NODES)
			switch ( requestType )
				{
				case PUT_OBJECT:
					{
					bytes = m.readBytes();
					}
				break;

				default: // attribute modification
					{
					bytes = getObject( objectID );

					// Modify the object
					// No need to specify gobType as we'll be explicitly modifying the attributes
					Gob gob = new Gob( 0, bytes );

					byte[] bytes0 = gob.returnBytes();
					modifyObjectAttribute( requestType, gob, m );

					bytes = gob.returnBytes();

					PairBL p = getObjectWithTimestamp( objectID, false );

					//					System.out.println( "PRE/POST LENGTH: " + objectID + "\t" + requestType + "\t" + bytes0.length + "\t" + bytes.length + "\t" + p.y );
					r.writeBytes( bytes );
					}
				break;
				}

			if ( bytes != null )
				{
				// Only use the cache if the set is the default owning server for the object
				boolean isDefaultOwningServer = ( (String) set.elementAt( 0 ) ).equals( MapServer.myIP );
				if ( isDefaultOwningServer )
					{
					//					System.out.println( "UPDATING CACHE: " + objectID );
					cacheObject( objectID, bytes, System.currentTimeMillis() );
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

					WriteManager.putObjectOrBytes( ip, objectID, bytes, false );
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
					case GET_OBJECT:
						{
						objectID = m.read8Bytes();

						byte[] bytes = getObject( objectID );
						r.writeBytes( bytes );
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
				cota.util.Util.printX( theX );
				out.writeByte( NOT_FOUND );
				out.flush();
				}
			catch ( Throwable theX )
				{
				cota.util.Util.printX( "ERROR IN ObjectServer.handleRequest objectID: " + objectID, theX );
				out.writeByte( 0 );
				out.flush();

				//				throw theX;
				}
			}
		}


	// Needs to have variable args and results
	public static Message sendRequest( int requestType, Message args ) throws Throwable
		{
		if ( requestType == GET_OBJECT )
			readCount++;
		else
			modCount++;

		if ( ( ( readCount + modCount ) % 10000 ) == 0 )
			{
			long elapsed = System.currentTimeMillis() - startTime;
			double avgRead = readCount * 1.0 / elapsed * 1000.0 * 60 * 60;
			double avgMods = modCount * 1.0 / elapsed * 1000.0 * 60 * 60;

			System.out.println( "ObjectServer: " + (int) avgRead + " read/h\t: " + (int) avgMods + " mods/h" );
			}

		// Combine this with Tools analyze to determine which calls are yielding the most writes
		if ( debug )
			if ( requestType != GET_OBJECT )
				Util.printStackTrace( "\n2ObjectServer.sendRequest " + requestType );

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

					System.out.println( "Previous ObjectServer connection no longer valid: " + ip );
					}
				catch ( NotFoundException theX )
					{
					System.out.println( "ObjectServer.NotFoundException: " + objectID );
					c.close();

					throw theX;
					}
				catch ( Throwable theX )
					{
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
