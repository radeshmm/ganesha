package cota.ganesha;

import java.net.InetAddress;
import java.net.Socket;

import cota.io.InStream;
import cota.io.Message;
import cota.io.OutStream;
import cota.networking.Connection;
import cota.networking.ConnectionPool;
import cota.networking.TCPServer;
import cota.objectstore.MemoryStoreServer;
import cota.util.ErrorHandler;
import cota.util.Fashtable_so;
import cota.util.LRU_lbt;
import cota.util.LRU_lo;
import cota.util.PairBL;
import cota.util.Queue;
import cota.util.Util;


// Caching here (on the default authority server) means that
// lookups on the other servers in the set can be avoided
public class MemoryServer extends TCPServer
	{
	static final int PORT = 20005;

	public static boolean debug = false;

	static long startTime = System.currentTimeMillis();
	static int modCount = 0;
	static int readCount = 0;

	// Object requests
	static final int APPEND_BYTES = 11;
	static final int GET_BYTES = 12;
	static final int INCREMENT_INT = 13;
	static final int GET_INT = 14;
	static final int SET_INT = 16;

	static final int NOT_FOUND = 50;
	static final int SUCCESS = 99;

	private LRU_lo locks = null;

	private static LRU_lbt cache = null;

	static int count = 0;


	public MemoryServer() throws Throwable
		{
		super( "MemoryServer", PORT );

		// No socket timeout
		socketTimeout = 0;

		// Used for locking the objects based on objectNum
		// 200MB will allow about 2 million locks (the limit of objects 
		// being modified simultaneously on one synced machine)
		locks = new LRU_lo( 1024L * 1024L * 200L );
		locks.name = "MemoryServer.locks";

		long heapSize = Runtime.getRuntime().totalMemory();
		long extra = ( heapSize - 1024L * 1024L * 1024L * 6L ) / 10;

		cache = new LRU_lbt( 1024L * 1024L * 100L + extra );

		cache.name = "MemoryServer.cache";
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


	void ______INTERNAL________()
		{
		}


	private static byte[] appendBytesWithMax( long id, byte[] bytesToAppend, int max ) throws Throwable
		{
		Queue q = new Queue();

		try
			{
			byte[] bytes0 = getBytes( id );

			Message m = new Message( bytes0 );

			int numObjects = m.read4Bytes( 0 );

			for ( int i = 0; i < numObjects; i++ )
				{
				byte[] bytes = m.readBytes();

				q.addObject( bytes );
				}
			}
		catch ( NotFoundException theX )
			{
			}

		q.addObject( bytesToAppend );

		if ( q.size() > max )
			q.removeFirst();

		Message newM = new Message();
		newM.write4Bytes( q.size() );

		for ( int i = 0; i < q.size(); i++ )
			{
			byte[] bytes = (byte[]) q.elementAt( i );

			newM.writeBytes( bytes );
			}

		return newM.returnBytes();
		}


	private static byte[] incrementCounter( long id ) throws Throwable
		{
		int counter = 0;
		byte[] bytes = null;

		try
			{
			bytes = getBytes( id );

			int c1 = bytes[ 0 ];
			int c2 = bytes[ 1 ];
			int c3 = bytes[ 2 ];
			int c4 = bytes[ 3 ];

			if ( c1 < 0 )
				c1 = c1 + 256;
			if ( c2 < 0 )
				c2 = c2 + 256;
			if ( c3 < 0 )
				c3 = c3 + 256;
			if ( c4 < 0 )
				c4 = c4 + 256;

			counter = ( ( c1 << 24 ) + ( c2 << 16 ) + ( c3 << 8 ) + c4 );
			}
		catch ( NotFoundException theX )
			{
			bytes = new byte[4];
			}

		counter++;

		bytes[ 0 ] = (byte) ( ( counter >> 24 ) & 0xFF );
		bytes[ 1 ] = (byte) ( ( counter >> 16 ) & 0xFF );
		bytes[ 2 ] = (byte) ( ( counter >> 8 ) & 0xFF );
		bytes[ 3 ] = (byte) ( counter & 0xFF );

		return bytes;
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
				return new PairBL( p.x, p.y );
			}

		Fashtable_so f = new Fashtable_so();
		long newestTimestamp = 0;
		PairBL newestP = null;

		// Pull the current objects from the given set
		Queue objects = new Queue();
		for ( int i = 0; i < set.size(); i++ )
			{
			String ip = (String) set.elementAt( i );

			if ( !MapServer.serverIsDown( ip ) )
				{
				PairBL p = MemoryStoreServer.getBytes( ip, objectID );

				if ( p != null )
					{
					long timestamp = p.y;

					// Used later to determine if the server needs updating
					f.put( ip, p );

					if ( timestamp > newestTimestamp )
						{
						newestTimestamp = timestamp;
						newestP = p;
						}

					objects.addObject( p );
					}
				}
			}

		// See how many objects hold the newest object value
		// MemoryStore doesn't use checksum, so any server holding the value
		// will be consider to have the correct one
		if ( objects.size() == set.size() ) // only explicit success if all servers have the same
			{
			if ( isDefaultOwningServer )
				cacheBytes( objectID, newestP.x, newestP.y );

			return new PairBL( newestP.x, newestP.y );
			}

		if ( newestP == null )
			throw new NotFoundException();

		// Update the servers that didn't have the value
		if ( sendUpdates )
			for ( int i = 0; i < set.size(); i++ )
				{
				String ip = (String) set.elementAt( i );
				PairBL t = (PairBL) f.get( ip );

				// Info doesn't exist?
				if ( t == null )
					WriteManager.putMemoryBytes( ip, objectID, newestP.x );
				}

		if ( isDefaultOwningServer )
			cacheBytes( objectID, newestP.x, newestP.y );

		return new PairBL( newestP.x, newestP.y );
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
				case APPEND_BYTES:
					{
					// 					System.out.println( "MemoryServer.APPEND_BYTES: " + objectID );
					byte[] bytesToAppend = m.readBytes();
					int max = m.read4Bytes();

					bytes = appendBytesWithMax( objectID, bytesToAppend, max );
					}
				break;

				case INCREMENT_INT:
					{
					bytes = incrementCounter( objectID );

					int c1 = bytes[ 0 ];
					int c2 = bytes[ 1 ];
					int c3 = bytes[ 2 ];
					int c4 = bytes[ 3 ];

					if ( c1 < 0 )
						c1 = c1 + 256;
					if ( c2 < 0 )
						c2 = c2 + 256;
					if ( c3 < 0 )
						c3 = c3 + 256;
					if ( c4 < 0 )
						c4 = c4 + 256;

					int value = ( ( c1 << 24 ) + ( c2 << 16 ) + ( c3 << 8 ) + c4 );

					r.write4Bytes( value );
					}
				break;

				case SET_INT:
					{
					int value = m.read4Bytes();

					bytes = new byte[4];

					bytes[ 0 ] = (byte) ( ( value >> 24 ) & 0xFF );
					bytes[ 1 ] = (byte) ( ( value >> 16 ) & 0xFF );
					bytes[ 2 ] = (byte) ( ( value >> 8 ) & 0xFF );
					bytes[ 3 ] = (byte) ( value & 0xFF );
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

					WriteManager.putMemoryBytes( ip, objectID, bytes );
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

					case GET_INT:
						{
						objectID = m.read8Bytes();

						byte[] bytes = getBytes( objectID );

						int c1 = bytes[ 0 ];
						int c2 = bytes[ 1 ];
						int c3 = bytes[ 2 ];
						int c4 = bytes[ 3 ];

						if ( c1 < 0 )
							c1 = c1 + 256;
						if ( c2 < 0 )
							c2 = c2 + 256;
						if ( c3 < 0 )
							c3 = c3 + 256;
						if ( c4 < 0 )
							c4 = c4 + 256;

						int value = ( ( c1 << 24 ) + ( c2 << 16 ) + ( c3 << 8 ) + c4 );

						r.write4Bytes( value );
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
				cota.util.Util.printX( "ERROR IN MemoryServer.handleRequest objectID: " + objectID, theX );
				out.writeByte( 0 );
				out.flush();

				//				throw theX;
				}
			}
		}


	// Needs to have variable args and results
	public static Message sendRequest( int requestType, Message args ) throws Throwable
		{
		if ( requestType == GET_BYTES )
			readCount++;
		else
			modCount++;

		if ( ( ( readCount + modCount ) % 1000 ) == 0 )
			{
			long elapsed = System.currentTimeMillis() - startTime;
			double avgRead = readCount * 1.0 / elapsed * 1000.0 * 60 * 60;
			double avgMods = modCount * 1.0 / elapsed * 1000.0 * 60 * 60;

			System.out.println( "MemoryServer: " + (int) avgRead + " read/h\t: " + (int) avgMods + " mods/h" );
			}

		// Combine this with Tools analyze to determine which calls are yielding the most writes
		if ( debug )
			if ( requestType != GET_BYTES )
				Util.printStackTrace( "\nMemoryServer.sendRequest " + requestType );

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

					System.out.println( "Previous MemoryServer connection no longer valid: " + ip );
					}
				catch ( NotFoundException theX )
					{
					Util.printStackTrace( "MemoryServer.NotFoundException: " + objectID );
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


	// This call will contact all of the other servers
	public static void rebuildServer()
		{
		Queue ips = MapServer.returnAllIPs();

		for ( int i = 0; i < ips.size(); i++ )
			{
			String ip = (String) ips.elementAt( i );

			//			System.out.println( "FOO: " + MapServer.myIP + "\t" + ip );

			if ( !ip.equals( MapServer.myIP ) )
				MemoryStoreServer.rebuildIP( ip );
			}
		}
	}
