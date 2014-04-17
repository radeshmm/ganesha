package cota.objectstore;

import java.net.InetAddress;
import java.net.Socket;

import cota.ganesha.MapServer;
import cota.io.InStream;
import cota.io.Message;
import cota.io.OutStream;
import cota.networking.Connection;
import cota.networking.ConnectionPool;
import cota.networking.TCPServer;
import cota.util.HashEntry_lo;
import cota.util.LRU_lbt;
import cota.util.PairBL;
import cota.util.Queue;
import cota.util.StringUtils;


// All in-memory store analygous to the HashStoreServer
// Note that memory stores only persist in memory as long as one server in an owning group still holds the data.
// If all servers owning a piece of data are down simultaneqously, the data is lost

public class MemoryStoreServer extends TCPServer
	{
	public static final int PORT = 20000;

	static final int PUT_BYTES = 0;
	static final int GET_BYTES = 1;
	static final int REBUILD_IP = 2;
	static final int PUT_BULK = 3;

	static final int kBulkSendSize = 100;

	public static final int SUCCESS = 99;

	private static LRU_lbt cache = null;

	public static boolean debug = false;


	public MemoryStoreServer() throws Throwable
		{
		super( "MemoryStoreServer", PORT );

		// No socket timeout
		socketTimeout = 0;

		long heapSize = Runtime.getRuntime().totalMemory();
		long extra = ( heapSize - 1024L * 1024L * 1024L * 6L ) / 10;

		cache = new LRU_lbt( 1024L * 1024L * 100L + extra );

		cache.name = "MemoryStoreServer.cache";
		}


	void ______SERVER________()
		{
		}


	private void rebuildIP0( String ip ) throws Throwable
		{
		// Go through the cache and and find objects owned by the given ip
		Queue q = new Queue();

		synchronized ( cache )
			{
			HashEntry_lo[] entries = cache.f.returnArrayOfEntries();

			for ( int i = 0; i < entries.length; i++ )
				{
				LRU_lbt.CacheObject o = (LRU_lbt.CacheObject) entries[ i ].value;

				q.addObject( o );
				}
			}

		Queue objectsToPut = new Queue();

		for ( int i = 0; i < q.size(); i++ )
			{
			LRU_lbt.CacheObject o = (LRU_lbt.CacheObject) q.elementAt( i );

			Queue owningSet = MapServer.returnOwningSet( o.key );

			for ( int k = 0; k < owningSet.size(); k++ )
				{
				String ip0 = (String) owningSet.elementAt( k );
				if ( ip0.equals( ip ) )
					{
					objectsToPut.addObject( o );

					if ( objectsToPut.size() > kBulkSendSize )
						{
						MemoryStoreServer.putBytesBulk( ip, objectsToPut );

						objectsToPut = new Queue();
						}
					}
				}

			if ( ( i % 2560000 ) == 0 )
				System.out.println( "Sending memory bytes to " + ip + ": " + StringUtils.formatPrice( 100.0 * i / q.size() ) + "%" );
			}

		if ( objectsToPut.size() != 0 )
			MemoryStoreServer.putBytesBulk( ip, objectsToPut );

		System.out.println( "Sending memory bytes to " + ip + ": done" );
		}


	protected Message handleRequest( int requestType, Message m ) throws Throwable
		{
		Message r = new Message();

		switch ( requestType )
			{
			case PUT_BYTES:
				{
				long objectID = m.read8Bytes();
				byte[] bytes = m.readBytes();
				long timestamp = m.read8Bytes();

				synchronized ( cache )
					{
					cache.put( objectID, bytes, timestamp );
					}
				}
			break;

			case PUT_BULK:
				{
				int num = m.read4Bytes();

				for ( int i = 0; i < num; i++ )
					{
					long objectID = m.read8Bytes();
					byte[] bytes = m.readBytes();
					long timestamp = m.read8Bytes();

					synchronized ( cache )
						{
						cache.put( objectID, bytes, timestamp );
						}
					}
				}
			break;

			case GET_BYTES:
				{
				long objectID = m.read8Bytes();
				PairBL t = null;

				synchronized ( cache )
					{
					t = cache.get( objectID );
					}

				if ( t == null )
					{
					r.writeBytes( new byte[1] );
					r.write8Bytes( 0 ); // timestamp
					}
				else
					{
					r.writeBytes( t.x );
					r.write8Bytes( t.y ); // timestamp
					}
				}
			break;

			case REBUILD_IP:
				{
				String ip = m.readString();

				try
					{
					rebuildIP0( ip );
					}
				catch ( Throwable theX )
					{
					System.out.println( "ERROR REBUILDING IP: " + ip );
					cota.util.Util.printX( theX );
					}
				}
			break;
			}

		// Don't write empty messages
		if ( r.index == 0 )
			r.writeByte( 0 );

		return r;
		}


	protected void handleRequest( Socket s, InStream in, OutStream out ) throws Throwable
		{
		InetAddress address = s.getInetAddress();
		String ip0 = address.getHostAddress();

		if ( !MapServer.isValidIP( ip0 ) )
			{
			System.out.println( "!!!!!!!  ATTEMPT TO ACCESS OBJECT SERVER FROM INVALID IP: " + ip0 );
			return;
			}

		// Allow connection to be reused
		while ( true )
			{
			try
				{
				int requestType = in.read4Bytes();
				Message m = in.readMessage();

				Message result = handleRequest( requestType, m );

				out.writeByte( SUCCESS );
				out.writeMessage( result );
				out.flush();
				}
			catch ( cota.io.StreamClosedX theX )
				{
				// Sometimes connections to the Server will break
				// Just exit the connection loop which will close the connection in the client
				// without returning it to the ConnectionPool
				break;
				}
			catch ( Throwable theX )
				{
				cota.util.Util.printX( theX );
				out.writeByte( 0 );
				out.flush();
				}
			}
		}


	// Needs to have variable m and results
	private static Message sendRequest( String ip, int requestType, Message m ) throws Throwable
		{
		if ( MapServer.serverIsDown( ip ) )
			throw new Throwable( "SERVER IS DOWN: " + ip );

		boolean requireNewConnection = false;
		for ( int i = 0; i < 2; i++ )
			{
			try
				{
				Connection c = ConnectionPool.returnConnection( ip, PORT, requireNewConnection );
				c.out.write4Bytes( requestType );
				c.out.writeMessage( m );

				c.out.flush();

				int status = c.in.readByte();
				if ( status != SUCCESS )
					throw new Throwable( "INVALID STATUS RECEIVED FROM " + ip );

				Message r = c.in.readMessage();

				ConnectionPool.returnConnectionToPool( c );

				return r;
				}
			catch ( cota.io.StreamClosedX theX )
				{
				// Looks like the cached connection we were using is no longer valid
				// Try again with a new connection
				requireNewConnection = true;

				System.out.println( "Previous MemoryStoreServer connection no longer valid: " + ip );
				}
			}

		throw new Throwable( "UNABLE TO CONNECT TO " + ip );
		}


	void ______API________()
		{
		}


	public static void putBytes( String ip, long objectID, byte[] bytes ) throws Throwable
		{
		if ( debug )
			{
			System.out.println( "MemoryStoreServer.putObject: " + ip + "\t" + objectID + "\t" + bytes.length );
			}

		Message m = new Message();
		m.write8Bytes( objectID );
		m.writeBytes( bytes );

		// Timestamp is just now
		m.write8Bytes( System.currentTimeMillis() );

		sendRequest( ip, PUT_BYTES, m );
		}


	public static void putBytes( String ip, long objectID, byte[] bytes, long timestamp ) throws Throwable
		{
		if ( debug )
			System.out.println( "MemoryStoreServer.putObject: " + ip + "\t" + objectID + "\t" + bytes.length );

		Message m = new Message();
		m.write8Bytes( objectID );
		m.writeBytes( bytes );
		m.write8Bytes( timestamp );

		sendRequest( ip, PUT_BYTES, m );
		}


	public static void putBytesBulk( String ip, Queue q ) throws Throwable
		{
		if ( debug )
			System.out.println( "MemoryStoreServer.putBulk: " + ip + "\t" + q.size() );

		Message m = new Message();

		m.write4Bytes( q.size() );
		for ( int i = 0; i < q.size(); i++ )
			{
			LRU_lbt.CacheObject o = (LRU_lbt.CacheObject) q.elementAt( i );

			m.write8Bytes( o.key );
			m.writeBytes( o.bytes );
			m.write8Bytes( o.timestamp );
			}

		sendRequest( ip, PUT_BULK, m );
		}


	public static PairBL getBytes( String ip, long objectID )
		{
		try
			{
			Message m = new Message();
			m.write8Bytes( objectID );

			Message r = sendRequest( ip, GET_BYTES, m );

			byte[] bytes = r.readBytes();
			long timestamp = r.read8Bytes();

			if ( timestamp == 0 )
				return null;

			return new PairBL( bytes, timestamp );
			}
		catch ( Throwable theX )
			{
			cota.util.ErrorHandler.error( "ERROR GETTING BYTES FROM " + ip, theX );
			}

		return null;
		}


	public static void rebuildIP( String ip )
		{
		try
			{
			if ( debug )
				System.out.println( "MemoryStoreServer.returnIDsOnIP: " + ip );

			Message m = new Message();
			m.writeString( MapServer.myIP );

			sendRequest( ip, REBUILD_IP, m );

			return;
			}
		catch ( Throwable theX )
			{
			cota.util.ErrorHandler.error( "ERROR REBUILDING IP", theX );
			}
		}
	}