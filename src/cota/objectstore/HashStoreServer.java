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
import cota.util.PairSL;
import cota.util.Queue;
import cota.util.TriOLI;


public class HashStoreServer extends TCPServer
	{
	public static final int PORT = 20001;

	static final int PUT_OBJECT = 0;
	static final int GET_OBJECT = 1;
	static final int PUT_BYTES = 2;
	static final int GET_BYTES = 3;
	static final int PUT_TRANSLATION = 4;
	static final int GET_TRANSLATION = 5;
	static final int REMOVE_TRANSLATION = 6;
	static final int REBUILD_IP = 9;
	static final int NAMES_AND_IDS_IN_TABLE = 10;
	static final int SEEN_ATTRIBUTES = 11;

	public static final int SUCCESS = 99;
	public static final int HEARTBEAT = 999;

	static HashStore objectStore = null;
	static HashStore binaryStore = null;
	static HashStore translationStore = null;

	public static boolean debug = false;


	public HashStoreServer() throws Throwable
		{
		super( "HashStoreServer", PORT );

		// No socket timeout
		socketTimeout = 0;

		long heapSize = Runtime.getRuntime().totalMemory();
		long extra = ( heapSize - 1024L * 1024L * 1024L * 6L ) / 5;

		binaryStore = new HashStore( "data_bytes", 256000000, 1024L * 1024L * 100L + extra );

		long extra2 = ( heapSize - 1024L * 1024L * 1024L * 6L ) / 20;
		long cacheSize = 1024L * 1024L * 100L + extra2;
		if ( cacheSize > 1024L * 1024L * 1024L * 1L )
			cacheSize = 1024L * 1024L * 1024L * 1L;

		objectStore = new HashStore( "data_objects", 256000000, cacheSize );
		translationStore = new HashStore( "data_translations", 25000000, 1024L * 1024L * 100L );
		}


	void ______SERVER________()
		{
		}


	public Message handleRequest( int requestType, Message m ) throws Throwable
		{
		Message r = new Message();

		switch ( requestType )
			{
			case PUT_OBJECT:
				{
				long objectID = m.read8Bytes();
				byte[] bytes = m.readBytes();
				long timestamp = m.read8Bytes();

				objectStore.put( objectID, bytes, timestamp );
				}
			break;

			case GET_OBJECT:
				{
				long objectID = m.read8Bytes();
				TriOLI t = objectStore.get( objectID );

				if ( t == null )
					{
					r.writeBytes( new byte[1] );
					r.write8Bytes( 0 ); // timestamp
					r.write4Bytes( 0 ); // checksum
					}
				else
					{
					r.writeBytes( (byte[]) t.x );
					r.write8Bytes( t.y ); // timestamp
					r.write4Bytes( t.z ); // checksum
					}
				}
			break;

			case PUT_TRANSLATION:
				{
				String key = m.readString();
				long id = m.read8Bytes();

				translationStore.put( key, id );
				}
			break;

			case GET_TRANSLATION:
				{
				String key = m.readString();
				long id = translationStore.get( key );

				r.write8Bytes( id );
				}
			break;

			case PUT_BYTES:
				{
				long objectID = m.read8Bytes();
				byte[] bytes = m.readBytes();
				long timestamp = m.read8Bytes();

				binaryStore.put( objectID, bytes, timestamp );
				}
			break;

			case GET_BYTES:
				{
				long objectID = m.read8Bytes();
				TriOLI t = binaryStore.get( objectID );

				if ( t == null )
					{
					r.writeBytes( new byte[1] );
					r.write8Bytes( 0 ); // timestamp
					r.write4Bytes( 0 ); // checksum
					}
				else
					{
					r.writeBytes( (byte[]) t.x );
					r.write8Bytes( t.y ); // timestamp
					r.write4Bytes( t.z ); // checksum
					}
				}
			break;

			case REMOVE_TRANSLATION:
				{
				String key = m.readString();

				translationStore.remove( key );
				}
			break;

			case REBUILD_IP:
				{
				String ip = m.readString();

				objectStore.rebuildIP( ip );
				binaryStore.rebuildIP( ip );
				translationStore.rebuildIP( ip );
				}
			break;

			case NAMES_AND_IDS_IN_TABLE:
				{
				String workspace = m.readString();
				String table = m.readString();

				Queue namesAndIDs = translationStore.namesAndIDsInTable( workspace, table );

				r.write4Bytes( namesAndIDs.size() );
				for ( int i = 0; i < namesAndIDs.size(); i++ )
					{
					PairSL p = (PairSL) namesAndIDs.elementAt( i );

					r.writeString( p.x );
					r.write8Bytes( p.y );
					}
				}
			break;

			case SEEN_ATTRIBUTES:
				{
				int gobType = m.read4Bytes();
				int attribute = m.read4Bytes();

				Queue attributes = objectStore.returnAllAttributes( gobType, attribute );

				r.write4Bytes( attributes.size() );
				for ( int i = 0; i < attributes.size(); i++ )
					{
					PairSL p = (PairSL) attributes.elementAt( i );

					r.writeString( p.x );
					r.write8Bytes( p.y );
					}
				}
			break;

			case HEARTBEAT:
				{
				}
			break;
			}

		// Don't write empty messages
		if ( r.index == 0 )
			r.writeByte( 0 );

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
			catch ( java.net.SocketException theX )
				{
				// Sometimes connections to the Server will break
				// Just exit the connection loop which will close the connection in the client
				// without returning it to the ConnectionPool
				break;
				}
			catch ( Throwable theX )
				{
				System.out.println( "REQUEST FROM " + ip0 + " FAILED" );

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

				System.out.println( "Previous HashStoreServer connection no longer valid: " + ip );
				}
			}

		throw new Throwable( "UNABLE TO CONNECT TO " + ip );
		}


	void ______API________()
		{
		}


	public static void putObject( String ip, long objectID, byte[] bytes ) throws Throwable
		{
		if ( debug )
			{
			System.out.println( "HashStoreServer.putObject: " + ip + "\t" + objectID + "\t" + bytes.length );

			//			Util.printStackTrace( "" );
			}

		Message m = new Message();
		m.write8Bytes( objectID );
		m.writeBytes( bytes );

		// Timestamp
		m.write8Bytes( System.currentTimeMillis() );

		sendRequest( ip, PUT_OBJECT, m );
		}


	public static void putObject( String ip, long objectID, byte[] bytes, long timestamp ) throws Throwable
		{
		if ( debug )
			{
			System.out.println( "HashStoreServer.putObject: " + ip + "\t" + objectID + "\t" + bytes.length );

			//			Util.printStackTrace( "" );
			}

		Message m = new Message();
		m.write8Bytes( objectID );
		m.writeBytes( bytes );
		m.write8Bytes( timestamp );

		sendRequest( ip, PUT_OBJECT, m );
		}


	public static TriOLI getObject( String ip, long objectID )
		{
		try
			{
			Message m = new Message();
			m.write8Bytes( objectID );

			Message r = sendRequest( ip, GET_OBJECT, m );

			byte[] bytes = r.readBytes();
			long timestamp = r.read8Bytes();
			int checksum = r.read4Bytes();

			if ( timestamp == 0 )
				return null;

			return new TriOLI( bytes, timestamp, checksum );
			}
		catch ( Throwable theX )
			{
			cota.util.ErrorHandler.error( "ERROR GETTING AN OBJECT", theX );
			}

		return null;
		}


	public static TriOLI getBytes( String ip, long objectID )
		{
		try
			{
			Message m = new Message();
			m.write8Bytes( objectID );

			Message r = sendRequest( ip, GET_BYTES, m );

			byte[] bytes = r.readBytes();
			long timestamp = r.read8Bytes();
			int checksum = r.read4Bytes();

			if ( timestamp == 0 )
				return null;

			return new TriOLI( bytes, timestamp, checksum );
			}
		catch ( Throwable theX )
			{
			cota.util.ErrorHandler.error( "ERROR GETTING AN OBJECT", theX );
			}

		return null;
		}


	public static void putBytes( String ip, long objectID, byte[] bytes ) throws Throwable
		{
		if ( debug )
			{
			System.out.println( "HashStoreServer.putBytes: " + ip + "\t" + objectID + "\t" + bytes.length );

			//			Util.printStackTrace( "" );
			}

		Message m = new Message();
		m.write8Bytes( objectID );
		m.writeBytes( bytes );

		// Timestamp
		m.write8Bytes( System.currentTimeMillis() );

		sendRequest( ip, PUT_BYTES, m );
		}


	public static void putBytes( String ip, long objectID, byte[] bytes, long timestamp ) throws Throwable
		{
		if ( debug )
			{
			System.out.println( "HashStoreServer.putBytes: " + ip + "\t" + objectID + "\t" + bytes.length );

			//			Util.printStackTrace( "" );
			}

		Message m = new Message();
		m.write8Bytes( objectID );
		m.writeBytes( bytes );
		m.write8Bytes( timestamp );

		sendRequest( ip, PUT_BYTES, m );
		}


	public static void putTranslation( String ip, String key, long objectID ) throws Throwable
		{
		if ( debug )
			System.out.println( "HashStoreServer.putTranslation: " + ip + "\t" + key + "\t" + objectID );

		Message m = new Message();
		m.writeString( key );
		m.write8Bytes( objectID );

		sendRequest( ip, PUT_TRANSLATION, m );
		}


	public static long getTranslation( String ip, String key )
		{
		try
			{
			if ( debug )
				System.out.println( "HashStoreServer.getTranslation: " + ip + "\t" + key );

			Message m = new Message();
			m.writeString( key );

			Message r = sendRequest( ip, GET_TRANSLATION, m );

			long objectID = r.read8Bytes();

			return objectID;
			}
		catch ( Throwable theX )
			{
			cota.util.ErrorHandler.error( "ERROR GETTING A TRANSLATION", theX );
			}

		return 0;
		}


	public static void removeTranslation( String ip, String key ) throws Throwable
		{
		if ( debug )
			System.out.println( "HashStoreServer.removeTranslation: " + ip + "\t" + key );

		Message m = new Message();
		m.writeString( key );

		sendRequest( ip, REMOVE_TRANSLATION, m );
		}


	public static void rebuildIP( String ip )
		{
		try
			{
			if ( debug )
				System.out.println( "HashStoreServer.rebuildIP: " + ip );

			Message m = new Message();
			m.writeString( ip );

			// Send the request to the same machine that is making the call
			sendRequest( MapServer.myIP, REBUILD_IP, m );

			return;
			}
		catch ( Throwable theX )
			{
			cota.util.ErrorHandler.error( "ERROR REBUILDING IP", theX );
			}
		}


	public static Queue namesAndIDsInTable( String workspace, String table )
		{
		try
			{
			if ( debug )
				System.out.println( "HashStoreServer.namesInTable: " + workspace + "\t" + table );

			Message m = new Message();
			m.writeString( workspace );
			m.writeString( table );

			// Send the request to the same machine that is making the call
			Message r = sendRequest( MapServer.myIP, NAMES_AND_IDS_IN_TABLE, m );

			Queue q = new Queue();

			int num = r.read4Bytes();
			for ( int i = 0; i < num; i++ )
				{
				String name = r.readString();
				long id = r.read8Bytes();

				PairSL p = new PairSL( name, id );

				q.addObject( p );
				}

			return q;
			}
		catch ( Throwable theX )
			{
			cota.util.ErrorHandler.error( "ERROR RETURNING IDS AND KEYS BY IP", theX );
			}

		return null;
		}


	public static Queue seenAttributes( String ip, int gobType, int attribute )
		{
		try
			{
			if ( debug )
				System.out.println( "HashStoreServer.allObjects: " + gobType );

			Message m = new Message();
			m.write4Bytes( gobType );
			m.write4Bytes( attribute );

			// Send the request to the same machine that is making the call
			Message r = sendRequest( ip, SEEN_ATTRIBUTES, m );

			Queue q = new Queue();

			int num = r.read4Bytes();
			for ( int i = 0; i < num; i++ )
				{
				String attributeValue = r.readString();
				long objectID = r.read8Bytes();

				q.addObject( new PairSL( attributeValue, objectID ) );
				}

			return q;
			}
		catch ( Throwable theX )
			{
			cota.util.ErrorHandler.error( "ERROR RETURNING SEEN ATTRIBUTES", theX );
			}

		return null;
		}
	}
