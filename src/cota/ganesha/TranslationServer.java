package cota.ganesha;

import java.net.InetAddress;
import java.net.Socket;

import cota.crypto.Murmur;
import cota.io.InStream;
import cota.io.Message;
import cota.io.OutStream;
import cota.networking.Connection;
import cota.networking.ConnectionPool;
import cota.networking.TCPServer;
import cota.objectstore.HashStoreServer;
import cota.util.ErrorHandler;
import cota.util.FashEntry_li;
import cota.util.FashEntry_sl;
import cota.util.Fashtable_li;
import cota.util.Fashtable_sl;
import cota.util.LRU_sl;
import cota.util.LRU_so;
import cota.util.PairLI;
import cota.util.PairSI;
import cota.util.Queue;
import cota.util.Util;


public class TranslationServer extends TCPServer
	{
	static final int PORT = 20004;

	public static boolean debug = false;

	static long startTime = System.currentTimeMillis();
	static int translationCount = 0;

	// Object requests
	static final int TRANSLATE = 4;

	static final int NOT_FOUND = 50;
	static final int SUCCESS = 99;

	// Constants
	static int kMinimumTranslationConsensusNum = 1;

	private LRU_so locks = null;

	private static LRU_sl cache = null;

	static int count = 0;


	public TranslationServer() throws Throwable
		{
		super( "TranslationServer", PORT );

		// No socket timeout
		socketTimeout = 0;

		// Used for locking the translations based on key
		locks = new LRU_so( 1024L * 1024L * 100L );
		locks.name = "TranslationServer.locks";

		cache = new LRU_sl( 1024L * 1024L * 100L );
		cache.name = "TranslationServer.cache";
		}


	// ================================================
	void ____INTERNAL________()
		{
		}


	private Object returnLock( String key )
		{
		Object lock = null;

		synchronized ( locks )
			{
			lock = locks.get( key );

			if ( lock == null )
				{
				lock = new Object();
				locks.put( key, lock );
				}
			}

		return lock;
		}


	void ______SERVER________()
		{
		}


	// Returns PairLI( objectID, count )
	private static PairLI checkTranslationConsistency( Queue ids )
		{
		Fashtable_li f = new Fashtable_li();

		for ( int i = 0; i < ids.size(); i++ )
			{
			long id = (Long) ids.elementAt( i );

			if ( id != 0 ) // Don't include non-existent translations
				{
				int count = f.get( id );
				if ( count == Fashtable_li.NOT_FOUND )
					f.put( id, 1 );
				else
					f.put( id, count + 1 );
				}
			}

		// Determine which id had the highest count
		int highestCount = 0;
		long bestID = 0;

		FashEntry_li[] entries = f.returnArrayOfEntries();
		for ( int i = 0; i < entries.length; i++ )
			{
			FashEntry_li entry = entries[ i ];

			if ( entry.value > highestCount )
				{
				highestCount = entry.value;
				bestID = entry.key;
				}
			}

		return new PairLI( bestID, highestCount );
		}


	private long handleTranslation( String key, boolean createNew ) throws Throwable
		{
		//		System.out.println( "handleTranslation: " + key + "\t" + createNew );
		Object lock = returnLock( key );

		// Only use the cache if the set is the default owning server for the object
		//		Queue defaultS = MapServer.returnOwningSet( key, 0, Ganesha.kSetSize, true );

		synchronized ( lock )
			{
			// For translation the set of all possible servers is looked at to select the set from (using a hashed key)
			// However it's possible that servers were added recently and that we'll have to 
			// go back to a previous set to find the translation that we're looking for
			// It's even possible that we'll have to go back all the way to the beginning
			int timeBack = 0;

			long correctID = -1;
			Fashtable_sl mostRecentSetF = null;
			Queue serversToRemove = null;

			Fashtable_sl cache = new Fashtable_sl();

			while ( true )
				{
				Queue set = MapServer.returnTranslationOwningSet( key, timeBack );
				if ( set == null )
					{
					//					System.out.println( "Should create new ID" );

					// Wasn't found anywhere in the map
					// Need to add a new value
					correctID = 0;

					if ( createNew )
						correctID = Ganesha.newID();

					break;
					}

				// See what the set and the given timeBack thinks the translation is
				Queue ids = new Queue();
				for ( int i = 0; i < set.size(); i++ )
					{
					String ip = (String) set.elementAt( i );

					if ( !MapServer.serverIsDown( ip ) )
						{
						long id = cache.get( ip );
						if ( id == Fashtable_sl.NOT_FOUND )
							{
							// Non existent translations show up with a value of 0
							id = HashStoreServer.getTranslation( ip, key );

							cache.put( ip, id );
							}

						if ( id != 0 )
							ids.addObject( id );
						}
					}

				PairLI p = checkTranslationConsistency( ids );

				//				System.out.println( "p.y: " + p.y );

				// Require at least a minimum number of ids to have a "correct value"
				if ( p.y >= kMinimumTranslationConsensusNum )
					correctID = p.x;

				if ( timeBack == 0 )
					{
					// If the recent servers have a unanimous value, then we're done
					if ( p.y == set.size() )
						{
						return p.x;
						}
					else
						{
						// Looks like there was a problem within the recent set
						mostRecentSetF = new Fashtable_sl();
						serversToRemove = new Queue();

						for ( int i = 0; i < set.size(); i++ )
							{
							String ip = (String) set.elementAt( i );

							long id = cache.get( ip );

							mostRecentSetF.put( ip, id );
							}
						}
					}
				else
					{
					// Looks like we're looking at an older set
					// Remove the value from servers in the older set that are not also
					// in the most recent set
					for ( int i = 0; i < set.size(); i++ )
						{
						String ip = (String) set.elementAt( i );

						long id = cache.get( ip );

						// Remove the id from the older server?
						if ( id != 0 )
							{
							// Make sure the ip is not in the most recent set
							if ( mostRecentSetF.get( ip ) == Fashtable_sl.NOT_FOUND )
								serversToRemove.addObject( ip );
							}
						}
					}

				//				System.out.println( "p.y: " + p.y );

				if ( p.y >= kMinimumTranslationConsensusNum )
					break;

				timeBack++;
				}

			// Update the recent servers
			FashEntry_sl[] entries = mostRecentSetF.returnArrayOfEntries();

			boolean errorSeen = false;
			for ( int i = 0; i < entries.length; i++ )
				{
				FashEntry_sl entry = entries[ i ];

				try
					{
					if ( entry.value != correctID )
						WriteManager.putTranslation( entry.key, key, correctID );
					}
				catch ( Throwable theX )
					{
					cota.util.Util.printX( theX );
					errorSeen = true;
					}
				}

			// If no error was encountered, then remove translations that were
			// incorrectly placed
			if ( !errorSeen )
				{
				for ( int i = 0; i < serversToRemove.size(); i++ )
					{
					String ip = (String) serversToRemove.elementAt( i );

					HashStoreServer.removeTranslation( ip, key );
					}
				}

			return correctID;
			}
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
		String key = null;

		while ( true )
			{
			try
				{
				int requestType = in.read4Bytes();
				Message m = in.readMessage();

				Message r = new Message();

				switch ( requestType )
					{
					case TRANSLATE:
						{
						key = m.readString();
						int createNew = m.readByte();

						long id = handleTranslation( key, ( createNew == 1 ) );

						r.write8Bytes( id );
						}
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
				cota.util.Util.printX( "ERROR IN TranslationServer.handleRequest objectID: " + objectID + "\tkey: " + key, theX );
				out.writeByte( 0 );
				out.flush();

				//				throw theX;
				}
			}
		}


	// Needs to have variable args and results
	public static Message sendRequest( int requestType, Message args ) throws Throwable
		{
		translationCount++;

		if ( ( translationCount % 1000 ) == 0 )
			{
			long elapsed = System.currentTimeMillis() - startTime;
			double avgCount = translationCount / elapsed * 1000.0 * 60 * 60;

			System.out.println( "TranslationServer: " + (int) avgCount + " translation/h" );
			}

		if ( debug )
			Util.printStackTrace( "\nTranslationServer.sendRequest " + requestType );

		// For translations the set of servers for synching (which happens on the server which 
		// handles the call we are about to make) is set to be a fixed set (by default all the recent servers)

		// This is because we can't really hash the key and determine a server set (because a hashed value wont
		// have the necessary timestamp needed to determine which servers were active at the given time).

		// It provides a reliable way of having the same server synch the same key all the time
		String key = args.readString( 0 );
		Queue setForSync0 = MapServer.returnAllServersAtGivenTimeBack( 0 );

		// Start at some random point in the Queue (based on the hash of the key)
		// This way the work can be divided evenly among the servers
		Queue setForSync = new Queue();
		int start = ( Murmur.hash( key, 0 ) % setForSync0.size() );

		//		System.out.println( "SET FOR SYNC SIZE: " + setForSync0.size() );
		//		System.out.println( "START: " + start );
		for ( int i = 0; i < setForSync0.size(); i++ )
			setForSync.addObject( setForSync0.elementAt( ( start + i ) % setForSync0.size() ) );

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
					PairSI p = (PairSI) setForSync.elementAt( i );
					ip = p.x;

					//					System.out.println( "SYNCHING: " + key + " " + ip );

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

					System.out.println( "Previous TranslationServer connection no longer valid: " + ip );
					}
				catch ( NotFoundException theX )
					{
					System.out.println( "TranslationServer.NotFoundException: " + key );
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


	public static void main( String[] args )
		{
		try
			{
			Queue q = new Queue();
			for ( int i = 0; i < 5; i++ )
				q.addObject( 100L );

			q.addObject( 101L );
			q.addObject( 102L );
			q.addObject( 102L );

			PairLI p = checkTranslationConsistency( q );
			System.out.println( p.x + "\t" + p.y );
			}
		catch ( Throwable theX )
			{
			cota.util.Util.printX( theX );
			}
		}
	}
