package cota.networking;

import cota.util.*;


public class ConnectionPool
	{
	static final int MAX_FREE_CONNECTIONS = 2048;
	static final int MAX_CONNECTION_AGE = 1000 * 60 * 5;

	// Maps ip:port to a Queue of Connections
	static Hashtable_so connections = null;

	static Hashtable_sl lastConnectionTimes = null;

	static int numFreeConnections = 0;

	static
		{
		connections = new Hashtable_so();
		lastConnectionTimes = new Hashtable_sl();
		}


	private static String connectionKeyWithOldestConnection()
		{
		long earliestTime = Long.MAX_VALUE;
		String connectionKey = null;

		HashEntry_sl[] entries = lastConnectionTimes.returnArrayOfEntries();
		for ( int i = 0; i <= entries.length; i++ )
			{
			long lastConnectionTime = entries[ i ].value;
			if ( lastConnectionTime < earliestTime )
				{
				earliestTime = lastConnectionTime;
				connectionKey = entries[ i ].key;
				}
			}

		return connectionKey;
		}


	public static void returnConnectionToPool( Connection c ) throws Throwable
		{
		c.s.getOutputStream().flush();

		synchronized ( connections )
			{
			while ( numFreeConnections >= MAX_FREE_CONNECTIONS )
				{
				// Too many connections
				String connectionKey = connectionKeyWithOldestConnection();

				Queue q = (Queue) connections.get( connectionKey );
				q.removeFirst();

				numFreeConnections--;
				}

			// Add the newly returned connection back in for later use
			Queue q = (Queue) connections.get( c.key );

			if ( q == null )
				{
				q = new Queue();

				connections.put( c.key, q );
				}

			q.addObject( c );

			numFreeConnections++;

			c.timeAddedToPool = System.currentTimeMillis();
			}
		}


	public static Connection returnConnection( String ip, int port, boolean requireNew ) throws Throwable
		{
		String key = ip + ":" + port;

		synchronized ( connections )
			{
			if ( !requireNew )
				{
				lastConnectionTimes.put( key, System.currentTimeMillis() );

				Queue q = (Queue) connections.get( key );

				if ( q == null )
					return new Connection( ip, port );

				if ( q.size() == 0 )
					return new Connection( ip, port );

				// Looks like there's an available Connection
				// Make sure it's not too old
				while ( q.size() > 0 )
					{
					numFreeConnections--;

					Connection c = (Connection) q.removeFirst();
					long age = System.currentTimeMillis() - c.timeAddedToPool;

					// If connections are "stale" we wont use them as them might possibly
					// have already been disconnected
					if ( age < MAX_CONNECTION_AGE )
						return c;
					}
				}

			// No fresh connections, just return a new one
			return new Connection( ip, port );
			}
		}


	public static void closeAllConnections()
		{
		HashEntry_so[] entries = connections.returnArrayOfEntries();

		for ( int i = 0; i <= entries.length; i++ )
			{
			Queue q = (Queue) entries[ i ].value;

			for ( int j = 0; j < q.size(); j++ )
				{
				Connection c = (Connection) q.elementAt( j );

				c.close();
				}
			}
		}

	}
