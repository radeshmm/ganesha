package cota.ganesha;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.net.Socket;

import cota.crypto.Murmur;
import cota.io.InStream;
import cota.io.Message;
import cota.io.OutStream;
import cota.networking.TCPServer;
import cota.objectstore.HashStoreServer;
import cota.util.ErrorHandler;
import cota.util.FashEntry_so;
import cota.util.Fashtable_si;
import cota.util.Fashtable_so;
import cota.util.PairOL;
import cota.util.PairSI;
import cota.util.Queue;
import cota.util.StringUtils;
import cota.util.Util;


// When the map is updated it is appended to and a timestamp is used to show
// when the new mapping information should start being used
public class MapServer extends TCPServer implements Runnable
	{
	// Server stuff
	static final int PORT = 20006;

	static final int JOIN_CLUSTER = 0;
	static final int SYNC_MAP = 1;

	static final int SUCCESS = 99;

	static MapServer singleton = null;

	public static String myIP = null;
	static int myServerNum = 0;

	static Queue map = new Queue();
	static Queue replicationFactors = new Queue();

	static Fashtable_so seenIPs = null;

	static Fashtable_si downServers = new Fashtable_si();
	public static boolean shouldMarkServersAsDown = false;


	public MapServer( boolean startServer ) throws Throwable
		{
		super( "MapServer", PORT );

		// No socket timeout
		socketTimeout = 0;

		if ( startServer )
			start();
		}


	public static void init( boolean startServer )
		{
		try
			{
			// The ip_address file will be written by startup.sh
			InStream in = new InStream( "config/ip_address" );
			Queue lines = in.readLines();

			myIP = (String) lines.elementAt( 0 );

			seenIPs = new Fashtable_so();

			singleton = new MapServer( startServer );

			parseMap();

			if ( startServer )
				syncWithOtherServers();

			// Start off by marking all the servers as down
			Queue ips = returnAllIPs();
			for ( int i = 0; i < ips.size(); i++ )
				{
				String ip = (String) ips.elementAt( i );

				synchronized ( downServers )
					{
					downServers.put( ip, 1 );
					}
				}

			// Start the thread which checks the servers
			singleton.checkServers();

			Thread t = new Thread( singleton );
			t.start();

			//			System.out.println( returnReplicationFactorAtGivenTimeBack( 0 ) );
			}
		catch ( Throwable theX )
			{
			//			cota.util.Util.printX( theX );
			}
		}


	private static void parseLines( Queue lines ) throws Throwable
		{
		//		System.out.println( "Parsing server map" );

		int replicationFactor0 = -2;
		long timeInSeconds0 = -2;

		int numSeenServers = 0;
		Fashtable_so seenServers = new Fashtable_so();

		Queue map2 = new Queue();
		Queue replicationFactors2 = new Queue();

		Queue servers = new Queue();
		for ( int i = 0; i < lines.size(); i++ )
			{
			String line = (String) lines.elementAt( i );

			if ( line.startsWith( "#" ) )
				{
				int index = line.indexOf( ":" );

				long timeInSeconds = Long.parseLong( line.substring( 1, index ) );
				int replicationFactor = Integer.parseInt( line.substring( index + 1, line.length() ) );

				if ( servers.size() > 0 )
					{
					PairOL p = new PairOL( servers, timeInSeconds0 );
					map2.addObject( p );

					p = new PairOL( replicationFactor0, timeInSeconds0 );
					replicationFactors2.addObject( p );

					servers = new Queue();
					}

				replicationFactor0 = replicationFactor;
				timeInSeconds0 = timeInSeconds;
				}
			else
				{
				while ( true )
					{
					int index = line.indexOf( "  " );
					if ( index == -1 )
						break;

					line = StringUtils.replace( line, "  ", " " );
					}

				line = StringUtils.replace( line, " ", "\t" );
				//				System.out.println( "#" + line + "#" );

				int index = line.indexOf( "\t" );

				if ( index != -1 )
					{
					String ip = line.substring( 0, index );

					Object o = seenServers.get( ip );
					if ( o == null )
						{
						if ( ip.equals( myIP ) )
							myServerNum = numSeenServers;

						seenServers.put( ip, "" );
						seenIPs.put( ip, "" );

						numSeenServers++;
						}

					int hashWeight = Integer.parseInt( line.substring( index + 1, line.length() ) );

					PairSI p = new PairSI( ip, hashWeight );
					servers.addObject( p );
					}
				}
			}

		if ( servers.size() > 0 )
			{
			PairOL p = new PairOL( servers, timeInSeconds0 );
			map2.addObject( p );

			p = new PairOL( replicationFactor0, timeInSeconds0 );
			replicationFactors2.addObject( p );
			}

		map = map2;
		replicationFactors = replicationFactors2;
		}


	// For a given time, return all the servers and their hashWeights
	private static Queue returnAllServersAtGivenTime( long timeInSeconds )
		{
		PairOL lastP = null;

		// The map should only grow
		// Synchronization is not necessary as the map is changed in a non-time critical way
		for ( int i = 0; i < map.size(); i++ )
			{
			PairOL p = (PairOL) map.elementAt( i );

			if ( p.y > timeInSeconds )
				break;

			lastP = p;
			}

		return (Queue) lastP.x;
		}


	// For a given time, return the replication factor
	private static int returnReplicationFactorAtGivenTime( long timeInSeconds )
		{
		PairOL lastP = null;

		// The map should only grow
		// Synchronization is not necessary as the map is changed in a non-time critical way
		for ( int i = 0; i < replicationFactors.size(); i++ )
			{
			PairOL p = (PairOL) replicationFactors.elementAt( i );

			if ( p.y > timeInSeconds )
				break;

			lastP = p;
			}

		return (Integer) lastP.x;
		}


	// For a given time, return the replicationFactor
	private static int returnReplicationFactorAtGivenTimeBack( int timeBack ) throws Throwable
		{
		//		System.out.println( "returnReplicationFactorAtGivenTimeBack: " + timeBack + "\t" + replicationFactors.size() );

		if ( timeBack >= replicationFactors.size() )
			{
			return -1;
			}

		// The map should only grow
		// Synchronization is not necessary as the map is changed in a non-time critical way
		PairOL p = (PairOL) replicationFactors.elementAt( map.size() - 1 - timeBack );

		return (Integer) p.x;
		}


	// For a given time, return all the servers and their hashWeights
	public static Queue returnAllServersAtGivenTimeBack( int timeBack )
		{
		if ( timeBack >= map.size() )
			return null;

		// The map should only grow
		// Synchronization is not necessary as the map is changed in a non-time critical way
		PairOL p = (PairOL) map.elementAt( map.size() - 1 - timeBack );

		return (Queue) p.x;
		}


	public static boolean isValidIP( String ip )
		{
		return ( seenIPs.get( ip ) != null );
		}


	// For a given id we need to be able to determine 
	// 1 - which servers were active at that time
	// 2 - the set of servers included for the hashed id (taking into account the hashWeights)
	//
	// Note the the sets are not in order, but rather probability likely based on the hashWeight
	// of each id
	//
	// All servers will return the same owning set for each given id
	public static Queue returnOwningSet( long id ) throws Throwable
		{
		if ( seenIPs == null )
			System.out.println( "Need to call MapServer.init()!!!!" );

		long timeInSeconds = Ganesha.timestampFromID( id );

		Queue q = returnAllServersAtGivenTime( timeInSeconds );
		int replicationFactor = returnReplicationFactorAtGivenTime( timeInSeconds );

		if ( q == null )
			return null;

		if ( q.size() < replicationFactor )
			{
			System.out.println( "\tLess than " + replicationFactor + " servers exist at the given time!!!!" );
			System.out.println( "\tChanging set size to: " + q.size() );

			replicationFactor = q.size();
			}

		// Determine the total hashWeight for all servers active at the given time
		int totalHashWeight = 0;
		for ( int i = 0; i < q.size(); i++ )
			{
			PairSI p = (PairSI) q.elementAt( i );

			totalHashWeight = totalHashWeight + p.y;
			}

		boolean[] exists = new boolean[q.size()];

		int n = 0;
		Queue owningSet = new Queue();

		while ( owningSet.size() < replicationFactor )
			{
			int hash = ( Murmur.hash( id, n ) % totalHashWeight );
			//			System.out.println( n + "\t" + hash );

			int total = 0;
			for ( int i = 0; i < q.size(); i++ )
				{
				PairSI p = (PairSI) q.elementAt( i );

				total = total + p.y;

				if ( !exists[ i ] )
					if ( total > hash )
						{
						owningSet.addObject( p.x );
						exists[ i ] = true;

						break;
						}
				}

			// This will increment the hash seed by one
			// Allows us to get well distributed hashes
			n++;
			}

		//		System.out.println( "iterations: " + n );

		return owningSet;
		}


	// For a given key we need to be able to determine 
	// 1 - which servers were active at the given time back
	// 2 - the set of servers included for the hashed key
	public static Queue returnTranslationOwningSet( String key, int timeBack ) throws Throwable
		{
		if ( seenIPs == null )
			System.out.println( "Need to call MapServer.init()!!!!" );

		int replicationFactor = returnReplicationFactorAtGivenTimeBack( timeBack );
		Queue q = returnAllServersAtGivenTimeBack( timeBack );

		//		System.out.println( "USING TRANSLATION REPLICATION FACTOR: " + replicationFactor );

		if ( q == null )
			{
			//			Util.printStackTrace( "TRANSLATION NOT FOUND: " + key );

			return null;
			}

		if ( q.size() < replicationFactor )
			{
			System.out.println( "\tLess than " + replicationFactor + " servers exist at the given time!!!!" );
			System.out.println( "\tChanging set size to: " + q.size() );

			replicationFactor = q.size();
			}

		// Determine the total hashWeight for all servers active at the given time back
		int totalHashWeight = 0;
		for ( int i = 0; i < q.size(); i++ )
			{
			PairSI p = (PairSI) q.elementAt( i );

			totalHashWeight = totalHashWeight + p.y;

			//			System.out.println( "\t\t" + p.x + "\t" + p.y );
			}

		boolean[] exists = new boolean[q.size()];

		int n = 0;
		Queue owningSet = new Queue();

		while ( owningSet.size() < replicationFactor )
			{
			int hash = ( Murmur.hash( key, n ) % totalHashWeight );
			//			System.out.println( n + "\t" + key + "\t" + hash );

			int total = 0;
			for ( int i = 0; i < q.size(); i++ )
				{
				PairSI p = (PairSI) q.elementAt( i );

				total = total + p.y;

				if ( !exists[ i ] )
					if ( total > hash )
						{
						owningSet.addObject( p.x );
						exists[ i ] = true;

						break;
						}
				}

			// This will increment the hash seed by one
			// Allows us to get well distributed hashes
			n++;
			}

		//		System.out.println( "iterations: " + n );

		return owningSet;
		}


	public static void parseMap() throws Throwable
		{
		//		System.out.println( "Parsing map" );

		File f = new File( "config/map" );
		long modDate = f.lastModified();

		InStream in = new InStream( new FileInputStream( f ) );

		Queue lines = in.readLines();
		parseLines( lines );
		}


	public static boolean serverIsDown( String ip )
		{
		synchronized ( downServers )
			{
			return ( downServers.get( ip ) == 1 );
			}
		}


	public void checkIP( String ip )
		{
		try
			{
			Socket s = new Socket( ip, HashStoreServer.PORT );

			Message m = new Message();
			m.writeByte( 0 );

			OutStream out = new OutStream( s.getOutputStream() );
			out.write4Bytes( HashStoreServer.HEARTBEAT );
			out.writeMessage( m );
			out.flush();
			out.close();

			synchronized ( downServers )
				{
				int previousStatus = downServers.get( ip );
				if ( previousStatus == 1 )
					System.out.println( "########## SERVER IS UP: " + ip );

				downServers.put( ip, 0 );
				}
			}
		catch ( Throwable theX )
			{
			//			cota.util.Util.printX( theX );

			if ( shouldMarkServersAsDown )
				{
				synchronized ( downServers )
					{
					int previousStatus = downServers.get( ip );
					if ( previousStatus == 0 )
						System.out.println( "########## SERVER IS DOWN: " + ip );
					else
						System.out.println( "########## SERVER IS STILL DOWN: " + ip );

					downServers.put( ip, 1 );
					}
				}
			}
		}


	public void checkServers()
		{
		Queue ips = returnAllIPs();
		//		System.out.println( "checkServers: " + ips.size() );

		for ( int i = 0; i < ips.size(); i++ )
			{
			String ip = (String) ips.elementAt( i );

			checkIP( ip );
			}
		}


	public void run()
		{
		while ( true )
			{
			try
				{
				checkServers();

				Thread.sleep( 1000L * 5 );
				}
			catch ( Throwable theX )
				{
				cota.util.Util.printX( theX );
				}
			}
		}


	public static void printMap()
		{
		for ( int i = 0; i < map.size(); i++ )
			{
			PairOL p = (PairOL) map.elementAt( i );

			System.out.println( "\nTIME: " + p.y );

			Queue q = (Queue) p.x;
			for ( int j = 0; j < q.size(); j++ )
				{
				PairSI s = (PairSI) q.elementAt( j );

				System.out.println( "\t" + s.x + "\t" + s.y );
				}
			}

		System.out.println( "" );
		}


	public static int returnServerNum()
		{
		return myServerNum;
		}


	public static Queue returnAllIPs()
		{
		FashEntry_so[] entries = seenIPs.returnArrayOfEntries();

		Queue q = new Queue();
		for ( int i = 0; i < entries.length; i++ )
			q.addObject( entries[ i ].key );

		return q;
		}


	void ______SERVER_STUFF________()
		{
		}


	private static void appendToLocalMap( String newInfo ) throws Throwable
		{
		//		Util.printStackTrace( "APPENDING NEW INFO SET TO LOCAL MAP" );

		System.out.println( "\n----- APPENDING NEW INFO SET TO LOCAL MAP -----" );
		System.out.println( newInfo );

		// Backup the existing map
		File f = new File( "config/map" );
		File f2 = new File( "config/map.bak" );

		Util.copyFile( f, f2 );

		// Append the new info
		OutStream out = new OutStream( new FileOutputStream( f, true ) );
		out.writeLine( newInfo );
		out.close();

		parseMap();
		}


	private static void setLocalMap( byte[] mapBytes ) throws Throwable
		{
		System.out.println( "\n----- UPDATING LOCAL MAP -----" );

		// Backup the existing map
		File f = new File( "config/map" );
		File f2 = new File( "config/map.bak" );

		Util.copyFile( f, f2 );

		// Append the new info
		OutStream out = new OutStream( new FileOutputStream( f ) );
		out.writeBytes( mapBytes );
		out.close();

		parseMap();
		}


	private static long mostRecentTimestamp()
		{
		PairOL p = (PairOL) map.elementAt( map.size() - 1 );

		return p.y;
		}


	private static byte[] returnMapBytes() throws Throwable
		{
		File f = new File( "config/map" );
		InStream mapIn = new InStream( new FileInputStream( f ) );

		byte[] mapBytes = new byte[(int) f.length()];
		mapIn.readBytes( mapBytes );

		mapIn.close();

		return mapBytes;
		}


	public static void syncWithOtherServers() throws Throwable
		{
		Queue currentServers = returnAllServersAtGivenTimeBack( 0 );
		for ( int i = 0; i < currentServers.size(); i++ )
			{
			PairSI p = (PairSI) currentServers.elementAt( i );

			String ip = "";

			try
				{
				ip = p.x;

				if ( !ip.equals( myIP ) )
					syncMapWithIP( ip );
				}
			catch ( Throwable theX )
				{
				System.out.println( "UNABLE TO SYNC MAP TO: " + ip );
				//				cota.util.Util.printX( theX );
				}
			}
		}


	public void handleRequest( Socket s, InStream in, OutStream out ) throws Throwable
		{
		InetAddress address = s.getInetAddress();
		String ip = address.getHostAddress();

		if ( !MapServer.isValidIP( ip ) )
			{
			// IPs that show up in the map are automatically considered valid
			// Allow IPs that have been manually whitelisted to be added to the cluster
			boolean whitelisted = false;

			try
				{
				InStream in2 = new InStream( "config/whitelisted_ips" );

				Queue lines = in2.readLines();
				for ( int i = 0; i < lines.size(); i++ )
					{
					String line = (String) lines.elementAt( i );

					// Remove whitespace
					line = line.replaceAll( "\\s+", "" );

					if ( line.equals( ip ) )
						whitelisted = true;
					}
				}
			catch ( Throwable theX )
				{
				cota.util.Util.printX( theX );
				}

			if ( !whitelisted )
				{
				System.out.println( "!!!!!!!  ATTEMPT TO ACCESS MAP SERVER FROM INVALID IP: " + ip );

				return;
				}
			}

		try
			{
			int requestType = in.read4Bytes();
			Message m = in.readMessage();

			Message r = new Message();
			switch ( requestType )
				{
				case JOIN_CLUSTER:
					{
					// Get the information from the server to be added
					int freeGBs = m.read2Bytes();
					int minuteDelay = m.read2Bytes();

					// If the the server is already present in the map, then don't do anything
					if ( seenIPs.get( ip ) != null )
						throw new Throwable( "SERVER IS TRYING TO JOIN, BUT ALREADY PRESENT: " + ip );

					long timeInSeconds = System.currentTimeMillis() / 1000L;
					long timeToTakeEffect = timeInSeconds + minuteDelay * 60;

					int currentReplicationFactor = returnReplicationFactorAtGivenTimeBack( 0 );

					// Generate a new set of information that includes the newly added server
					// The new information will take effect after a certain delay
					StringBuffer sb = new StringBuffer();
					sb.append( "#" + timeToTakeEffect + ":" + currentReplicationFactor + "\n" );

					Queue existingServers = returnAllServersAtGivenTimeBack( 0 );
					for ( int i = 0; i < existingServers.size(); i++ )
						{
						PairSI p = (PairSI) existingServers.elementAt( i );

						String ip0 = p.x;
						int freeGBs0 = p.y;

						sb.append( ip0 + "\t" + freeGBs0 + "\n" );
						}

					// Add the entry for the new server
					sb.append( ip + "\t" + freeGBs );

					String newInfo = sb.toString();
					appendToLocalMap( newInfo );

					// Send the entire existing map
					byte[] mapBytes = returnMapBytes();
					r.writeBytes( mapBytes );

					// Tell the existing servers to update to the new map
					for ( int i = 0; i < existingServers.size(); i++ )
						{
						PairSI p = (PairSI) existingServers.elementAt( i );

						String ip0 = p.x;

						if ( !ip0.equals( myIP ) )
							{
							try
								{
								syncMapWithIP( ip0 );
								}
							catch ( Throwable theX )
								{
								System.out.println( "######## UNABLE TO UPDATE " + ip0 + " ###########" );
								System.out.println( ip0 + " will pull new map info from other servers on startup (assuming it can access at least one of them)" );
								}
							}
						}
					}
				break;

				case SYNC_MAP:
					{
					// Read in the information provided by the calling server
					long mostRecentTimestamp0 = m.read8Bytes();
					byte[] mapBytes0 = m.readBytes();

					long mostRecentTimestamp = mostRecentTimestamp();

					// Same timestamp? - do nothing
					if ( mostRecentTimestamp0 == mostRecentTimestamp )
						r.writeBytes( new byte[1] );

					// Calling server has a newer map - write it to the local map
					if ( mostRecentTimestamp0 > mostRecentTimestamp )
						{
						setLocalMap( mapBytes0 );

						r.writeBytes( new byte[1] );
						}

					// This server has a newer map - return it so that calling server can use it
					if ( mostRecentTimestamp0 < mostRecentTimestamp )
						{
						byte[] mapBytes = returnMapBytes();

						r.writeBytes( mapBytes );
						}
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
		catch ( Throwable theX )
			{
			cota.util.Util.printX( "ERROR IN MapServer.handleRequest: " + ip, theX );

			out.writeByte( 0 );
			out.flush();

			//				throw theX;
			}
		}


	// Needs to have variable args and results
	public static Message sendRequest( String ip, int requestType, Message args ) throws Throwable
		{
		try
			{
			Socket s = new Socket( ip, PORT );

			OutStream out = new OutStream( s.getOutputStream() );
			InStream in = new InStream( s.getInputStream() );

			out.write4Bytes( requestType );
			out.writeMessage( args );
			out.flush();

			int status = in.readByte();
			if ( status != SUCCESS )
				throw new Throwable( "INVALID STATUS RECEIVED" );

			Message r = in.readMessage();

			out.close();
			in.close();

			return r;
			}
		catch ( Throwable theX )
			{
			throw new Throwable( theX );
			}
		}


	public static void joinCluster( String ip, int minuteDelay ) throws Throwable
		{
		// Determine the number of free GBs available on this server
		File file = new File( "." );

		long freeBytes = file.getUsableSpace();
		int freeGBs = (int) ( freeBytes / 1024 / 1024 / 1024 );

		Message args = new Message();
		args.write2Bytes( freeGBs );
		args.write2Bytes( minuteDelay );

		Message r = sendRequest( ip, JOIN_CLUSTER, args );

		byte[] mapBytes = r.readBytes();

		// Write the current version of the map that the contacted server had
		OutStream out = new OutStream( new FileOutputStream( "config/map" ) );
		out.writeBytes( mapBytes );
		out.close();

		// Run init so that the map will be loaded
		init( false );

		System.out.println( "SERVER ADDED TO CLUSTER SUCCESSFULLY" );

		String hasS = "";
		if ( minuteDelay > 1 )
			hasS = "s";

		System.out.println( "The cluster will begin storing data on this server in " + minuteDelay + " minute" + hasS + "\n" );

		System.out.println( "Start Ganesha on this server using something like this (ideally before the cluster starts storing data on it):\n     java -cp ganesha_all.jar -Xms6G -Xmx6G cota.ganesha.Ganesha\n" );
		}


	// If both servers have the same newest data, nothing happens
	// If the receiving server has newer data, it sends the newer data back to the calling server
	// If the sending server has newer data, the receiving server adds it to its map
	public static void syncMapWithIP( String ip ) throws Throwable
		{
		//		Util.printStackTrace( "SYNCHING MAP" );

		System.out.println( "\tSynching map with: " + ip );

		long mostRecentTimestamp = mostRecentTimestamp();
		byte[] mapBytes = returnMapBytes();

		Message args = new Message();
		args.write8Bytes( mostRecentTimestamp );
		args.writeBytes( mapBytes );

		Message r = sendRequest( ip, SYNC_MAP, args );
		byte[] mapBytes0 = r.readBytes();

		if ( mapBytes0.length > 1 )
			setLocalMap( mapBytes0 );
		}


	public static void useReplicationFactor( int replicationFactor, int minuteDelay ) throws Throwable
		{
		long timeInSeconds = System.currentTimeMillis() / 1000L;
		long timeToTakeEffect = timeInSeconds + minuteDelay * 60;

		// Generate a new set of information that includes the modified replication factor
		// The new information will take effect after a certain delay
		StringBuffer sb = new StringBuffer();
		sb.append( "#" + timeToTakeEffect + ":" + replicationFactor + "\n" );

		Queue existingServers = returnAllServersAtGivenTimeBack( 0 );
		for ( int i = 0; i < existingServers.size(); i++ )
			{
			PairSI p = (PairSI) existingServers.elementAt( i );

			String ip0 = p.x;
			int freeGBs0 = p.y;

			sb.append( ip0 + "\t" + freeGBs0 );

			if ( i != ( existingServers.size() - 1 ) )
				sb.append( "\n" );
			}

		String newInfo = sb.toString();
		appendToLocalMap( newInfo );

		// Tell the existing servers to update to the new map
		for ( int i = 0; i < existingServers.size(); i++ )
			{
			PairSI p = (PairSI) existingServers.elementAt( i );

			String ip0 = p.x;

			if ( !ip0.equals( myIP ) )
				{
				try
					{
					syncMapWithIP( ip0 );
					}
				catch ( Throwable theX )
					{
					System.out.println( "######## UNABLE TO UPDATE " + ip0 + " ###########" );
					System.out.println( ip0 + " will pull new map info from other servers on startup (assuming it can access at least one of them)" );
					}
				}
			}
		}


	public static void createFirstMap( int replicationFactor ) throws Throwable
		{
		File f = new File( "config/map" );
		if ( f.exists() )
			{
			System.out.println( "CALLING 'MapServer init' EVEN THOUGH MAP ALREADY EXISTS!" );

			return;
			}

		System.out.println( "Creating map with initial replication factor: " + replicationFactor );

		OutStream out = new OutStream( new FileOutputStream( f ) );

		// The timestamp marks the time when this map information becomes valid
		long nowInSeconds = System.currentTimeMillis() / 1000;

		out.writeLine( "#" + nowInSeconds + ":" + replicationFactor );

		File file = new File( "." );

		long freeBytes = file.getUsableSpace();
		long freeGBs = freeBytes / 1024 / 1024 / 1024;

		InStream in = new InStream( "config/ip_address" );
		Queue lines = in.readLines();

		myIP = (String) lines.elementAt( 0 );

		out.writeLine( myIP + "\t" + freeGBs );

		out.flush();
		out.close();

		System.out.println( "\nTo join the cluster from another machine, first start Ganesha on this server using something like:\n     java -cp ganesha_all.jar -Xms6G -Xmx6G cota.ganesha.Ganesha\n\nThen use the following command on the server to add:\n     java -cp ganesha_all.jar cota.ganesha.MapServer join " + myIP + " 1\n" );
		}


	public static void main( String[] args )
		{
		try
			{
			if ( args[ 0 ].equals( "create" ) )
				{
				int replicationFactor = Integer.parseInt( args[ 1 ] );

				createFirstMap( replicationFactor );
				}

			if ( args[ 0 ].equals( "join" ) )
				{
				String ip = args[ 1 ];
				int minuteDelay = Integer.parseInt( args[ 2 ] );

				joinCluster( ip, minuteDelay );
				}

			if ( args[ 0 ].equals( "use_rf" ) )
				{
				init( false );

				File f = new File( "config/map" );
				if ( !f.exists() )
					{
					System.out.println( "NEED TO JOIN A CLUSTER BEFORE CHANGING THE REPLCIATION FACTOR!" );

					return;
					}

				int rf = Integer.parseInt( args[ 1 ] );
				int minuteDelay = Integer.parseInt( args[ 2 ] );

				useReplicationFactor( rf, minuteDelay );
				}
			}
		catch ( Throwable theX )
			{
			cota.util.Util.printX( theX );
			}
		}
	}
