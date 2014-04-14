package cota.networking;

import cota.util.*;
import java.net.*;


public class SocketReaper implements Runnable
	{
	static SocketReaper singleton = null;

	static Queue sockets = new Queue();

	static
		{
		singleton = new SocketReaper();
		}


	public SocketReaper()
		{
		Thread t = new Thread( this );
		t.start();
		}


	public static void addSocket( Socket s )
		{
		// Sockets are automatically destroyed after 10 minutes
		PairOL p = new PairOL( s, System.currentTimeMillis() + 1000L * 60L * 10L );

		synchronized ( sockets )
			{
			sockets.addObject( p );
			}
		}


	public static void remove( Socket s )
		{
		//		System.out.println( "REMOVING SOCKET A: " + sockets.size() );
		synchronized ( sockets )
			{
			for ( int i = 0; i < sockets.size(); i++ )
				{
				PairOL p = (PairOL) sockets.elementAt( i );
				Socket s0 = (Socket) p.x;

				if ( s0 == s )
					{
					//				System.out.println( "\tSOCKET REMOVED" );
					sockets.removeWithoutOrder( i );
					}
				}
			}

		//		System.out.println( "REMOVING SOCKET B: " + sockets.size() );
		}


	public void run()
		{
		while ( true )
			{
			try
				{
				Thread.sleep( 10 * 1000 );

				Queue q = new Queue();

				synchronized ( sockets )
					{
					q.append( sockets );
					}

				long now = System.currentTimeMillis();
				for ( int i = 0; i < q.size(); i++ )
					{
					PairOL p = (PairOL) q.elementAt( i );
					Socket s = (Socket) p.x;

					if ( s.isClosed() )
						remove( s );

					if ( now > p.y )
						{
						try
							{
							InetAddress address = s.getInetAddress();
							String ip = address.getHostAddress();

							System.out.println( "FORCE CLOSING SOCKET: " + ip );

							s.close();

							remove( s );
							}
						catch ( Throwable theX )
							{
							cota.util.Util.printX( theX );
							}
						}
					}
				}
			catch ( Throwable theX )
				{
				cota.util.Util.printX( theX );
				}
			}
		}
	}
