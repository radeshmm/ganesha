package cota.ganeshatest;

import cota.ganesha.Gob;
import cota.ganesha.MapServer;
import cota.io.InStream;
import cota.io.OutStream;
import cota.util.Hashtable_lo;
import cota.util.Queue;


public class Test implements Runnable
	{
	static Object lock = new Object();

	static int count = 0;

	Hashtable_lo f = new Hashtable_lo();

	static Queue gobs = new Queue();
	static int totalCount = 0;
	static long startTime = 0;

	static boolean reading = false;


	public Test()
		{
		Thread t = new Thread();

		t = new Thread( this );
		t.start();
		}


	public void run()
		{
		try
			{
			for ( int i = 0; i < 10000000; i++ )
				{
				Gob gob = null;
				synchronized ( gobs )
					{
					gob = (Gob) gobs.elementAt( (int) ( Math.random() * gobs.size() ) );
					}

				if ( reading )
					gob = new Gob( gob.id );
				else
					gob.put( "seenCount", 0 );

				totalCount++;

				if ( ( totalCount % 10000 ) == 0 )
					{
					long elapsed = System.currentTimeMillis() - startTime;

					System.out.println( totalCount + ": " + totalCount * 1000.0 / elapsed );
					}
				}
			}
		catch ( Throwable theX )
			{
			cota.util.Util.printX( theX );
			}
		}


	public static void main( String[] args )
		{
		try
			{
			MapServer.init( false );

			reading = args[ 0 ].equals( "reading" );

			if ( args[ 0 ].equals( "gen" ) )
				{
				OutStream out = new OutStream( "ids" );

				for ( int i = 0; i < 10000; i++ )
					{
					Gob gob = new Gob();

					out.write8Bytes( gob.id );

					if ( ( i % 1000 ) == 0 )
						System.out.println( i );
					}

				out.close();

				return;
				}

			InStream in = new InStream( "ids" );

			for ( int i = 0; i < 10000; i++ )
				{
				long id = in.read8Bytes();

				Gob gob = new Gob( id );
				gobs.addObject( gob );
				}

			System.out.println( "Gobs loaded" );

			startTime = System.currentTimeMillis();
			for ( int i = 0; i < 50; i++ )
				{
				Test g = new Test();
				}

			}
		catch ( Throwable theX )
			{
			cota.util.Util.printX( theX );
			}
		}
	}
