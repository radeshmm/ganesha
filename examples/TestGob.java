package cota.ganeshatest;

import cota.ganesha.Ganesha;
import cota.ganesha.Gob;
import cota.ganesha.MapServer;
import cota.util.Queue;


public class TestGob
	{
	public static void print( Gob gob ) throws Throwable
		{
		System.out.println( "name: " + gob.getString( "name" ) );
		System.out.println( "seenCount: " + gob.getInt( "seenCount" ) );

		Queue ids = gob.getIDs( "parts" );
		if ( ids.size() > 0 )
			{
			System.out.println( "Parts: " );

			for ( int i = 0; i < ids.size(); i++ )
				{
				long id = (Long) ids.elementAt( i );

				Gob part = new Gob( id );
				System.out.println( "\t" + id + "\t" + part.getString( "name" ) );
				}
			}

		}


	public static void main( String[] args )
		{
		try
			{
			MapServer.init( false );

			if ( args[ 0 ].equals( "store" ) )
				{
				// Create the objects
				Gob cloud = new Gob();
				cloud.put( "name", "cloud" );

				Gob sky = new Gob();
				sky.put( "name", "sky" );

				// test increment and putInt
				sky.increment( "seenCount" );
				cloud.put( "seenCount", 100 );

				// test lists
				sky.appendID( "parts", cloud.id );

				System.out.println( "cloud id: " + cloud.id );
				System.out.println( "sky id: " + sky.id );
				}

			if ( args[ 0 ].equals( "print" ) )
				{
				long id = Long.parseLong( args[ 1 ] );

				Ganesha.printIDInfo( id );

				Gob gob = new Gob( id );
				print( gob );
				}
			}
		catch ( Throwable theX )
			{
			cota.util.Util.printX( theX );
			}
		}
	}