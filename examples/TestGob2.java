package cota.ganeshatest;

import cota.ganesha.Gob;
import cota.ganesha.MapServer;
import cota.util.Queue;


public class TestGob2
	{
	public static void print( Gob gob ) throws Throwable
		{
		System.out.println( "" );
		System.out.println( "id: " + gob.id );
		System.out.println( "name: " + gob.getString( "name" ) );
		System.out.println( "color: " + gob.getString( "color" ) );
		System.out.println( "weight: " + gob.getLong( "weight" ) );
		}


	public static void main( String[] args )
		{
		try
			{
			MapServer.init( false );

			if ( args[ 0 ].equals( "store" ) )
				{
				// Store the objects by name
				Gob sun = new Gob( "galaxy", "stars", "sun" );
				sun.put( "color", "orangeish" );
				sun.put( "weight", 3300000000000L );

				Gob otherSun = new Gob( "other_galaxy", "stars", "sun" );
				otherSun.put( "color", "red" );
				otherSun.put( "weight", 10000000000L );

				// Modify
				sun.put( "color", "orange" );

				System.out.println( "Objects stored" );
				}

			if ( args[ 0 ].equals( "print" ) )
				{
				// Retrieve by name and print
				Gob sun2 = new Gob( "galaxy", "stars", "sun" );
				Gob otherSun2 = new Gob( "other_galaxy", "stars", "sun" );

				print( sun2 );
				print( otherSun2 );
				}

			if ( args[ 0 ].equals( "planets" ) )
				{
				// Store
				Gob sun = new Gob( "universe", "stars", "the sun" );

				Gob earth = new Gob( "universe", "planets", "earth" );
				Gob venus = new Gob( "universe", "planets", "venus" );

				sun.append( "planets", earth );
				sun.append( "planets", venus );

				earth.put( "star", sun );
				venus.put( "star", sun );

				System.out.println( "Objects stored" );
				}

			if ( args[ 0 ].equals( "orbits" ) )
				{
				// Retrieve
				Gob earth2 = new Gob( "universe", "planets", "earth" );
				Gob sun2 = earth2.getGob( "star" );

				Queue planets = sun2.getGobs( "planets" );
				for ( int i = 0; i < planets.size(); i++ )
					{
					Gob planet = (Gob) planets.elementAt( i );

					System.out.println( planet.getString( "name" ) + " orbits around " + sun2.getString( "name" ) );
					}
				}
			}
		catch ( Throwable theX )
			{
			cota.util.Util.printX( theX );
			}
		}
	}