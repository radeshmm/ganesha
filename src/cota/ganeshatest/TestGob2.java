package cota.ganeshatest;

import cota.ganesha.Ganesha;
import cota.ganesha.Gob;
import cota.ganesha.MapServer;


public class TestGob2 extends Gob
	{
	public static final String table = "Test2";

	// GOB_TYPE should be unique across each of the various gobs that are defined in the workspace.
	// GOB_TYPE is essentially used for error checking and ensuring that requested attributes actually belong
	// to the gob being queried (attribute ids contain the GOB_TYPE and TYPE within the attribute id itself)

	// This MUST be an integer left-shifted by 24-bits to work correctly
	public static final int GOB_TYPE = ( 0 << 24 );

	// Attributes MUST be constructed in the following form
	// attribute = unique id + GOB_TYPE + ATTRIBUTE_TYPE
	public static int nameOfStar = 0 + GOB_TYPE + STRING;
	public static int color = 1 + GOB_TYPE + STRING;
	public static int weight = 2 + GOB_TYPE + LONG;


	public TestGob2( String workspace, String name0, String color0, long weight0 ) throws Throwable
		{
		super( GOB_TYPE );

		f.put( nameOfStar, name0 );
		f.put( color, color0 );
		f.put( weight, weight0 );

		Ganesha.addObject( workspace, table, name0, this );
		}


	// Used to retrieve a previously added object
	public TestGob2( String workspace, String name0 ) throws Throwable
		{
		super( GOB_TYPE, workspace, table, name0 );
		}


	public void print() throws Throwable
		{
		System.out.println( "" );
		System.out.println( "id: " + id );
		System.out.println( "name of star: " + getString( nameOfStar ) );
		System.out.println( "color: " + getString( color ) );
		System.out.println( "weight: " + getLong( weight ) );
		}


	public static void main( String[] args )
		{
		try
			{
			MapServer.init( false );

			if ( args[ 0 ].equals( "store" ) )
				{
				// Store the objects by name
				TestGob2 sun = new TestGob2( "galaxy", "sun", "orangeish", 3300000000000L );
				TestGob2 otherSun = new TestGob2( "other_galaxy", "sun", "red", 10000000000L );

				// Modify
				sun.putString( color, "orange" );
				otherSun.putLong( weight, 99999999999999L );

				System.out.println( "Objects stored" );
				}

			if ( args[ 0 ].equals( "print" ) )
				{
				// Retrieve by name and print
				TestGob2 sun2 = new TestGob2( "galaxy", "sun" );
				TestGob2 otherSun2 = new TestGob2( "other_galaxy", "sun" );

				sun2.print();
				otherSun2.print();
				}
			}
		catch ( Throwable theX )
			{
			cota.util.Util.printX( theX );
			}
		}
	}