package cota.ganeshatest;

import cota.ganesha.Ganesha;
import cota.ganesha.Gob;
import cota.ganesha.MapServer;
import cota.util.Queue;


public class TestGob extends Gob
	{
	// GOB_TYPE should be unique across each of the various gobs that are defined in the workspace.
	// GOB_TYPE is essentially used for error checking and ensuring that requested attributes actually belong
	// to the gob being queried (attribute ids contain the GOB_TYPE and TYPE within the attribute id itself)

	// This MUST be an integer left-shifted by 24-bits to work correctly
	public static final int GOB_TYPE = ( 0 << 24 );

	// Attributes MUST be constructed in the following form
	// attribute = unique id + GOB_TYPE + ATTRIBUTE_TYPE
	public static int name = 0 + GOB_TYPE + STRING;
	public static int seenCount = 1 + GOB_TYPE + INT;
	public static int parts = 2 + GOB_TYPE + LIST;


	// Used for creating new objects
	private TestGob( String name0 ) throws Throwable
		{
		super( GOB_TYPE );

		f.put( name, name0 );
		f.put( seenCount, 0 );
		f.put( parts, Ganesha.createEmptyList() );

		Ganesha.addObject( this );
		}


	// Used to retrieve the object based on id
	public TestGob( long id ) throws Throwable
		{
		super( GOB_TYPE, id );
		}


	public void print() throws Throwable
		{
		System.out.println( "name: " + getString( name ) );
		System.out.println( "seenCount: " + getInt( seenCount ) );

		Queue ids = getIDs( parts );
		if ( ids.size() > 0 )
			{
			System.out.println( "Parts: " );

			for ( int i = 0; i < ids.size(); i++ )
				{
				long id = (Long) ids.elementAt( i );

				TestGob part = new TestGob( id );
				System.out.println( "\t" + id + "\t" + part.getString( name ) );
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
				TestGob sun = new TestGob( "sun" );
				TestGob cloud = new TestGob( "cloudddd" );
				TestGob sky = new TestGob( "sky" );

				cloud.putString( TestGob.name, "cloud" );

				sun.increment( TestGob.seenCount );
				sky.increment( TestGob.seenCount );

				for ( int i = 0; i < 100; i++ )
					cloud.increment( TestGob.seenCount );

				sky.appendID( TestGob.parts, sun.id );
				sky.appendID( TestGob.parts, cloud.id );

				System.out.println( "sun id: " + sun.id );
				System.out.println( "cloud id: " + cloud.id );
				System.out.println( "sky id: " + sky.id );
				}

			if ( args[ 0 ].equals( "print" ) )
				{
				long id = Long.parseLong( args[ 1 ] );

				Ganesha.printIDInfo( id );

				TestGob gob = new TestGob( id );
				gob.print();
				}
			}
		catch ( Throwable theX )
			{
			cota.util.Util.printX( theX );
			}
		}
	}