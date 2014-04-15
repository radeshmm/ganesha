package cota.util;

import cota.util.Hashtable_so;



// Simply responsible for emailing alerts when something goes wrong
public class ErrorHandler
	{
	static Hashtable_so reported = new Hashtable_so();
	public static long startupTime = 0;


	public static void show()
		{
		startupTime = 1;
		}


	public static void init()
		{
		if ( startupTime == 0 )
			startupTime = System.currentTimeMillis();
		}


	public static void error( String error, Throwable theX )
		{
		long elapsed = System.currentTimeMillis() - startupTime;
		if ( elapsed < 1000 * 60 ) // Ignore errors in the first minute
			return;

		if ( reported.get( error ) != null )
			return;

		reported.put( error, "" );


		System.out.println( "################################################" );
		System.out.println( error );
		System.out.println( "################################################" );

		if ( theX != null )
			cota.util.Util.printX( theX );
		}


	public static void error( String error )
		{
		error( error, null );
		}
	}
