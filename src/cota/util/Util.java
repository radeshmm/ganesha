package cota.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.channels.FileChannel;
import java.util.Calendar;
import java.util.Date;

import cota.io.InStream;


// Utility methods
public class Util
	{
	public static void copyFile( File source, File dest ) throws IOException
		{
		FileChannel inputChannel = null;
		FileChannel outputChannel = null;
		try
			{
			inputChannel = new FileInputStream( source ).getChannel();
			outputChannel = new FileOutputStream( dest ).getChannel();
			outputChannel.transferFrom( inputChannel, 0, inputChannel.size() );
			}
		finally
			{
			inputChannel.close();
			outputChannel.close();
			}
		}


	public static long getAvailableMemory()
		{
		Runtime runtime = Runtime.getRuntime();

		long totalMemory = runtime.totalMemory(); // current heap allocated to the VM process
		long freeMemory = runtime.freeMemory(); // out of the current heap, how much is free
		long maxMemory = runtime.maxMemory(); // Max heap VM can use e.g. Xmx setting
		long usedMemory = totalMemory - freeMemory; // how much of the current heap the VM is using

		long availableMemory = maxMemory - usedMemory; // available memory i.e. Maximum heap size minus the current amount used

		return availableMemory;
		}


	public static String waitString( int numSeconds )
		{
		if ( numSeconds < 2 )
			return "a moment";
		if ( numSeconds < 60 )
			return numSeconds + " seconds";

		int minutes = numSeconds / 60;
		if ( minutes == 0 )
			return "1 minute";
		if ( minutes < 60 )
			return ( minutes + 1 ) + " minutes";

		int hours = minutes / 60;
		if ( hours == 0 )
			return "1 hour";
		if ( hours < 24 )
			return ( hours + 1 ) + " hours";

		int days = hours / 24;
		if ( days == 0 )
			return "1 day";
		if ( days < 30 )
			return ( days + 1 ) + " days";

		return "";
		}


	public static void sleep( int count )
		{
		try
			{
			Thread.sleep( count );
			}
		catch ( Throwable theX )
			{
			System.out.println( "ERROR SLEEPING: " + theX );
			}
		}


	// Print an exception string
	public static void printX( Throwable theX )
		{
		printX( "ERROR", theX );
		}


	// Print an exception string
	public static void printX( String s, Throwable theX )
		{
		Date d = new Date();

		System.out.println( d.toString() + ": " + s + "\n" + theX );
		theX.printStackTrace( System.out );
		}


	public static String currentStackTrace()
		{
		StringWriter stringWriter = new StringWriter();
		PrintWriter printWriter = new PrintWriter( stringWriter );
		( new Throwable() ).printStackTrace( printWriter );

		return stringWriter.toString();
		}


	public static void printStackTrace( String header ) throws Throwable
		{
		StringWriter stringWriter = new StringWriter();
		PrintWriter printWriter = new PrintWriter( stringWriter );
		( new Throwable() ).printStackTrace( printWriter );

		String s = stringWriter.toString();
		InStream in = new InStream( s.getBytes() );

		Queue lines = in.readLines();

		System.out.println( header );
		for ( int i = 2; i < lines.size(); i++ )
			{
			String line = (String) lines.elementAt( i );

			System.out.println( line );
			}
		}


	// return the corresponding value of the month, corresponding to the given
	// String
	public static int returnMonth( String month )
		{
		if ( month.toLowerCase().equals( "jan" ) )
			{
			return 0;
			}

		if ( month.toLowerCase().equals( "feb" ) )
			{
			return 1;
			}

		if ( month.toLowerCase().equals( "mar" ) )
			{
			return 2;
			}

		if ( month.toLowerCase().equals( "apr" ) )
			{
			return 3;
			}

		if ( month.toLowerCase().equals( "may" ) )
			{
			return 4;
			}

		if ( month.toLowerCase().equals( "jun" ) )
			{
			return 5;
			}

		if ( month.toLowerCase().equals( "jul" ) )
			{
			return 6;
			}

		if ( month.toLowerCase().equals( "aug" ) )
			{
			return 7;
			}

		if ( month.toLowerCase().equals( "sep" ) )
			{
			return 8;
			}

		if ( month.toLowerCase().equals( "oct" ) )
			{
			return 9;
			}

		if ( month.toLowerCase().equals( "nov" ) )
			{
			return 10;
			}

		if ( month.toLowerCase().equals( "dec" ) )
			{
			return 11;
			}

		return -1;
		}


	public static String returnMonth( int month )
		{
		switch ( month )
			{
			case 0:
				return "Jan";

			case 1:
				return "Feb";

			case 2:
				return "Mar";

			case 3:
				return "Apr";

			case 4:
				return "May";

			case 5:
				return "Jun";

			case 6:
				return "Jul";

			case 7:
				return "Aug";

			case 8:
				return "Sep";

			case 9:
				return "Oct";

			case 10:
				return "Nov";

			case 11:
				return "Dec";
			}

		return "   ";
		}


	public static boolean isSubclassOf( Object o, String c )
		{
		Class cl = o.getClass();
		String name = cl.getName();
		while ( !name.equals( "java.lang.Object" ) )
			{
			if ( name.equals( c ) )
				{
				return true;
				}

			cl = cl.getSuperclass();
			name = cl.getName();
			}

		return false;
		}


	// Like System.println, but storing the output to disk
	public synchronized static void print( String string )
		{
		printPlain( string + "\n" );
		}


	// Like System.println, but storing the output to disk
	public static void printPlain( String string )
		{
		Calendar c = Calendar.getInstance();

		int hours = c.get( Calendar.HOUR );
		int minutes = c.get( Calendar.MINUTE );
		int seconds = c.get( Calendar.SECOND );

		String time = "";

		if ( hours < 10 )
			time = "0" + Integer.toString( hours );
		else
			time = Integer.toString( hours );

		if ( minutes < 10 )
			time = time + ":0" + Integer.toString( minutes );
		else
			time = time + ":" + Integer.toString( minutes );

		if ( seconds < 10 )
			time = time + ":0" + Integer.toString( seconds );
		else
			time = time + ":" + Integer.toString( seconds );

		System.out.print( time + " " + string );
		}


	public static int percentMemoryFull() throws Throwable
		{
		System.gc();

		Runtime r = Runtime.getRuntime();

		double free = r.freeMemory();
		double total = r.totalMemory();
		double used = total - free;

		double percent = used / total;

		return (int) ( percent * 100.0 );
		}


	/*
		public static void applescript( String s ) throws Throwable
			{
			// Runtime r = Runtime.getRuntime();
			// Process p = r.exec( "osascript " + s );
			}


		public static void reboot() throws Throwable
			{
			// Runtime r = Runtime.getRuntime();
			// Process p = r.exec( "reboot" );
			}
	*/

	public static double toDouble( String s )
		{
		return Double.valueOf( s ).doubleValue();
		}


	public static double toRadians( double degrees )
		{
		return ( Math.PI * degrees ) / 180.0;
		}


	@SuppressWarnings( "deprecation" )
	public static String getExceptionText( Throwable t )
		{
		try
			{
			ByteArrayOutputStream o = new ByteArrayOutputStream();
			PrintStream ps = new PrintStream( o );
			t.printStackTrace( ps );
			ps.flush();

			String errorString = o.toString( 0 );
			ps.close();
			o.close();

			return errorString;
			}
		catch ( Throwable theX )
			{
			Util.printX( theX );
			}

		return "";
		}
	}
