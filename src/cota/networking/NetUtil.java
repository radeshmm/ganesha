package cota.networking;

import java.net.Socket;
import java.util.Date;

import cota.io.InStream;
import cota.io.OutStream;
import cota.util.Hashtable_so;
import cota.util.Queue;
import cota.util.StringUtils;


public class NetUtil
	{
	static public String fiveYearsFromNow = "";
	static public String fiveYearsAgo = "";

	static
		{
		init();
		}


	public static void closeSocket( Socket s )
		{
		try
			{
			s.getOutputStream().close();
			}
		catch ( Throwable ignored )
			{
			}

		try
			{
			s.getInputStream().close();
			}
		catch ( Throwable ignored )
			{
			}

		try
			{
			s.close();
			}
		catch ( Throwable ignored )
			{
			cota.util.Util.printX( "ERROR IN NetUtil.closeSocket", ignored );
			}
		}


	public static void closeConnection( Socket s, InStream in, OutStream out )
		{
		try
			{
			out.close();
			}
		catch ( Throwable ignored )
			{
			}

		try
			{
			in.close();
			}
		catch ( Throwable ignored )
			{
			}

		try
			{
			s.close();
			}
		catch ( Throwable ignored )
			{
			//			cota.util.Util.printX( "ERROR IN NetUtil.closeConnection", ignored );
			}
		}


	@SuppressWarnings( "deprecation" )
	static void init()
		{
		long now = System.currentTimeMillis();
		now = now + ( 1000L * 60L * 60L * 24L * 365L * 5L );

		Date then = new Date( now );
		fiveYearsFromNow = then.toGMTString();

		now = System.currentTimeMillis();
		now = now - ( 1000L * 60L * 60L * 24L * 365L * 5L );

		then = new Date( now );
		fiveYearsAgo = then.toGMTString();
		}


	// Find any cookies in the response
	public static Hashtable_so findClientCookies( Queue header )
		{
		Hashtable_so results = new Hashtable_so();
		try
			{
			// check for cookies
			int size = header.size();
			for ( int i = 0; i < size; i++ )
				{
				String line = (String) header.elementAt( i );

				if ( line.charAt( 6 ) == ':' )
					{
					String capped = line.toUpperCase();
					if ( capped.startsWith( "COOKIE:" ) )
						{
						int start = 0;
						while ( true )
							{
							int index1 = line.indexOf( " ", start );
							int index2 = line.indexOf( "=", start );
							if ( index2 == -1 )
								break;

							int index3 = line.indexOf( ";", start );
							if ( index3 == -1 )
								index3 = line.indexOf( " ", index2 + 1 );
							if ( index3 == -1 )
								index3 = line.length();

							String cookie = line.substring( index1 + 1, index2 );
							String value = line.substring( index2 + 1, index3 );

							//							System.out.println( "#" + cookie + "#" + value + "#" );
							results.put( cookie, value );

							start = index3 + 1;
							}
						}
					}
				}
			}
		catch ( Throwable theX )
			{
			}

		return results;
		}


	public static byte[] createRedirectHeader( String url, String host, String stateIndexToRecord ) throws Throwable
		{
		StringBuffer sb = new StringBuffer();

		String d = "<html><head></head><body>Please click <a href=\"" + url + "\">here</a> to continue.</body></html>";

		sb.append( "HTTP/1.0 302 OK\r\n" );
		sb.append( "Location: " + url + "\r\n" );

		sb.append( "Pragma: no-cache\r\n" );
		sb.append( "Cache-Control: no-cache\r\n" );
		sb.append( "Expires: 0\r\n" );

		if ( stateIndexToRecord != null )
			{
			String portlessHost = host;
			int index = host.indexOf( ":" );
			if ( index != -1 )
				portlessHost = host.substring( 0, index );

			sb.append( "Set-Cookie: state_" + host.replace( ':', '_' ).replace( '.', '_' ) + "=" + stateIndexToRecord + "; path=/; expires=" + NetUtil.fiveYearsFromNow + "; domain=" + portlessHost + ";\r\n" );
			}

		sb.append( "Content-type: text/html;charset=iso-8859-1\r\n" );
		sb.append( "Content-Length: " + d.length() + "\r\n" );
		sb.append( "\r\n" );

		sb.append( d );

		//		System.out.println( sb.toString() );

		return sb.toString().getBytes();
		}


	// Send the redirection header
	public static void sendRedirectHeader( OutStream out, String url ) throws Throwable
		{
		String mimeType = "text/html;charset=iso-8859-1";
		String d = "<html><head></head><body>Please click <a href=\"" + url + "\">here</a> to continue.</body></html>";

		out.writeLine( "HTTP/1.0 302 OK" );
		out.writeLine( "Location: " + url );

		out.writeLine( "Pragma: no-cache" );
		out.writeLine( "Cache-Control: no-cache" );
		out.writeLine( "Expires: 0" );

		out.writeLine( "Content-type: " + mimeType );
		out.writeLine( "Content-Length: " + d.length() );
		out.writeLine( "" );

		out.writePlainString( d );
		out.flush();
		}


	// Send the redirection header
	public static void sendPermanentRedirectHeader( OutStream out, String url ) throws Throwable
		{
		String mimeType = "text/html;charset=iso-8859-1";
		String d = "<html><head></head><body>Please click <a href=\"" + url + "\">here</a> to continue.</body></html>";

		out.writeLine( "HTTP/1.0 301 OK" );
		out.writeLine( "Location: " + url );

		out.writeLine( "Pragma: no-cache" );
		out.writeLine( "Cache-Control: no-cache" );
		out.writeLine( "Expires: 0" );

		out.writeLine( "Content-type: " + mimeType );
		out.writeLine( "Content-Length: " + d.length() );
		out.writeLine( "" );

		out.writePlainString( d );
		out.flush();
		}


	public static String extractReferrer( Queue header ) throws Throwable
		{
		int size = header.size();
		for ( int i = 0; i < size; i++ )
			{
			String line = (String) header.elementAt( i );

			if ( line.toLowerCase().startsWith( "referer" ) )
				{
				int index = line.indexOf( " " );

				String ref = line.toLowerCase().substring( index + 1, line.length() );
				if ( ref.startsWith( "http://" ) )
					ref = ref.substring( 7, ref.length() );

				return ref;
				}
			}

		return "";
		}


	// Returns a Fashtable (name, value) of arguments from the request
	public static Hashtable_so extractURLArgs( String request ) throws Throwable
		{
		Hashtable_so args = new Hashtable_so();

		int questionIndex = request.indexOf( "?" );

		int start = questionIndex + 1;
		boolean done = false;
		while ( !done )
			{
			int end = request.indexOf( "&", start );

			if ( end == -1 )
				{
				end = request.length();

				// Chop off ending spaces
				while ( request.charAt( end - 1 ) == ' ' )
					end--;

				done = true;
				}

			int equalIndex = request.indexOf( "=", start );
			String name = StringUtils.decodeURLString( request.substring( start, equalIndex ) );
			String value = StringUtils.decodeURLString( request.substring( equalIndex + 1, end ) );

			// System.out.println( "#" + name + "#" + value + "#" );

			args.put( name, value );

			start = end + 1;
			}

		return args;
		}
	}