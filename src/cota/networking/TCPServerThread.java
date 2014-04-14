package cota.networking;

import java.net.Socket;

import cota.io.InStream;
import cota.io.OutStream;
import cota.threads.ReusableThread;
import cota.util.Fashtable_so;
import cota.util.Util;


// A reusable thread that keeps the InStream and OutStream (and all their buffers)
// around so that they can be reused
public class TCPServerThread extends ReusableThread
	{
	InStream in = new InStream();
	OutStream out = new OutStream();

	// Used for servers that connect to web services
	InStream webServiceIn = null;
	OutStream webServiceOut = null;


	public TCPServerThread()
		{
		}


	public void performTask( Fashtable_so threadArgs ) throws Throwable
		{
		Socket s = (Socket) threadArgs.get( "Socket" );
		TCPServer server = (TCPServer) threadArgs.get( "Server" );

		Throwable errorX = null;

		if ( server.usesExternalWebService )
			{
			// Make sure that the needed IO Streams for making connections to
			// separate web services are there
			if ( webServiceIn == null )
				webServiceIn = new InStream();

			if ( webServiceOut == null )
				webServiceOut = new OutStream();
			}

		try
			{
			in.reassign( s.getInputStream() );
			out.reassign( s.getOutputStream() );

			if ( server.useReaper )
				SocketReaper.addSocket( s );

			if ( server.usesExternalWebService )
				server.handleRequest( s, in, out, webServiceIn, webServiceOut );
			else
				server.handleRequest( s, in, out );
			}
		catch ( Throwable theX )
			{
			errorX = theX;
			Util.printX( "TCPServerThread.performTask", theX );
			}
		finally
			{
			//			System.out.println( s + " A" );
			if ( errorX != null )
				{
				server.handleError( errorX, out );
				NetUtil.closeConnection( s, in, out );
				}

			//		System.out.println( s + " B" );
			try
				{
				out.flush();
				}
			catch ( Throwable ignored )
				{
				}

			//		System.out.println( s + " C" );
			try
				{
				// Allow 30 seconds for the last bytes to come in
				long startTime = System.currentTimeMillis();

				while ( in.readByte() != -1 )
					{
					long elapsed = System.currentTimeMillis() - startTime;
					if ( elapsed > 30 * 1000 )
						break;
					}
				}
			catch ( Throwable ignored )
				{
				}

			//		System.out.println( s + " D" );
			try
				{
				out.close();
				}
			catch ( Throwable ignored )
				{
				}

			//		System.out.println( s + " E" );
			try
				{
				in.close();
				}
			catch ( Throwable ignored )
				{
				}

			//		System.out.println( s + " F" );
			if ( server.usesExternalWebService )
				{
				try
					{
					webServiceOut.close();
					}
				catch ( Throwable ignored )
					{
					}

				try
					{
					webServiceIn.close();
					}
				catch ( Throwable ignored )
					{
					}
				}

			//		System.out.println( s + " G" );
			try
				{
				s.close();

				if ( server.useReaper )
					SocketReaper.remove( s );
				}
			catch ( Throwable ignored )
				{
				}

			//			System.out.println( s + " H" );
			}
		}
	}
