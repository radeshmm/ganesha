package cota.networking;

import java.io.InterruptedIOException;
import java.net.ServerSocket;
import java.net.Socket;

import cota.util.Util;


// TCPServerLoop
// This is the actual loop where the server waits for connections
// This was done so that WebServices which extend the TCPServer class could implement Runnable
// and not squash the preexisting run() method
public class TCPServerLoop implements Runnable
	{
	TCPServer server = null;


	public TCPServerLoop( TCPServer server )
		{
		this.server = server;

		Thread t = new Thread( this );
		t.start();
		}


	public void run()
		{
		// Create the nec essary socket
		ServerSocket serverSocket = null;
		try
			{
			serverSocket = server.createSocket( server.port );

			// Default 30 second timeout
			// Allow timeouts in case something goes wrong in the accept. True?
			serverSocket.setSoTimeout( 30 * 1000 );
			}
		catch ( Throwable theX )
			{
			Util.printX( theX );

			// Can't continue if there was a problem creating the socket
			return;
			}

		// Infinite loop
		TCPServerThread thread = null;
		while ( true )
			{
			Socket s = null;

			try
				{
				s = serverSocket.accept();

				if ( TCPServer.refuseConnections )
					return;

				s.setSoTimeout( server.socketTimeout );
				s.setSoLinger( true, 60 * 5 );

				thread = (TCPServerThread) server.pool.getThread();

				thread.addArg( "Socket", s );
				thread.addArg( "Server", server );

				thread.activate();
				}
			catch ( InterruptedIOException timeout )
				{
				// Looks like a ServerSocket.accept timeout occured
				try
					{
					s.close();
					}
				catch ( Throwable theX )
					{
					}
				}
			catch ( Throwable theX )
				{
				Util.printX( "ERROR WITHIN THE TCP SERVER " + server.serverName, theX );
				try
					{
					s.close();
					}
				catch ( Throwable theX2 )
					{
					}
				}
			}
		}
	}