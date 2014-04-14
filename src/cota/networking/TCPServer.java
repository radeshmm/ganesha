package cota.networking;

import java.net.ServerSocket;
import java.net.Socket;

import cota.io.InStream;
import cota.io.OutStream;
import cota.threads.ThreadPool;
import cota.util.Util;


// TCPServer
// A generic class that implements a memory efficient multithreaded TCP server
public class TCPServer
	{
	// Setting this to true will prevent the server from accepting any more connections
	public static boolean refuseConnections = false;

	public String serverName = null;
	public int port = 0;

	String threadClass = null;

	ThreadPool pool = null;
	boolean debug = false;
	boolean useReaper = false;

	// Some TCPServerThreads will need to communication with both the client and
	// a web service
	// In that instance, an extra set of IO Streams is created for the web
	// service
	public boolean usesExternalWebService = false;

	TCPServerLoop serverLoop = null;

	public int socketTimeout = 60 * 1000;

	static
		{
		System.runFinalizersOnExit( true );
		}


	public TCPServer( String serverName, int port ) throws Throwable
		{
		this.serverName = serverName;
		this.port = port;

		useReaper = false;
		}


	protected void finalize() throws Throwable
		{
		if ( pool == null )
			System.out.println( "TCP Server '" + serverName + "' exiting without ever being started.  Remember to call start()" );

		super.finalize();
		}


	public void start() throws Throwable
		{
		Util.print( "Starting " + serverName + " on port " + port );

		pool = new ThreadPool( "cota.networking.TCPServerThread" );

		serverLoop = new TCPServerLoop( this );
		}


	public ServerSocket createSocket( int port ) throws Throwable
		{
		return new ServerSocket( port );
		}


	public void handleRequest( Socket s, InStream in, OutStream out ) throws Throwable
		{
		throw new Throwable( "handleRequest( s, in, out ) NOT IMPLEMENTED" );
		}


	public void handleRequest( Socket s, InStream in, OutStream out, InStream webServiceIn, OutStream webServiceOut ) throws Throwable
		{
		throw new Throwable( "handleRequest( s, in, out, webServiceIn, webServiceOut ) NOT IMPLEMENTED" );
		}


	public void handleError( Throwable errorX, OutStream out ) throws Throwable
		{
		}
	}