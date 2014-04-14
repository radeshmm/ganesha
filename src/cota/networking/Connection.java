package cota.networking;

import java.net.Socket;

import cota.ganesha.MapServer;
import cota.io.InStream;
import cota.io.OutStream;
import cota.util.Util;


public class Connection
	{
	public String key = null;

	public Socket s = null;
	public InStream in = null;
	public OutStream out = null;

	public long timeAddedToPool = 0;

	public static boolean debug = false;


	public Connection( String ip, int port ) throws Throwable
		{
		if ( MapServer.serverIsDown( ip ) )
			throw new Throwable( "IP IS DOWN: " + ip );

		if ( ip == null )
			throw new Throwable( "TRYING TO CREATE CONNECTION WITH NULL IP" );

		this.key = ip + ":" + port;

		if ( debug )
			Util.printStackTrace( "Creating new Connection: " + key );

		s = new Socket( ip, port );

		in = new InStream( s.getInputStream() );
		out = new OutStream( s.getOutputStream() );
		}


	public void close()
		{
		NetUtil.closeConnection( s, in, out );
		}


	protected void finalize() throws Throwable
		{
		NetUtil.closeConnection( s, in, out );

		super.finalize();
		}
	}
