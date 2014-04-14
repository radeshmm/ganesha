package cota.ganesha;

import java.io.File;
import java.io.FileInputStream;

import cota.io.InStream;
import cota.io.Message;
import cota.objectstore.HashStoreServer;
import cota.objectstore.MemoryStoreServer;
import cota.util.StringUtils;


public class DirtyManager implements Runnable
	{
	static File dir = null;

	public static boolean debug = false;

	static DirtyManager singleton = null;


	public DirtyManager()
		{
		Thread t = new Thread( this );
		t.start();
		}


	public static void init() throws Throwable
		{
		dir = new File( "dirty" );
		if ( !dir.exists() )
			dir.mkdir();

		singleton = new DirtyManager();
		}


	public void processDirtyTranslation( String ip, Message m ) throws Throwable
		{
		String key = m.readString();
		long id = m.read8Bytes();

		//		if ( debug )
		//		System.out.println( "Processing dirty translation: " + ip + "\t" + key );

		HashStoreServer.putTranslation( ip, key, id );
		}


	public void processDirtyObject( String ip, Message m ) throws Throwable
		{
		long id = m.read8Bytes();
		byte[] bytes = m.readBytes();
		long timestamp = m.read8Bytes();

		//		if ( debug )
		//		System.out.println( "Processing dirty object: " + ip + "\t" + id );

		HashStoreServer.putObject( ip, id, bytes, timestamp );
		}


	public void processDirtyBytes( String ip, Message m ) throws Throwable
		{
		long id = m.read8Bytes();
		byte[] bytes = m.readBytes();
		long timestamp = m.read8Bytes();

		//		if ( debug )
		//		System.out.println( "Processing dirty object: " + ip + "\t" + id );

		HashStoreServer.putBytes( ip, id, bytes, timestamp );
		}


	public void processDirtyMemoryBytes( String ip, Message m ) throws Throwable
		{
		long id = m.read8Bytes();
		byte[] bytes = m.readBytes();
		long timestamp = m.read8Bytes();

		//		if ( debug )
		//		System.out.println( "Processing dirty object: " + ip + "\t" + id );

		MemoryStoreServer.putBytes( ip, id, bytes, timestamp );
		}


	public void processFile( String ip, File f ) throws Throwable
		{
		InStream in = new InStream( new FileInputStream( f ) );

		long total = f.length();
		int count = 0;
		long lastTime = 0;

		try
			{
			while ( true )
				{
				Message m = in.readMessage();

				count = count + m.maxIndex;

				if ( debug )
					if ( ( System.currentTimeMillis() - lastTime ) > 1000 * 5 )
						{
						lastTime = System.currentTimeMillis();
						System.out.println( "Processing dirty file " + ip + ": " + StringUtils.formatPrice( count * 100.0 / total ) + "%" );
						}

				int type = m.readByte();

				if ( type == WriteManager.TRANSLATION )
					processDirtyTranslation( ip, m );

				if ( type == WriteManager.OBJECT )
					processDirtyObject( ip, m );

				if ( type == WriteManager.BYTES )
					processDirtyBytes( ip, m );

				if ( type == WriteManager.MEMORY )
					processDirtyMemoryBytes( ip, m );
				}
			}
		catch ( java.net.ConnectException theX )
			{
			System.out.println( "DirtyManager cannot connect to: " + ip );
			}
		catch ( cota.io.StreamClosedX endX )
			{
			// Finished, just delete the file
			f.delete();
			}
		catch ( Throwable theX )
			{
			// An error occurred, this file will have to be processed again later
			cota.util.Util.printX( theX );
			}

		if ( debug )
			System.out.println( "Processing dirty file " + ip + ": done" );

		in.close();
		}


	public void processDirtyDir() throws Throwable
		{
		String[] files = dir.list();

		// Take care of the .inprocess dirty files first
		for ( int i = 0; i < files.length; i++ )
			{
			String f = files[ i ];

			if ( f.endsWith( ".inprocess" ) )
				{
				String ip = f.substring( 0, f.indexOf( ".inprocess" ) );

				processFile( ip, new File( dir, f ) );
				}
			}

		// Take care of the .new dirty files
		// Simply rename them as .inprocess if there's not currently one
		for ( int i = 0; i < files.length; i++ )
			{
			String f = files[ i ];

			if ( f.endsWith( ".new" ) )
				{
				String ip = f.substring( 0, f.indexOf( ".new" ) );
				File f2 = new File( dir, ip + ".inprocess" );

				if ( !f2.exists() )
					{
					File file = new File( dir, f );

					file.renameTo( f2 );
					}
				}
			}
		}


	public void run()
		{
		while ( true )
			{
			try
				{
				processDirtyDir();

				// Try reprocessing every second
				Thread.sleep( 1000 );
				}
			catch ( Throwable theX )
				{
				cota.util.Util.printX( theX );
				}
			}
		}
	}
