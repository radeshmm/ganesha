package cota.ganesha;

import java.io.File;
import java.io.FileOutputStream;

import cota.crypto.Murmur;
import cota.io.Message;
import cota.io.OutStream;
import cota.objectstore.HashStoreServer;
import cota.objectstore.MemoryStoreServer;
import cota.util.ErrorHandler;
import cota.util.LRU_sl;
import cota.util.LRU_so;


// WriteManager is responsible for updating the dirty file if a server cannot be updated correctly

// The nice thing about the WriteManager is that each object will only
// be attempted to be written from only one GobServer (the 'owning' GobServer)
// This 'owning' server provides all relevant synchronization
public class WriteManager
	{
	public class ObjectToWrite
		{
		public String ip = null;
		public long id = 0;
		public byte[] bytes = null;


		public ObjectToWrite( String ip, long id, byte[] bytes )
			{
			this.ip = ip;
			this.id = id;
			this.bytes = bytes;
			}
		};

	static final int OBJECT = 0;
	static final int TRANSLATION = 1;
	static final int BYTES = 2;
	static final int MEMORY = 3;

	static File dir = null;

	//String ip, long id, byte[] bytes	static Queue objectsToWrite = new Queue();

	static LRU_so dirtyFileLocks = null;
	static LRU_sl recentlyMarkedDirty = null;

	// 10 GB maximum dirty file size
	static final long kMaximumDirtyFileSize = 1024L * 1024L * 1024L * 10L;

	public static boolean debug = false;

	static WriteManager singleton = null;

	static
		{
		try
			{
			dir = new File( "dirty" );

			dirtyFileLocks = new LRU_so( 1024L * 1024L * 200L );
			dirtyFileLocks.name = "WriteManager.dirtyFileLocks";

			recentlyMarkedDirty = new LRU_sl( 1024L * 1024L * 1024L );
			recentlyMarkedDirty.name = "WriteManager.recentlyMarkedDirty";

			singleton = new WriteManager();

			//			Thread t = new Thread( singleton );
			//		t.start();
			}
		catch ( Throwable theX )
			{
			cota.util.Util.printX( theX );
			}
		}


	private static Object returnDirtyLock( String ip )
		{
		Object lock = null;

		synchronized ( dirtyFileLocks )
			{
			lock = dirtyFileLocks.get( ip );

			if ( lock == null )
				{
				lock = new Object();
				dirtyFileLocks.put( ip, lock );
				}
			}

		return lock;
		}


	public static void appendMessage( String ip, Message m ) throws Throwable
		{
		Object lock = returnDirtyLock( ip );

		synchronized ( lock )
			{
			File f = new File( dir, ip + ".new" );

			if ( f.length() > kMaximumDirtyFileSize )
				{
				ErrorHandler.error( "MAXIMUM DIRTY FILE SIZE REACHED!!!!!!!" );

				return;
				}

			OutStream out = new OutStream( new FileOutputStream( f, true ) );

			out.writeMessage( m );

			out.close();
			}
		}


	public static void markTranslationAsDirty( String ip, String key, long id )
		{
		// Don't bother marking the same thing as dirty again
		synchronized ( recentlyMarkedDirty )
			{
			long v = recentlyMarkedDirty.get( ip + "_" + key );

			if ( v == id )
				return;

			recentlyMarkedDirty.put( ip + "_" + key, id );
			}

		try
			{
			Message m = new Message();
			m.writeByte( TRANSLATION );
			m.writeString( key );
			m.write8Bytes( id );

			if ( debug )
				System.out.println( "MARKING TRANSLATION AS DIRTY: " + ip + "\t" + key );

			appendMessage( ip, m );
			}
		catch ( Throwable theX )
			{
			cota.util.Util.printX( theX );
			}
		}


	public static void markBytesAsDirty( String ip, long id, byte[] bytes, int type )
		{
		int checksum = Murmur.hash( bytes, 0 );

		// Don't bother marking the same thing as dirty again
		synchronized ( recentlyMarkedDirty )
			{
			long v = recentlyMarkedDirty.get( ip + "_" + id );

			if ( v == checksum )
				return;

			recentlyMarkedDirty.put( ip + "_" + id, checksum );
			}

		try
			{
			Message m = new Message();
			m.writeByte( type );
			m.write8Bytes( id );
			m.writeBytes( bytes );

			// Use the current time as the timestamp
			m.write8Bytes( System.currentTimeMillis() );

			if ( debug )
				System.out.println( "MARKING OBJECT AS DIRTY: " + ip + "\t" + id );

			appendMessage( ip, m );
			}
		catch ( Throwable theX )
			{
			cota.util.Util.printX( theX );
			}
		}


	// If anything goes wrong, then write to disk for updating the server later
	public static void putTranslation( String ip, String key, long id )
		{
		try
			{
			HashStoreServer.putTranslation( ip, key, id );
			}
		catch ( Throwable theX )
			{
			markTranslationAsDirty( ip, key, id );
			}
		}


	public static void putObjectOrBytes( String ip, long id, byte[] bytes, boolean isBinary ) throws Throwable
		{
		if ( bytes.length == 0 )
			throw new Throwable( "TRYING TO PUT BYTES OF ZERO LENGTH" );

		if ( isBinary )
			{
			try
				{
				HashStoreServer.putBytes( ip, id, bytes );
				}
			catch ( Throwable theX )
				{
				markBytesAsDirty( ip, id, bytes, BYTES );
				}
			}
		else
			{
			try
				{
				HashStoreServer.putObject( ip, id, bytes );
				}
			catch ( Throwable theX )
				{
				markBytesAsDirty( ip, id, bytes, OBJECT );
				}
			}
		}


	public static void putMemoryBytes( String ip, long id, byte[] bytes ) throws Throwable
		{
		if ( bytes.length == 0 )
			throw new Throwable( "TRYING TO PUT BYTES OF ZERO LENGTH" );

		try
			{
			MemoryStoreServer.putBytes( ip, id, bytes );
			}
		catch ( Throwable theX )
			{
			markBytesAsDirty( ip, id, bytes, MEMORY );
			}
		}
	}