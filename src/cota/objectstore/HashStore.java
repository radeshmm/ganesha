package cota.objectstore;

import java.io.File;
import java.io.FileInputStream;
import java.util.zip.CRC32;

import cota.crypto.Murmur;
import cota.ganesha.MapServer;
import cota.ganesha.StrictGob;
import cota.io.InStream;
import cota.io.Message;
import cota.util.Hashtable_lo;
import cota.util.LRU_io;
import cota.util.PairSL;
import cota.util.Queue;
import cota.util.StringUtils;
import cota.util.TriOLI;


public class HashStore extends ObjectStore
	{
	public LRU_io locks = null;
	int numBuckets = 0;

	String dirName = null;


	public HashStore( String dirName, int numBuckets, long cacheSize ) throws Throwable
		{
		super( numBuckets, dirName, cacheSize );

		this.dirName = dirName;
		this.numBuckets = numBuckets;

		// Used for locking the objects based on objectNum
		// 100MB will allow about 1.2 million locks (the limit of objects being modified simultaneously on the server)
		locks = new LRU_io( 1024L * 1024L * 100L );
		locks.name = "HashStore.locks";
		}


	// Not currently used
	static byte[] compress( byte[] bytes ) throws Throwable
		{
		//		if ( 1 == 1 )
		return bytes;
		/*
				if ( bytes.length < 20 )
					{
					// uncompressed
					byte[] bytes2 = new byte[bytes.length + 1];
					System.arraycopy( bytes, 0, bytes2, 0, bytes.length );

					return bytes2;
					}

				byte[] compressed = Snappy.compress( bytes );
				if ( compressed.length < bytes.length )
					{
					// compressed
					byte[] bytes2 = new byte[compressed.length + 1];
					System.arraycopy( compressed, 0, bytes2, 0, compressed.length );

					bytes2[ bytes2.length - 1 ] = 1;

					return bytes2;
					}

				// uncompressed
				byte[] bytes2 = new byte[bytes.length + 1];
				System.arraycopy( bytes, 0, bytes2, 0, bytes.length );

				return bytes2;
		*/}


	static byte[] decompress( byte[] bytes ) throws Throwable
		{
		//		if ( 1 == 1 )
		return bytes;

		/*	if ( bytes[ bytes.length - 1 ] == 0 )
				{
				// uncompressed
				byte[] bytes2 = new byte[bytes.length - 1];
				System.arraycopy( bytes, 0, bytes2, 0, bytes.length - 1 );

				return bytes2;
				}

			byte[] bytes2 = new byte[bytes.length - 1];
			System.arraycopy( bytes, 0, bytes2, 0, bytes.length - 1 );

			return Snappy.uncompress( bytes2 );
		*/}


	// Store and retrieve the bucket applying a CRC32 checksum as well
	// to make sure that the bucket is intact
	// ================================================
	void ____BUCKETS________()
		{
		}


	private void storeBucketObjects( int bucketNum, Queue q ) throws Throwable
		{
		//		System.out.println( "Store in bucket " + bucketNum + ": " + q.size() );

		Message m = new Message();

		m.write4Bytes( q.size() );
		for ( int i = 0; i < q.size(); i++ )
			{
			BucketObject b = (BucketObject) q.elementAt( i );
			b.writeToMessage( m );
			}

		int checksum = m.checksum();
		m.write4Bytes( checksum );

		put( bucketNum, m.returnBytes() );
		}


	private Queue retrieveBucketObjects( int bucketNum ) throws Throwable
		{
		byte[] bytes = get( bucketNum );
		if ( bytes == null )
			return null;

		//		System.out.println( "Original bytes in bucket " + bucketNum + ": " + bytes.length );
		int c1 = bytes[ bytes.length - 4 ];
		int c2 = bytes[ bytes.length - 3 ];
		int c3 = bytes[ bytes.length - 2 ];
		int c4 = bytes[ bytes.length - 1 ];

		if ( c1 < 0 )
			c1 = c1 + 256;
		if ( c2 < 0 )
			c2 = c2 + 256;
		if ( c3 < 0 )
			c3 = c3 + 256;
		if ( c4 < 0 )
			c4 = c4 + 256;

		int checksum0 = ( ( c1 << 24 ) + ( c2 << 16 ) + ( c3 << 8 ) + c4 );

		CRC32 crc = new CRC32();
		crc.update( bytes, 0, bytes.length - 4 );
		int checksum = (int) crc.getValue();

		if ( checksum != checksum0 )
			{
			// Any errors found while trying to extract an object from the bucket
			// means that the bucket is corrupted
			// It will therefore be emptied so that the correct objects can be added later
			System.out.println( "CORRUPTED BUCKET!!!!!!!: " + bucketNum );
			System.out.println( checksum0 + "\t" + checksum );

			storeBucketObjects( bucketNum, new Queue() );

			return null;
			}

		Queue q = new Queue();
		Message m = new Message( bytes );

		int numObjects = m.read4Bytes();
		for ( int i = 0; i < numObjects; i++ )
			{
			BucketObject b = new BucketObject( m );

			q.addObject( b );
			}

		return q;
		}


	private void putIntoBucket( int bucketNum, BucketObject b ) throws Throwable
		{
		Queue q0 = retrieveBucketObjects( bucketNum );
		if ( q0 == null )
			q0 = new Queue();

		//		System.out.println( "Originally in bucket " + bucketNum + ": " + q0.size() );

		Queue q = new Queue(); // build the new Queue
		for ( int i = 0; i < q0.size(); i++ )
			{
			BucketObject b0 = (BucketObject) q0.elementAt( i );

			q.addObject( b0 );

			// deal with duplicates
			if ( b.type == BucketObject.OBJECT )
				{
				if ( b.id == b0.id )
					{
					// if the object in the bucket is newer than the object that we're trying to
					// add, then don't do anything
					if ( b0.timestamp > b.timestamp )
						return;

					q.removeLast(); // remove the object that was just added
					}
				}

			if ( b.type == BucketObject.TRANSLATION )
				{
				if ( b.key.equals( b0.key ) )
					{
					// Translation already added?
					// Just bail as translations never change (they cannot change by definition)
					return;
					}
				}
			}

		// Add the new (or updated) BucketObject
		q.addObject( b );

		storeBucketObjects( bucketNum, q );
		}


	private void removeFromBucket( int bucketNum, BucketObject b ) throws Throwable
		{
		Queue q0 = retrieveBucketObjects( bucketNum );
		if ( q0 == null )
			return;

		Queue q = new Queue();
		for ( int i = 0; i < q0.size(); i++ )
			{
			BucketObject b0 = (BucketObject) q0.elementAt( i );

			q.addObject( b0 );

			// remove
			if ( b.type == BucketObject.OBJECT )
				{
				if ( b.id == b0.id )
					q.removeLast();
				}

			if ( b.type == BucketObject.TRANSLATION )
				{
				if ( b.key.equals( b0.key ) )
					q.removeLast();
				}
			}

		storeBucketObjects( bucketNum, q );
		}


	private TriOLI locateObjectInBucket( int bucketNum, long id ) throws Throwable
		{
		Queue q = retrieveBucketObjects( bucketNum );

		if ( q == null )
			return null;

		for ( int i = 0; i < q.size(); i++ )
			{
			BucketObject b = (BucketObject) q.elementAt( i );

			if ( b.type == BucketObject.OBJECT )
				if ( b.id == id )
					return new TriOLI( b.bytes, b.timestamp, b.checksum );
			}

		return null;
		}


	private long locateTranslationInBucket( int bucketNum, String key ) throws Throwable
		{
		Queue q = retrieveBucketObjects( bucketNum );

		if ( q == null )
			return 0;

		for ( int i = 0; i < q.size(); i++ )
			{
			BucketObject b = (BucketObject) q.elementAt( i );

			if ( b.type == BucketObject.TRANSLATION )
				if ( b.key.equals( key ) )
					return b.id;
			}

		return 0;
		}


	// ================================================
	void ____UTIL________()
		{
		}


	private int returnBucketNum( long id )
		{
		int bucketNum = Murmur.hash( id );
		if ( bucketNum < 0 )
			bucketNum = bucketNum + Integer.MAX_VALUE;

		return bucketNum % numBuckets;
		}


	private int returnBucketNum( String id )
		{
		int bucketNum = Murmur.hash( id, 0 );
		if ( bucketNum < 0 )
			bucketNum = bucketNum + Integer.MAX_VALUE;

		return bucketNum % numBuckets;
		}


	public Object returnLock( int bucketNum )
		{
		Object lock = null;

		synchronized ( locks )
			{
			lock = locks.get( bucketNum );

			if ( lock == null )
				{
				lock = new Object();
				locks.put( bucketNum, lock );
				}
			}

		//		System.out.println( "" + lock );

		return lock;
		}


	// At this level objects will be synchronized on the bucketNum
	// ================================================
	void ____API________()
		{
		}


	public void put( long id, byte[] bytes, long timestamp ) throws Throwable
		{
		if ( bytes.length == 0 )
			throw new Throwable( "TRYING TO PUT BYTES OF 0 LENGTH" );

		bytes = compress( bytes );

		BucketObject bo = new BucketObject( id, bytes, timestamp );

		int bucketNum = returnBucketNum( id );

		Object lock = returnLock( bucketNum );
		synchronized ( lock )
			{
			putIntoBucket( bucketNum, bo );
			}
		}


	public void put( String key, long id ) throws Throwable
		{
		BucketObject bo = new BucketObject( key, id );

		int bucketNum = returnBucketNum( key );

		Object lock = returnLock( bucketNum );
		synchronized ( lock )
			{
			putIntoBucket( bucketNum, bo );
			}
		}


	public void remove( long id ) throws Throwable
		{
		BucketObject bo = new BucketObject( id, null, 0 ); // null bytes

		int bucketNum = returnBucketNum( id );

		Object lock = returnLock( bucketNum );
		synchronized ( lock )
			{
			removeFromBucket( bucketNum, bo );
			}
		}


	public void remove( String key ) throws Throwable
		{
		BucketObject bo = new BucketObject( key, 0 );

		int bucketNum = returnBucketNum( key );

		Object lock = returnLock( bucketNum );
		synchronized ( lock )
			{
			removeFromBucket( bucketNum, bo );
			}
		}


	// Returns
	//		bytes
	//		timestamp
	//		checksum
	public TriOLI get( long id ) throws Throwable
		{
		int bucketNum = returnBucketNum( id );

		Object lock = returnLock( bucketNum );

		TriOLI t = null;
		synchronized ( lock )
			{
			t = locateObjectInBucket( bucketNum, id );
			}

		if ( t != null )
			t.x = decompress( (byte[]) t.x );

		return t;
		}


	// Add the object and return the key
	public long get( String key ) throws Throwable
		{
		int bucketNum = returnBucketNum( key );

		Object lock = returnLock( bucketNum );
		synchronized ( lock )
			{
			long id = locateTranslationInBucket( bucketNum, key );

			return id;
			}
		}


	// Rebuilding happens by looking at all the BucketObjects contained in all the buckets on the server
	// By looking at the servers which own the bucket, we can determine if if ip needs to be updated with
	// the given bucket
	// If the data we're sending over is more recent than what the server to update has, it will be written.

	// Note that a read could have been used to update the data on the ip, but that would require reads from
	// three servers which is likely to take longer
	public void rebuildIP( String ip ) throws Throwable
		{
		// Go through all the buckets and find objects owned by the given ip
		for ( int i = 0; i < numBuckets; i++ )
			{
			Queue q = retrieveBucketObjects( i );

			if ( q != null )
				{
				for ( int j = 0; j < q.size(); j++ )
					{
					BucketObject bo = (BucketObject) q.elementAt( j );

					Queue owningSet = null;

					if ( bo.type == BucketObject.OBJECT )
						owningSet = MapServer.returnOwningSet( bo.id );

					if ( bo.type == BucketObject.TRANSLATION )
						owningSet = MapServer.returnTranslationOwningSet( bo.key, 0 );

					// For each bucket check which servers own it
					for ( int k = 0; k < owningSet.size(); k++ )
						{
						// If one of the servers is the one we want to update
						// then tell it that it needs the current data
						// If the data is more recent than what it has, then it will be written
						// Otherwise it wont be written
						String ip0 = (String) owningSet.elementAt( k );
						if ( ip0.equals( ip ) )
							{
							if ( dirName.equals( "data_objects" ) )
								HashStoreServer.putObject( ip, bo.id, bo.bytes, bo.timestamp );

							if ( dirName.equals( "data_bytes" ) )
								HashStoreServer.putBytes( ip, bo.id, bo.bytes, bo.timestamp );

							if ( dirName.equals( "data_translations" ) )
								HashStoreServer.putTranslation( ip, bo.key, bo.id );

							break;
							}
						}
					}
				}

			if ( ( i % 2560000 ) == 0 )
				System.out.println( "Sending " + dirName + " to " + ip + ": " + StringUtils.formatPrice( 100.0 * i / numBuckets ) + "%" );
			}

		System.out.println( "Sending " + dirName + " to " + ip + ": 100%\n" );
		}


	// See which translations exist from the workspace/table
	public Queue namesAndIDsInTable( String workspace, String table ) throws Throwable
		{
		Queue q = new Queue();
		Hashtable_lo ft = new Hashtable_lo();

		File dir = new File( "data_translations" );
		String[] files = dir.list();
		for ( int i = 0; i < files.length; i++ )
			{
			if ( files[ i ].startsWith( "block_" ) )
				{
				File f = new File( dir, files[ i ] );
				if ( ( (int) f.length() ) > 0 )
					{
					byte[] bytes = new byte[(int) f.length()];
					InStream in = new InStream( new FileInputStream( f ) );
					in.readBytes( bytes );
					in.close();

					// Each Translation entry has bytes in the form of
					// workspace%table%name
					String prefix = workspace + "%" + table;
					char[] prefixChars = prefix.toCharArray();

					Message m = new Message( bytes );

					int start = 0;
					while ( start != -1 )
						{
						start = StringUtils.findWithCase( bytes, prefixChars, start );
						if ( start != -1 )
							{
							// Backup three bytes to get the length
							start = start - 3;

							String key = m.readString( start );
							long objectID = m.read8Bytes();

							if ( ft.get( objectID ) == null )
								{
								ft.put( objectID, "" );

								int index1 = key.indexOf( "%" );
								int index2 = key.indexOf( "%", index1 + 1 );

								key = key.substring( index2 + 1, key.length() );

								q.addObject( new PairSL( key, objectID ) );
								}

							start = start + 5;
							}
						}
					}
				}

			System.out.println( q.size() + " names and ids found in " + workspace + "%" + table + ": " + i + " / " + files.length );
			}

		return q;
		}


	public Queue returnAllAttributes( int gobType, int attribute ) throws Throwable
		{
		Queue objectIDs = new Queue();

		// Go through all the buckets and find objects owned by the given ip
		for ( int i = 0; i < numBuckets; i++ )
			{
			Queue q = retrieveBucketObjects( i );

			if ( q != null )
				{
				for ( int j = 0; j < q.size(); j++ )
					{
					BucketObject bo = (BucketObject) q.elementAt( j );

					if ( bo.type == BucketObject.OBJECT )
						{
						try
							{
							StrictGob gob = new StrictGob( gobType, bo.bytes );

							String s = gob.getString( attribute );

							if ( s != null )
								objectIDs.addObject( new PairSL( s, bo.id ) );
							}
						catch ( Throwable theX )
							{
							// If the attempt to create the GobObject fails, then it is not of the type we are looking for
							}
						}
					}
				}

			if ( ( i % 2560000 ) == 0 )
				System.out.println( "Retrieving all objects: " + StringUtils.formatPrice( 100.0 * i / numBuckets ) + "%" );
			}

		return objectIDs;
		}
	}
