package cota.objectstore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.NumberFormat;

import cota.io.InStream;
import cota.io.OutStream;
import cota.util.LRU_ib;
import cota.util.Queue;
import cota.util.StringUtils;


// Simply stores an array of bytes representing an object at a given objectNum
// Note that object writes are not synchronized
// Callers must ensure that objects are not written to simultaneously
// Objects are stored in very large files (block files)
// Because block files contain blocks of identical size, each object's location on disk can be determined by:
// 		- knowing the object's size (and therefore which block file it is in)
// 		- knowing which index into the block file the object is located at
//
// This class simply handles reading, writing and updating those objects
//
// Note that remove is not implemented
// The idea being that all information added to the database should pertain in case it
// needs to be referenced later (for some reason)

public class ObjectStore
	{
	// With the rate of growing blocks, allows a maximum block size of 1TB (not gonna happen!)
	static final int numBlockTypes = 256;

	static long[] blockSizes = new long[numBlockTypes];

	static int count = 0;
	static int kMaxSizeInCache = 1024 * 1024 * 10; // No objects larger than 10MB in the cache

	// =============================================

	// Used to mark free blocks
	// Note that blocks can be freed when an object changes size and is written somewhere else
	Queue[] freeIndices = null;

	// Where in the block files the next objects should be written
	// This also shows the number of objects currently used per block type
	int[] nextIndicesToUse = new int[numBlockTypes];

	FileChannel[] dataFileChannels = null;

	public RandomAccessFile[] dataFiles = null;
	public RandomAccessFile locationsSizesFile = null;

	// Each object's index into a block file
	// Will look at the size of the object to determine which block size to use
	// Note that location = index * blockSize
	int[] indices = null;

	// The actual sizes of the objects, not the sizes of the blocks that contain them
	// Will be smaller to or equal to the blocks that contain them 
	int[] sizes = null;

	File dataDir = null;

	private LRU_ib cache = null;

	static
		{
		try
			{
			// Create the different sizes of available free blocks
			// The block sizes are generated in an exponential series

			// The smallest block will be 4 bytes
			long size = 4;
			for ( int i = 0; i < numBlockTypes; i++ )
				{
				/*				long megs = size / 1024 / 1024;
								if ( megs < 1024 )
									System.out.println( i + "\t" + size + "\t" + size / 1024 / 1024 + "M" );
								else
									{
									long gigs = megs / 1024;

									if ( gigs < 1024 )
										System.out.println( i + "\t" + size + "\t" + megs / 1024 + "GB" );
									else
										System.out.println( i + "\t" + size + "\t" + gigs / 1024 + "TB" );
									}
				*/
				blockSizes[ i ] = size;

				long size0 = size;
				size = (long) ( size * 1.11 );

				if ( size == size0 )
					size++;
				}
			}
		catch ( Throwable theX )
			{
			cota.util.Util.printX( theX );
			}
		}


	// ================================================
	void ____INIT________()
		{
		}


	public ObjectStore( int numObjects, String dirName, long cacheSize ) throws Throwable
		{
		System.out.println( "\n\tObjectStore init: " + dirName );

		NumberFormat nf = NumberFormat.getInstance();
		System.out.println( "\t\tInitializing with " + nf.format( numObjects ) + " objects" );

		freeIndices = new Queue[numBlockTypes];
		for ( int i = 0; i < numBlockTypes; i++ )
			freeIndices[ i ] = new Queue();

		indices = new int[numObjects];
		sizes = new int[numObjects];

		// Make sure the data directory is there
		dataDir = new File( dirName );
		if ( !dataDir.exists() )
			dataDir.mkdir();

		// Initialize the file objects
		dataFileChannels = new FileChannel[numBlockTypes];
		dataFiles = new RandomAccessFile[numBlockTypes];

		// Make sure all the block files exist
		for ( int i = 0; i < numBlockTypes; i++ )
			{
			File f = new File( dataDir, "block_" + i );

			if ( !f.exists() )
				f.createNewFile();

			dataFileChannels[ i ] = new RandomAccessFile( f, "r" ).getChannel();
			dataFiles[ i ] = new RandomAccessFile( f, "rw" );
			}

		///		// Give the files a chance to be created
		//	Thread.sleep( 5000 );

		loadLocationsIntoMemory( numObjects, dataDir );

		determineNextIndices( numObjects );
		determineFreeBlocks( numObjects );

		locationsSizesFile = new RandomAccessFile( new File( dataDir, "locations_and_sizes" ), "rw" );

		// Create the cache
		cache = new LRU_ib( cacheSize );
		cache.name = dirName + ".cache";
		}


	private void loadLocationsIntoMemory( int numObjects, File dir ) throws Throwable
		{
		File f = new File( dir, "locations_and_sizes" );
		if ( !f.exists() )
			{
			// Initialize the file with all zeroes
			OutStream out = new OutStream( new FileOutputStream( f ) );

			out.write4Bytes( 0 );
			out.write4Bytes( 0 );

			out.close();

			return;
			}

		InStream in = new InStream( new FileInputStream( f ) );
		for ( int i = 0; i < numObjects; i++ )
			{
			try
				{
				indices[ i ] = in.read4Bytes();
				sizes[ i ] = in.read4Bytes();
				}
			catch ( Throwable theX )
				{
				}
			}

		in.close();
		}


	private void determineNextIndices( int numObjects )
		{
		long wastedSpace = 0;

		// Look at all the object locations and determine the next available index in each block file
		int i = 0;
		for ( i = 0; i < numObjects; i++ )
			{
			int size = sizes[ i ];

			if ( size != 0 )
				{
				int index = indices[ i ];
				int blockType = smallestBlockTypeContainingSize( size );

				wastedSpace = wastedSpace + ( blockSizes[ blockType ] - size );

				if ( index >= nextIndicesToUse[ blockType ] )
					nextIndicesToUse[ blockType ] = index + 1;
				}
			}

		System.out.println( "\t\tWastedSpace (due to block usage): " + wastedSpace / 1024 / 1024 + "M" );

		long total = 0;
		for ( int blockType = 0; blockType < numBlockTypes; blockType++ )
			{
			if ( nextIndicesToUse[ blockType ] != 0 )
				{
				long used = nextIndicesToUse[ blockType ];

				used = used * blockSizes[ blockType ];
				total = total + used;
				}
			}

		System.out.println( "\t\tTotal Space Used: " + StringUtils.formatPrice( total / 1024 / 1024 / 1024.0 ) + "GB" );
		}


	// determineFreeBlocks manipulation doesn't need to be synchronized here as it is only called from the constructor
	private void determineFreeBlocks( int numObjects ) throws Throwable
		{
		boolean[][] usedIndices = new boolean[numBlockTypes][];
		for ( int blockType = 0; blockType < numBlockTypes; blockType++ )
			usedIndices[ blockType ] = new boolean[nextIndicesToUse[ blockType ]];

		for ( int i = 0; i < numObjects; i++ )
			{
			int index = indices[ i ];
			int size = sizes[ i ];

			if ( size != 0 )
				{
				int blockType = smallestBlockTypeContainingSize( size );

				usedIndices[ blockType ][ index ] = true;
				}
			}

		// Look through all the existing blocks and determine which are free
		for ( int blockType = 0; blockType < numBlockTypes; blockType++ )
			{
			boolean[] used = usedIndices[ blockType ];
			for ( int j = 0; j < used.length; j++ )
				{
				if ( !used[ j ] )
					freeIndices[ blockType ].addObject( new Integer( j ) );
				}
			}

		long total = 0;
		for ( int blockType = 0; blockType < numBlockTypes; blockType++ )
			{
			if ( freeIndices[ blockType ].size() != 0 )
				{
				long free = freeIndices[ blockType ].size();

				free = free * blockSizes[ blockType ];
				total = total + free;
				}
			}

		System.out.println( "\t\tTotal Freed/Fragmented Blocks: " + total / 1024 / 1024 + "M" );
		}


	// ================================================
	void ____INTERNAL________()
		{
		}


	private void markIndexAsFree( int blockType, int index )
		{
		synchronized ( freeIndices[ blockType ] )
			{
			Queue q = freeIndices[ blockType ];

			q.addObject( new Integer( index ) );
			}
		}


	private int returnNewIndexToUse( int blockType, int size )
		{
		int indexToUse = -1;
		synchronized ( freeIndices[ blockType ] )
			{
			Queue q = freeIndices[ blockType ];

			// Check to see if there are any available freed blocks of the given size

			if ( q.size() > 0 )
				indexToUse = (Integer) q.removeFirst();
			else
				{
				// Use a new block
				indexToUse = nextIndicesToUse[ blockType ];
				nextIndicesToUse[ blockType ] = nextIndicesToUse[ blockType ] + 1;
				}
			}

		return indexToUse;
		}


	private void setObjectInPreviousLocation( int blockType, int index, int objectNum, byte[] bytes ) throws Throwable
		{
		long location = index * blockSizes[ blockType ];

		//		System.out.println( ( System.currentTimeMillis() ) + "  UPDATE " + objectNum + ": " + blockType + "\t" + index );

		// Synchronization is necessary here because multiple objectNums may be simultaneously written
		// to the same file

		// Only need to update the data
		synchronized ( dataFiles[ blockType ] )
			{
			dataFiles[ blockType ].seek( location );
			dataFiles[ blockType ].write( bytes );
			}
		}


	private void setObjectInNewLocation( int blockType, int objectNum, byte[] bytes ) throws Throwable
		{
		// Update memory locations
		int index = returnNewIndexToUse( blockType, bytes.length );

		//		System.out.println( ( System.currentTimeMillis() ) + "  NEW_LOC " + objectNum + ": " + blockType + "\t" + index );

		long location = index * blockSizes[ blockType ];

		synchronized ( indices )
			{
			indices[ objectNum ] = index;
			sizes[ objectNum ] = bytes.length;
			}

		// Synchronization is necessary here because multiple objectNums may be simultaneously written
		// to the same file

		// Write the data first
		synchronized ( dataFiles[ blockType ] )
			{
			dataFiles[ blockType ].seek( location );
			dataFiles[ blockType ].write( bytes );
			}

		// Then write location
		synchronized ( locationsSizesFile )
			{
			locationsSizesFile.seek( objectNum * 8L );
			locationsSizesFile.writeInt( index );
			locationsSizesFile.writeInt( bytes.length );
			}
		}


	// ================================================
	void ____CACHE________()
		{
		}


	private void cacheBytes( int objectNum, byte[] bytes )
		{
		count++;

		if ( ( count % 100000 ) == 0 )
			{
			Runtime r = Runtime.getRuntime();

			double free = r.freeMemory() / 1024 / 1024;
			double total = r.totalMemory() / 1024 / 1024;
			double used = total - free;

			System.out.println( "" );
			System.out.println( "\t\tMemory: " + (int) used + "M / " + total + "M" );
			}

		synchronized ( cache )
			{
			cache.put( objectNum, bytes );
			}
		}


	private byte[] getBytesFromCache( int objectNum ) throws Throwable
		{
		//		Util.printStackTrace( "HERE" );

		synchronized ( cache )
			{
			return cache.get( objectNum );
			}
		}


	// ================================================
	// Calls at this level check the cache first
	void ____API________()
		{
		}


	public void put( int objectNum, byte[] bytes ) throws Throwable
		{
		// Cache first thing in case the object is read before write to disk is completed
		if ( bytes.length < kMaxSizeInCache )
			synchronized ( cache )
				{
				cacheBytes( objectNum, bytes );
				}

		//		System.out.println( "\tPUT " + objectNum + ": A" );
		if ( bytes.length == 0 )
			return;

		int size0 = 0;
		int index0 = 0;

		synchronized ( indices )
			{
			// Check to see if the size has changed
			// If so, we'll have to allocate new space for the object
			size0 = sizes[ objectNum ];
			index0 = indices[ objectNum ];
			}

		//		System.out.println( "LAST 5 BYTES " + objectNum + ": " + bytes[ bytes.length - 5 ] + "\t" + bytes[ bytes.length - 4 ] + "\t" + bytes[ bytes.length - 3 ] + "\t" + bytes[ bytes.length - 2 ] + "\t" + bytes[ bytes.length - 1 ] );

		//		System.out.println( objectNum + "\t" + size0 + "\t" + bytes.length );

		// Check to see if the sizes are exactly the same
		// They need to be exactly the same as otherwise the location/size value will need to be updated as well
		if ( bytes.length == size0 )
			{
			int blockType = smallestBlockTypeContainingSize( bytes.length );

			// Same block size?  Use the same location as before
			setObjectInPreviousLocation( blockType, index0, objectNum, bytes );
			}
		else
			{
			int blockType0 = smallestBlockTypeContainingSize( size0 );
			int blockType = smallestBlockTypeContainingSize( bytes.length );

			setObjectInNewLocation( blockType, objectNum, bytes );

			// Now mark as free the previously used space
			if ( blockType0 != -1 )
				markIndexAsFree( blockType0, index0 );
			}

		//		System.out.println( "\tPUT " + objectNum + ": B" );
		}


	// Add the object and return the key
	public byte[] get( int objectNum ) throws Throwable
		{
		synchronized ( cache )
			{
			byte[] bytes = getBytesFromCache( objectNum );

			if ( bytes != null )
				return bytes;
			}

		// Load the values from disk
		int size = 0;
		long index = 0;
		synchronized ( indices )
			{
			size = sizes[ objectNum ];
			index = indices[ objectNum ];
			}

		//		System.out.println( ( System.currentTimeMillis() ) + " READ " + objectNum + "\t" + size + "\t" + index );

		if ( size == 0 )
			return null;

		int blockType = smallestBlockTypeContainingSize( size );
		if ( blockType == -1 )
			return null;

		long location = index * blockSizes[ blockType ];

		byte[] bytes = new byte[size];
		ByteBuffer bb = ByteBuffer.wrap( bytes );

		// Using data file channels because they allow atomic read/seek
		int read = dataFileChannels[ blockType ].read( bb, location );
		int totalRead = read;

		if ( totalRead == -1 )
			return null;

		while ( totalRead < size )
			{
			//			System.out.println( ( System.currentTimeMillis() ) + " READ " + objectNum + "\t" + location + "\t" + totalRead );

			// It may be necessary to update the ByteBuffer's position here before the FileChannel read
			read = dataFileChannels[ blockType ].read( bb, location + totalRead );

			totalRead = totalRead + read;
			}

		if ( bytes.length < kMaxSizeInCache )
			synchronized ( cache )
				{
				cacheBytes( objectNum, bytes );
				}

		return bytes;
		}


	// ================================================
	void ____UTILS________()
		{
		}


	private static int smallestBlockTypeContainingSize( int size )
		{
		if ( size == 0 )
			return -1;

		for ( int i = 0; i < numBlockTypes; i++ )
			{
			if ( size <= blockSizes[ i ] )
				return i;
			}

		return -1; // Block size too large
		}


	public static void main( String[] args )
		{
		try
			{
			// BENCHMARKS (on quad3)

			// serial update 100 bytes: 101729 ops/sec
			// random read 100 bytes: 404858 ops/sec

			// random update 100k bytes: 7890 ops/sec
			// random read 100k bytes: 34106 ops/sec

			// Compare here
			// http://leveldb.googlecode.com/svn/trunk/doc/benchmark.html

			int n = 1000000;
			int z = 100000;

			ObjectStore o = new ObjectStore( n, "data", 1024L * 1024L );

			long start = System.currentTimeMillis();

			for ( int i = 0; i < z; i++ )
				{
				int r = i;

				byte[] b = new byte[100];
				b[ 0 ] = (byte) ( r % 99 );
				o.put( r, b );

				/*
								int r = (int) ( Math.random() * z );
								byte[] b2 = o.get( r );

								if ( b2[ 0 ] != ( r % 99 ) )
									System.out.println( "ERROR" );
					*/}

			long elapsed = System.currentTimeMillis() - start;

			System.out.println( "ops/sec: " + z * 1000 / elapsed );
			}
		catch ( Throwable theX )
			{
			cota.util.Util.printX( theX );
			}
		}
	}
