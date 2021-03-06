package cota.util;

// Cache used per 1 million objects: 74M

// This LRU class is pretty cool
// It keeps a cache of recent Objects in memory, and removes those that haven't been accessed recently
public class LRU_io
	{
	private class CacheObject
		{
		int key = 0;
		public Object object = null;

		CacheObject olderObject = null;
		CacheObject newerObject = null;


		// Constructor
		public CacheObject( int key, Object object )
			{
			this.key = key;
			this.object = object;
			}
		}

	// The correct value for this is dependent on the system 
	// This value works on CentOS 64-bit Java 1.6
	static int kMemoryFudge = 22;

	long currentCacheSize = 0;
	long maxCacheSize = 0;

	Hashtable_io f = new Hashtable_io();
	CacheObject newest = null;
	CacheObject oldest = null;

	// For stats
	static final int hitRateAveragePeriod = 10000;
	static final double hitRateExponent = 2.0 / ( hitRateAveragePeriod + 1 );

	public double averageHitRate = 0;
	public String name = "";
	int count = 0;


	public LRU_io( long maxCacheSizeInBytes ) throws Throwable
		{
		if ( maxCacheSizeInBytes == 0 )
			throw new Throwable( "STARTING WITH CACHE SIZE OF ZERO" );

		this.maxCacheSize = maxCacheSizeInBytes;
		}


	public Object get( int key )
		{
		CacheObject o = (CacheObject) f.get( key );

		count++;
		if ( ( count % 100000 ) == 0 )
			System.out.println( "\t\t\t\tio cache " + name + ": " + currentCacheSize / 1024 / 1024 + "M / " + maxCacheSize / 1024 / 1024 + "M\t\t" + StringUtils.formatPrice( averageHitRate * 100 ) + "% hit" );

		if ( o != null )
			{
			if ( o != newest )
				{
				// Make the object the newest
				CacheObject older = o.olderObject;
				CacheObject newer = o.newerObject;

				if ( newer != null )
					newer.olderObject = older;

				if ( older != null )
					older.newerObject = newer;

				if ( oldest == o )
					oldest = newer;

				o.newerObject = null;

				o.olderObject = newest;

				if ( newest != null )
					newest.newerObject = o;

				newest = o;
				}

			averageHitRate = averageHitRate * ( 1.0 - hitRateExponent ) + hitRateExponent;

			return o.object;
			}

		averageHitRate = averageHitRate * ( 1.0 - hitRateExponent ); // + 0 * hitRateExponent;

		return null;
		}


	// Basically removes the object, and decreases the value of currentCacheSize
	public boolean remove( int key )
		{
		CacheObject o = (CacheObject) f.remove( key );

		if ( o != null )
			{
			CacheObject older = o.olderObject;
			CacheObject newer = o.newerObject;

			if ( newer != null )
				newer.olderObject = older;

			if ( older != null )
				older.newerObject = newer;

			currentCacheSize = currentCacheSize - ( kMemoryFudge + 8 + 48 );

			if ( newest == o )
				newest = older;

			if ( oldest == o )
				oldest = newer;
			}

		return ( o != null );
		}


	public void removeOldest()
		{
		CacheObject o = oldest;

		if ( o != null )
			{
			f.remove( o.key );

			CacheObject newer = o.newerObject;

			if ( newer != null )
				newer.olderObject = null;

			currentCacheSize = currentCacheSize - ( kMemoryFudge + 8 + 48 );

			oldest = newer;
			}
		}


	// We can explicitly declare that we're using more memory than just the
	// pointer by
	// including the dataSize as an argument (as when we are caching web data,
	// but don't want to store too much in memory)
	public void put( int key, Object object )
		{
		// Try to remove it in case it's already there
		remove( key );

		currentCacheSize = currentCacheSize + ( kMemoryFudge + 8 + 48 );

		CacheObject o = new CacheObject( key, object );

		f.put( key, o );

		if ( newest != null )
			newest.newerObject = o;

		o.olderObject = newest;
		newest = o;

		if ( oldest == null )
			oldest = o;

		while ( currentCacheSize > maxCacheSize )
			removeOldest();

		//System.out.println( currentCacheSize + "\t" + maxCacheSize );
		}


	public static void main( String[] args )
		{
		try
			{
			kMemoryFudge = Integer.parseInt( args[ 0 ] );

			System.gc();

			Runtime r0 = Runtime.getRuntime();

			double free0 = r0.freeMemory() / 1024 / 1024;
			double total0 = r0.totalMemory() / 1024 / 1024;
			double used0 = total0 - free0;

			LRU_io cache = new LRU_io( 1000000000 );
			for ( int i = 0; i <= 10000000; i++ )
				{
				cache.put( i, new Object() );

				if ( ( i % 1000000 ) == 0 )
					{
					System.gc();

					Runtime r = Runtime.getRuntime();

					double free = r.freeMemory() / 1024 / 1024;
					double total = r.totalMemory() / 1024 / 1024;
					double used = total - free;

					//					System.out.println( "\n" + i );
					//					System.out.println( "Mem used: " + ( used - used0 ) + "M" );
					System.out.println( "Cache used for " + i + " objects: " + cache.currentCacheSize / 1024 / 1024 + "M" );
					}
				}
			}
		catch ( Throwable theX )
			{
			cota.util.Util.printX( theX );
			}

		if ( 1 == 1 )
			return;

		/*		int size = 50;
				LRU_lb lru = new LRU_lb( size );

				byte[] b =
					{ 0 };

				for ( int i = 0; i < 1000; i++ )
					{
					lru.put( i, b );

					if ( ( i % 40 ) == 0 )
						lru.get( 0 );
					}

				for ( int i = 0; i < 1000; i++ )
					System.out.println( i + "\t" + lru.get( i ) );
		*/
		//	lru.put( "a", "1", 1 );
		//	lru.put( "a", "2", 1 );

		//System.out.println( (String) lru.get( "a" ) );

		/*		System.out.println( "start" );
				for ( int i = 0; i < 200000; i++ )
					{
					int r = (int) ( Math.random() * 20 );
					System.out.println( i + "\t" + r );

					lru.put( "key:" + r, "value: " + r, 1 );

					CacheObject d = lru.newest;
					while ( d != null )
						{
						System.out.println( "\t" + d.data );

						d = d.olderObject;
						}

					System.out.println( "" );
					}
					*/
		}
	}