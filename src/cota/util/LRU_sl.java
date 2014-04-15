package cota.util;

/*
LRU_sl uses s.length() + 72 for storage

key string				s.length()
key						8~
older/newer				16
value						8
fashtable bin pointer	8
fashentry key			8~
fashentry value			8
next					8
*/

// This LRU class is pretty ool
// It keeps a cache of recent Objects in memory, and removes those that haven't been accessed recently
public class LRU_sl
	{
	private class CacheObject
		{
		String key = null;
		long value = 0;

		CacheObject olderObject = null;
		CacheObject newerObject = null;


		// Constructor
		public CacheObject( String key, long value )
			{
			this.key = key;
			this.value = value;
			}
		}

	// The correct value for this is dependent on the system
	// This value works on CentOS 64-bit Java 1.6
	static int kMemoryFudge = 56;

	public static final long NOT_FOUND = Long.MAX_VALUE;

	long currentCacheSize = 0;
	long maxCacheSize = 0;

	Hashtable_so f = new Hashtable_so();
	CacheObject newest = null;
	CacheObject oldest = null;

	// For stats
	static final int hitRateAveragePeriod = 10000;
	static final double hitRateExponent = 2.0 / ( hitRateAveragePeriod + 1 );

	public double averageHitRate = 0;
	public String name = "";
	int count = 0;


	public LRU_sl( long maxCacheSizeInBytes ) throws Throwable
		{
		if ( maxCacheSizeInBytes == 0 )
			throw new Throwable( "STARTING WITH CACHE SIZE OF ZERO" );

		this.maxCacheSize = maxCacheSizeInBytes;
		}


	public long get( String key )
		{
		CacheObject o = (CacheObject) f.get( key );

		count++;
		if ( ( count % 100000 ) == 0 )
			System.out.println( "\t\t\t\tsl cache " + name + ": " + currentCacheSize / 1024 / 1024 + "M / " + maxCacheSize / 1024 / 1024 + "M\t\t" + StringUtils.formatPrice( averageHitRate * 100 ) + "% hit" );

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

			return o.value;
			}

		averageHitRate = averageHitRate * ( 1.0 - hitRateExponent ); // + 0 * hitRateExponent;

		return NOT_FOUND;
		}


	// Basically removes the object, and decreases the value of currentCacheSize
	public void remove( String key )
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

			currentCacheSize = currentCacheSize - ( kMemoryFudge + key.length() * 2 + 72 );

			if ( newest == o )
				newest = older;

			if ( oldest == o )
				oldest = newer;
			}
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

			currentCacheSize = currentCacheSize - ( kMemoryFudge + o.key.length() * 2 + 72 );

			oldest = newer;
			}
		}


	// We can explicitly declare that we're using more memory than just the
	// pointer by
	// including the dataSize as an argument (as when we are caching web data,
	// but don't want to store too much in memory)
	public void put( String key, long value )
		{
		// Try to remove it in case it's already there
		remove( key );

		currentCacheSize = currentCacheSize + ( kMemoryFudge + key.length() * 2 + 72 );

		CacheObject o = new CacheObject( key, value );

		f.put( key, o );

		if ( newest != null )
			newest.newerObject = o;

		o.olderObject = newest;
		newest = o;

		if ( oldest == null )
			oldest = o;

		while ( currentCacheSize > maxCacheSize )
			removeOldest();

		//		System.out.println( currentCacheSize + "\t" + maxCacheSize );
		}


	public static void main( String[] args )
		{
		kMemoryFudge = Integer.parseInt( args[ 0 ] );

		System.gc();

		Runtime r0 = Runtime.getRuntime();

		double free0 = r0.freeMemory() / 1024 / 1024;
		double total0 = r0.totalMemory() / 1024 / 1024;
		double used0 = total0 - free0;

		/*		LRU_sl cache = new LRU_sl( 1000000000 );
				for ( int i = 0; i < 10000000; i++ )
					{
					cache.put( "" + i, i );

					if ( ( i % 100000 ) == 0 )
						{
						System.gc();

						Runtime r = Runtime.getRuntime();

						double free = r.freeMemory() / 1024 / 1024;
						double total = r.totalMemory() / 1024 / 1024;
						double used = total - free;

						System.out.println( "\n" + i );
						System.out.println( "Mem used: " + ( used - used0 ) + "M" );
						System.out.println( "Cache used: " + cache.currentCacheSize / 1024 / 1024 + "M" );
						}
					}
			*/}
	}