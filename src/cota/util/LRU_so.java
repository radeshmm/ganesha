package cota.util;

/*
LRU_sb uses keyLength * 2 + 48 + byte length for storage

key						8~
older/newer				16
byte size				b
fashtable bin pointer	8
fashentry key			8~
fashentry value			8
next					8
*/

// This LRU class is pretty ool
// It keeps a cache of recent Objects in memory, and removes those that haven't been accessed recently
public class LRU_so
	{
	public static boolean print = true;

	private class CacheObject
		{
		String key = null;
		Object object = null;

		CacheObject olderObject = null;
		CacheObject newerObject = null;


		// Constructor
		public CacheObject( String key, Object o )
			{
			this.key = key;
			this.object = o;
			}
		}

	long currentCost = 0;
	long maxTotalCost = 0;

	Fashtable_so f = new Fashtable_so();
	CacheObject newest = null;
	CacheObject oldest = null;

	// For stats
	static final int hitRateAveragePeriod = 10000;
	static final double hitRateExponent = 2.0 / ( hitRateAveragePeriod + 1 );

	public double averageHitRate = 0;
	public String name = "";
	int count = 0;


	public LRU_so( long maxTotalCost ) throws Throwable
		{
		if ( maxTotalCost == 0 )
			throw new Throwable( "STARTING WITH MAX COST OF ZERO" );

		this.maxTotalCost = maxTotalCost;
		}


	public Object get( String key )
		{
		CacheObject o = (CacheObject) f.get( key );

		count++;
		if ( print )
			if ( ( count % 100000 ) == 0 )
				System.out.println( "\t\t\t\tso cache " + name + ": " + currentCost / 1024 / 1024 + "M / " + maxTotalCost / 1024 / 1024 + "M\t\t" + StringUtils.formatPrice( averageHitRate * 100 ) + "% hit" );
		//			System.out.println( "\t\t\t\tCache " + name + ": " + currentCost / 1024 / 1024 + " / " + maxTotalCost / 1024 / 1024 + "\t\t" + StringUtils.formatPrice( averageHitRate * 100 ) + "% hit" );

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

			currentCost = currentCost - 1;

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

			currentCost = currentCost - 1;

			oldest = newer;
			}
		}


	// We can explicitly declare that we're using more memory than just the
	// pointer by
	// including the dataSize as an argument (as when we are caching web data,
	// but don't want to store too much in memory)
	public void put( String key, Object object, int cost )
		{
		// Try to remove it in case it's already there
		remove( key );

		currentCost = currentCost + cost;

		CacheObject o = new CacheObject( key, object );

		f.put( key, o );

		if ( newest != null )
			newest.newerObject = o;

		o.olderObject = newest;
		newest = o;

		if ( oldest == null )
			oldest = o;

		while ( currentCost > maxTotalCost )
			removeOldest();

		//		System.out.println( currentCacheSize + "\t" + maxCacheSize );
		}


	// We can explicitly declare that we're using more memory than just the
	// pointer by
	// including the dataSize as an argument (as when we are caching web data,
	// but don't want to store too much in memory)
	public void put( String key, Object object )
		{
		put( key, object, 1 );
		}


	public static void main( String[] args )
		{
		System.gc();

		Runtime r0 = Runtime.getRuntime();

		double free0 = r0.freeMemory() / 1024 / 1024;
		double total0 = r0.totalMemory() / 1024 / 1024;
		double used0 = total0 - free0;
		/*
				LRU_lb cache = new LRU_lb( 1000000000 );
				for ( int i = 0; i < 10000000; i++ )
					{
					byte[] bytes = new byte[4];
					cache.put( i, bytes );

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
		*/
		if ( 1 == 1 )
			return;
		/*
				int size = 50;
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