package cota.util;

//A fast, memory efficient Hashtable mapping String->long

public class Hashtable_sl
	{
	public int numBins = 8;
	public int numBinsMinusOne = 7;

	public HashEntry_sl[] entries = null;

	public int size = 0;

	public static final long NOT_FOUND = Long.MAX_VALUE;


	// Constructor
	public Hashtable_sl()
		{
		entries = new HashEntry_sl[numBins];
		}


	public void addTable( Hashtable_sl f )
		{
		HashEntry_sl[] entries = f.returnArrayOfEntries();
		for ( int i = 0; i < entries.length; i++ )
			{
			HashEntry_sl entry = entries[ i ];

			put( entry.key, entry.value );
			}
		}


	public void put( String key, long value )
		{
		// null entries are not allowed
		if ( value == NOT_FOUND )
			{
			remove( key );

			return;
			}

		int hash = key.hashCode();
		int index = hash & numBinsMinusOne;

		HashEntry_sl entry0 = entries[ index ];
		if ( entry0 == null )
			{
			entry0 = entries[ index ] = new HashEntry_sl( key, value, hash );
			size++;

			if ( size > numBins )
				rehash();

			return;
			}

		// See if the key is already there
		HashEntry_sl entry = entry0;
		while ( entry != null )
			{
			if ( entry.key.equals( key ) )
				{
				entry.value = value;

				return;
				}

			entry = entry.next;
			}

		entry = new HashEntry_sl( key, value, hash );
		entry.next = entries[ index ];
		entries[ index ] = entry;

		size++;
		if ( size > numBins )
			rehash();
		}


	public Object remove( String key )
		{
		int hash = key.hashCode();
		int index = hash & numBinsMinusOne;

		if ( entries[ index ] == null )
			return null;

		int count = 0;

		HashEntry_sl prevEntry = null;
		HashEntry_sl entry = entries[ index ];
		while ( entry != null )
			{
			if ( entry.key.equals( key ) )
				{
				Object v = entry.value;

				// Remove the entry
				if ( count == 0 )
					entries[ index ] = entry.next;
				else
					prevEntry.next = entry.next;

				size--;

				return v;
				}

			prevEntry = entry;

			entry = entry.next;
			count++;
			}

		return null;
		}


	public long findKey( HashEntry_sl entry, String key )
		{
		while ( entry != null )
			{
			if ( entry.key.equals( key ) )
				{
				return entry.value;
				}

			entry = entry.next;
			}

		return NOT_FOUND;
		}


	public long get( String key )
		{
		if ( key == null )
			return NOT_FOUND;

		int hash = key.hashCode();
		int index = hash & numBinsMinusOne;

		return findKey( entries[ index ], key );
		}


	public void clear()
		{
		for ( int i = 0; i < numBins; i++ )
			entries[ i ] = null;

		size = 0;
		}


	public void rehash()
		{
		HashEntry_sl[] oldEntries = entries;
		numBins = numBins << 1;
		entries = new HashEntry_sl[numBins];
		numBinsMinusOne = numBins - 1;

		for ( int i = 0; i < oldEntries.length; i++ )
			{
			HashEntry_sl entry = oldEntries[ i ];
			while ( entry != null )
				{
				HashEntry_sl next = entry.next;
				entry.next = null;

				int index = entry.hash & numBinsMinusOne;

				if ( entries[ index ] == null )
					entries[ index ] = entry;
				else
					{
					entry.next = entries[ index ];
					entries[ index ] = entry;
					}

				entry = next;
				}
			}
		}


	public Queue returnKeys()
		{
		Queue q = new Queue();
		for ( int i = 0; i < numBins; i++ )
			{
			HashEntry_sl entry = entries[ i ];
			while ( entry != null )
				{
				if ( entry.value != NOT_FOUND )
					q.addObject( entry.key );

				entry = entry.next;
				}
			}

		return q;
		}


	public Queue returnValues()
		{
		Queue q = new Queue();
		for ( int i = 0; i < numBins; i++ )
			{
			HashEntry_sl entry = entries[ i ];
			while ( entry != null )
				{
				if ( entry.value != NOT_FOUND )
					q.addObject( entry.value );

				entry = entry.next;
				}
			}

		return q;
		}


	public Queue returnEntries()
		{
		Queue q = new Queue();
		for ( int i = 0; i < numBins; i++ )
			{
			HashEntry_sl entry = entries[ i ];
			while ( entry != null )
				{
				if ( entry.value != NOT_FOUND )
					{
					// System.out.println( "DDDDDD: " + entry.key + " " +
					// entry.value );
					q.addObject( new PairSO( entry.key, entry.value ) );
					}

				entry = entry.next;
				}
			}

		return q;
		}


	public HashEntry_sl[] returnArrayOfEntries()
		{
		int c = 0;
		HashEntry_sl[] e = new HashEntry_sl[size];
		for ( int i = 0; i < numBins; i++ )
			{
			HashEntry_sl entry = entries[ i ];
			while ( entry != null )
				{
				if ( entry.value != NOT_FOUND )
					e[ c++ ] = entry;

				entry = entry.next;
				}
			}

		return e;
		}


	public void printTable()
		{
		for ( int i = 0; i < numBins; i++ )
			{
			HashEntry_sl entry = entries[ i ];
			while ( entry != null )
				{
				System.out.println( "HASH: " + entry.key + " " + entry.value );

				entry = entry.next;
				}
			}
		}


	public static void main( String[] args )
		{
		int z = 1;
		for ( int z0 = 0; z0 < 1000000; z0 = z0 + 10000 )
			{
			z = z * 2;

			System.gc();

			System.out.println( "-----" + z + "-----" );
			Hashtable_sl f = new Hashtable_sl();

			try
				{
				long time = System.currentTimeMillis();
				for ( int i = 0; i < z; i++ )
					{
					f.put( "01234567890123456789" + i, i );
					}

				System.out.print( "FashtableL: " + ( System.currentTimeMillis() - time ) );
				time = System.currentTimeMillis();
				for ( int i = 0; i < z; i++ )
					{
					long v = f.get( "01234567890123456789" + i );

					if ( v != i )
						System.out.println( "ERRRROR" );
					}
				System.out.println( ":" + ( System.currentTimeMillis() - time ) );
				}
			catch ( Throwable theX )
				{
				Util.printX( "", theX );
				}

			Hashtable_so f2 = new Hashtable_so();
			try
				{
				long time = System.currentTimeMillis();
				for ( int i = 0; i < z; i++ )
					{
					f2.put( "key" + i, "value" + i );
					}

				System.out.print( "Fashtable: " + ( System.currentTimeMillis() - time ) );
				time = System.currentTimeMillis();
				for ( int i = 0; i < z; i++ )
					{
					Object v = f2.get( "key" + i );

					if ( !v.equals( "value" + i ) )
						System.out.println( "ERRRROR" );
					}
				System.out.println( ":" + ( System.currentTimeMillis() - time ) );
				}
			catch ( Throwable theX )
				{
				Util.printX( "", theX );
				}

			/*
						try
							{
							Hashtable h = new Hashtable();

							long time = System.currentTimeMillis();
							for ( int i = 0; i < z; i++ )
								{
								h.put( "key" + i, "value" + i );
								}

							System.out.print( "Hashtable: " + ( System.currentTimeMillis() - time ) );
							time = System.currentTimeMillis();
							for ( int i = 0; i < z; i++ )
								{
								Object v = h.get( "key" + i );

								if ( !v.equals( "value" + i ) )
									System.out.println( "ERRRROR" );
								}
							System.out.println( ":" + ( System.currentTimeMillis() - time ) );
							}
						catch ( Throwable theX )
							{
							Util.printX( "", theX );
							}
			*/
			}
		}
	}