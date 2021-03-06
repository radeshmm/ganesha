package cota.util;

//A fast, memory efficient Hashtable mapping long->Object

public class Hashtable_lo
	{
	public int numBins = 8;
	public int numBinsMinusOne = 7;

	public HashEntry_lo[] entries = null;

	public int size = 0;


	// Constructor
	public Hashtable_lo()
		{
		entries = new HashEntry_lo[numBins];
		}


	public void addTable( Hashtable_lo f )
		{
		HashEntry_lo[] entries = f.returnArrayOfEntries();
		for ( int i = 0; i < entries.length; i++ )
			{
			HashEntry_lo entry = entries[ i ];

			put( entry.key, entry.value );
			}
		}


	public void put( long key, Object value )
		{
		// null entries are not allowed
		if ( value == null )
			{
			remove( key );

			return;
			}

		int index = (int) ( key & numBinsMinusOne );

		HashEntry_lo entry0 = entries[ index ];
		if ( entry0 == null )
			{
			entry0 = entries[ index ] = new HashEntry_lo( key, value );
			size++;

			if ( size > numBins )
				rehash();

			return;
			}

		// See if the key is already there
		HashEntry_lo entry = entry0;
		while ( entry != null )
			{
			if ( entry.key == key )
				{
				entry.value = value;

				return;
				}

			entry = entry.next;
			}

		entry = new HashEntry_lo( key, value );
		entry.next = entries[ index ];
		entries[ index ] = entry;

		size++;
		if ( size > numBins )
			rehash();
		}


	public Object remove( long key )
		{
		int index = (int) ( key & numBinsMinusOne );

		if ( entries[ index ] == null )
			return null;

		int count = 0;

		HashEntry_lo prevEntry = null;
		HashEntry_lo entry = entries[ index ];
		while ( entry != null )
			{
			if ( entry.key == key )
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


	public Object findKey( HashEntry_lo entry, long key )
		{
		while ( entry != null )
			{
			if ( entry.key == key )
				{
				return entry.value;
				}

			entry = entry.next;
			}

		return null;
		}


	public Object get( long key )
		{
		int index = (int) ( key & numBinsMinusOne );

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
		HashEntry_lo[] oldEntries = entries;
		numBins = numBins << 1;
		entries = new HashEntry_lo[numBins];
		numBinsMinusOne = numBins - 1;

		for ( int i = 0; i < oldEntries.length; i++ )
			{
			HashEntry_lo entry = oldEntries[ i ];
			while ( entry != null )
				{
				HashEntry_lo next = entry.next;
				entry.next = null;

				int index = (int) ( entry.key & numBinsMinusOne );

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
			HashEntry_lo entry = entries[ i ];
			while ( entry != null )
				{
				if ( entry.value != null )
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
			HashEntry_lo entry = entries[ i ];
			while ( entry != null )
				{
				if ( entry.value != null )
					q.addObject( entry.value );

				entry = entry.next;
				}
			}

		return q;
		}


	/*
		public Queue returnEntries()
			{
			Queue q = new Queue();
			for ( int i = 0; i < numBins; i++ )
				{
				LashEntry entry = entries[ i ];
				while ( entry != null )
					{
					if ( entry.value != null )
						{
						// System.out.println( "DDDDDD: " + entry.key + " " +
						// entry.value );
						q.addObject( new PairIO( entry.key, entry.value ) );
						}

					entry = entry.next;
					}
				}

			return q;
			}

	*/
	public HashEntry_lo[] returnArrayOfEntries()
		{
		int c = 0;
		HashEntry_lo[] e = new HashEntry_lo[size];
		for ( int i = 0; i < numBins; i++ )
			{
			HashEntry_lo entry = entries[ i ];
			while ( entry != null )
				{
				if ( entry.value != null )
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
			HashEntry_lo entry = entries[ i ];
			while ( entry != null )
				{
				System.out.println( "HASH: " + entry.key + " " + entry.value );

				entry = entry.next;
				}
			}
		}


	public static void main( String[] args )
		{
		System.out.println( System.currentTimeMillis() );

		int z = 1;
		for ( int z0 = 0; z0 < 1000000; z0 = z0 + 10000 )
			{
			z = z * 2;

			System.gc();

			System.out.println( "-----" + z + "-----" );

			Hashtable_io n2 = new Hashtable_io();

			try
				{
				long time = System.currentTimeMillis();
				for ( int i = 0; i < z; i++ )
					{
					n2.put( i, "value" + i );
					}

				System.out.print( "Nashtable: " + ( System.currentTimeMillis() - time ) );
				time = System.currentTimeMillis();
				for ( int i = 0; i < z; i++ )
					{
					Object v = n2.get( i );

					if ( !v.equals( "value" + i ) )
						System.out.println( "ERRRROR" );
					}
				System.out.println( ":" + ( System.currentTimeMillis() - time ) );
				}
			catch ( Throwable theX )
				{
				Util.printX( "", theX );
				}

			Hashtable_lo f2 = new Hashtable_lo();

			try
				{
				long time = System.currentTimeMillis();
				for ( int i = 0; i < z; i++ )
					{
					f2.put( i, "value" + i );
					}

				System.out.print( "Lashtable: " + ( System.currentTimeMillis() - time ) );
				time = System.currentTimeMillis();
				for ( int i = 0; i < z; i++ )
					{
					Object v = f2.get( i );

					if ( !v.equals( "value" + i ) )
						System.out.println( "ERRRROR" );
					}
				System.out.println( ":" + ( System.currentTimeMillis() - time ) );
				}
			catch ( Throwable theX )
				{
				Util.printX( "", theX );
				}

			}
		}
	}