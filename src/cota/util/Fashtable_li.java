package cota.util;

public class Fashtable_li
	{
	public int numBins = 8;
	public int numBinsMinusOne = 7;

	public FashEntry_li[] entries = null;

	public int size = 0;

	public static final int NOT_FOUND = Integer.MAX_VALUE;


	// Constructor
	public Fashtable_li()
		{
		entries = new FashEntry_li[numBins];
		}


	public void addTable( Fashtable_li f )
		{
		FashEntry_li[] entries = f.returnArrayOfEntries();
		for ( int i = 0; i < entries.length; i++ )
			{
			FashEntry_li entry = entries[ i ];

			put( entry.key, entry.value );
			}
		}


	public void put( long key, int value )
		{
		int index = (int) ( key & numBinsMinusOne );

		FashEntry_li entry0 = entries[ index ];
		if ( entry0 == null )
			{
			entry0 = entries[ index ] = new FashEntry_li( key, value );
			size++;

			if ( size > numBins )
				rehash();

			return;
			}

		// See if the key is already there
		FashEntry_li entry = entry0;
		while ( entry != null )
			{
			if ( entry.key == key )
				{
				entry.value = value;

				return;
				}

			entry = entry.next;
			}

		entry = new FashEntry_li( key, value );
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

		FashEntry_li prevEntry = null;
		FashEntry_li entry = entries[ index ];
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


	public int findKey( FashEntry_li entry, long key )
		{
		while ( entry != null )
			{
			if ( entry.key == key )
				{
				return entry.value;
				}

			entry = entry.next;
			}

		return NOT_FOUND;
		}


	public int get( long key )
		{
		int index = (int) ( key & numBinsMinusOne );

		return findKey( entries[ index ], key );
		}


	public String getString( long key )
		{
		Object o = get( key );

		if ( o == null )
			return "";

		String s = (String) o;
		while ( s.startsWith( " " ) )
			{
			s = s.substring( 1, s.length() );
			}

		while ( s.endsWith( " " ) )
			{
			s = s.substring( 0, s.length() - 1 );
			}

		return s;
		}


	public void clear()
		{
		for ( int i = 0; i < numBins; i++ )
			entries[ i ] = null;

		size = 0;
		}


	public void rehash()
		{
		FashEntry_li[] oldEntries = entries;
		numBins = numBins << 1;
		entries = new FashEntry_li[numBins];
		numBinsMinusOne = numBins - 1;

		for ( int i = 0; i < oldEntries.length; i++ )
			{
			FashEntry_li entry = oldEntries[ i ];
			while ( entry != null )
				{
				FashEntry_li next = entry.next;
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
			FashEntry_li entry = entries[ i ];
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
			FashEntry_li entry = entries[ i ];
			while ( entry != null )
				{
				if ( entry.value != NOT_FOUND )
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
				NashEntryI entry = entries[ i ];
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
	public FashEntry_li[] returnArrayOfEntries()
		{
		int c = 0;
		FashEntry_li[] e = new FashEntry_li[size];
		for ( int i = 0; i < numBins; i++ )
			{
			FashEntry_li entry = entries[ i ];
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
			FashEntry_li entry = entries[ i ];
			while ( entry != null )
				{
				System.out.println( "HASH: " + entry.key + " " + entry.value );

				entry = entry.next;
				}
			}
		}


	public static void main( String[] args )
		{
		long[] test = new long[1000000000];

		long start = System.currentTimeMillis();

		long key = 5;
		int n = 0;
		for ( n = 0; n < 1000000000; n++ )
			{
			if ( test[ n ] == key )
				break;
			}

		System.out.println( "ELAPSED: " + ( System.currentTimeMillis() - start ) );

		if ( 1 == 1 )
			return;

		int z = 1;
		for ( int z0 = 0; z0 < 1000000; z0 = z0 + 10000 )
			{
			z = z * 2;

			System.gc();

			System.out.println( "-----" + z + "-----" );

			Fashtable_li f2 = new Fashtable_li();

			try
				{
				long time = System.currentTimeMillis();
				for ( int i = 0; i < z; i++ )
					{
					f2.put( i, i );
					}

				System.out.print( "Fashtable_li: " + ( System.currentTimeMillis() - time ) );
				time = System.currentTimeMillis();
				for ( int i = 0; i < z; i++ )
					{
					Object v = f2.get( i );

					if ( !v.equals( i ) )
						System.out.println( "ERRRROR" );
					}
				System.out.println( ":" + ( System.currentTimeMillis() - time ) );
				}
			catch ( Throwable theX )
				{
				Util.printX( "", theX );
				}
			/*
						Fashtable_io f = new Fashtable_io();

						try
							{
							long time = System.currentTimeMillis();
							for ( int i = 0; i < z; i++ )
								{
								f.put( i, "value" + i );
								}

							System.out.print( "Nashtable: " + ( System.currentTimeMillis() - time ) );
							time = System.currentTimeMillis();
							for ( int i = 0; i < z; i++ )
								{
								Object v = f.get( i );

								if ( !v.equals( "value" + i ) )
									System.out.println( "ERRRROR" );
								}
							System.out.println( ":" + ( System.currentTimeMillis() - time ) );
							}
						catch ( Throwable theX )
							{
							Util.printX( "", theX );
							}
				*//*
				try
				{
				Hashtable h = new Hashtable();

				long time = System.currentTimeMillis();
				for ( int i = 0; i < z; i++ )
				{
				h.put( new Integer( i ), "value" + i );
				}

				System.out.print( "Hashtable: " + ( System.currentTimeMillis() - time ) );
				time = System.currentTimeMillis();
				for ( int i = 0; i < z; i++ )
				{
				Object v = h.get( new Integer( i ) );

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