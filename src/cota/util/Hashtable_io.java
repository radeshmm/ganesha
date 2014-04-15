package cota.util;

//A fast, memory efficient Hashtable mapping int->Object

public class Hashtable_io
	{
	public int numBins = 8;
	public int numBinsMinusOne = 7;

	public HashEntry_io[] entries = null;

	public int size = 0;


	// Constructor
	public Hashtable_io()
		{
		entries = new HashEntry_io[numBins];
		}


	public void addTable( Hashtable_io f )
		{
		HashEntry_io[] entries = f.returnArrayOfEntries();
		for ( int i = 0; i < entries.length; i++ )
			{
			HashEntry_io entry = entries[ i ];

			put( entry.key, entry.value );
			}
		}


	public void put( int key, Object value )
		{
		// null entries are not allowed
		if ( value == null )
			{
			remove( key );

			return;
			}

		int index = key & numBinsMinusOne;

		HashEntry_io entry0 = entries[ index ];
		if ( entry0 == null )
			{
			entry0 = entries[ index ] = new HashEntry_io( key, value );
			size++;

			if ( size > numBins )
				rehash();

			return;
			}

		// See if the key is already there
		HashEntry_io entry = entry0;
		while ( entry != null )
			{
			if ( entry.key == key )
				{
				entry.value = value;

				return;
				}

			entry = entry.next;
			}

		entry = new HashEntry_io( key, value );
		entry.next = entries[ index ];
		entries[ index ] = entry;

		size++;
		if ( size > numBins )
			rehash();
		}


	public Object remove( int key )
		{
		int index = key & numBinsMinusOne;

		if ( entries[ index ] == null )
			return null;

		int count = 0;

		HashEntry_io prevEntry = null;
		HashEntry_io entry = entries[ index ];
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


	public Object findKey( HashEntry_io entry, int key )
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


	public Object get( int key )
		{
		int index = key & numBinsMinusOne;

		return findKey( entries[ index ], key );
		}


	public String getString( int key )
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
		HashEntry_io[] oldEntries = entries;
		numBins = numBins << 1;
		entries = new HashEntry_io[numBins];
		numBinsMinusOne = numBins - 1;

		for ( int i = 0; i < oldEntries.length; i++ )
			{
			HashEntry_io entry = oldEntries[ i ];
			while ( entry != null )
				{
				HashEntry_io next = entry.next;
				entry.next = null;

				int index = entry.key & numBinsMinusOne;

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
			HashEntry_io entry = entries[ i ];
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
			HashEntry_io entry = entries[ i ];
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
				NashEntry entry = entries[ i ];
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
	public HashEntry_io[] returnArrayOfEntries()
		{
		int c = 0;
		HashEntry_io[] e = new HashEntry_io[size];
		for ( int i = 0; i < numBins; i++ )
			{
			HashEntry_io entry = entries[ i ];
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
			HashEntry_io entry = entries[ i ];
			while ( entry != null )
				{
				System.out.println( "HASH: " + entry.key + " " + entry.value );

				entry = entry.next;
				}
			}
		}


	public static void main( String[] args )
		{
		/*int x = 100000000;
		
				for ( int i = 0; i < 1000; i++ )
					{
					System.gc();

					int[] foo = new int[x];
					int[] foo2 = new int[x];

					System.out.println( x );

					//		x = x * 2;
					}
		*/
		System.out.println( System.currentTimeMillis() );

		int z = 1;
		for ( int z0 = 0; z0 < 1000000; z0 = z0 + 10000 )
			{
			z = z * 2;

			System.gc();

			System.out.println( "-----" + z + "-----" );

			Hashtable_io f2 = new Hashtable_io();

			try
				{
				long time = System.currentTimeMillis();
				for ( int i = 0; i < z; i++ )
					{
					f2.put( i, "value" + i );
					}

				System.out.print( "Nashtable: " + ( System.currentTimeMillis() - time ) );
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