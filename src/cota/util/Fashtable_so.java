package cota.util;

import java.util.Hashtable;


public class Fashtable_so
	{
	public int numBins = 8;
	public int numBinsMinusOne = 7;

	public FashEntry_so[] entries = null;

	public int size = 0;


	// Constructor
	public Fashtable_so()
		{
		entries = new FashEntry_so[numBins];
		}


	public void addTable( Fashtable_so f )
		{
		FashEntry_so[] entries = f.returnArrayOfEntries();
		for ( int i = 0; i < entries.length; i++ )
			{
			FashEntry_so entry = entries[ i ];

			put( entry.key, entry.value );
			}
		}


	public void put( String key, Object value )
		{
		// null entries are not allowed
		if ( value == null )
			{
			remove( key );

			return;
			}

		int hash = key.hashCode();
		int index = hash & numBinsMinusOne;

		FashEntry_so entry0 = entries[ index ];
		if ( entry0 == null )
			{
			entry0 = entries[ index ] = new FashEntry_so( key, value, hash );
			size++;

			if ( size > numBins )
				rehash();

			return;
			}

		// See if the key is already there
		FashEntry_so entry = entry0;
		while ( entry != null )
			{
			if ( entry.key.equals( key ) )
				{
				entry.value = value;

				return;
				}

			entry = entry.next;
			}

		entry = new FashEntry_so( key, value, hash );
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

		FashEntry_so prevEntry = null;
		FashEntry_so entry = entries[ index ];
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


	public Object findKey( FashEntry_so entry, String key )
		{
		while ( entry != null )
			{
			if ( entry.key.equals( key ) )
				return entry.value;

			entry = entry.next;
			}

		return null;
		}


	public Object get( String key )
		{
		if ( key == null )
			return null;

		int hash = key.hashCode();
		int index = hash & numBinsMinusOne;

		return findKey( entries[ index ], key );
		}


	public Integer getInt( String key )
		{
		if ( key == null )
			return null;

		int hash = key.hashCode();
		int index = hash & numBinsMinusOne;

		return (Integer) findKey( entries[ index ], key );
		}


	public String getString( String key )
		{
		Object o = get( key );

		if ( o == null )
			return "";

		String s = (String) o;
		while ( s.startsWith( " " ) )
			s = s.substring( 1, s.length() );

		while ( s.endsWith( " " ) )
			s = s.substring( 0, s.length() - 1 );

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
		FashEntry_so[] oldEntries = entries;
		numBins = numBins << 1;
		entries = new FashEntry_so[numBins];
		numBinsMinusOne = numBins - 1;

		for ( int i = 0; i < oldEntries.length; i++ )
			{
			FashEntry_so entry = oldEntries[ i ];
			while ( entry != null )
				{
				FashEntry_so next = entry.next;
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
			FashEntry_so entry = entries[ i ];
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
			FashEntry_so entry = entries[ i ];
			while ( entry != null )
				{
				if ( entry.value != null )
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
			FashEntry_so entry = entries[ i ];
			while ( entry != null )
				{
				if ( entry.value != null )
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


	public FashEntry_so[] returnArrayOfEntries()
		{
		int c = 0;
		FashEntry_so[] e = new FashEntry_so[size];
		for ( int i = 0; i < numBins; i++ )
			{
			FashEntry_so entry = entries[ i ];
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
			FashEntry_so entry = entries[ i ];
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

			Fashtable_so f = new Fashtable_so();
			try
				{
				long time = System.currentTimeMillis();
				for ( int i = 0; i < z; i++ )
					{
					f.put( "key" + i, "value" + i );
					}

				System.out.print( "Fashtable1: " + ( System.currentTimeMillis() - time ) );
				time = System.currentTimeMillis();
				for ( int i = 0; i < z; i++ )
					{
					Object v = f.get( "key" + i );

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