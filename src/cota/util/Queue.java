package cota.util;

import java.io.InputStream;

import cota.crypto.SHA;
import cota.io.InStream;
import java.util.Vector;


// A single growable array that wraps around
public class Queue
	{
	Object[] objects = null;
	int length = 16; // Default initial length
	int start = 0;
	int next = 0;


	// Constructor
	public Queue()
		{
		objects = new Object[length];
		}


	public Queue( InputStream in0 ) throws Throwable
		{
		objects = new Object[length];

		InStream in = new InStream( in0 );

		Queue q = in.readLines();
		in.close();

		append( q );
		}


	// Constructor
	public Queue( Queue q )
		{
		length = q.length;
		objects = new Object[length];

		System.arraycopy( q.objects, 0, objects, 0, length );
		start = q.start;
		next = q.next;
		}


	// Constructor
	public Queue( int size )
		{
		objects = new Object[size];
		length = size;
		}


	public void append( Queue q )
		{
		for ( int i = 0; i < q.size(); i++ )
			addObject( q.elementAt( i ) );
		}


	public Queue returnReverse()
		{
		Queue q = new Queue();
		for ( int i = size() - 1; i >= 0; i-- )
			q.addObject( elementAt( i ) );

		return q;
		}


	// add an object to the Queue
	public void addObject( Object o )
		{
		objects[ next++ ] = o;

		if ( next >= length )
			next = 0;

		// Check to see if we need to grow the array
		if ( next == start )
			grow();
		}


	// add an object to the front of the Queue
	public void addInFront( Object o )
		{
		int newStart = start - 1;
		if ( newStart < 0 )
			newStart = length - 1;

		start = newStart;

		objects[ start ] = o;

		// Check to see if we need to grow the array
		if ( next == start )
			grow();
		}


	// return the object at the given location
	public Object elementAt( int i )
		{
		int loc = start + i;

		if ( loc >= length )
			loc = loc - length;

		return objects[ loc ];
		}


	// replace the object at the given location
	public void replaceAt( int i, Object o )
		{
		int loc = start + i;

		if ( loc >= length )
			loc = loc - length;

		objects[ loc ] = o;
		}


	// swap the objects at the given location
	public void swap( int i, int j )
		{
		int loc1 = start + i;
		if ( loc1 >= length )
			loc1 = loc1 - length;

		int loc2 = start + j;
		if ( loc2 >= length )
			loc2 = loc2 - length;

		Object t = objects[ loc1 ];
		objects[ loc1 ] = objects[ loc2 ];
		objects[ loc2 ] = t;
		}


	// increase the capacity of the Queue
	public void grow()
		{
		int newLength = length * 2;
		Object[] newObjects = new Object[newLength];
		if ( start < next )
			{
			System.arraycopy( objects, start, newObjects, 0, next - start );
			next = next - start;
			}
		else
			{
			int firstBlock = length - start;
			System.arraycopy( objects, start, newObjects, 0, firstBlock );
			System.arraycopy( objects, 0, newObjects, firstBlock, next );
			next = firstBlock + next;
			}

		start = 0;
		objects = newObjects;
		length = newLength;
		}


	public int size()
		{
		if ( start == next )
			{
			return 0;
			}

		if ( start < next )
			{
			return next - start;
			}

		return length - start + next;
		}


	// remove an object from the front of the queue
	public Object removeFirst()
		{
		Object o = objects[ start ];
		objects[ start ] = null;
		start++;

		if ( start >= objects.length )
			start = 0;

		return o;
		}


	// remove an object from the back of the queue
	public Object removeLast()
		{
		next = next - 1;
		if ( next == -1 )
			next = length - 1;

		Object o = objects[ next ];
		objects[ next ] = null;

		return o;
		}


	// Remove an object at a specific location
	// The order of the Queue is not kept intact
	public Object removeWithoutOrder( int index )
		{
		Object o2 = elementAt( index );

		Object o = removeLast();

		replaceAt( index, o );

		return o2;
		}


	// remove all objects
	public void clear()
		{
		start = next = 0;
		length = 16;
		objects = new Object[length];
		}


	public void removeAllObjects()
		{
		clear();
		}


	public void printLines()
		{
		int size = size();
		for ( int i = 0; i < size; i++ )
			{
			String line = (String) elementAt( i );

			System.out.println( line );
			}
		}


	// This method is used to perform the recursive quicksort
	// Least to greatest
	public void sortPairDouble( int left, int right )
		{
		if ( right > left )
			{
			PairOD p = (PairOD) elementAt( right );
			double pivot = p.y;

			int i = left - 1;
			int j = right;

			while ( true )
				{
				while ( ( (PairOD) elementAt( ++i ) ).y < pivot )
					{
					if ( i >= right )
						break;
					}

				while ( ( (PairOD) elementAt( --j ) ).y >= pivot )
					{
					if ( j == 0 )
						break;
					}

				if ( i >= j )
					break;

				swap( i, j );
				}

			swap( i, right );

			sortPairDouble( left, i - 1 );
			sortPairDouble( i + 1, right );
			}
		}


	// Use quicksort to sort the list of records by a certain field
	// Assumes the Queue holds Pairs, with the second element being a double
	// Least to greatest
	public void sortPairDouble() throws Throwable
		{
		sortPairDouble( 0, size() - 1 );
		}


	// This method is used to perform the recursive quicksort
	// Least to greatest
	public void sortPairString( int left, int right )
		{
		if ( right > left )
			{
			PairOS p = (PairOS) elementAt( right );
			String pivot = p.y;

			int i = left - 1;
			int j = right;

			while ( true )
				{
				while ( ( (PairOS) elementAt( ++i ) ).y.compareTo( pivot ) < 0 )
					{
					if ( i >= right )
						break;
					}

				while ( ( (PairOS) elementAt( --j ) ).y.compareTo( pivot ) >= 0 )
					{
					if ( j == 0 )
						break;
					}

				if ( i >= j )
					break;

				swap( i, j );
				}

			swap( i, right );

			sortPairString( left, i - 1 );
			sortPairString( i + 1, right );
			}
		}


	// Use quicksort to sort the list of records by a certain field
	// Assumes the Queue holds Pairs, with the second element being a double
	// Least to greatest
	public void sortPairString() throws Throwable
		{
		sortPairString( 0, size() - 1 );
		}


	// This method is used to perform the recursive quicksort
	// Greatest to least
	public void sortPairDoubleReverse( int left, int right )
		{
		if ( right > left )
			{
			PairOD p = (PairOD) elementAt( right );
			double pivot = p.y;

			int i = left - 1;
			int j = right;

			while ( true )
				{
				while ( ( (PairOD) elementAt( ++i ) ).y >= pivot )
					{
					if ( i >= right )
						break;
					}

				while ( ( (PairOD) elementAt( --j ) ).y < pivot )
					{
					if ( j == 0 )
						break;
					}

				if ( i >= j )
					break;

				swap( i, j );
				}

			swap( i, right );

			sortPairDoubleReverse( left, i - 1 );
			sortPairDoubleReverse( i + 1, right );
			}
		}


	// Use quicksort to sort the list of records by a certain field
	// Assumes the Queue holds Pairs, with the second element being a double
	public void sortPairDoubleReverse() throws Throwable
		{
		sortPairDoubleReverse( 0, size() - 1 );
		}


	// This method is used to perform the recursive quicksort
	// Least to greatest
	public void sortPairInt( int left, int right )
		{
		if ( right > left )
			{
			PairOI p = (PairOI) elementAt( right );
			int pivot = p.y;

			int i = left - 1;
			int j = right;

			while ( true )
				{
				while ( ( (PairOI) elementAt( ++i ) ).y < pivot )
					{
					if ( i >= right )
						break;
					}

				while ( ( (PairOI) elementAt( --j ) ).y >= pivot )
					{
					if ( j == 0 )
						break;
					}

				if ( i >= j )
					break;

				swap( i, j );
				}

			swap( i, right );

			sortPairInt( left, i - 1 );
			sortPairInt( i + 1, right );
			}
		}


	// Use quicksort to sort the list of records by a certain field
	// Assumes the Queue holds Pairs, with the second element being a int
	// Least to greatest
	public void sortPairInt() throws Throwable
		{
		sortPairInt( 0, size() - 1 );
		}


	public String[] returnStringArray()
		{
		String[] a = new String[size()];
		for ( int i = 0; i < a.length; i++ )
			a[ i ] = (String) elementAt( i );

		return a;
		}


	public int[] returnIntArray()
		{
		int[] a = new int[size()];
		for ( int i = 0; i < a.length; i++ )
			a[ i ] = (Integer) elementAt( i );

		return a;
		}


	public long[] returnLongArray()
		{
		long[] a = new long[size()];
		for ( int i = 0; i < a.length; i++ )
			a[ i ] = (Long) elementAt( i );

		return a;
		}


	// Find the first String sandwiched between a prefix and a suffix
	public String findString( String prefix, String suffix )
		{
		for ( int i = 0; i < size(); i++ )
			{
			String line = (String) elementAt( i );

			int index = line.indexOf( prefix );
			if ( index != -1 )
				{
				int index2 = line.indexOf( suffix, index + prefix.length() );

				return line.substring( index + prefix.length(), index2 );
				}
			}

		return null;
		}


	public String returnSHA()
		{
		SHA sha = new SHA();
		int size = size();
		for ( int i = 0; i < size; i++ )
			{
			String line = (String) elementAt( i );

			sha.encodeString( line );
			}

		return sha.returnHexDigest();
		}


	public static void main( String[] args )
		{
		try
			{
			for ( double j = 1; j < 10000000; j = j * 2 )
				{
				/*				Queue q = new Queue();
								for ( int i = 0; i < j; i++ )
									{
									q.addObject( new PairOD( "", Math.random() ) );
									}

								long startTime = System.currentTimeMillis();
								q.insertSortPairDouble();
								System.out.println( j + " insert: " + ( System.currentTimeMillis() - startTime ) );
				*/
				Queue q = new Queue();
				for ( int i = 0; i < j; i++ )
					{
					q.addObject( new PairOD( "", (int) ( Math.random() * 1000 ) ) );
					}

				long startTime = System.currentTimeMillis();
				q.sortPairDouble();
				System.out.println( j + " quick: " + ( System.currentTimeMillis() - startTime ) );

				System.out.println( "" );
				}
			}
		catch ( Throwable theX )
			{
			cota.util.Util.printX( theX );
			}
		}

	}
