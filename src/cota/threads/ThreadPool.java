package cota.threads;

import cota.util.Queue;
import cota.util.Util;


public class ThreadPool
	{
	Queue freeThreads = new Queue();

	Class threadClass = null;

	int minFree = 2;
	int maxFree = 128;

	int used = 0;


	public ThreadPool( String className ) throws Throwable
		{
		init( className, minFree, maxFree );
		}


	public ThreadPool( String className, int minFree, int maxFree ) throws Throwable
		{
		init( className, minFree, maxFree );
		}


	public void init( String className, int minFree, int maxFree ) throws Throwable
		{
		try
			{
			threadClass = Class.forName( className );
			}
		catch ( Throwable theX )
			{
			Util.printX( "ATTEMPT TO INITIALIZE THE ThreadPool WITH ILLEGAL CLASS NAME: "
					+ className, theX );
			}

		this.minFree = minFree;
		this.maxFree = maxFree;

		try
			{
			// create the initial pool
			synchronized ( freeThreads )
				{
				while ( freeThreads.size() < minFree )
					{
					ReusableThread newThread = (ReusableThread) threadClass.newInstance();
					newThread.init( this );

					freeThreads.addObject( newThread );
					}
				}
			}
		catch ( java.lang.InstantiationException theX )
			{
			Util.printX( "***Problem creating a ReusableThread of type " + className
					+ "\n***Make sure that " + className
					+ " has a default constructor:\n***public " + className + "()", theX );
			}
		}


	public ReusableThread getThread() throws Throwable
		{
		ReusableThread thread = null;
		synchronized ( freeThreads )
			{
			// There should always be at least one free thread in the queue
			thread = (ReusableThread) freeThreads.removeFirst();

			if ( freeThreads.size() < minFree )
				{
				ReusableThread newThread = (ReusableThread) threadClass.newInstance();
				newThread.init( this );

				freeThreads.addObject( newThread );
				}

			used++;
			// Util.print( "Threads: " + freeThreads.size() + " free " + used +
			// " used" );
			}

		return thread;
		}


	// Indicate that the thread has finished
	// Returns true if the thread is reinserted into the pool
	public boolean shouldKeepTheThread( ReusableThread thread )
		{
		// if we haven't exceeded the maximum number of threads,
		// then return the thread in question to the pool
		boolean keep = false;
		synchronized ( freeThreads )
			{
			if ( freeThreads.size() < maxFree )
				{
				freeThreads.addObject( thread );
				keep = true;
				}

			used--;
			// System.out.println( "Thread finished: " + freeThreads.size() + "
			// free " + used + " used" );
			}

		return keep;
		}
	}
