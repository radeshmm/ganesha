package cota.threads;

import cota.util.Fashtable_so;
import cota.util.Util;


public abstract class ReusableThread extends Thread
	{
	public static int MAX_SLEEP = 1000 * 30;

	ThreadPool pool = null;

	Object activationLock = new Object();
	boolean active = false;

	private Fashtable_so threadArgs = new Fashtable_so();


	public void init( ThreadPool pool )
		{
		this.pool = pool;

		setDaemon( true );
		start();
		}


	public void run()
		{
		while ( true )
			{
			try
				{
				synchronized ( activationLock )
					{
					if ( !active )
						{
						// Really should wait forever, but there seems to be a
						// problem with the wait/notifyAll call
						activationLock.wait( MAX_SLEEP );
						}
					}

				if ( active )
					performTask( threadArgs );
				}
			catch ( Throwable theX )
				{
				Util.printX( "ERROR WITHIN A ReusableThread", theX );
				}

			if ( active )
				{
				active = false;
				threadArgs.clear();

				if ( !pool.shouldKeepTheThread( this ) )
					return;
				}
			}
		}


	public void addArg( String key, Object value )
		{
		threadArgs.put( key, value );
		}


	public void activate()
		{
		synchronized ( activationLock )
			{
			active = true;
			activationLock.notifyAll();
			}
		}


	public abstract void performTask( Fashtable_so args ) throws Throwable;
	}
