package cota.ganesha;

import cota.io.Message;
import cota.util.LRU_sl;
import cota.util.StringUtils;


// Basically just adds a cache to the GobServer.translate call
public class Translator
	{
	static private LRU_sl cache = null;

	static
		{
		try
			{
			cache = new LRU_sl( 1024L * 1024L * 100L );
			cache.name = "Translator.cache";
			}
		catch ( Throwable theX )
			{
			cota.util.Util.printX( theX );
			}
		}


	void ______PUBLIC________()
		{
		}


	// Returns 0 if no translation is found
	public static long get( String comboKey, boolean createNew ) throws Throwable
		{
		// First check the LRU cache
		synchronized ( cache )
			{
			long objectID = cache.get( comboKey );
			if ( objectID != LRU_sl.NOT_FOUND )
				return objectID;
			}

		Message m = new Message();
		m.writeString( comboKey );
		m.writeByte( createNew ? 1 : 0 );

		Message r = TranslationServer.sendRequest( TranslationServer.TRANSLATE, m );

		long objectID = r.read8Bytes();

		if ( objectID != 0 )
			synchronized ( cache )
				{
				cache.put( comboKey, objectID );
				}

		return objectID;
		}


	// Returns 0 if no translation is found
	public static long translate0( String workspace, String tableName, String name, boolean createNew ) throws Throwable
		{
		String key = workspace + "%" + tableName + "%" + name;

		//		System.out.println( "Translator.get: " + key + "\t" + createNew );

		// First check the LRU cache
		synchronized ( cache )
			{
			long objectID = cache.get( key );
			if ( objectID != LRU_sl.NOT_FOUND )
				return objectID;
			}

		Message m = new Message();
		m.writeString( key );
		m.writeByte( createNew ? 1 : 0 );

		Message r = TranslationServer.sendRequest( TranslationServer.TRANSLATE, m );

		long objectID = r.read8Bytes();

		//	System.out.println( "Translator.get2: " + objectID );

		if ( objectID != 0 )
			synchronized ( cache )
				{
				cache.put( key, objectID );
				}

		return objectID;
		}


	public static String addUnicodeWeirdness( String s )
		{
		StringBuffer sb = new StringBuffer();

		for ( int i = 0; i < s.length(); i++ )
			{
			char c = s.charAt( i );

			//			System.out.println( (int) c + "\t" + c );

			sb.append( c );
			if ( c > 255 )
				sb.append( "️" );
			}

		return sb.toString();
		}


	public static long translate( String workspace, String tableName, String name ) throws Throwable
		{
		return translate( workspace, tableName, name, false );
		}


	// Because iOS can introduce weird unicode characters, we need to check for 
	// translations that were made using iOS 6 (without the weird characters as well)
	public static long translate( String workspace, String tableName, String name, boolean createNew ) throws Throwable
		{
		long objectID = translate0( workspace, tableName, name, createNew );

		if ( objectID != 0 )
			return objectID;

		// Didn't find the translation under the original name
		String newName = StringUtils.replace( name, "️", "" );
		if ( !newName.equals( name ) )
			{
			// Try the version without the unicode weirdness
			objectID = translate0( workspace, tableName, newName, createNew );

			if ( objectID != 0 )
				return objectID;
			}

		// Try a version with the unicode weirdness added
		String weirdName = addUnicodeWeirdness( name );
		if ( !weirdName.equals( name ) )
			{
			objectID = translate0( workspace, tableName, weirdName, createNew );

			if ( objectID != 0 )
				return objectID;
			}

		// Looks like objectID is 0, just return it
		return objectID;
		}

	}
