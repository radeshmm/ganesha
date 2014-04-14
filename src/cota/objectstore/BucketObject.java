package cota.objectstore;

import java.util.zip.CRC32;

import cota.io.Message;


public class BucketObject
	{
	static final int OBJECT = 0;
	static final int TRANSLATION = 1;

	int type = 0;

	long id = 0;
	String key = null;

	long timestamp = 0;
	byte[] bytes = null;

	// This checksum is simply the checksum of the bytes
	// Not necessarily what they should be but what they currently are
	// It's used as a quick way to see if all servers currently have the same
	// representation of an object
	int checksum = 0;


	public BucketObject( Message m ) throws Throwable
		{
		type = m.readByte();

		if ( type == OBJECT )
			{
			id = m.read8Bytes();
			timestamp = m.read8Bytes();
			bytes = m.readBytes();

			CRC32 crc = new CRC32();
			crc.update( bytes );

			checksum = (int) ( crc.getValue() );
			}

		if ( type == TRANSLATION )
			{
			key = m.readString();
			id = m.read8Bytes();
			}
		}


	public BucketObject( long id, byte[] bytes, long timestamp )
		{
		this.type = OBJECT;
		this.id = id;
		this.bytes = bytes;
		this.timestamp = timestamp;

		CRC32 crc = new CRC32();
		crc.update( bytes );

		checksum = (int) ( crc.getValue() );
		}


	public BucketObject( String key, long id )
		{
		this.type = TRANSLATION;
		this.key = key;
		this.id = id;
		}


	public void writeToMessage( Message m ) throws Throwable
		{
		m.writeByte( type );

		if ( type == OBJECT )
			{
			m.write8Bytes( id );
			m.write8Bytes( timestamp );
			m.writeBytes( bytes );
			}

		if ( type == TRANSLATION )
			{
			m.writeString( key );
			m.write8Bytes( id );
			}
		}
	}
