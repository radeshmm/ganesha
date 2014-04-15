package cota.util;

public class HashEntry_lo
	{
	public long key = 0;
	public Object value = null;

	public HashEntry_lo next = null;


	public HashEntry_lo( long key, Object value )
		{
		this.key = key;
		this.value = value;
		}
	}
