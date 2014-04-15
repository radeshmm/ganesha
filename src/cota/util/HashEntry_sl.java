package cota.util;

public class HashEntry_sl
	{
	public String key = null;
	public long value = 0;

	public HashEntry_sl next = null;

	public int hash = 0;


	public HashEntry_sl( String key, long value, int hash )
		{
		this.key = key;
		this.value = value;
		this.hash = hash;
		}
	}
