package cota.util;

public class HashEntry_so
	{
	public String key = null;
	public Object value = null;

	public HashEntry_so next = null;

	public int hash = 0;


	public HashEntry_so( String key, Object value, int hash )
		{
		this.key = key;
		this.value = value;
		this.hash = hash;
		}
	}
