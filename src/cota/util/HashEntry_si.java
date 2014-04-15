package cota.util;

public class HashEntry_si
	{
	public String key = null;
	public int value = 0;

	public HashEntry_si next = null;

	public int hash = 0;


	public HashEntry_si( String key, int value, int hash )
		{
		this.key = key;
		this.value = value;
		this.hash = hash;
		}
	}
