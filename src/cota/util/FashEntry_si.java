package cota.util;

public class FashEntry_si
	{
	public String key = null;
	public int value = 0;

	public FashEntry_si next = null;

	public int hash = 0;


	public FashEntry_si( String key, int value, int hash )
		{
		this.key = key;
		this.value = value;
		this.hash = hash;
		}
	}
