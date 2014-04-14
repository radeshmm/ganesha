package cota.util;

public class FashEntry_sl
	{
	public String key = null;
	public long value = 0;

	public FashEntry_sl next = null;

	public int hash = 0;


	public FashEntry_sl( String key, long value, int hash )
		{
		this.key = key;
		this.value = value;
		this.hash = hash;
		}
	}
