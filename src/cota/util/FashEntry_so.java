package cota.util;

public class FashEntry_so
	{
	public String key = null;
	public Object value = null;

	public FashEntry_so next = null;

	public int hash = 0;


	public FashEntry_so( String key, Object value, int hash )
		{
		this.key = key;
		this.value = value;
		this.hash = hash;
		}
	}
