package cota.util;

public class HashEntry_io
	{
	public int key = 0;
	public Object value = null;

	public HashEntry_io next = null;


	public HashEntry_io( int key, Object value )
		{
		this.key = key;
		this.value = value;
		}
	}
