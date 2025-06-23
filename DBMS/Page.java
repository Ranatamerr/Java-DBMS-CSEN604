package DBMS;

import java.io.Serializable;
import java.util.ArrayList;

public class Page implements Serializable
{

	private ArrayList<String []> records;
	
	
	public Page() 
	{
		super();
		this.records = new ArrayList<String[]>();
	}
	
	public boolean insert(String []record)
	{
		if(records.size()<DBApp.dataPageSize)
		{
			this.records.add(record);
			return true;
		}
		return false;
	}
	
	public ArrayList<String[]> select()
	{
		return this.records;
	}
	
	public ArrayList<String []> select(String[] cond)
	{
		ArrayList<String []> res = new ArrayList<String []>();
		
		for(int i=0;i<this.records.size();i++)
		{
			boolean flag = true;
			for(int j=0;j<cond.length;j++)
			{
				if(cond[j]!=null)
				{
					if(!cond[j].equals(this.records.get(i)[j]))
					{
						flag = false;
						break;
					}
				}
			}
			if(flag)
				res.add(this.records.get(i));
		}
		
		return res;
	}
	
	public ArrayList<String []> select(int i)
	{
		ArrayList<String []> res = new ArrayList<String []>();
		res.add(this.records.get(i));
		return res;
	}

}
