package www.aaronstechcenter.com.datatools;

public class Address 
{
	private String type;
	private String line1;
	private String line2;
	private City city;
	
	public Address()
	{	
	}
	
	public String getType()
	{
		return type;
	}
	
	public void setType(String value)
	{
		type = value;
	}

	public String getLine1()
	{
		return line1;
	}
	
	public void setLine1(String value)
	{
		line1 = value;
	}

	public String getLine2()
	{
		return line2;
	}
	
	public void setLine2(String value)
	{
		line2 = value;
	}

	public City getCity()
	{
		return city;
	}
	
	public void setcity(City value)
	{
		city = value;
	}
}
