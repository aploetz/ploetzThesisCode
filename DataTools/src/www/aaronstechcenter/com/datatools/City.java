package www.aaronstechcenter.com.datatools;

public class City 
{
	private String name;
	private String state;
	private String postal;
	private String country;
	
	public City()
	{
		name = "";
		state = "";
		postal = "";
		country = "";
	}
	
	public City(String strName, String strState, String strZip)
	{
		name = strName;
		state = strState;
		postal = strZip;
		country = "USA";
	}

	public City(String strName, String strState, String strZip, String strCountry)
	{
		name = strName;
		state = strState;
		postal = strZip;
		country = strCountry;
	}
	
	public String toString()
	{
		return name + " " + state + " " + postal + " " + country;
	}
	
	public String getCity()
	{
		return name;
	}
	
	public void setCity(String value)
	{
		name = value;
	}
	
	public String getState()
	{
		return state;
	}
	
	public void setState(String value)
	{
		state = value;
	}
	
	public String getZip()
	{
		return postal;
	}
	
	public void setZip(String value)
	{
		postal = value;
	}
	
	public String getCountry()
	{
		return country;
	}
	
	public void setCountry(String value)
	{
		country = value;
	}
}
