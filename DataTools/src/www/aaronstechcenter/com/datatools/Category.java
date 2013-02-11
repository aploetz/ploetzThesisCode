package www.aaronstechcenter.com.datatools;

public class Category 
{
	private String name;
	private String hierarchy;
	private String abbrev;
	
	public Category()
	{
		name = "";
		hierarchy = "";
		abbrev = "";
	}
	
	public Category(String strName, String strHier, String strAbbrev)
	{
		name = strName;
		hierarchy = strHier;
		abbrev = strAbbrev;
	}

	public String getName()
	{
		return name;
	}
	
	public void setName(String value)
	{
		name = value;
	}
	
	public String getHierarchy()
	{
		return hierarchy;
	}
	
	public void setHierarchy(String value)
	{
		hierarchy = value;
	}
	
	public String getAbbreviation()
	{
		return abbrev;
	}
	
	public void setAbbreviation(String value)
	{
		abbrev = value;
	}
}
