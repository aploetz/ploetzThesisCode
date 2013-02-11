package www.aaronstechcenter.com.datatools;

public class Customer 
{
	private String firstName;
	private String lastName;
	private String email;
	private String password;

	public Customer()
	{
	}
	
	public Customer(String first, String last)
	{
		firstName = first;
		lastName = last;
	}
	
	public Customer(String first, String last, String emailaddy, String pass)
	{
		firstName = first;
		lastName = last;
		email = emailaddy;
		password = pass;
	}
	
	public String getFirstName()
	{
		return firstName;
	}
	
	public void setFirstName(String value)
	{
		firstName = value;
	}

	public String getLastName()
	{
		return lastName;
	}
	
	public void setLastName(String value)
	{
		lastName = value;
	}

	public String getEmail()
	{
		return email;
	}
	
	public void setEmail(String value)
	{
		email = value;
	}

	public String getPassword()
	{
		return password;
	}
	
	public void setPassword(String value)
	{
		password = value;
	}
}

