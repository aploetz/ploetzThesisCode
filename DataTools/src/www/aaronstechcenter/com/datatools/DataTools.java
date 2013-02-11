package www.aaronstechcenter.com.datatools;

import java.io.*;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Date;
//import javax.sql.DataSource;
import java.sql.*;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.DBObject;
import com.mongodb.DBCollection;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftCluster;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import oracle.jdbc.pool.OracleDataSource;

public class DataTools 
{

	private static ArrayList<String> getNames(String strFileLocation)
	{
		ArrayList<String> returnList = new ArrayList<String>();
		BufferedReader Reader;
		String name = null;		
		
		try 
		{
			//Reader = new BufferedReader(new InputStreamReader(new FileInputStream(strFileLocation), "UTF-16"));
			Reader = new BufferedReader(new FileReader(strFileLocation));
			String line = null;
	
			while ((line = Reader.readLine()) != null)
			{
				if (line.trim().length() > 0)
				{
					name = line;
					returnList.add(name.trim());
				}
			}
			
			Reader.close();
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return returnList;
	}
	
	private static ArrayList<City> getCities(String strFileLocation)
	{
		ArrayList<City> returnList = new ArrayList<City>();
		BufferedReader Reader;
		String name = null;
		String state = null;
		String zip = null;
		String[] inData;
		City tempCity;
		
		try 
		{
			Reader = new BufferedReader(new FileReader(strFileLocation));
			String line = null;
	
			while ((line = Reader.readLine()) != null)
			{
				inData = line.split(",");
				
				if (inData.length == 3)
				{
				    name = inData[0].trim();
				    state = inData[1].trim();
				    zip = inData[2].trim();
				    
				    tempCity = new City(name, state, zip);
				    
					returnList.add(tempCity);
				}
			}
			
			Reader.close();
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return returnList;
	}
	
	private static ArrayList<Category> getCategories(String strFileLocation)
	{
		ArrayList<Category> returnList = new ArrayList<Category>();
		BufferedReader Reader;
		String name = null;
		String hierarchy = null;
		String abbrev = null;
		String[] inData;
		Category tempCat;
		
		try 
		{
			Reader = new BufferedReader(new FileReader(strFileLocation));
			String line = null;
	
			while ((line = Reader.readLine()) != null)
			{
				inData = line.split("\\|");
				
				if (inData.length == 3)
				{
				    abbrev = inData[0].trim();
				    name = inData[1].trim();
				    hierarchy = inData[2].trim();
				    
				    tempCat = new Category(name, hierarchy, abbrev);
				    
					returnList.add(tempCat);
				}
			}
			
			Reader.close();
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return returnList;
	}
	
    private static int randomNumber(int min, int max)
    {
        if (min >= max)
        {
            return max;
        }
        else
        {
            Random random = new Random();
            return random.nextInt(max - min + 1) + min;
        }
    }

    private static String genPassword()
    {
    	ArrayList<Character> passwordLetters = new ArrayList<Character>();
    	String txtPassword = "";
    	int intPasswordLen = 8;
    	int intNumericChars = randomNumber(1,7);
    	int intSpecialChars = randomNumber(1,7);
    	//int intMaxConsecChars = 2;
    	int intNumCounter = 0;
    	int intLetterCounter = 0;
    	int intSpecCounter = 0;
    	int intLetterASCII = 0;
    	boolean blnGood = false;
    	//boolean blnConsChars = false;
    	
    	int intNumLetters = intPasswordLen - intNumericChars - intSpecialChars;

    	if (intNumLetters < 1)
    	{
    		intNumLetters = 1;
    	}

    	//generate each character of the password
    	for (int intCounterJ = 0; intCounterJ < intPasswordLen; intCounterJ++)
    	{
    		blnGood = false;
    	    intLetterASCII = 0;

    	        while (!blnGood)
    	        {
    	        	//max ASCII char range of 33 to 126
    	        	intLetterASCII = randomNumber(33, 126);

    	                if (intLetterASCII > 47 && intLetterASCII < 58 && intNumCounter < intNumericChars)
    	                { //numbers
    	                	intNumCounter++;
    	                    blnGood = true;
    	                }
    	                else if (intLetterASCII > 64 && intLetterASCII < 91 && intLetterCounter < intNumLetters)
    	                { //capital letters
    	                        intLetterCounter++;
    	                        blnGood = true;
    	                }
    	                else if (intLetterASCII > 96 && intLetterASCII < 123 && intLetterCounter < intNumLetters)
    	                { //lowercase letters
    	                        intLetterCounter++;
    	                        blnGood = true;
    	                }
    	                else if (intLetterASCII != 124 && intSpecCounter < intSpecialChars)
    	                { //special characters
                          //for the purposes of this exercise, don't allow pipes
    	                        intSpecCounter++;
    	                        blnGood = true;
    	                }
    	        }

    	    passwordLetters.add((char)intLetterASCII);

    	    txtPassword += (char)intLetterASCII;
    	}
    	return txtPassword;
    }
    
    private static String genEmail(String first,String last,ArrayList<String> providers)
    {
    	String strReturnEmail = null;
    	int intEmailIndex = randomNumber(0,providers.size() - 1);
    	
    	strReturnEmail = first + "." + last + "@" + providers.get(intEmailIndex);
    	
    	return strReturnEmail;
    }
    
    private static String gen4DigitNumber(int intNumber)
    {
    	String strReturnNumber = String.valueOf(intNumber);
    	
    	if (strReturnNumber.length() < 4)
    	{
    		//int intDigits = 4 - strReturnNumber.length();
    		
    		//for (int intCounterJ = 0; intCounterJ < intDigits; intCounterJ++)
    		//{
    		//	strReturnNumber = "0" + strReturnNumber;
    		//}
    		strReturnNumber = String.format("%04d", intNumber);
    	}
    	
    	return strReturnNumber;
    }

    private static String genVersion()
    {
    	String strReturn = null;
    	int intRandomNum = randomNumber(0,9);
    	
		switch(intRandomNum)
		{
			case 1:
				strReturn = "Sigma ";
				break;
		    case 2:
		        strReturn = "Alpha ";
		        break;
		    case 3:
		        strReturn = "Lite ";
		        break;
		    case 4:
		        strReturn = "Xtreme ";
		        break;
		    case 5:
		        strReturn = "Quick ";
		        break;
		    case 6:
		        strReturn = "Phoenix ";
		        break;
		    case 7:
		        strReturn = "Enterprise ";
		        break;
		    case 8:
		        strReturn = "Excelsior ";
		        break;
		    case 9:
		        strReturn = "Reliant ";
		        break;
		    default:
		        strReturn = "";
		        break;
		}	
    	
    	return strReturn;
    }
    
    private static ArrayList<String> selectKeywords(ArrayList<String> productKeywords)
    {
    	ArrayList<String> localKeywordList = new ArrayList<String>();
    	int intNumKeywords = randomNumber(1,3);
    	String strKeyword = null;
    	
    	for (int intCounterJ = 0; intCounterJ < intNumKeywords; intCounterJ++)
    	{
    		strKeyword = productKeywords.get(randomNumber(0,productKeywords.size() - 1));
    		localKeywordList.add(strKeyword);
    	}
    	
    	return localKeywordList;
    }
    
	private static String genDescription(String productHeadline,ArrayList<String> currentKeywords, String productType, String companyName)
	{
		String strDescription = "<p>The " + productHeadline + " is ";
		ArrayList<String> randomWords = new ArrayList<String>();
		int intWordCount = randomNumber(800,1200);

		//build random words list
		randomWords.add(productType);
		randomWords.add(companyName);
		for(String keyword : currentKeywords)
		{
			randomWords.add(keyword);
		}
		
		//use random words to build the product description
		for (int intCounterJ = 0; intCounterJ < intWordCount; intCounterJ++)
		{
			if (randomNumber(1,5) == 1)
			{
				strDescription += randomWords.get(randomNumber(0,randomWords.size() - 1)) + " ";
			}
			else
			{
				strDescription += "blah ";
			}
		}
		
		return strDescription + "</p>";
	}

	private static String genBullets(ArrayList<String> currentKeywords, String productType, String productVersion)
	{
		String strBullets = null;
		String strCondition = null;
		int intRandomCondition = randomNumber(0,6);
		
		switch(intRandomCondition)
		{
			case 1:
				strCondition = "All New ";
				break;
		    case 2:
		    	strCondition = "New ";
		        break;
		    case 3:
		    	strCondition = "Refurbished ";
		        break;
		    case 4:
		    	strCondition = "Reengineered ";
		        break;
		    case 5:
		    	strCondition = "Rebuilt ";
		        break;
		    case 6:
		    	strCondition = "Redesigned ";
		        break;
		    default:
		    	strCondition = "";
		        break;
		}	
		
		//bullet #1 is always a combination of condition, type and version
		strBullets = "&bull;" + strCondition + productType + " " + productVersion + "<BR>";
		
		for(String keyword : currentKeywords)
		{
			strBullets += "&bull;" + keyword + "<BR>";
		}
		
		return strBullets;
	}

    
    private static void generateProductData(int intProductCount)
    {
    	FileWriter fwOutN = null;
    	FileWriter fwOut = null;
    	FileWriter priceOut = null;

    	String strSKU = null;
    	//String productName = null;
    	String productHeadline = null;
    	String productDescription = null;
    	String productBullet = null;
    	String companyName = null;
    	String version = null;
    	String productType = null;
    	String categoryName = null;
    	String abbrev = null;
    	String hierarchyName = null;
    	int intSKUCounter = 0;
    	int intNumberOfBreaks = 0;
    	int intQty = 0;
    	int intPrice = 0;
    	int intOrigPrice = 0;
    	//HashMap<Integer,Integer> prices = new HashMap<Integer,Integer>();
    	//ArrayList<Category> mainCategories = getCategories("C:\\Documents and Settings\\e04199\\My Documents\\school\\MCT624\\thesis_data\\productCategories.txt");
    	ArrayList<Category> mainCategories = getCategories("/home/aploetz/programming/java/thesis_data/productCategories2.txt");
    	ArrayList<String> currentKeywords = new ArrayList<String>();
    	ArrayList<String> productCompanies = getNames("/home/aploetz/programming/java/thesis_data/productCompanies.txt");
    	ArrayList<String> productTypes = getNames("/home/aploetz/programming/java/thesis_data/productsData.txt");
    	ArrayList<String> productKeywords = getNames("/home/aploetz/programming/java/thesis_data/productKeywords.txt");
    	ArrayList<String> skus = new ArrayList<String>();
    	
    	try
    	{
    		fwOutN = new FileWriter("/home/aploetz/programming/java/thesis_data/SKUDataNormalized.txt");
    		fwOutN.write("SKU|productName|productDescription|productBullet|categoryID\r\n");
    		fwOut = new FileWriter("/home/aploetz/programming/java/thesis_data/SKUData.txt");
    		fwOut.write("SKU|productName|productDescription|productBullet|categoryID|categoryName|hierarchy\r\n");
    		priceOut = new FileWriter("/home/aploetz/programming/java/thesis_data/SKUPrices.txt");
    		priceOut.write("SKU|qty|price\r\n");
    	}
    	catch (IOException e)
    	{
    		System.out.println(e.toString());
    		return;
    	}
   	
    	//define main
    	for (Category currentCat : mainCategories)
    	{
    		//generate {intProductCount} products under each main hierarchy
	    	for (intSKUCounter = 0; intSKUCounter < intProductCount; intSKUCounter++)
	    	{
	    		//get category properties
	    		categoryName = currentCat.getName();
	    		abbrev = currentCat.getAbbreviation();
	    		hierarchyName = currentCat.getHierarchy();

	    		//make sure that we don't have a dupe
	    		boolean uniqueSKU = false;
	    		
	    		while (!uniqueSKU) {
		    		//build SKU number
		    		strSKU = abbrev + gen4DigitNumber(intSKUCounter);

		    		if (skus.contains(strSKU)) {
		    			uniqueSKU = false;
		    		} else {
		    			uniqueSKU = true;
		    			skus.add(strSKU);
		    		}
	    		}
	    		
	    		//generate a random company name
	    		companyName = productCompanies.get(randomNumber(0, productCompanies.size() - 1));

	    		//generate a product version
	    		version = genVersion();
	    		
	    		//generate a product type
	    		productType = productTypes.get(randomNumber(0, productTypes.size() - 1));
	    		
	    		//generate up to 3 random keywords
	    		currentKeywords.clear();
	    		currentKeywords = selectKeywords(productKeywords);
	    		
	    		//build product copy
	        	//productName = "";
	        	productHeadline = companyName + " " + categoryName + " " + version + productType;
	        	productDescription = genDescription(productHeadline, currentKeywords, productType, companyName);
	        	productBullet = genBullets(currentKeywords, productType, version);
	        	
	        	//generate price breaks
	        	// prices range between $4.99 and $99.99
	        	// prices will be stored as an integer of cents.  ex: $4.99 = 499
	        	// each product will have between 1 and 3 breaks
	        	// the price for each break will be 10% less than the prior price
	        	intNumberOfBreaks = randomNumber(1,3);
	        	
	        	for (int intCounterK = 0; intCounterK < intNumberOfBreaks; intCounterK++)
	        	{
	        		if (intCounterK == 0)
	        		{
		        		intOrigPrice = randomNumber(499,9999);
	        			intPrice = intOrigPrice;
	        			intQty = 1;
	        		}
	        		else
	        		{
	        			intPrice = (int)((double)intOrigPrice * (1 - (.1 * (double)intCounterK)));
	        			intQty = randomNumber(intQty+1,intQty+9);
	        		}
	        		
	        		try
	        		{
		        		priceOut.flush();
		        		priceOut.write(strSKU + "|" + String.valueOf(intQty) + "|" + String.valueOf(intPrice) + "\r\n");
	        		}
		        	catch (IOException e)
		        	{
		        		System.out.println(e.toString());
		        	}
	        	}

	        	try
	        	{
		        	fwOut.flush();
		        	fwOut.write(strSKU + "|" + productHeadline + "|" + productDescription + "|" + productBullet + "|" + abbrev + "|" + categoryName + "|" + hierarchyName + "\r\n");
		        	fwOutN.flush();
		        	fwOutN.write(strSKU + "|" + productHeadline + "|" + productDescription + "|" + productBullet + "|" + abbrev + "\r\n");
	        	}
	        	catch (IOException e)
	        	{
	        		System.out.println(e.toString());
	        	}
	    	}
    	}
    	
    	try
    	{
        	fwOut.close();
        	fwOutN.close();
        	priceOut.close();
    	}
    	catch (IOException e)
    	{
    		System.out.println(e.toString());
    	}
    }
    
	public static void generateCustomersWAddresses(int intCustomerCount)
	{
		int intFirstNameIndex = 0;
		int intLastNameIndex = 0;
		int intStreetNameIndex = 0;
		int intStreetTypeIndex = 0;
		int intCityIndex = 0;
		int intNumOfAddresses = 0;
		int intStreetNumber = 0;
		int intAddressLine2 = 0;
		String strEmail = null;
		String strFirstName = null;
		String strLastName = null;
		String strPassword = null;
		String strAddress = null;
		
		try 
		{
			FileWriter fwOut = new FileWriter("/home/aploetz/programming/java/thesis_data/customerData.txt");
		
			//ArrayList<String> firstNames = getNames("C:\\Documents and Settings\\e04199\\My Documents\\school\\MCT624\\thesis_data\\firstNames.txt");
			ArrayList<String> firstNames = getNames("/home/aploetz/programming/java/thesis_data/firstNames.txt");
			//ArrayList<String> lastNames = getNames("C:\\Documents and Settings\\e04199\\My Documents\\school\\MCT624\\thesis_data\\lastNames.txt");
			ArrayList<String> lastNames = getNames("/home/aploetz/programming/java/thesis_data/lastNames.txt");
			//ArrayList<String> streetNames = getNames("C:\\Documents and Settings\\e04199\\My Documents\\school\\MCT624\\thesis_data\\streetNames.txt");
			ArrayList<String> streetNames = getNames("/home/aploetz/programming/java/thesis_data/streetNames.txt");
			//ArrayList<City> cities = getCities("C:\\Documents and Settings\\e04199\\My Documents\\school\\MCT624\\thesis_data\\cityNames.txt");
			ArrayList<City> cities = getCities("/home/aploetz/programming/java/thesis_data/cityNames.txt");
			//ArrayList<String> streetTypes = getNames("C:\\Documents and Settings\\e04199\\My Documents\\school\\MCT624\\thesis_data\\streetTypes.txt");
			ArrayList<String> streetTypes = getNames("/home/aploetz/programming/java/thesis_data/streetTypes.txt");
			//ArrayList<String> emailProviders = getNames("C:\\Documents and Settings\\e04199\\My Documents\\school\\MCT624\\thesis_data\\emailProviders.txt");
			ArrayList<String> emailProviders = getNames("/home/aploetz/programming/java/thesis_data/emailProviders.txt");
			
			//************************************************
			//******************build 1500******************** 
			//************************************************
			for (int intCounterJ = 0; intCounterJ < intCustomerCount; intCounterJ++)
			{
				intFirstNameIndex = randomNumber(0,firstNames.size() - 1);
				intLastNameIndex = randomNumber(0,lastNames.size() - 1);
				//make sure that we don't have anyone with the same first and last name
				while (firstNames.get(intFirstNameIndex) == lastNames.get(intLastNameIndex))
				{
					intLastNameIndex = randomNumber(0,lastNames.size() - 1);
				}
	
				strFirstName = firstNames.get(intFirstNameIndex);
				strLastName = lastNames.get(intLastNameIndex);
				strEmail = genEmail(strFirstName,strLastName,emailProviders);
				strPassword = genPassword();
				
				//gen one or two addresses
				intNumOfAddresses = randomNumber(1,2);
				
				for (int intCounterK = 0; intCounterK < intNumOfAddresses; intCounterK++)
				{
					intStreetNameIndex = randomNumber(0,streetNames.size() - 1);
					intCityIndex = randomNumber(0,cities.size() - 1);
					intStreetTypeIndex = randomNumber(0,streetTypes.size() - 1);
					intAddressLine2 = randomNumber(0,5);
					intStreetNumber = randomNumber(100,99999);
					strAddress = "";
					
//					if (intCounterK == 0)
//					{
//						strAddress = strFirstName + " " + strLastName;
//					}
//					else
//					{
//						strAddress = "";
//						
//						for (int intCounterL = 0; intCounterL < (strFirstName + " " + strLastName).length(); intCounterL++)
//						{
//							strAddress += " ";
//						}
//					}
					
					strAddress += Integer.toString(intStreetNumber);
					strAddress += " " + streetNames.get(intStreetNameIndex);
					strAddress += " " + streetTypes.get(intStreetTypeIndex);
					
					if (intAddressLine2 == 4)
					{
						strAddress += " Apt " + Integer.toString(randomNumber(0,9999));
					}
					else if (intAddressLine2 == 5)
					{
						strAddress += " Ste " + Integer.toString(randomNumber(0,9999));
					}
					
					strAddress += "|" + cities.get(intCityIndex).getCity();
					strAddress += "|" + cities.get(intCityIndex).getState();
					strAddress += "|" + cities.get(intCityIndex).getZip();
					strAddress += "|" + cities.get(intCityIndex).getCountry();
					
					System.out.println(strFirstName + "|" + strLastName + "|" + strEmail + "|" + strPassword + "|" + strAddress);
					//write address to file
					fwOut.flush();
					//fwOut.write(strFirstName + "|" + strLastName + "|" + strEmail + "|" + strPassword + "\r\n");
					fwOut.write(strFirstName + "|" + strLastName + "|" + strEmail + "|" + strPassword + "|" + strAddress + "\r\n");
				}
			}
			fwOut.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static Customer splitCustomer(String inLine)
	{
		String[] cData = inLine.split("\\|");
		Customer localCustomer = new Customer(cData[0],cData[1],cData[2],cData[3]);
		
		return localCustomer;
	}

	private static Address splitAddress(String inLine)
	{
		String[] cData = inLine.split("\\|");
		Address localAddress = new Address();
		City localCity = new City();
		localAddress.setLine1(cData[4]);
		localCity.setCity(cData[5]);
		localCity.setState(cData[6]);
		localCity.setZip(cData[7]);
		localCity.setCountry(cData[8]);
		localAddress.setcity(localCity);
		
		return localAddress;
	}
	
	private static int getCustomerIDFromOracle(Connection oConn, String email)
	{
		PreparedStatement stmt;
		ResultSet rset;
		int intCustomerID = 0;

		String strSQL = "SELECT customer_id FROM customers ";
		strSQL += "WHERE customer_email = ? ";
		
		try {
		    stmt = oConn.prepareStatement(strSQL);
		    stmt.setString(1, email);
		    rset = stmt.executeQuery();

		    if (rset.next()) {
				//customer found
				intCustomerID = Integer.parseInt(rset.getString(1));
			}
		    
		    if (rset != null) {
		    	rset.close();
		    }
		    
		    stmt.close();
		} catch (SQLException se) {
			System.out.println(se.toString());
		}
		
		return intCustomerID;
	}
	
	private static void writeDataToOracle(Customer custData, Address addressData, Connection oConn)
	{
		PreparedStatement stmt;
		int intCustomerID;
		int intAddressCount = 0;
		String strSQL = "INSERT INTO customers (customer_email,customer_password,customer_first_name,customer_last_name) ";
	    strSQL += "VALUES(?,?,?,?) ";
		
		//get customer search keys (firstname, lastname)
		String firstname = custData.getFirstName();
		String lastname = custData.getLastName();
		String email = custData.getEmail();

		intCustomerID = getCustomerIDFromOracle(oConn, email);

		if (intCustomerID == 0) {
			//INSERT customer data into customers table
			try {
				stmt = oConn.prepareStatement(strSQL);
				stmt.setString(1, email);
				stmt.setString(2, custData.getPassword());
				stmt.setString(3, firstname);
				stmt.setString(4, lastname);
				
//				java.util.Date utilDate = new java.util.Date();
//			    //java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
//			    java.sql.Timestamp sqlDate = new java.sql.Timestamp(utilDate.getTime());
//				
//				//stmt.setDate(5, sqlDate);
//				stmt.setTimestamp(5, sqlDate);
				stmt.execute();
				stmt.close();
				oConn.commit();
			} catch (SQLException se) {
				System.out.println(se.toString());
			}
			
			//get new customerID for later
			intCustomerID = getCustomerIDFromOracle(oConn, email);
		} else {
			//customer exists, get address count
			strSQL = "SELECT COUNT(*) FROM Address WHERE customer_id = ?";
			PreparedStatement stmtAddCount;
			ResultSet rset;
			
			try {
				stmtAddCount = oConn.prepareStatement(strSQL);
				stmtAddCount.setInt(1,intCustomerID);
				rset = stmtAddCount.executeQuery();
				//stmtAddCount.execute();
				//oConn.commit();
				
				if (rset.next()) {
					intAddressCount = Integer.parseInt(rset.getString(1));
					//got address count from the database, now increment to get the next count
					intAddressCount += 1;
				}
				
				if (rset != null) {
					rset.close();
				}
				stmtAddCount.close();
				
			} catch (SQLException se) {
				System.out.println(se.toString());
				intAddressCount = 0;
			}
			
		}
		
		//add address to customer
    	City tempCity = new City();
    	tempCity = addressData.getCity();

		strSQL = "INSERT INTO Address (address_id,customer_id,address_type,address_line1,address_line2,address_city,address_state_province,address_country,address_postal_code) ";
		strSQL += "VALUES (?,?,?,?,?,?,?,?,?) ";
		
		PreparedStatement stmtAddress;
		
		try {
			stmtAddress = oConn.prepareStatement(strSQL);
			stmtAddress.setInt(1, intAddressCount);
			stmtAddress.setInt(2, intCustomerID);
			stmtAddress.setString(3, addressData.getType());
			stmtAddress.setString(4, addressData.getLine1());
			stmtAddress.setString(5, addressData.getLine2());
			stmtAddress.setString(6, tempCity.getCity());
			stmtAddress.setString(7, tempCity.getState());
			stmtAddress.setString(8, tempCity.getCountry());
			stmtAddress.setString(9, tempCity.getZip());
//			
//			java.util.Date utilDate = new java.util.Date();
//		    //java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
//		    java.sql.Timestamp sqlDate = new java.sql.Timestamp(utilDate.getTime());
//			
//			//stmt.setDate(5, sqlDate);
//		    stmtAddress.setTimestamp(10, sqlDate);
//			
			stmtAddress.execute();
			stmtAddress.close();
			oConn.commit();
			
		} catch (SQLException se) {
			System.out.println(se.toString());
		}
		
	}
	
	private static ArrayList<DBObject> deserializeAddressList(String strBSON) {
//		[ { "type" :  null  , 
//			"line1" : "Paul Friend 92422 40th Court" , 
//			"line2" :  null  , 
//			"name" : "Salina" , 
//			"state" : "UT" , 
//			"postal" : "84654" , 
//			"country" : "USA"},
//		  {
//			"type" : "shipping" ,
//			"line1" : "2738 N. 31st Place" ,
//			"name" : "Sheboygan" ,
//			"state" : "WI" ,
//			"postal" : "53083" ,
//			"country" : "USA"}
//		]
		String key;
		String value;
		ArrayList<DBObject> addressList = new ArrayList<DBObject>();
		DBObject dsAddress = new BasicDBObject();
		
		//get rid of brackets
		strBSON = strBSON.replace("[", "");
		strBSON = strBSON.replace("]", "");
		//split array entries on closing brace and comma combo
		String[] strBSONItems = strBSON.split("},");
		
		for (String address : strBSONItems) {
			dsAddress = new BasicDBObject();
			//get rid of any remaining braces
			address = address.replace("{", "");
			address = address.replace("}", "");
			//split key/value pairs on comma 
			String[] strBSONKVPairs = address.split(",");
			for (String kvPair : strBSONKVPairs) {
				//get rid of quotes
				kvPair = kvPair.replace("\"", "");
				String[] kvArray = kvPair.split(":");
				
				key = kvArray[0].trim();
				value = kvArray[1].trim();

				dsAddress.put(key, value);
			}
			addressList.add(dsAddress);
		}
		
		return addressList;
	}
	
	private static void writeDataToMongoDB(Customer custData, Address addressData, DBCollection customerCollection)
	{
		//boolean blnNewCustomer = false;
		
		ArrayList<DBObject> addressList = new ArrayList<DBObject>();
		
		//get customer search keys (firstname, lastname)
		String firstname = custData.getFirstName();
		String lastname = custData.getLastName();
		String email = custData.getEmail();
		
		DBObject findCustomer = new BasicDBObject();
		findCustomer.put("email", email);
		
        //check for existing customer
        DBObject newCustomer = customerCollection.findOne(findCustomer);
        DBObject newAddress = new BasicDBObject();
    	City tempCity = new City();
    	tempCity = addressData.getCity();        
        
        //System.out.println(newCustomer);
        if (newCustomer == null) {
        	//INSERT	
        	newCustomer = new BasicDBObject();
        	newCustomer.put("firstname", firstname);
        	newCustomer.put("lastname", lastname);
        	newCustomer.put("email", email);
        	newCustomer.put("password", custData.getPassword());
        	//newCustomer.put("creationtime", new Date());
        	//TODO - get db to do the timestamp legwork
        	//newCustomer.put("databasetime", "`db.serverStatus().localTime)`");
        	//newCustomer.put("databasetime", new BasicDBObject("databasetime", "db.serverStatus().localTime)"));

        	if (addressData.getType() != null) {
        		newAddress.put("type", addressData.getType());
        	}
        	
        	newAddress.put("line1", addressData.getLine1());
        	
        	if (addressData.getLine2() != null) {
        		newAddress.put("line2", addressData.getLine2());
        	}
        	
        	newAddress.put("city", tempCity.getCity());
        	newAddress.put("state", tempCity.getState());			
        	newAddress.put("postal", tempCity.getZip());
        	newAddress.put("country", tempCity.getCountry());
        	
        	addressList.add(newAddress);
        	newCustomer.put("address", addressList);
        	
        	customerCollection.insert(newCustomer);
        } else {
        	//UPDATE
        	String c_id = newCustomer.get("_id").toString();  	
        	
        	if (addressData.getType() != null) {
        		newAddress.put("type", addressData.getType());
        	}
        	
        	newAddress.put("line1", addressData.getLine1());
        	
        	if (addressData.getLine2() != null) {
        		newAddress.put("line2", addressData.getLine2());
        	}
        	
        	newAddress.put("name", tempCity.getCity());
        	newAddress.put("state", tempCity.getState());
        	newAddress.put("postal", tempCity.getZip());
        	newAddress.put("country", tempCity.getCountry());
        	
        	BasicDBObject custMod = new BasicDBObject();
        	custMod.put("$addToSet", new BasicDBObject("address", newAddress));
        	customerCollection.update(findCustomer, custMod, true, false);
        }
	}
	
	private static void writeDataToCassandra() {
		
	}

	private static void loadCustomerData()
	{
		try
		{
			//define local Oracle connection
		    // Create a OracleDataSource instance and set properties
		    OracleDataSource ods = new OracleDataSource();
		    //ods.setUser("system");
		    //ods.setPassword("green123");
//                       jdbc:oracle:thin:[USER/PASSWORD]@//[HOST][:PORT]/SERVICE
//		    String connect_string = "jdbc:oracle:thin:system/Sandan68@//localhost:1521/xe";
			String connect_string = "jdbc:oracle:thin:system/green123@//192.168.0.105:1521/EcommDB";
			ods.setURL(connect_string);
			Connection oConn = ods.getConnection();
			
			//define local MongoDB connection
//			Mongo mConn = new Mongo("localhost",27017);
			Mongo mConn = new Mongo("192.168.0.105",27017);
			DB mDB = mConn.getDB("ecommerceDB");
//			if (!mDB.authenticate(username, passwd)) {
//				throw Exception("Mongo authentication failure.");
//			}
			//String strTimeTest = mDB.command("db.serverStatus().localTime").toString();
			DBCollection mCustCollection = mDB.getCollection("customers");
			
			//locate customer data file
			FileInputStream inFile = new FileInputStream("/home/aploetz/programming/java/thesis_data/generated/customerData.txt");
			//FileInputStream inFile = new FileInputStream("C:\\Documents and Settings\\e04199\\My Documents\\school\\mct624\\thesis_workspace\\DataTools\\customerData.txt");
			DataInputStream in = new DataInputStream(inFile);
		    BufferedReader rdr = new BufferedReader(new InputStreamReader(in));
		    
			String strLine = "";
			Customer tempCustomer;
			//Customer lastCustomer;
			Address tempAddress;
			
			while ((strLine = rdr.readLine()) != null)
			{
				tempCustomer = splitCustomer(strLine);
				tempAddress = splitAddress(strLine);
				
//				writeDataToOracle(tempCustomer,tempAddress,oConn);
				writeDataToMongoDB(tempCustomer,tempAddress,mCustCollection);
				
				//lastCustomer = tempCustomer;
				tempAddress = null;
				tempCustomer = null;
			}
			
			oConn.close();
			mConn.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	private static HashMap<Integer,Integer> genPriceBreaks() {
		HashMap<Integer,Integer> localPrices = new HashMap<Integer,Integer>();
		
    	//generate price breaks
    	// prices range between $4.99 and $99.99
    	// prices will be stored as an integer of cents.  ex: $4.99 = 499
    	// each product will have between 1 and 3 breaks
    	// the price for each break will be 10% less than the prior price
    	int intNumberOfBreaks = randomNumber(1,3);
    	int intOrigPrice = 0;
    	int intPrice = 0;
    	int intQty = 0;
    	
    	for (int intCounterK = 0; intCounterK < intNumberOfBreaks; intCounterK++)
    	{
    		if (intCounterK == 0)
    		{
        		intOrigPrice = randomNumber(499,9999);
    			intPrice = intOrigPrice;
    			intQty = 1;
    		}
    		else
    		{
    			intPrice = (int)((double)intOrigPrice * (1 - (.1 * (double)intCounterK)));
    			intQty = randomNumber(intQty+1,intQty+9);
    		}
    		
    		localPrices.put(intQty, intPrice);
    	}
		
		return localPrices;
	}
	
	private static void writeProductDataToOracle(Product tempProduct,HashMap<Integer,Integer> prices,Connection oConn) {
		String sku = tempProduct.getSku();
		//INSERT product data into products table
		PreparedStatement prodStmt;
		String strDesc1 = tempProduct.getDesc();
		
		if (strDesc1.length() > 3999) {
			strDesc1 = strDesc1.substring(0,3999);
			//String strDesc2 = tempProduct.getDesc().substring(3999);
		}
		String strSQL = "INSERT INTO products (sku,product_name,product_description,product_bullet) ";
	    strSQL += "VALUES (?,?,?,?)";

		try {
			prodStmt = oConn.prepareStatement(strSQL);
			prodStmt.setString(1, sku);
			prodStmt.setString(2, tempProduct.getName());
			prodStmt.setString(3, strDesc1);
			//prodStmt.setString(3, tempProduct.getDesc());
//			prodStmt.setString(4, strDesc2);
			prodStmt.setString(4, tempProduct.getBullet());
			
//			java.util.Date utilDate = new java.util.Date();
//		    //java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
//		    java.sql.Timestamp sqlDate = new java.sql.Timestamp(utilDate.getTime());
//			
//		    prodStmt.setTimestamp(6, sqlDate);
//		    //prodStmt.setDate(6, sqlDate);
		    prodStmt.execute();
		    prodStmt.close();
			oConn.commit();
			
		} catch (SQLException se) {
			System.out.println(se.toString());
		}
		
		//INSERT product-to-hierarchy relationship data
		PreparedStatement prodToHierStmt;
		strSQL = "INSERT INTO ProductToHier (sku,hierarchy_id) VALUES(?,?) ";
		
		try {
			prodToHierStmt = oConn.prepareStatement(strSQL);
			prodToHierStmt.setString(1, sku);
			prodToHierStmt.setString(2, tempProduct.getCategory_id());
			prodToHierStmt.execute();
			prodToHierStmt.close();
			oConn.commit();
		} catch (SQLException se) {
			System.out.println(se.toString());
		}
		
		//INSERT price break data
		Integer localQty = 0;
		Integer localPrice = 0;
		
		for(Integer entry : prices.keySet())
		{		
			localQty = entry;
			localPrice = prices.get(entry);
			
			PreparedStatement priceStmt;
			strSQL = "INSERT INTO Pricing (sku,price_qty,price) ";
			strSQL += "VALUES (?,?,?) ";
			
			try {
				priceStmt = oConn.prepareStatement(strSQL);
				priceStmt.setString(1, sku);
				priceStmt.setInt(2, localQty);
				priceStmt.setInt(3, localPrice);
				
//				java.util.Date utilDate = new java.util.Date();
//			    //java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
//			    java.sql.Timestamp sqlDate = new java.sql.Timestamp(utilDate.getTime());
//				
//			    priceStmt.setTimestamp(4, sqlDate);
//			    //priceStmt.setDate(4, sqlDate);
			    priceStmt.execute();
			    priceStmt.close();
				oConn.commit();
				
			} catch (SQLException se) {
				System.out.println(se.toString());
			}
		}
	}
	
	private static void writeProductDataToMongoDB(Product tempProduct,HashMap<Integer,Integer> prices,DBCollection mProdCollection) {
		ArrayList<DBObject> priceList = new ArrayList<DBObject>();
		
		DBObject newProduct = new BasicDBObject();
		DBObject newPrice = new BasicDBObject();
		
    	//INSERT	
		newProduct = new BasicDBObject();
		newProduct.put("sku", tempProduct.getSku());
		newProduct.put("product_name", tempProduct.getName());
		newProduct.put("product_description", tempProduct.getDesc());
		newProduct.put("product_bullet", tempProduct.getBullet());
		newProduct.put("category_id", tempProduct.getCategory_id());
		newProduct.put("category_name", tempProduct.getCategory_name());
		newProduct.put("hierarchy", tempProduct.getHierarchy_name());
//		newProduct.put("softwaretime", new Date());
//    	//newProduct.put("databasetime",mongoConn.)

		Integer localQty = 0;
		Integer localPrice = 0;
		
		for(Integer entry : prices.keySet())
		{		
			localQty = entry;
			localPrice = prices.get(entry);
		
			newPrice.put("qty", localQty);
			newPrice.put("price", localPrice);
	    	
	    	priceList.add(newPrice);
		}

		newProduct.put("pricing", priceList);
    	
    	mProdCollection.insert(newProduct);
	}
	
	private static void writeProductDataToCassandra(Product tempProduct,HashMap<Integer,Integer> prices,Keyspace keyspace) {
		StringBuilder strProduct = new StringBuilder();
		String delimiter = "|";
		strProduct.append(tempProduct.getSku());
		strProduct.append(delimiter);
		strProduct.append(tempProduct.getBullet());
		strProduct.append(delimiter);
		strProduct.append(tempProduct.getCategory_name());
		strProduct.append(delimiter);
		strProduct.append(tempProduct.getDesc());
		strProduct.append(delimiter);
		strProduct.append(tempProduct.getHierarchy_name());
		strProduct.append(delimiter);
		strProduct.append(tempProduct.getName());
		
		CassandraDAO dao = new CassandraDAO(keyspace, "PRODUCT_INFO");
		dao.insert(tempProduct.getSku(), strProduct.toString());
	}
	
	private static Keyspace getCassandraConnection(Cluster cCluster, String cluster, String port) {
		// initialize the cluster
		Keyspace returnVal;
		String keyspace = "PRODUCTS";

		// first check if the key space exists
		KeyspaceDefinition keyspaceDetail = cCluster.describeKeyspace(keyspace);

//		// if not, create one
//		if (keyspaceDetail == null) {
//			CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator(port);
//			ThriftCluster cassandraCluster = new ThriftCluster(cluster, cassandraHostConfigurator);
//
//			ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspace, "PRODUCT_INFO");
//			cassandraCluster.addKeyspace(new ThriftKsDef(keyspace, "org.apache.cassandra.locator.SimpleStrategy", 1,
//					Arrays.asList(cfDef)));
//		} else {
//			// even if the key space exists, we need to check if the column family exists
//			List<ColumnFamilyDefinition> columnFamilyDefinitions = keyspaceDetail.getCfDefs();
//			for (ColumnFamilyDefinition def : columnFamilyDefinitions) {
//				String columnFamilyName = def.getName();
//				if (columnFamilyName.equals(cfName))
//					cfExists = true;
//			}
//		}
//
//		// create the column families required, if they do not exist yet
//		if (!cfExists) {
//			// add column family
//			ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspace, cfName);
//			cCluster.addColumnFamily(cfDef);
//		}

		// now we are sure, that the required key-space and column family exists.
		// Thus we can get the reference to be used in the DAO
		returnVal = HFactory.createKeyspace(keyspace, cCluster);
		return returnVal;
	}

	//post experiment function...
	//spins through all categories in categories collection
	//  locates all SKUs in product collection with that category
	//  updates categories collection with list of SKUs
	private static void loadCategoryData() {
		try {
		Mongo mConn = new Mongo("192.168.0.106",27017);
		DB mDB = mConn.getDB("ecommerceDB");
		DBCollection mProductsCollection = mDB.getCollection("products");
		DBCollection mCategoryCollection = mDB.getCollection("categories");
		
		//get all categories
		List<String> categoryList = mCategoryCollection.distinct("category");
		//iterate through them all
		for (String categoryName : categoryList) {
			//find category object first
			
			DBObject q2 = new BasicDBObject();
			q2.put("category", categoryName);
			DBObject newCategory = mCategoryCollection.findOne(q2);
			
			if (newCategory != null) {
				//get list of SKUs for each category
			    DBObject query3 = new BasicDBObject();
				query3.put("category_name", categoryName);
				List<String> productList = mProductsCollection.distinct("sku",query3);
				List<DBObject> categoryProductList = new ArrayList<DBObject>();
				
				for (String strSKU : productList) {
					DBObject newLineItem = new BasicDBObject();
					newLineItem.put("sku", strSKU);
					categoryProductList.add(newLineItem);
				}
				
//				DBObject o = new BasicDBObject();
//				o.put("skuList", categoryProductList);
				newCategory.put("skuList", categoryProductList);
				newCategory.put("numSKUs", productList.size());
				
				//mCategoryCollection.update(newCategory, o);
				mCategoryCollection.save(newCategory);
			}
		}
		
		} catch (Exception ex) {
			System.out.println(ex.toString());
		}
	}
	
	private static void loadProductData()
	{
		try
		{
			int intProductCount = 0;
			//define local Oracle connection
		    // Create a OracleDataSource instance and set properties
		    OracleDataSource ods = new OracleDataSource();
		    //ods.setUser("system");
		    //ods.setPassword("green123");
//		    String connect_string = "jdbc:oracle:thin:system/Sandan68@//localhost:1521/xe";
		    String connect_string = "jdbc:oracle:thin:system/green123@//192.168.0.105:1521/EcommDB";
		    ods.setURL(connect_string);
			Connection oConn = ods.getConnection();
			
			//define local MongoDB connection
//			Mongo mConn = new Mongo("localhost",27017);
			Mongo mConn = new Mongo("192.168.0.105",27017);
			DB mDB = mConn.getDB("ecommerceDB");
			DBCollection mProdCollection = mDB.getCollection("products");
			
//			//define local Cassandra connection
//			String cClusterName = "Test Cluster";
//			String cPort = "localhost:9160";
//			Cluster cCluster = HFactory.getOrCreateCluster(cClusterName, cPort);
//			Keyspace cKeyspace = getCassandraConnection(cCluster, cClusterName, cPort);
			
			//locate customer data file
			FileInputStream inFile = new FileInputStream("/home/aploetz/programming/java/thesis_data/generated/SKUData.txt");
			//FileInputStream inFile = new FileInputStream("C:\\Documents and Settings\\e04199\\My Documents\\school\\mct624\\thesis_workspace\\DataTools\\skuData.txt");
			DataInputStream in = new DataInputStream(inFile);
		    BufferedReader rdr = new BufferedReader(new InputStreamReader(in));
		    
			String strLine = "";
			String[] strProdArray;
			Product tempProduct;
			HashMap<Integer,Integer> prices;
		
			while ((strLine = rdr.readLine()) != null)
			{
				strProdArray = strLine.split("\\|");
				tempProduct = new Product(strProdArray[0],strProdArray[1],strProdArray[2],strProdArray[3],strProdArray[4],strProdArray[5],strProdArray[6]);
				
				prices = genPriceBreaks(); 
				
//				writeProductDataToOracle(tempProduct,prices,oConn);
				writeProductDataToMongoDB(tempProduct,prices,mProdCollection);
//				writeProductDataToCassandra(tempProduct,prices,cKeyspace);
				
				tempProduct = null;
				intProductCount++;
				
				if (intProductCount % 100 == 0) {
					System.out.println(intProductCount + " products");
				}
			}
			
			oConn.close();
//			mConn.close();
			// shutdown the client connection manager
//			cCluster.getConnectionManager().shutdown();
//			cKeyspace = null;
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args)
	{
		boolean blnGenerateCustomerData = false;
		boolean blnGenerateProductData = false;
		boolean blnLoadCustomerData = false;
		boolean blnLoadProductData = false;
		boolean blnLoadCategoryData = true;
		
		//generate data
		
		if (blnGenerateCustomerData)
		{
			generateCustomersWAddresses(1500);
		}
		
		if (blnGenerateProductData)
		{
			generateProductData(5000);
		}
		
		//load data into database
		
		if (blnLoadCustomerData)
		{
			loadCustomerData();
		}

		if (blnLoadProductData)
		{
			loadProductData();
		}
		
		if (blnLoadCategoryData)
		{
			loadCategoryData();
		}
		
	}
}
