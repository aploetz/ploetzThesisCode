package www.aaronstechcenter.com.thesis;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.regex.Pattern;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.BasicDBList;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.DBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import oracle.jdbc.pool.OracleDataSource;

import org.joda.time.DateTime;

public class ThesisFramework {
	
	private static Connection getOracleConnection() throws SQLException {
	    OracleDataSource ods = new OracleDataSource();
//	    ods.setUser("system");
//	    ods.setPassword("Sandan68");
	    //                       jdbc:oracle:thin:[USER/PASSWORD]@//[HOST][:PORT]/SERVICE
		//String connect_string = "jdbc:oracle:thin:hr/hr@//localhost:1521/xe";
	    String connect_string = "jdbc:oracle:thin:system/green123@//192.168.0.105:1521/EcommDB";
		ods.setURL(connect_string);
		return ods.getConnection();
	}

	private static Mongo getMongoDBConnection() throws Exception {
		//define local MongoDB connection
//		Mongo mConn = new Mongo("localhost",27017);
		Mongo mConn = new Mongo("192.168.0.106",27017);
		return mConn;
	}

	private static ArrayList<String> loadCustomersFromOracle() {
		ArrayList<String> localCustomers = new ArrayList<String>();
		//get data from Oracle
		try {
			Connection conn = getOracleConnection();
			String strSQL = "SELECT customer_id FROM customers ";
			PreparedStatement stmt = conn.prepareStatement(strSQL);
			ResultSet rset = stmt.executeQuery();
	
		    while (rset.next()) {
				//customer found
				localCustomers.add(rset.getString("customer_id"));
			}
		    
		    rset.close();
		    stmt.close();
			conn.close();
			
		} catch (SQLException ex) {
			System.out.println(ex);
		}
		
		return localCustomers;
	}

	
	private static ArrayList<String> loadCustomersFromFile(String strPath_) {
		ArrayList<String> localCustomers = new ArrayList<String>();

		localCustomers = getNames(strPath_);
		
		return localCustomers;
	}
	
	private static ArrayList<String> loadProductsFromOracle() {
		ArrayList<String> localProducts = new ArrayList<String>();
		//get data from Oracle
		try {
			Connection conn = getOracleConnection();
			String strSQL = "SELECT sku FROM products ";
			PreparedStatement stmt = conn.prepareStatement(strSQL);
			ResultSet rset = stmt.executeQuery();
	
		    while (rset.next()) {
				//customer found
		    	localProducts.add(rset.getString("sku"));
			}
		    
		    rset.close();
		    stmt.close();
			conn.close();
			
		} catch (SQLException ex) {
			System.out.println(ex);
		}
		
		return localProducts;
	}
	
	private static ArrayList<String> loadProductsFromFile() {
		ArrayList<String> localProducts = new ArrayList<String>();

		localProducts = getNames("/home/aploetz/workspace/DataTools/SKUsOnly.txt");
		
		return localProducts;
	}	
	
    private static int randomNumber(int min, int max)
    {
        if (min >= max) {
            return max;
        } else {
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

    	if (intNumLetters < 1) {
    		intNumLetters = 1;
    	}

    	//generate each character of the password
    	for (int intCounterJ = 0; intCounterJ < intPasswordLen; intCounterJ++) {
    		blnGood = false;
    	    intLetterASCII = 0;

    	        while (!blnGood) {
    	        	//max ASCII char range of 33 to 126
    	        	intLetterASCII = randomNumber(33, 126);

    	                if (intLetterASCII > 47 && intLetterASCII < 58 && intNumCounter < intNumericChars) { 
    	                	//numbers
    	                	intNumCounter++;
    	                    blnGood = true;
    	                } else if (intLetterASCII > 64 && intLetterASCII < 91 && intLetterCounter < intNumLetters) { 
    	                	//capital letters
    	                    intLetterCounter++;
    	                    blnGood = true;
    	                } else if (intLetterASCII > 96 && intLetterASCII < 123 && intLetterCounter < intNumLetters) { 
    	                	//lowercase letters
    	                    intLetterCounter++;
    	                    blnGood = true;
    	                } else if (intLetterASCII != 124 && intSpecCounter < intSpecialChars) { 
    	                	//special characters
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
    
    private static String getShipMethod() {
    	Integer intShipDays = randomNumber(0,2);
    	int intShipType = randomNumber(0,3);
    	String method;
    	
    	switch(intShipType) {
    		case 0:
    			method = "USPS-" + intShipDays.toString();
    			break;
    		case 1:
    			method = "UPS-" + intShipDays.toString();
    			break;
    		case 2:
    			method = "FEDX-" + intShipDays.toString();
    			break;
    		default:
    			method = "FEDX-" + intShipDays.toString();
    			break;
    	}
    	
    	return method;
    }
    
    private static int getShipCharge(String strShipMethod,int intOrderTotal) {
    	int factor = 1;
    	
    	if (strShipMethod.indexOf("-1") > -1) {
    		factor = 3;
    	} else if (strShipMethod.indexOf("-2") > -1) {
    		factor = 2;
    	}
    	
    	//take 10% of the total
    	int charge = (int)(intOrderTotal * .1);
    	charge = charge * factor;
    	
    	return charge;
    }
    
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
	
	private static void simluateOracleNavigations(int reps_, ArrayList<String> products_, ArrayList<TLog> log_) {
		List<String> h1ID = new ArrayList<String>();
		
		try {
			//top level hierarchy nav
			Connection conn = getOracleConnection();
			
			String strSQL = "SELECT h.hierarchy_id FROM hierarchy h ";
			strSQL += "INNER JOIN hierarchy h2 ON h.hierarchy_id = h2.parent_id ";
			strSQL += "WHERE h.parent_id = '0' ";
			strSQL += "AND (SELECT COUNT(*) FROM producttohier p2h WHERE p2h.hierarchy_id = h2.hierarchy_id) > 0 ";
			PreparedStatement stmt = conn.prepareStatement(strSQL);
			//set beginTime
			long beginDate = System.nanoTime();
			//
			ResultSet rset = stmt.executeQuery();
			//
		    long endDate = System.nanoTime();
		    TLog localLog = new TLog(beginDate, endDate, strSQL);
		    log_.add(localLog);

		    //h1ID list is going to be the same every time, so build it outside of the loop
		    while (rset.next()) {
		    	//process hierarchy results
		    	h1ID.add(rset.getString(1));
		    }
		    rset.close();
		    stmt.close();
			    
			for (int intCounterJ = 0; intCounterJ < reps_; intCounterJ++) {
				//initialize/destroy all lists here, so that they don't get exponentially big
				List<String> h2ID = new ArrayList<String>();
				List<String> SKUList = new ArrayList<String>();
				String strSKU;

				//randomly pick one navigation point to follow
			    int intH1Nav = randomNumber(0,h1ID.size() - 1);
			    
			    //2nd level hierarchy nav
			    strSQL = "SELECT h.hierarchy_id FROM hierarchy h WHERE h.parent_id = ? ";
			    strSQL += "AND (SELECT COUNT(*) FROM producttohier p2h WHERE p2h.hierarchy_id = h.hierarchy_id) > 0 ";
			    PreparedStatement stmt2 = conn.prepareStatement(strSQL);
			    stmt2.setString(1,h1ID.get(intH1Nav));
			    //set beginTime
				beginDate = System.nanoTime();
				//
				rset = stmt2.executeQuery();
				//
			    endDate = System.nanoTime();
			    localLog = new TLog(beginDate, endDate, strSQL);
			    log_.add(localLog);
	
			    while (rset.next()) {
			    	//process hierarchy results
			    	h2ID.add(rset.getString(1));
			    }
			    rset.close();
			    stmt2.close();
			    
			    //randomly pick one navigation point to follow
			    int intH2Nav = randomNumber(0,h2ID.size() - 1);
			    
			    //SKU level nav
			    strSQL = "SELECT SKU FROM producttohier p WHERE p.hierarchy_id = ? ";
			    PreparedStatement stmt3 = conn.prepareStatement(strSQL);
			    stmt3.setString(1,h2ID.get(intH2Nav));
			    //set beginTime
				beginDate = System.nanoTime();
				//
				rset = stmt3.executeQuery();
				//
			    endDate = System.nanoTime();
			    localLog = new TLog(beginDate, endDate, strSQL);
			    log_.add(localLog);
			    
			    while (rset.next()) {
			    	//process SKU nav results
			    	SKUList.add(rset.getString(1));
			    }
			    rset.close();
			    stmt3.close();
			    
			    //randomly pick one SKU to follow
			    int intSKUNav = randomNumber(0,SKUList.size() - 1);
			    
			    //SKU data query
			    strSQL = "SELECT * FROM products WHERE SKU = ? ";
			    PreparedStatement stmt4 = conn.prepareStatement(strSQL);
			    stmt4.setString(1,SKUList.get(intSKUNav));
			    //set beginTime
				beginDate = System.nanoTime();
				//
				rset = stmt4.executeQuery();
				//
			    endDate = System.nanoTime();
			    localLog = new TLog(beginDate, endDate, strSQL);
			    log_.add(localLog);	
			    //testing only
//			    while (rset.next()) {
//			    	//process SKU nav results
//				    System.out.println(rset.getString(2));
//			    	//should only process one result
//			    }
			    
			    rset.close();
			    stmt4.close();
			}
		    
		    //all done, close connection
			conn.close();
			
		} catch (SQLException ex) {
			System.out.println(ex);
		}
	}
	
	private static void simulateOracleOrders(int reps_, ArrayList<String> customers_, ArrayList<String> products_, ArrayList<TLog> log_) {
		try {
			//top level hierarchy nav
			Connection conn = getOracleConnection();

			for (int intCounterJ = 0; intCounterJ < reps_; intCounterJ++) {
				//gen order number
				Long lngOrderNumber = System.currentTimeMillis();
				String strOrderNumber = lngOrderNumber.toString();
				//get random customer
				int intCustomerIndex = randomNumber(0,customers_.size() - 1);
				String strCustomerID = customers_.get(intCustomerIndex);
				//get ship method
				String strShipMethod = getShipMethod();
				//get random number of product lines
				int intNumberOfProducts = randomNumber(1,5);
				List<String> productsToBuy = new ArrayList<String>(intNumberOfProducts);
				int intOrderTotal = 0;
				
				for (int intCounterK = 0; intCounterK < intNumberOfProducts; intCounterK++) {
					//get random product number
					int intProductIndex = randomNumber(0,products_.size() - 1);
					String strSKU = products_.get(intProductIndex);
					//add to list
					productsToBuy.add(strSKU);
					
					//gen product qty
					int intQty = randomNumber(1,12);
					
				    //insert individual line items
				    String strLineSQL = "INSERT INTO orderitems(order_id, line_item_id, sku, qty) VALUES (?,?,?,?) ";
				    PreparedStatement orderItemStmt = conn.prepareStatement(strLineSQL);
				    orderItemStmt.setString(1, strOrderNumber);
				    orderItemStmt.setInt(2, intCounterK + 1);
				    orderItemStmt.setString(3, strSKU);
				    orderItemStmt.setInt(4, intQty);
				    
				    long beginDate = System.nanoTime();
				    orderItemStmt.executeUpdate();
					long endDate = System.nanoTime();
					TLog localLog = new TLog(beginDate, endDate, strLineSQL);
				    log_.add(localLog);
				    
				    orderItemStmt.close();

					//lookup product price
					String strPriceSQL = "SELECT price FROM pricing WHERE sku = ? AND price_qty <= ? AND ROWNUM = 1 ORDER BY price_qty DESC ";
					PreparedStatement priceStmt = conn.prepareStatement(strPriceSQL);
					priceStmt.setString(1,strSKU);
					priceStmt.setInt(2, intQty);
					//add product price to total
					beginDate = System.nanoTime();
					ResultSet rset = priceStmt.executeQuery();
					endDate = System.nanoTime();
					localLog = new TLog(beginDate, endDate, strPriceSQL);
				    log_.add(localLog);
				    
					int intPrice = 0;
					while (rset.next() && intPrice == 0) {
						intPrice = Integer.parseInt(rset.getString(1));
					}
					rset.close();
					intOrderTotal += (intPrice * intQty);
					
					priceStmt.close();
				}
				
				//compute shipping charge
				int intShipCharge = getShipCharge(strShipMethod,intOrderTotal);
				
				//tax rate is 5%
				int intTaxCharge = (int)(intOrderTotal * .05);
				
				java.sql.Date sqlDate = new java.sql.Date(lngOrderNumber);
				
				String strSQL = "INSERT INTO orders(order_id, customer_id, order_date, order_ship_method, order_ship_charge, order_tax_charge, order_total) VALUES(?,?,?,?,?,?,?) ";
				PreparedStatement stmt1 = conn.prepareStatement(strSQL);
				stmt1.setString(1, strOrderNumber);
				stmt1.setString(2, strCustomerID);
				stmt1.setDate(3, sqlDate);
				stmt1.setString(4, strShipMethod);
				stmt1.setInt(5, intShipCharge);
				stmt1.setInt(6, intTaxCharge);
				stmt1.setInt(7, intOrderTotal);

				//insert record into orders table
				long beginDate = System.nanoTime();
				stmt1.executeUpdate();
				long endDate = System.nanoTime();
			    TLog localLog = new TLog(beginDate, endDate, strSQL);
			    log_.add(localLog);
			    
			    stmt1.close();
			}
		    //all done, close connection
			conn.close();
			
		} catch (SQLException ex) {
			System.out.println(ex);
		}
	}

	private static void simulateOracleCustomerUpdates(ArrayList<String> customers_, ArrayList<TLog> log_) {
		try {
			Connection conn = getOracleConnection();
			String strSQL = "UPDATE customers " +
				"SET customer_password = ? " +
				"WHERE customer_email = ? ";
	
			for (String email : customers_) {
				try {
					String strPassword = genPassword();
					PreparedStatement stmt = conn.prepareStatement(strSQL);
					stmt.setString(1, strPassword);
					
					//java.sql.Timestamp sqlDate = new java.sql.Timestamp(utilDate.getTime());
					//org.joda.time.DateTime dateTime = new org.joda.time.DateTime();
					//java.sql.Timestamp sqlDate = new java.sql.Timestamp(dateTime.getMillis());
					//long timeInMillis = System.currentTimeMillis();
					//long timeInNanos = System.nanoTime();
					//java.sql.Timestamp sqlDate = new java.sql.Timestamp(timeInNanos);
					//sqlDate.setNanos((int) (timeInNanos % 1000000000));
					
					//stmt.setTimestamp(2, sqlDate);
					stmt.setString(2, email);
					//java.util.Date beginDate = new java.util.Date();
					//long beginDate = new org.joda.time.DateTime().getMillis();
					long beginDate = System.nanoTime();
					stmt.executeUpdate();
					conn.commit();
				    stmt.close();
				    //add transaction to log
				    //java.util.Date endDate = new java.util.Date();
				    //long endDate = new org.joda.time.DateTime().getMillis();
				    long endDate = System.nanoTime();
				    TLog localLog = new TLog(beginDate, endDate, strSQL);
				    log_.add(localLog);
				} catch (SQLException ex) {
					System.out.println(ex);
				}
			}
		    //all done, close connection
			conn.close();
		} catch (SQLException ex) {
			System.out.println(ex);
		}
	}
	
	private static void simulateOracleProductSearch(int reps_, ArrayList<String> keywords_, ArrayList<String> companies_, ArrayList<String> products_, ArrayList<TLog> log_) {
		try {
			Connection conn = getOracleConnection();
			String strSQL = "SELECT * " +
				"FROM products " +
				"WHERE regexp_like(product_description, ? ,'i') ";
	
			List<String> searchKeywords = new ArrayList<String>();
			
			for (String company : companies_) {
				searchKeywords.add(company);
			}
			
			for (String keyword : keywords_) {
				searchKeywords.add(keyword);
			}
	
			//search for #reps product records from Oracle	
			for (int intCounterJ = 0; intCounterJ < reps_; intCounterJ++) {
				int intKeywordIndex = randomNumber(0,searchKeywords.size() - 1);
				String strKeyword = searchKeywords.get(intKeywordIndex);
	
				//return all Oracle records with the keyword in their copy
				String match = "^.*" + strKeyword + ".*$";

				PreparedStatement stmt = conn.prepareStatement(strSQL);
				stmt.setString(1, match);

				//set beginTime
				long beginDate = System.nanoTime();
				//execute search
				ResultSet rset = stmt.executeQuery();
				//set endDate
				long endDate = System.nanoTime();
				int resultCount = 0;
				
//				while (rset.next()) {
//					//testing only
//					//System.out.println(rset.getString("sku") + " - " + rset.getString("product_description"));
//					//keeping this to do a count without calling the database again
//					resultCount++;
//				}
				rset.close();
				stmt.close();

				TLog localLog = new TLog(beginDate, endDate, "SELECT * FROM products WHERE regexp_like(product_description, '^.*" + strKeyword + ".*$') - returned " + resultCount + " results");
			    log_.add(localLog);
			    
				if (intCounterJ % 100 == 0) {
					System.out.println(intCounterJ + " products");
				}
			}
			conn.close();
		} catch (SQLException ex) {
			System.out.println(ex);
		}

	}
	
	private static void simulateOraclePriceChange(int reps_, ArrayList<String> products_, ArrayList<TLog> log_) {
		//get random #reps product records from Oracle
		try {
			Connection conn = getOracleConnection();
	
			//update #reps product prices in Mongo
			for (int intCounterJ = 0; intCounterJ < reps_; intCounterJ++) {
				int intProductIndex = randomNumber(0,products_.size() - 1);
				String strProductID = products_.get(intProductIndex);
	
				//get price record
				String strSQL = "SELECT * FROM pricing WHERE sku = ? AND price_qty = 1 ";
				PreparedStatement stmt = conn.prepareStatement(strSQL);
				stmt.setString(1, strProductID);
				
				//set beginTime
				long beginDate = System.nanoTime();
				//get product
				ResultSet rset = stmt.executeQuery();
				//set endDate
				long endDate = System.nanoTime();
			    TLog localLog = new TLog(beginDate, endDate, "SELECT * FROM pricing WHERE sku = " + strProductID + " AND price_qty = 1 ");
			    log_.add(localLog);
				
				//get qty 1 price from pricing
				
			    
				Integer localPrice = 0;
				Integer localQty = 1;
				
				while (rset.next() && localPrice == 0) {
					localPrice = rset.getInt("price"); 
				}

				stmt.close();
				rset.close();

				//decrement price by 15%
				double dblLocalPrice = (double)localPrice * (.85);
				Integer intLocalPrice = (int)dblLocalPrice;
				
				//update pricelist on the product
				String updSQL = "UPDATE pricing SET price = ? WHERE sku = ? AND price_qty = 1 ";
				PreparedStatement updStmt = conn.prepareStatement(updSQL);
				updStmt.setInt(1, intLocalPrice);
				updStmt.setString(2, strProductID);
				
				//set beginTime
				beginDate = System.nanoTime();
				//update product
				updStmt.executeUpdate();
				//set endDate
				endDate = System.nanoTime();
				localLog = new TLog(beginDate, endDate, "UPDATE pricing SET price = " + intLocalPrice + " WHERE sku = " + strProductID + " AND price_qty = 1 ");
			    log_.add(localLog);
			    updStmt.close();
			    
				if (intCounterJ % 100 == 0) {
					System.out.println(intCounterJ + " products");
				}
			}
			
			conn.close();
		} catch (Exception ex) {
			System.out.println(ex);
		}
	}
	
	private static void simluateMongoNavigations(int reps_, ArrayList<String> products_, ArrayList<TLog> log_) {
		try {
			Mongo mConn = getMongoDBConnection();
			DB mDB = mConn.getDB("ecommerceDB");
			DBCollection productCollection = mDB.getCollection("products");

			for (int intCounterJ = 0; intCounterJ < reps_; intCounterJ++) {
				//set beginTime
				long beginDate = System.nanoTime();
				//String strSQL = "SELECT hierarchy_id FROM hierarchy h WHERE h.parent_id = '0' ";
				List<String> h1ID = productCollection.distinct("hierarchy");
				//set endDate
				long endDate = System.nanoTime();
			    TLog localLog1 = new TLog(beginDate, endDate, "db.categories.distinct(\"hierarchy\")");
			    log_.add(localLog1);
			    
			    //randomly pick one hierarchy point to follow
			    int intH1Nav = randomNumber(0,h1ID.size() - 1);
			    
			    DBObject query2 = new BasicDBObject();
				query2.put("hierarchy", h1ID.get(intH1Nav));
				
				//set beginTime
				beginDate = System.nanoTime();
				//strSQL = "SELECT category_id FROM hierarchy h WHERE h.hierarchy_id = ? ";
				//DBCursor hierarchyCursor = productCollection.find(query);
				List<String> h2ID = productCollection.distinct("category_id", query2);
				//set endDate
				endDate = System.nanoTime();
			    TLog localLog2 = new TLog(beginDate, endDate, "db.products.distinct(\"category_id\",{hierarchy_id : \"" + h1ID.get(intH1Nav) + "\"})");
			    log_.add(localLog2);
			    
			    //randomly pick one category to follow
			    int intH2Nav = randomNumber(0,h2ID.size() - 1);
	
			    DBObject query3 = new BasicDBObject();
				query3.put("category_id", h2ID.get(intH2Nav));
	
				//set beginTime
				beginDate = System.nanoTime();
			    //strSQL = "SELECT SKU FROM producttohier p WHERE category_id = ? ";
				//DBCursor skuListCursor = productCollection.find(query3);
				List<String> skuList = productCollection.distinct("sku",query3);
				//set endDate
				endDate = System.nanoTime();
			    TLog localLog3 = new TLog(beginDate, endDate, "db.products.find({category_id : \"" + h2ID.get(intH2Nav) + "\"},{sku : 1})");
			    log_.add(localLog3);
	
			    //too slow.  Found a better, faster way to get the distinct SKUs into an ArrayList
			    //List<String> skuList = new ArrayList<String>();
//			    while (skuListCursor.hasNext()) {
//			    	skuList.add(skuListCursor.next().get("sku").toString());
//			    }
			    
			    //randomly pick one SKU to follow
			    int intSKUNav = randomNumber(0,skuList.size() - 1);
	
			    DBObject query4 = new BasicDBObject();
				query4.put("sku", skuList.get(intSKUNav));
			    
				//set beginTime
				beginDate = System.nanoTime();
				//strSQL = "SELECT * FROM products WHERE SKU = ? ";
				DBObject skuData = productCollection.findOne(query4);
				//set endDate
				endDate = System.nanoTime();
			    TLog localLog4 = new TLog(beginDate, endDate, "db.products.findOne({sku : \"" + skuList.get(intSKUNav) + "\"})");
			    log_.add(localLog4);
			    
				if (intCounterJ % 100 == 0) {
					System.out.println(intCounterJ + " products");
				}
			}
			
			mConn.close();
		} catch (Exception ex) {
			System.out.println(ex);
		}
	}
	
	private static void simluateMongoNavigations2(int reps_, ArrayList<String> products_, ArrayList<TLog> log_) {
		try {
			Mongo mConn = getMongoDBConnection();
			DB mDB = mConn.getDB("ecommerceDB");
			DBCollection productCollection = mDB.getCollection("products");
			DBCollection categoryCollection = mDB.getCollection("categories");

			for (int intCounterJ = 0; intCounterJ < reps_; intCounterJ++) {
				//ensure that we're only following hierarchies with products in them
				DBObject q1 = new BasicDBObject();
				DBObject q12 = new BasicDBObject();
				q12.put("$gt", 0);
				q1.put("numSKUs", q12);

				//set beginTime
				long beginDate = System.nanoTime();
				//String strSQL = "SELECT hierarchy_id FROM hierarchy h WHERE h.parent_id = '0' ";
				List<String> h1ID = categoryCollection.distinct("hierarchy",q1);
				//set endDate
				long endDate = System.nanoTime();
			    TLog localLog1 = new TLog(beginDate, endDate, "db.categories.distinct(\"hierarchy\")");
			    log_.add(localLog1);
			    
			    //randomly pick one hierarchy point to follow
			    int intH1Nav = randomNumber(0,h1ID.size() - 1);
			    
			    DBObject query2 = new BasicDBObject();
				query2.put("hierarchy", h1ID.get(intH1Nav));
				query2.put("numSKUs", q12);
				
				//set beginTime
				beginDate = System.nanoTime();
				//strSQL = "SELECT category_id FROM hierarchy h WHERE h.hierarchy_id = ? ";
				//DBCursor hierarchyCursor = productCollection.find(query);
				List<String> h2ID = categoryCollection.distinct("category", query2);
				//set endDate
				endDate = System.nanoTime();
			    TLog localLog2 = new TLog(beginDate, endDate, "db.categories.distinct(\"category\",{hierarchy : \"" + h1ID.get(intH1Nav) + "\"})");
			    log_.add(localLog2);
			    
			    //randomly pick one category to follow
			    int intH2Nav = randomNumber(0,h2ID.size() - 1);
	
			    DBObject query3 = new BasicDBObject();
				query3.put("category", h2ID.get(intH2Nav));
	
				//set beginTime
				beginDate = System.nanoTime();
			    //strSQL = "SELECT SKU FROM producttohier p WHERE category_id = ? ";
				//DBCursor skuListCursor = productCollection.find(query3);
				//List<String> skuList = productCollection.distinct("sku",query3);
				DBCursor category = categoryCollection.find(query3);
				//set endDate
				endDate = System.nanoTime();
			    TLog localLog3 = new TLog(beginDate, endDate, "db.categories.find({category : \"" + h2ID.get(intH2Nav) + "\"},{sku : 1})");
			    log_.add(localLog3);
	
			    List<String> skuList = new ArrayList<String>();
			    //should only be one entry
			    while (category.hasNext()) {
					BasicDBList productList = (BasicDBList) category.next().get("skuList");
					
					for (int intCounterL = 0; intCounterL < productList.size(); intCounterL++) {
						DBObject skuFromCategory = (DBObject)productList.get(intCounterL);

						skuList.add(skuFromCategory.toString());
					}
			    }
			    
			    //randomly pick one SKU to follow
			    int intSKUNav = randomNumber(0,skuList.size() - 1);
	
			    DBObject query4 = new BasicDBObject();
				query4.put("sku", skuList.get(intSKUNav));
			    
				//set beginTime
				beginDate = System.nanoTime();
				//strSQL = "SELECT * FROM products WHERE SKU = ? ";
				DBObject skuData = productCollection.findOne(query4);
				//set endDate
				endDate = System.nanoTime();
			    TLog localLog4 = new TLog(beginDate, endDate, "db.products.findOne({sku : \"" + skuList.get(intSKUNav) + "\"})");
			    log_.add(localLog4);
			    
				if (intCounterJ % 100 == 0) {
					System.out.println(intCounterJ + " products");
				}
			}
			
			mConn.close();
		} catch (Exception ex) {
			System.out.println(ex);
		}
	}

	
	private static void simulateMongoOrders(int reps_, ArrayList<String> customerEmails_, ArrayList<String> products_, ArrayList<TLog> log_) {
		long beginDate = 0;
		long endDate = 0;
		
		try {
			//get random #reps customer records from Mongo
			Mongo mConn = getMongoDBConnection();
			DB mDB = mConn.getDB("ecommerceDB");
			DBCollection orderCollection = mDB.getCollection("orders");
			DBCollection productCollection = mDB.getCollection("products");

			for (int intCounterJ = 0; intCounterJ < reps_; intCounterJ++) {
				DBObject newOrder = new BasicDBObject();
				ArrayList<DBObject> itemList = new ArrayList<DBObject>();
				
				//gen order number
				Long lngOrderNumber = System.currentTimeMillis();
				String strOrderNumber = lngOrderNumber.toString();
				//gen order date
				java.sql.Date sqlDate = new java.sql.Date(lngOrderNumber);
				//get random customer
				int intCustomerIndex = randomNumber(0,customerEmails_.size() - 1);
				String strCustomerEmail = customerEmails_.get(intCustomerIndex);
				//get ship method
				String strShipMethod = getShipMethod();
				//get random number of product lines
				int intNumberOfProducts = randomNumber(1,5);
				List<String> productsToBuy = new ArrayList<String>(intNumberOfProducts);
				int intOrderTotal = 0;
				
				for (int intCounterK = 0; intCounterK < intNumberOfProducts; intCounterK++) {
					DBObject newLineItem = new BasicDBObject();
					
					//get random product number
					int intProductIndex = randomNumber(0,products_.size() - 1);
					String strSKU = products_.get(intProductIndex);
					//add to list
					productsToBuy.add(strSKU);
					
					//gen product qty
					int intQty = randomNumber(1,12);
					
					//line number
					int intLineItemNumber = intCounterK + 1;

					//lookup product price
					int intPrice = 0;
					DBObject priceQuery = new BasicDBObject();
					priceQuery.put("sku", strSKU);
					priceQuery.put("pricing.qty", new BasicDBObject("$lte", intQty));
					
					//sort returned prices in descending order
					DBObject priceSort = new BasicDBObject();
					//priceSort.put("qty", "-1");
					
					beginDate = System.nanoTime();
					//DBCursor priceListCursor = productCollection.find(priceQuery).sort(priceSort);
					DBCursor priceListCursor = productCollection.find(priceQuery);
					endDate = System.nanoTime();
					TLog localLog = new TLog(beginDate, endDate, "db.products.find({'sku' : \"" + strSKU + "\", 'pricing.qty': {$lte: " + intQty + "}},{'pricing' : 1}).sort({'qty': -1})");
					log_.add(localLog);
					
					while (priceListCursor.hasNext() && intPrice == 0) {
						DBObject price = priceListCursor.next();
						BasicDBList priceList = (BasicDBList) price.get("pricing");

						String strPrice = "";
						for (int intCounterL = 0; intCounterL < priceList.size(); intCounterL++) {
							DBObject priceBreak = (DBObject)priceList.get(intCounterL);
							strPrice = priceBreak.get("price").toString();
							intCounterL = priceList.size();
						}
						intPrice = Integer.parseInt(strPrice);
					}
					
					//set properties on line item
					newLineItem.put("line_item_id", intLineItemNumber);
					newLineItem.put("sku", strSKU);
					newLineItem.put("item_price", intPrice);
					newLineItem.put("qty", intQty);
					itemList.add(newLineItem);

					//add product price to total
					intOrderTotal += (intPrice * intQty);
				}

				//compute shipping charge
				int intShipCharge = getShipCharge(strShipMethod,intOrderTotal);
				
				//tax rate is 5%
				int intTaxCharge = (int)(intOrderTotal * .05);

				//set properties on order object
				newOrder.put("order_id", strOrderNumber);
				newOrder.put("customer_email", strCustomerEmail);
				newOrder.put("order_date", sqlDate.toString());
				newOrder.put("order_ship_method", strShipMethod);
				newOrder.put("order_ship_charge", intShipCharge);
				newOrder.put("order_tax_charge", intTaxCharge);
				newOrder.put("order_total", intOrderTotal);
				newOrder.put("order_items", itemList);
				
				//insert record into Mongo
				beginDate = System.nanoTime();
				orderCollection.insert(newOrder);
				endDate = System.nanoTime();
				TLog localLog = new TLog(beginDate, endDate, "db.orders.insert(order_id: \"" + strOrderNumber + " \"");
			    log_.add(localLog);
			    
				if (intCounterJ % 100 == 0) {
					System.out.println(intCounterJ + " products");
				}
			}
			mConn.close();
		} catch (Exception ex) {
			System.out.println(ex);
		}			
	}

	private static void simulateMongoCustomerUpdates(ArrayList<String> customerEmails_, ArrayList<TLog> log_) {
		try {
			Mongo mConn = getMongoDBConnection();
			DB mDB = mConn.getDB("ecommerceDB");
			//mDB.command("db.setProfilingLevel(2);");
			//truncate the profiler 
			//06/10/2012 - don't mess with the profiler
			DBCollection customerCollection = mDB.getCollection("customers");
			
			for (String email : customerEmails_) {
				//generate the email
				String strPassword = genPassword();
				
				//find the customer
//				DBObject findCustomer = new BasicDBObject();
//				findCustomer.put("email", email);
//				DBObject foundCustomer = customerCollection.findOne(findCustomer);
				
//				if (foundCustomer != null) {
					//set the field (password) to be updated
					BasicDBObject custMod = new BasicDBObject();
					custMod.put("email", email);
					custMod.put("password", strPassword);
					
					//set beginTime
					long beginDate = System.nanoTime();
	
					//update the customer
					customerCollection.update(new BasicDBObject().append("email", email), custMod);					
					//customerCollection.update(foundCustomer, custMod, false, false);
					
					//set endDate
					long endDate = System.nanoTime();
				    TLog localLog = new TLog(beginDate, endDate, "db.customers.update({email: \"" + email + "\"},{password: \"" + strPassword + "\"})");
				    log_.add(localLog);
//				}
				//customerCollection(custMod);
			}
		    //all done, disable profiler and close connection
			//mDB.command("db.setProfilingLevel(0);");
			//06/10/2012 - don't mess with the profiler
			mConn.close();
		} catch (Exception ex) {
			System.out.println(ex);
		}
	}
	
	private static void simulateMongoProductSearch(int reps_, ArrayList<String> keywords_, ArrayList<String> companies_, ArrayList<String> products_, ArrayList<TLog> log_) {
		try {
			Mongo mConn = getMongoDBConnection();
			DB mDB = mConn.getDB("ecommerceDB");
			DBCollection productCollection = mDB.getCollection("products");
			
			//combine keywords and companies into one list of keywords
			List<String> searchKeywords = new ArrayList<String>();
			
			for (String company : companies_) {
				searchKeywords.add(company);
			}
			
			for (String keyword : keywords_) {
				searchKeywords.add(keyword);
			}

			//update #reps product records from Mongo	
			for (int intCounterJ = 0; intCounterJ < reps_; intCounterJ++) {
				int intKeywordIndex = randomNumber(0,searchKeywords.size() - 1);
				String strKeyword = searchKeywords.get(intKeywordIndex);

				//return all mongo records with the keyword in their copy
				//Pattern match = Pattern.compile("^.*" + strKeyword + ".*$", Pattern.CASE_INSENSITIVE + Pattern.MULTILINE);
				Pattern match = Pattern.compile("^.*" + strKeyword + ".*$", Pattern.MULTILINE);
                BasicDBObject query = new BasicDBObject("product_description", match);
                
				//set beginTime
				long beginDate = System.nanoTime();
				//execute search
				DBCursor searchCursor = productCollection.find(query);
				//set endDate
				long endDate = System.nanoTime();
				int resultCount = 0;
				//resultCount = searchCursor.count();
				int incCount = 0;
				TLog localLog = new TLog(beginDate, endDate, "db.products.find({product_description: {$regex: \"" + strKeyword + "\", $options: \"i\"}}) - returned " + resultCount + " results");
			    log_.add(localLog);
			    
//			    //needed for testing only
//			    while (searchCursor.hasNext()) {
//			    	String desc = searchCursor.next().get("product_description").toString();
//			    	incCount++;
//			    }
//			    System.out.println(incCount);
			    
				if (intCounterJ % 10 == 0) {
					System.out.println(intCounterJ + " products");
				}
			}
			
			mConn.close();
		} catch (Exception ex) {
			System.out.println(ex);
		}
	}
	
	private static void simulateMongoPriceChange(int reps_, ArrayList<String> products_, ArrayList<TLog> log_) {
		try {
			Mongo mConn = getMongoDBConnection();
			DB mDB = mConn.getDB("ecommerceDB");
			DBCollection productCollection = mDB.getCollection("products");
	
			//update #reps product prices in Mongo
			for (int intCounterJ = 0; intCounterJ < reps_; intCounterJ++) {
				int intProductIndex = randomNumber(0,products_.size() - 1);
				String strProductID = products_.get(intProductIndex);
	
				//get product record
				DBObject prodQuery = new BasicDBObject();
				prodQuery.put("sku", strProductID);
				
				//set beginTime
				long beginDate = System.nanoTime();
				//get product
				DBObject product = productCollection.findOne(prodQuery);
				//set endDate
				long endDate = System.nanoTime();
			    TLog localLog = new TLog(beginDate, endDate, "db.products.findOne({sku: \"" + strProductID + "\"}");
			    log_.add(localLog);
				
				//get pricelist from product
				BasicDBList priceList = (BasicDBList) product.get("pricing");
				ArrayList<DBObject> localPriceList = new ArrayList<DBObject>();
				
				String localPrice = "";
				Integer localQty = 0;
				boolean blnFirst = true;
				//HashMap<Integer,Integer> localPrices = new HashMap<Integer,Integer>();
				
				for (int intCounterL = 0; intCounterL < priceList.size(); intCounterL++) {
					DBObject priceBreak = (DBObject)priceList.get(intCounterL);
					
					if (blnFirst) {
						localPrice = priceBreak.get("price").toString();
						localQty = Integer.parseInt(priceBreak.get("qty").toString());
						blnFirst = false;
					} else {
						DBObject dbPrice = new BasicDBObject();
						int lPrice = Integer.parseInt(priceBreak.get("price").toString());
						int lQty = Integer.parseInt(priceBreak.get("qty").toString());
						//localPrices.put(lQty, lPrice);
						dbPrice.put("qty", lQty);
						dbPrice.put("price", lPrice);
						localPriceList.add(dbPrice);
					}
				}
				
				//decrement price by 15%
				double dblLocalPrice = Double.parseDouble(localPrice);
				dblLocalPrice = (double)dblLocalPrice * (.85);
				Integer intLocalPrice = (int)dblLocalPrice;
				
				//update pricelist on the product
				DBObject reducedPrice = new BasicDBObject();
				reducedPrice.put("qty", localQty);
				reducedPrice.put("price", intLocalPrice);
				localPriceList.add(reducedPrice);

				//DBObject priceUpdate = new BasicDBObject();
				//priceUpdate.put("pricing", localPriceList);
				product.put("pricing", localPriceList);
	    	
				//set beginTime
				beginDate = System.nanoTime();
				//update product
				//productCollection.update(prodQuery, priceUpdate, true, false);
				productCollection.save(product);
				//set endDate
				endDate = System.nanoTime();
				localLog = new TLog(beginDate, endDate, "db.products.update({sku: \"" + strProductID + "\"},{pricing: {pricelist}}");
			    log_.add(localLog);
			    
				if (intCounterJ % 100 == 0) {
					System.out.println(intCounterJ + " products");
				}
			}
			
			mConn.close();
		} catch (Exception ex) {
			System.out.println(ex);
		}
	}
	
	private static void writeOutLog(ArrayList<TLog> log_, String type_) {
		try {
			FileWriter out = new FileWriter("/home/aploetz/thesisData/" + type_ + ".txt");
			
			for (TLog logEntry : log_) {
	        	out.flush();
	        	out.write(logEntry.getBegin() + "|" + logEntry.getEnd() + "|" + logEntry.getQueryString() + "\r\n");
			}
        	out.close();
		} catch (IOException ex) {
			System.out.println(ex.toString());
		}
	}	
	
	private static void writeOutLogLong(ArrayList<TLog> log_, String type_) {
		try {
			FileWriter out = new FileWriter("/home/aploetz/thesisData/" + type_ + ".txt");
			
			for (TLog logEntry : log_) {
	        	out.flush();
	        	out.write(logEntry.getBeginLong() + "|" + logEntry.getEndLong() + "|" + logEntry.getQueryString() + "\r\n");
			}
        	out.close();
		} catch (IOException ex) {
			System.out.println(ex.toString());
		}
	}

	public static void main(String[] args) {
		boolean blnOracle = true;
		boolean blnSimNavigation = false;
		boolean blnSimOrders = false;
		boolean blnSimCustomer = false;
		boolean blnSimSearch = true;
		boolean blnSimPricing = false;
		String strType = "";
		
		ArrayList<String> customers = loadCustomersFromFile("/home/aploetz/workspace/DataTools/CustomerIDs.txt");
		ArrayList<String> customerEmails = loadCustomersFromFile("/home/aploetz/workspace/DataTools/CustomerEmails.txt");
		ArrayList<String> products = loadProductsFromFile();
    	ArrayList<String> currentKeywords = getNames("/home/aploetz/programming/java/thesis_data/productKeywords.txt");
    	ArrayList<String> productCompanies = getNames("/home/aploetz/programming/java/thesis_data/productCompanies.txt");

		ArrayList<TLog> log = new ArrayList<TLog>();
		
		if (blnOracle) {
			if (blnSimNavigation) {
				simluateOracleNavigations(1000,products,log);
				strType = "OracleNavigation";
				writeOutLogLong(log, strType);
			}
			
			if (blnSimOrders) {
				simulateOracleOrders(10000,customers,products,log);
				strType = "OracleOrders";
				writeOutLogLong(log, strType);
			}
			
			if (blnSimCustomer) {
				simulateOracleCustomerUpdates(customers,log);
				strType = "OracleCustomerUpdates";
				writeOutLogLong(log, strType);
			}
			
			if (blnSimSearch) {
				simulateOracleProductSearch(10000,currentKeywords,productCompanies,products,log);
				strType = "OracleProductSearches";
				writeOutLogLong(log, strType);
			}
			
			if (blnSimPricing) {
				simulateOraclePriceChange(10000,products,log);
				strType = "OraclePriceUpdates";
				writeOutLogLong(log, strType);
			}
		} else {
			if (blnSimNavigation) {
				simluateMongoNavigations2(1000,products,log);
				strType = "MongoNavigations2";
				writeOutLogLong(log, strType);
			}
			
			if (blnSimOrders) {
				simulateMongoOrders(10000,customerEmails,products,log);
				strType = "MongoOrders";					
				writeOutLogLong(log, strType);
			}
			
			if (blnSimCustomer) {
				simulateMongoCustomerUpdates(customerEmails,log);
				strType = "MongoCustomerUpdates";
				writeOutLogLong(log, strType);
			}
			
			if (blnSimSearch) {
				simulateMongoProductSearch(10000,currentKeywords,productCompanies,products,log);
				strType = "MongoProductSearches";
				writeOutLogLong(log, strType);
			}
			
			if (blnSimPricing) {
				simulateMongoPriceChange(10000,products,log);
				strType = "MongoPriceUpdates";
				writeOutLogLong(log, strType);
			}
		}
		//writeOutLog(log, strType);
	}

}
