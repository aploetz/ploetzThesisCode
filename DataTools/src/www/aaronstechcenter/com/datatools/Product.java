package www.aaronstechcenter.com.datatools;

import java.util.HashMap;
import java.util.ArrayList;

public class Product {
	private String sku;
	private String name;
	private String desc;
	private String bullet;
	private String category_id;
	private String category_name;
	private String hierarchy_name;
	
	private HashMap<Integer,Integer> pricing;
	private ArrayList<String> keywords;
	
	public Product() {
		pricing = new HashMap<Integer,Integer>();
		keywords = new ArrayList<String>();
	}
	
	public Product(String _sku, String _name, String _desc, String _bullet) {
		sku = _sku;
		name = _name;
		desc = _desc;
		bullet = _bullet;
		
		pricing = new HashMap<Integer,Integer>();
		keywords = new ArrayList<String>();
	}
	
	public Product(String _sku, String _name, String _desc, String _bullet, String _category_id, String _category_name, String _hierarchy_name) {
		sku = _sku;
		name = _name;
		desc = _desc;
		bullet = _bullet;
		category_id = _category_id;
		category_name = _category_name;
		hierarchy_name = _hierarchy_name;
		
		pricing = new HashMap<Integer,Integer>();
		keywords = new ArrayList<String>();
	}
	
	public String getSku() {
		return sku;
	}
	
	public void setSKU(String value) {
		sku = value;
	}
		
	public String getName() {
		return name;
	}
	
	public void setName(String value) {
		name = value;
	}
		
	public String getDesc() {
		return desc;
	}
	
	public void setDesc(String value) {
		desc = value;
	}
	
	public String getBullet() {
		return bullet;
	}
	
	public void setBullet(String value) {
		bullet = value;
	}
	
	public String getCategory_id() {
		return category_id;
	}
	
	public void setCategory_id(String value) {
		category_id = value;
	}
	
	public String getCategory_name() {
		return category_name;
	}
	
	public void setCategory_name(String value) {
		category_name = value;
	}
	
	public String getHierarchy_name() {
		return hierarchy_name;
	}
	
	public void setHierarchy_name(String value) {
		hierarchy_name = value;
	}
	
	public HashMap<Integer,Integer> getPriceList() {
		return pricing;
	}
	
	public void addPrice(Integer qty, Integer price) {
		pricing.put(qty, price);
	}
	
	public ArrayList<String> getKeywordList() {
		return keywords;
	}
	
	public void addKeyword(String keyword) {
		keywords.add(keyword);
	}
}
