package www.aaronstechcenter.com.thesis;

public class TLog {
	java.util.Date begin;
	java.util.Date end;
	long lngBegin;
	long lngEnd;
	String queryString;
	
	public TLog() {
		queryString = "";
	}
	
	public TLog(long begin_, long end_, String query_) {
		lngBegin = begin_;
		lngEnd = end_;
		queryString = query_;
	}
	
	public TLog(java.util.Date begin_, java.util.Date end_, String query_) {
		begin = begin_;
		end = end_;
		queryString = query_;
	}
	
	public java.util.Date getBegin() {
		return begin;
	}
	
	public void setBegin(java.util.Date value_) {
		begin = value_;
	}
	
	public java.util.Date getEnd() {
		return end;
	}
	
	public void setEnd(java.util.Date value_) {
		end = value_;
	}
	
	public String getQueryString() {
		return queryString;
	}
	
	public void setQueryString(String value_) {
		queryString = value_;
	}
	
	public long getBeginLong() {
		return lngBegin;
	}
	
	public void setBeginLong(long value_) {
		lngBegin = value_;
	}
	
	public long getEndLong() {
		return lngEnd;
	}
	
	public void setEndLong(long value_) {
		lngEnd = value_;
	}
}