package edu.hramc.helper;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 
 * @author hramc
 * 
 * This helper class provides all the credentials information to reader and writer the data.
 *
 */
public class Helper {
	
	
	public static Map<String,String> getSnowFlakeCredentialsInMap(){
		Map<String,String> credentials = new HashMap<String,String>();
		credentials.put("user", Constants.SNOWFLAKE_USER );
		credentials.put("password", Constants.SNOWFLAKE_PASSWORD);
		credentials.put("warehouse", Constants.SNOWFLAKE_WAREHOUSE);  
		credentials.put("db", Constants.SNOWFLAKE_DATABASE);     
		credentials.put("schema", Constants.SNOWFLAKE_SCHEMA); 
		credentials.put("Driver", Constants.SNOWFLAKE_JDBC_DRIVER_NAME);
		return credentials;
	}
	
	public static Properties getSnowFlakeCredentialsInProp(){
		Properties credentials = new Properties();
		credentials.put("user", Constants.SNOWFLAKE_USER );
		credentials.put("password", Constants.SNOWFLAKE_PASSWORD);
		credentials.put("warehouse", Constants.SNOWFLAKE_WAREHOUSE);  
		credentials.put("db", Constants.SNOWFLAKE_DATABASE);     
		credentials.put("schema", Constants.SNOWFLAKE_SCHEMA); 
		credentials.put("Driver", Constants.SNOWFLAKE_JDBC_DRIVER_NAME);
		
		return credentials;
	}
	
	

}
