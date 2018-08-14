package edu.hramc.helper;

public interface Constants {
	
	String EVENTHUB_FORMAT="eventhubs";
	
	String LAST_EVENTHUB_ENQUEUE_TIME="2018-08-13T00:00:00.000Z";
	
	String EVENTHUB_NAME = "<Event Hub Name>";
	
	String EVENTHUB_CONNECTION_STRING = "<Event Hub Connection String>";
	
	String CONSUMER_GROUP = "<consumer group>";
	
	// Get the snowfalke connection usually in the format - jdbc:snowflake://<hostname>
	String SNOWFLAKE_CONNECTION_STRING = "<Snowflake Connection String>";
	
	
	/**
	 *  Please create a table in Snowflake 
	 *  Table Structure offset Long, Enqueue Time (Timestamp), Body (Variant) and CurrentTime (Timestamp)
	 */
	
	String INSERT_DATA_INTO_SNOWFLAKE = "insert into sparkEventHubPOC "
					 +"select '%s','%s', parse_json('%s'),CURRENT_TIMESTAMP::timestamp_ntz";
	
	String SNOWFLAKE_JDBC_DRIVER_NAME="net.snowflake.client.jdbc.SnowflakeDriver";
	
	String SNOWFLAKE_SPARK_FORMAT="net.snowflake.spark.snowflake";
	
	// Snowflake credentails
	
	String SNOWFLAKE_USER="<UserName>";
	String SNOWFLAKE_PASSWORD="<Password>";
	String SNOWFLAKE_WAREHOUSE="<Warehouse>";
	String SNOWFLAKE_DATABASE="<Database>";
	String SNOWFLAKE_SCHEMA="<Schema>";

}
