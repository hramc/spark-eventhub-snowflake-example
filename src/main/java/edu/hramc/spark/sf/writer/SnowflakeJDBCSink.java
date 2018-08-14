package edu.hramc.spark.sf.writer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.spark.sql.Row;

import edu.hramc.helper.Constants;
import edu.hramc.helper.Helper;

/**
 * 
 * This class is used to inigest the data into snowflake account
 * 
 * @author hramc
 *
 */
public class SnowflakeJDBCSink extends org.apache.spark.sql.ForeachWriter<Row>{

			// Default Serialization ID
			private static final long serialVersionUID = 1L;
			
			Connection connection = null;
			Statement statement = null;
			
			@Override
			public void close(Throwable arg0) {
				// TODO Auto-generated method stub
				try {
					connection.close();
					statement.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			@Override
			public boolean open(long paritionId, long version) {
				// TODO Auto-generated method stub
					try {
						Class.forName(Constants.SNOWFLAKE_JDBC_DRIVER_NAME);
						connection = DriverManager.getConnection(Constants.SNOWFLAKE_CONNECTION_STRING,
								Helper.getSnowFlakeCredentialsInProp());
						statement = connection.createStatement();
					} catch (ClassNotFoundException | SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				   
				return true;
			}

			@Override
			public void process(Row row) {
				// TODO Auto-generated method stub
				/*
				 * create or replace table sparkEventHubPOC (offset string, time timestamp, body variant);
				 */
				try {
					statement.executeUpdate(
							String.format(Constants.INSERT_DATA_INTO_SNOWFLAKE, 
									row.getString(0),row.getTimestamp(1).toString(),row.getString(3))
						     );
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
}


