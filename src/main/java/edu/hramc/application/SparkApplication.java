package edu.hramc.application;

import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import edu.hramc.helper.Constants;
import edu.hramc.spark.eh.reader.SparkEventHubReader;
import edu.hramc.spark.sf.writer.SnowflakeJDBCSink;

public class SparkApplication {

	public static void main(String[] args) throws StreamingQueryException {
		// TODO Auto-generated method stub
		
		// Create a local spark session
		SparkSession spark = SparkSession
                .builder()
                .appName("Spark Event Hub Snwoflake Example")
                .master("local")
                .getOrCreate();
		
		// create a Event hub Spark Streaming
		Dataset<Row> incomingStream = SparkEventHubReader.getEventHubReader(spark);
		
		// it will print the schema of the message 
		incomingStream.printSchema();
		
		// Read the eventhub in the below format
		Dataset<Row> messages = incomingStream
					.withColumn("Offset", incomingStream.col("offset"))
					.withColumn("Time (readable)", incomingStream.col("enqueuedTime"))
					.withColumn("Timestamp", incomingStream.col("enqueuedTime"))
					.withColumn("Body", incomingStream.col("body").cast("String"))
					.select("Offset", "Time (readable)", "Timestamp", "Body");

		// Print the revised schema
		messages.printSchema();
		
		
		// Read the data from the eventhub and inserts in to snowflake writer
		messages.writeStream().format(Constants.SNOWFLAKE_SPARK_FORMAT)
				.trigger(Trigger.ProcessingTime(3, TimeUnit.MINUTES))
				.foreach(new SnowflakeJDBCSink())
		.start().awaitTermination();
		
		spark.stop();
		spark.close();	

	}

}
