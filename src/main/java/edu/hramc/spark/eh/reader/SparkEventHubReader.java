package edu.hramc.spark.eh.reader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.spark.eventhubs.ConnectionStringBuilder;
import org.apache.spark.eventhubs.EventHubsConf;
import org.apache.spark.eventhubs.EventPosition;
import org.apache.spark.eventhubs.NameAndPartition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.Trigger;

import edu.hramc.helper.Constants;
import scala.Tuple2;
import scala.collection.JavaConversions;

/**
 * 
 * @author hramc
 * 
 * This class is used to reader the streaming data from the event hub
 *
 */
public class SparkEventHubReader {

	
	
	/**
	 * 
	 * This listener will be used to gets metrics information about the 
	 * stream batch.
	 * 
	 * @return queryListener Instance
	 */
	public static StreamingQueryListener getStreamingQueryListener(){
		return new StreamingQueryListener() {
		    
			// We are logging the metric information in a file here.
			String fileName;
		    
            @Override
            public void onQueryStarted(QueryStartedEvent queryStarted) {
            	fileName = queryStarted.runId().toString();
                System.out.println("Query started: " + queryStarted.runId());
            }
            @Override
            public void onQueryTerminated(QueryTerminatedEvent queryTerminated) {
                System.out.println("Query terminated: " + queryTerminated.id());
            }
            @Override
            public void onQueryProgress(QueryProgressEvent queryProgress) {
            	// Please provide file path below - where to log the metrics
            	File f = new File("<File Path>"+fileName+".json");
            	
            	BufferedWriter bw = null;
        		FileWriter fw = null;
        		try {
					fw = new FileWriter(f);
					bw = new BufferedWriter(fw);
	    			bw.write(queryProgress.progress().prettyJson());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}finally{
					try {
						bw.close();
						fw.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					
				}
            }
        };
	}
	
	public static Dataset<Row> getEventHubReader(SparkSession spark) throws StreamingQueryException {
		
		// Add the listener to get Metric information about the Stream
		 spark.streams().addListener(getStreamingQueryListener());

		// Event hub configurations
		// Replace values below with yours        
		
		// Create EventHubConf instancec using all the above information. 
		EventHubsConf customEventhubParameters = new 
				EventHubsConf(new ConnectionStringBuilder(Constants.EVENTHUB_CONNECTION_STRING)
				.setEventHubName(Constants.EVENTHUB_NAME).build());
		
		// Assing the consumer drop - if you didn't provide, it will go to default consumer group
		customEventhubParameters.setConsumerGroup(Constants.CONSUMER_GROUP);
		
		/*
		 * It is one of the batching parameter, we can limit number of events to be processed 
		 * within a single stream batch
		 */
		customEventhubParameters.setMaxEventsPerTrigger(20);
		System.out.println("Name of the Event Hub>>"+customEventhubParameters.name());
		System.out.println("Log Name of the Event Hub>>"+customEventhubParameters.logName());
		
		// This is one of the bookmarking technique.
		customEventhubParameters.setStartingPosition(
				EventPosition.fromEnqueuedTime(Instant.parse(Constants.LAST_EVENTHUB_ENQUEUE_TIME)));
		
		
		// Create a Dataset to read the information from the eventhub
		Dataset<Row> incomingStream = spark.readStream().format(Constants.EVENTHUB_FORMAT)
				.options(customEventhubParameters.toMap()).load();
		
		
		return incomingStream;
	}

}
