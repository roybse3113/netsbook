package edu.upenn.cis.nets2120.hw2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;


import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import edu.upenn.cis.nets2120.config.Config;
import edu.upenn.cis.nets2120.storage.DynamoConnector;
import edu.upenn.cis.nets2120.storage.SparkConnector;
import scala.Tuple2;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

public class LoadNetwork {
	/**
	 * The basic logger
	 */
	static Logger logger = LogManager.getLogger(LoadNetwork.class);

	/**
	 * Connection to DynamoDB
	 */
	DynamoDB db;
	Table talks;
	
	CSVParser parser;
	
	
	/**
	 * Connection to Apache Spark
	 */
	SparkSession spark;
	
	JavaSparkContext context;
	
	/**
	 * Helper function: swap key and value in a JavaPairRDD
	 * 
	 * @author zives
	 *
	 */
	static class SwapKeyValue<T1,T2> implements PairFunction<Tuple2<T1,T2>, T2,T1> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<T2, T1> call(Tuple2<T1, T2> t) throws Exception {
			return new Tuple2<>(t._2, t._1);
		}
		
	}
	
	
	public LoadNetwork() {
		System.setProperty("file.encoding", "UTF-8");
		parser = new CSVParser();
	}
	
	private void initializeTables() throws DynamoDbException, InterruptedException {
		try {
			talks = db.createTable("news", Arrays.asList(new KeySchemaElement("link", KeyType.HASH)), // Partition
																												// key
					Arrays.asList(new AttributeDefinition("link", ScalarAttributeType.S)),
					new ProvisionedThroughput(25L, 25L)); // Stay within the free tier

			talks.waitForActive();
		} catch (final ResourceInUseException exists) {
			talks = db.getTable("news");
		}

	}
	

	/**
	 * Initialize the database connection and open the file
	 * 
	 * @throws IOException
	 * @throws InterruptedException 
	 * @throws DynamoDbException 
	 */
	public void initialize() throws IOException, DynamoDbException, InterruptedException {
		logger.info("Connecting to DynamoDB...");
		db = DynamoConnector.getConnection(Config.DYNAMODB_URL);
		
		spark = SparkConnector.getSparkConnection();
		context = SparkConnector.getSparkContext();
		
		initializeTables();
		
		logger.debug("Connected!");
	}


	
	/**
	 * Fetch the social network from the S3 path, and create a (followed, follower) edge graph
	 * 
	 * @param filePath
	 * @return JavaPairRDD: (followed: int, follower: int)
	 */
	JavaPairRDD<Integer,Integer> getSocialNetwork(String filePath) {
		// Read into RDD with lines as strings
		JavaRDD<String[]> file = context.textFile(filePath, Config.PARTITIONS)
				.map(line -> line.toString().split(" "));
		
		//map adding one per frequency
		JavaPairRDD<Integer, Integer> pairRDD = file.mapToPair(f -> 
				 new Tuple2<Integer, Integer>(
						Integer.parseInt(f[0]), 1
				));
		
		// add up the frequencies
		JavaPairRDD<Integer,Integer> count = pairRDD.reduceByKey((a,b) -> a + b);
		
		// reverse the key so we can sort by freq.
		JavaPairRDD<Integer, Integer> reversed = count.mapToPair(f -> 
			 new Tuple2<Integer, Integer>(
					f._2, f._1
			));
		
		// sort
		JavaPairRDD<Integer, Integer> sorted = reversed.sortByKey(false);
		
		//reverse again
		return sorted.mapToPair(f -> new Tuple2<Integer, Integer>(f._2, f._1));
				
	}

	/**
	 * Returns an RDD of parsed talk data
	 * 
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	JavaRDD<Row> getTalks() throws IOException {
		List<String[]> list = new ArrayList<>();
		List<Item> items = new ArrayList<>();
		List<Item> interest_to_articles = new ArrayList<>();
		
		// to add rows
		final StructType rowSchema = new StructType()
				.add("link", "string")
				.add("authors","string")
				.add("category","string")
				.add("headline", "string")
				.add("short-description", "string")
				.add("date", "string");
		
		try {
			String url = "https://penn-cis545-files.s3.amazonaws.com/News_Category_Dataset_v2.json";
			// use buffered reader to read from url
			BufferedReader reader = new BufferedReader(new InputStreamReader(new URL(url).openStream()));
			// read contents while there are more lines left
			String line = reader.readLine();
			int inxid = 0;
			while (line != null) {
				JsonObject jsonObject = new JsonParser().parse(line).getAsJsonObject();
				String[] stringArr = new String[6];
				stringArr[0] = jsonObject.get("link").getAsString();
				stringArr[1] = jsonObject.get("authors").getAsString();
				stringArr[2] = jsonObject.get("category").getAsString();
				stringArr[3] = jsonObject.get("headline").getAsString();
				stringArr[4] = jsonObject.get("short_description").getAsString();
				stringArr[5] = jsonObject.get("date").getAsString();
				Item item = new Item()
						.withPrimaryKey("link", stringArr[0])
						.withString("authors", stringArr[1])
						.withString("category", stringArr[2])
						.withString("headline", stringArr[3])
						.withString("short-description", stringArr[4])
						.withString("date", stringArr[5]);
				
				Item articleItem = new Item()
						.withPrimaryKey("category", stringArr[2])
						.withString("link", stringArr[0])
						.withNumber("inxid", inxid);
				
				interest_to_articles.add(articleItem);
				items.add(item);
				list.add(stringArr);
				TableWriteItems writer;
				if (items.size() == 25) {
					try {
						
						writer = new TableWriteItems("news").withItemsToPut(items);
						BatchWriteItemOutcome outcome = db.batchWriteItem(writer);
						
						// check of unprocessed items
						do {
							Map<String, List<WriteRequest>> unprocessed = outcome.getUnprocessedItems();
							if (outcome.getUnprocessedItems().size() != 0) {
								outcome = db.batchWriteItemUnprocessed(unprocessed);
							}
						}
						while (outcome.getUnprocessedItems().size() > 0);
						
					}
					catch (Exception e) {
						e.printStackTrace(System.err);
					}
					items = new ArrayList<>();
				}

				if (interest_to_articles.size() == 25) {
					try {
						
						writer = new TableWriteItems("interest_to_article").withItemsToPut(interest_to_articles);
						BatchWriteItemOutcome outcome = db.batchWriteItem(writer);
						
						// check of unprocessed items
						do {
							Map<String, List<WriteRequest>> unprocessed = outcome.getUnprocessedItems();
							if (outcome.getUnprocessedItems().size() != 0) {
								outcome = db.batchWriteItemUnprocessed(unprocessed);
							}
						}
						while (outcome.getUnprocessedItems().size() > 0);
						
					}
					catch (Exception e) {
						e.printStackTrace(System.err);
					}
					items = new ArrayList<>();
				}

				line = reader.readLine();
				inxid++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		// make a list of rows
		List<Row> listOfTalks = list.parallelStream()
				.map(stringList -> {
					Object[] row = new Object[6];
					if (stringList.length != 6) {
						return new GenericRowWithSchema(row, rowSchema);
					}
					
					row[0] = (stringList[0]);
					row[1] = stringList[1];
					row[2] = stringList[2];
					row[3] = stringList[3];
					row[4] = (stringList[4]);
					row[5] = (stringList[5]);
					
					
					return new GenericRowWithSchema(row, rowSchema);
					
				})
				.collect(Collectors.toList());
		
		//return rdd of rows
    	return context.parallelize(listOfTalks);

	}



	/**
	 * Graceful shutdown
	 */
	public void shutdown() {
		logger.info("Shutting down");
		
		DynamoConnector.shutdown();
		
		if (spark != null)
			spark.close();
	}
	
	public static void main(String[] args) {
		final LoadNetwork ln = new LoadNetwork();

		try {
			ln.initialize();
			ln.getTalks();
			ln.shutdown();
		} catch (final IOException ie) {
			logger.error("I/O error: ");
			ie.printStackTrace();
		} catch (final DynamoDbException e) {
			e.printStackTrace();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		} finally {
			ln.shutdown();
		}
	}

}
