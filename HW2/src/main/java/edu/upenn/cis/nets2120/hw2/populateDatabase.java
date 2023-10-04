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

public class populateDatabase {

    

    public static void main(String[] args) {
        DynamoDB db = DynamoConnector.getConnection(Config.DYNAMODB_URL);

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
				// stringArr[4] = jsonObject.get("short_description").getAsString();
				// stringArr[5] = jsonObject.get("date").getAsString();
				// Item item = new Item()
				// 		.withPrimaryKey("link", stringArr[0])
				// 		.withString("authors", stringArr[1])
				// 		.withString("category", stringArr[2])
				// 		.withString("headline", stringArr[3])
				// 		.withString("short-description", stringArr[4])
				// 		.withString("date", stringArr[5]);
				Item category_to_head = new Item()
                        .withPrimaryKey("interest", stringArr[2])
						.withNumber("inxid", inxid)
                        .withString("link", stringArr[0]);
				System.out.println(stringArr[3]);
                interest_to_articles.add(category_to_head);
				// items.add(item);
				list.add(stringArr);
				TableWriteItems writer;
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
                    interest_to_articles = new ArrayList<>();
                }

				// if (items.size() == 25) {
				// 	try {
						
				// 		writer = new TableWriteItems("news").withItemsToPut(items);
				// 		BatchWriteItemOutcome outcome = db.batchWriteItem(writer);
						
				// 		// check of unprocessed items
				// 		do {
				// 			Map<String, List<WriteRequest>> unprocessed = outcome.getUnprocessedItems();
				// 			if (outcome.getUnprocessedItems().size() != 0) {
				// 				outcome = db.batchWriteItemUnprocessed(unprocessed);
				// 			}
				// 		}
				// 		while (outcome.getUnprocessedItems().size() > 0);
						
				// 	}
				// 	catch (Exception e) {
				// 		e.printStackTrace(System.err);
				// 	}
				// 	items = new ArrayList<>();
				// }
				inxid++;
				line = reader.readLine();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		TableWriteItems writer;
        // add remaining items
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

        // try {
						
        //     writer = new TableWriteItems("news").withItemsToPut(items);
        //     BatchWriteItemOutcome outcome = db.batchWriteItem(writer);
            
        //     // check of unprocessed items
        //     do {
        //         Map<String, List<WriteRequest>> unprocessed = outcome.getUnprocessedItems();
        //         if (outcome.getUnprocessedItems().size() != 0) {
        //             outcome = db.batchWriteItemUnprocessed(unprocessed);
        //         }
        //     }
        //     while (outcome.getUnprocessedItems().size() > 0);
            
        // }
        // catch (Exception e) {
        //     e.printStackTrace(System.err);
        // }

    }
}
