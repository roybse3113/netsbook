// main function
package edu.upenn.cis.nets2120.hw2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import edu.upenn.cis.nets2120.config.Config;
import edu.upenn.cis.nets2120.storage.DynamoConnector;
import opennlp.tools.stemmer.PorterStemmer;
import opennlp.tools.stemmer.Stemmer;
import opennlp.tools.tokenize.SimpleTokenizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

import opennlp.tools.stemmer.PorterStemmer;
import opennlp.tools.stemmer.Stemmer;
import opennlp.tools.tokenize.SimpleTokenizer;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

public class test {


    public static void main(String[] args) throws IOException {
        String url = "https://penn-cis545-files.s3.amazonaws.com/News_Category_Dataset_v2.json";
        // use buffered reader to read from url
        BufferedReader br = new BufferedReader(new InputStreamReader(new URL(url).openStream()));
        // read contents while there are more lines left
        String line = br.readLine();
        while (line != null) {
            line = br.readLine();
            // use GSON to turn line into jsonObject 
            // then use jsonObject to get the category

            JsonObject jsonObject = new JsonParser().parse(line).getAsJsonObject();
            String category = jsonObject.get("category").getAsString();
            System.out.println(category);

        }


    }

    private boolean isValid(char c) {
		return ((c >= 65 && c <= 90) || (c >= 97 && c <= 122));
	}

    public void getKeyWords() throws IOException {
        String url = "https://penn-cis545-files.s3.amazonaws.com/News_Category_Dataset_v2.json";
        // use buffered reader to read from url
        BufferedReader br = new BufferedReader(new InputStreamReader(new URL(url).openStream()));
        SimpleTokenizer model = SimpleTokenizer.INSTANCE;
        PorterStemmer stemmer = new PorterStemmer();
        DynamoDB db = DynamoConnector.getConnection(Config.DYNAMODB_URL);;

        Set<String> seenWords = new HashSet<>();
		Set<String> stopWords = new HashSet<>(Arrays.asList("a", "all", "any", "but", "the"));
		Set<String> tokenWords = new HashSet<>
		(Arrays.asList("title", "speaker_1", "all_speakers", "occupations", 
				"about_speakers", "topics", "description", "transcript", "related_talks"));
		TableWriteItems writer;
		List<Item> list = new ArrayList<>();

        // read contents while there are more lines left
        String line = br.readLine();
        while (line != null) {
            line = br.readLine();
            // use GSON to turn line into jsonObject 
            // then use jsonObject to get the category

            JsonObject jsonObject = new JsonParser().parse(line).getAsJsonObject();
            String shortDes = jsonObject.get("short-description").getAsString();
            String link = jsonObject.get("link").getAsString();
            String headline = jsonObject.get("headline").getAsString();

            String[] words = model.tokenize(shortDes);

				for (String word : words) {
					word = word.toLowerCase();
					if (stopWords.contains(word)) {
						continue;
					}
					// check if it has a-z and A-Z

					boolean validWord = true;
					for (int j = 0; j < word.length(); j++) {
						if (!isValid(word.charAt(j))) {
							validWord = false; 
						}
					}
					if (!validWord) {
						continue;
					}
					
					// stem words
					CharSequence cs = stemmer.stem(word);
					word = cs.toString();
					
					// check that the word hasn't been seen in the curren talkID
					if (seenWords.contains(word)) {
						continue;
					} 
					
					seenWords.add(word);

					Item item = new Item()
							.withPrimaryKey("keyword", word)
							.withString("url", url)
							.withString("headline", headline);
							
					list.add(item);
					
					// once list of words reaches 25, batch write items
					if (list.size() == 25) {
						try {
							
							writer = new TableWriteItems("keyword_by_articles").withItemsToPut(list);
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
						list = new ArrayList<>();
					}
				} 

        }
    }
}