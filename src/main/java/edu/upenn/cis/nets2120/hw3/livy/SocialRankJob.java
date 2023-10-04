package edu.upenn.cis.nets2120.hw3.livy;

import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.model.*;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;

import edu.upenn.cis.nets2120.config.Config;
import edu.upenn.cis.nets2120.storage.SparkConnector;

import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.livy.Job;
import org.apache.livy.JobContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;



class DynamoConnector {
	/**
	 * This is our connection
	 */
	static DynamoDB client;

	/**
	 * Singleton pattern: get the client connection if one exists, else create one
	 * 
	 * @param url
	 * @return
	 */
	public static DynamoDB getConnection(final String url) {
		if (client != null)
			return client;
		
	    	client = new DynamoDB( 
	    			AmazonDynamoDBClientBuilder.standard()
					.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
						"https://dynamodb.us-east-1.amazonaws.com", "us-east-1"))
        			.withCredentials(new DefaultAWSCredentialsProviderChain())
					.build());

    	return client;
	}
	
	/**
	 * Orderly shutdown
	 */
	public static void shutdown() {
		if (client != null) {
			client.shutdown();
			client = null;
		}
		System.out.println("Shut down DynamoDB factory");
	}
}

public class SocialRankJob implements Job<List<MyPair<Integer,Double>>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Connection to Apache Spark
	 */
	SparkSession spark;
	JavaSparkContext context;
	DynamoDB db;
	Table table;
	

	/**
	 * Initialize the database connection and open the file
	 * 
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public void initialize() throws IOException, InterruptedException {
		System.out.println("Connecting to Spark...");
		db = DynamoConnector.getConnection("https://dynamodb.us-east-1.amazonaws.com");
		spark = SparkConnector.getSparkConnection();
		context = SparkConnector.getSparkContext();
		
		System.out.println("Connected!");
	}


	private void initializeTables() throws DynamoDbException, InterruptedException {
		try {
			
			table = db.createTable("adsorption", Arrays.asList(new KeySchemaElement("url", KeyType.HASH), // Partition key
							new KeySchemaElement("username", KeyType.RANGE)), Arrays.asList(new AttributeDefinition("url", ScalarAttributeType.S),
									new AttributeDefinition("username", ScalarAttributeType.S)),
							new ProvisionedThroughput(25L, 25L));
			table.waitForActive();
		} catch (final ResourceInUseException exists) {
			table = db.getTable("adsorption");
		}

	}

	void writeToTable(JavaPairRDD<String, Tuple2<String, Double>> data) {
		Map<String, String> map = data.mapToPair(x -> {
				return new Tuple2<String, String>(x._1, x._2._1 + "X" + Double.toString(x._2._2));
				}
		).collectAsMap();

		TableWriteItems writer;
		DynamoDB docClient = DynamoConnector.getConnection("https://dynamodb.us-east-1.amazonaws.com");
		List<Item> list = new ArrayList<>();
		Set<String> used = new HashSet<>();
		
		for (String url : map.keySet()) {
			String userWeight = map.get(url);
			String[] split = userWeight.split("X");
			String user = split[0];
			
			if (used.add(user + "$" + url)) {
				Double weight = Double.parseDouble(split[1]);
				Item item = new Item().withPrimaryKey("username", user)
						.withString("url", url)
						.withNumber("weight", weight);
				list.add(item);

				if (list.size() == 25) {
					try {
					writer = new TableWriteItems("adsorption").withItemsToPut(list);
					BatchWriteItemOutcome outcome = docClient.batchWriteItem(writer);
					
					// check of unprocessed items
					do {
						Map<String, List<WriteRequest>> unprocessed = outcome.getUnprocessedItems();
						if (outcome.getUnprocessedItems().size() != 0) {
							outcome = db.batchWriteItemUnprocessed(unprocessed);
						}
					}
					while (outcome.getUnprocessedItems().size() > 0);
					} catch (Exception e) {
						e.printStackTrace(System.err);
					}
					list = new ArrayList<>();

				}
			}
			
		}
		if (list.size() > 0) {
			try {
				System.out.println("done");
				writer = new TableWriteItems("adsorption").withItemsToPut(list);
				BatchWriteItemOutcome outcome = docClient.batchWriteItem(writer);
				
				// check of unprocessed items
				do {
					Map<String, List<WriteRequest>> unprocessed = outcome.getUnprocessedItems();
					if (outcome.getUnprocessedItems().size() != 0) {
						outcome = db.batchWriteItemUnprocessed(unprocessed);
					}
				}
				while (outcome.getUnprocessedItems().size() > 0);
				} catch (Exception e) {
					e.printStackTrace(System.err);
				}
		}
	}

	/**
	 * Main functionality in the program: read and process the social network
	 * 
	 * @throws IOException File read, network, and other errors
	 * @throws InterruptedException User presses Ctrl-C
	 */
	public List<MyPair<Integer,Double>> run() throws IOException, InterruptedException {


		// For each friendship, should be (a, b) and (b, a) edges
		// scan users table, for each user, loop through friends list,
		// then create a list edges, a list of Tuple2<>
		String userTableName = "users";
	    AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient();
	    ScanRequest scanRequest = new ScanRequest()
	        .withTableName(userTableName);
		
		// can reuse users_result for (user -> friend) and (user -> interest)
	    ScanResult users_result = dynamoDBClient.scan(scanRequest);

		JavaPairRDD<String, Tuple2<String, Double>> friendEdgeWeightRDD = helper_friendEdges(users_result);

		// user->liked articles edges
	    // reuse ScanResult users_result = dynamoDBClient.scan(scanRequest);
		JavaRDD<Tuple2<String, String>> likedEdges = helper_likedEdges(users_result);
		JavaPairRDD<String, Tuple2<String, Double>> userToArticleEdgeWeightRDD = helper_userToArticle(likedEdges);
		JavaPairRDD<String, Tuple2<String, Double>> articleToUserEdgeWeightRDD = helper_likedArticleToUser(likedEdges, userToArticleEdgeWeightRDD);
	    
	    //Scan table for news: (article, category)	
		JavaPairRDD<String, String> articleCategoryRDD = helper_articleCategoryRDD(dynamoDBClient);
		JavaPairRDD<String, Tuple2<String, Double>> acEdgeWeightRDD = helper_articleCategoryEdges(articleCategoryRDD);
	    JavaPairRDD<String, Tuple2<String, Double>> caEdgeWeightRDD = helper_categoryArticleEdges(articleCategoryRDD);
	    

		// user->interests edges
	    // reuse ScanResult users_result = dynamoDBClient.scan(scanRequest);
	    List<Tuple2<String, String>> userInterestList = new ArrayList<>();
	    for (Map<String, AttributeValue> item : users_result.getItems()) {
			String currUser = item.get("username").getS();
			for (int i = 0; i < item.get("interests").getL().size(); i++) {
				userInterestList.add(new Tuple2<String, String>(currUser, item.get("interests").getL().get(i).getS()));
			}
	    }
	    JavaPairRDD<String, String> usersToInterestsRDD = context.parallelizePairs(userInterestList);
	    JavaPairRDD<String, Integer> numUsersToInterestsRDD = usersToInterestsRDD.mapValues(x->1).reduceByKey((x, y) -> (x+y));
	    JavaPairRDD<String, Double> uiPropRDD = numUsersToInterestsRDD.mapValues(i -> (double)(0.3)/i);
	    //(user, (category, scale)) edges
	    JavaPairRDD<String, Tuple2<String, Double>> userToCategoryEdgeWeightRDD = usersToInterestsRDD.join(uiPropRDD);
	    
	    JavaPairRDD<String, String> categoryToUserRDD = usersToInterestsRDD.mapToPair(x ->
	    	new Tuple2<String, String>(x._2, x._1));
	    JavaPairRDD<String, Integer> numCategoryUsersRDD = categoryToUserRDD.mapValues(x->1).reduceByKey((x,y) -> (x+y));
	    JavaPairRDD<String, Double> cuPropRDD = numCategoryUsersRDD.mapValues(i -> (double)(0.3)/i);
	    //(category, (user, weight/scale)) edges
	    JavaPairRDD<String, Tuple2<String, Double>> catToUserEdgeWeightRDD = categoryToUserRDD.join(cuPropRDD);


	    JavaPairRDD<String, Tuple2<String, Double>> edges = friendEdgeWeightRDD.union(userToArticleEdgeWeightRDD)
		.union(articleToUserEdgeWeightRDD).union(userToCategoryEdgeWeightRDD).union(catToUserEdgeWeightRDD).union(acEdgeWeightRDD).union(caEdgeWeightRDD);
	    
	    JavaPairRDD<String, Tuple2<String, Double>> userRDD = numUsersToInterestsRDD.mapToPair(
		x -> new Tuple2<String, Tuple2<String, Double>>(x._1, new Tuple2<String, Double>(x._1, 1.0)));
	    
	    JavaPairRDD<Tuple2<String, String>, Double> userHardCode = userRDD.mapToPair(x -> 
	    		new Tuple2<>(new Tuple2<>(x._1, x._1), (double)1.0));
	    
	    JavaPairRDD<String, Tuple2<String, Double>> nodeRDD = numUsersToInterestsRDD.mapToPair(
	    		x -> new Tuple2<String, Tuple2<String, Double>>(x._1, new Tuple2<String, Double>(x._1, 1.0)));
	    
	    JavaPairRDD<Tuple2<String, String>, Double> labeledNodes = nodeRDD.mapToPair(x ->
	    		new Tuple2<>(new Tuple2<>(x._1, x._2._1), x._2._2));
	    
	    for (int i = 0; i < 15; ++i) {
			System.out.println(i);
	    	JavaPairRDD<String, Tuple2<Tuple2<String, Double>, Tuple2<String, Double>>> propogate = nodeRDD.join(edges);
	    	JavaPairRDD<String, Tuple2<String, Double>> newNodes = propogate.mapToPair(x -> new Tuple2<>
	    		(x._2._2._1, new Tuple2<>(x._2._1._1,x._2._1._2 * x._2._2._2)));
	    	JavaPairRDD<String, Double> doubleWeights = newNodes.mapToPair(x -> new Tuple2<>(x._1, x._2._2));
	    	JavaPairRDD<String, Double> sumOfNodes = doubleWeights.reduceByKey((x, y) -> (x+y));
	    	JavaPairRDD<String, Tuple2<Tuple2<String, Double>, Double>> normalize = newNodes.join(sumOfNodes);
	    	newNodes = normalize.mapToPair(x -> new Tuple2<>(x._1, new Tuple2<>(x._2._1._1, x._2._1._2/x._2._2)));
	    	JavaPairRDD<Tuple2<String, String>, Double> mergeSameLabel = newNodes.mapToPair(x ->
	    			new Tuple2<>(new Tuple2<>(x._1, x._2._1),( x._2._2)));
		    
	    	mergeSameLabel = mergeSameLabel.reduceByKey((x, y)->(x+y));
	    	mergeSameLabel = mergeSameLabel.subtractByKey(userHardCode);
	    	mergeSameLabel = mergeSameLabel.union(userHardCode);
	    	
	    	JavaPairRDD<Tuple2<String, String>, Tuple2<Double, Double>> pairDiff = labeledNodes.join(mergeSameLabel);
	    	JavaRDD<Double> diff = pairDiff.map(t -> Math.abs(t._2._1-t._2._2));
	    	
	    	if (i > 0) {
	    		boolean stop = diff.filter(x -> x > 0.15).count() == 0;
		    	if (stop) {
		    		break;
		    	}
	    	}
	    	
	    	labeledNodes = mergeSameLabel;
	    	newNodes = mergeSameLabel.mapToPair(x -> new Tuple2<>(x._1._1, new Tuple2<>(x._1._2, x._2)));
	    	nodeRDD = newNodes;
	    	
		    	
	    }
	    System.out.println("Initializing tables...");
	    initializeTables();
	    System.out.println("Writing to database");

	    writeToTable(nodeRDD);
			

		return new ArrayList<MyPair<Integer, Double>>();
	}
	
	private JavaPairRDD<String, Tuple2<String, Double>> helper_friendEdges(ScanResult users_result){
		List<Tuple2<String, String>> friendEdgeList = new ArrayList<>();

		for (Map<String, AttributeValue> item : users_result.getItems()) {
			String currUser = item.get("username").getS();
			List<String> listAttr = item.get("friends").getL().stream()
					.map(AttributeValue::getS).collect(Collectors.toList());
			for (String friend : listAttr) {
				friendEdgeList.add(new Tuple2<String, String>(currUser, friend));
			}
	    }
		
	    JavaRDD<Tuple2<String, String>> friendEdges = context.parallelize(friendEdgeList);
	    JavaPairRDD<String, String> friendEdgesRDD = friendEdges.mapToPair(x -> new Tuple2<String, String>(x._1, x._2));
	    JavaPairRDD<String, Integer> friendCountRDD = friendEdgesRDD.mapValues(x->1).reduceByKey((x, y) -> (x+y));
	    JavaPairRDD<String, Double> friendTransferRDD = friendCountRDD.mapValues(i -> (double)0.3/i);
	    
	    JavaPairRDD<String, Tuple2<String, Double>> friendEdgeWeightRDD = friendEdgesRDD.join(friendTransferRDD);
		return friendEdgeWeightRDD;
	}
	// end helper_friendEdges
	
	private JavaRDD<Tuple2<String, String>> helper_likedEdges(ScanResult users_result) {
		List<Tuple2<String, String>> likesList = new ArrayList<>();
	    for (Map<String, AttributeValue> item : users_result.getItems()) {
			String currUser = item.get("username").getS();
			for (int i = 0; i < item.get("likedArticles").getL().size(); i++) {
				likesList.add(new Tuple2<String, String>(currUser, item.get("likedArticles").getL().get(i).getS()));
			}
		}
		JavaRDD<Tuple2<String, String>> likeEdges = context.parallelize(likesList);
		return likeEdges;
	}

	private JavaPairRDD<String, Tuple2<String, Double>> helper_userToArticle(JavaRDD<Tuple2<String, String>> likeEdges) {
	    JavaPairRDD<String, String> likeEdgesRDD = likeEdges.mapToPair(x -> 
	    new Tuple2<String, String>(x._1, x._2));
	    JavaPairRDD<String, Integer> numLikesRDD = likeEdgesRDD.mapValues(x->1).reduceByKey((x, y) -> (x+y));
	    JavaPairRDD<String, Double> likesPropRDD = numLikesRDD.mapValues(i -> (double)(0.4)/i);
	    // (user, (article, weight) edges
	    JavaPairRDD<String, Tuple2<String, Double>> userToArticleEdgeWeightRDD = likeEdgesRDD.join(likesPropRDD);
		return userToArticleEdgeWeightRDD;
	}

	private JavaPairRDD<String, Tuple2<String, Double>> helper_likedArticleToUser(JavaRDD<Tuple2<String, String>> likeEdges, JavaPairRDD<String, Tuple2<String, Double>> userToArticleEdgeWeightRDD) {
	    JavaPairRDD<String, String> likedEdgesRDD= likeEdges.mapToPair(x -> 
	    	new Tuple2<String, String>(x._2, x._1));
	    JavaPairRDD<String, Integer> numLikedRDD = likedEdgesRDD.mapValues(x->1).reduceByKey((x, y) -> (x+y));
	    JavaPairRDD<String, Double> likesPropogationRDD = numLikedRDD.mapValues(i -> (double)(0.4)/i);
	    // (article, (user, weight) edges
	    JavaPairRDD<String, Tuple2<String, Double>> articleToUserEdgeWeightRDD = likedEdgesRDD.join(likesPropogationRDD);
		return articleToUserEdgeWeightRDD;
	}
	// end of helper



	private JavaPairRDD<String, String> helper_articleCategoryRDD(AmazonDynamoDBClient client) {
		String interest_to_article_TableName = "interest_to_article";
	    List<Tuple2<String, String>> articleCategoryList = new ArrayList<>();
	    ScanRequest scanReq = new ScanRequest().withTableName(interest_to_article_TableName);
	    ScanResult interest_to_article_result = client.scan(scanReq);
	    for (Map<String, AttributeValue> item: interest_to_article_result.getItems()) {
			String category = item.get("interest").getS();
			String link = item.get("link").getS();
	    	articleCategoryList.add(new Tuple2<>(link, category));
	    }
	    JavaPairRDD<String, String> articleCategoryRDD = context.parallelizePairs(articleCategoryList);
		return articleCategoryRDD;
	}

	private JavaPairRDD<String, Tuple2<String, Double>> helper_articleCategoryEdges(JavaPairRDD<String, String> articleCategoryRDD) {
	    // (a, (c, weight)) edges between article and category
	    JavaPairRDD<String, Tuple2<String, Double>> acEdgeWeightRDD = articleCategoryRDD.mapToPair(x ->
	    		new Tuple2<>(x._1, new Tuple2<>(x._2, (double)(1.0))));
		return acEdgeWeightRDD;
	}

	private JavaPairRDD<String, Tuple2<String, Double>>helper_categoryArticleEdges(JavaPairRDD<String, String> articleCategoryRDD) {
	    JavaPairRDD<String, String> categoryArticleRDD = articleCategoryRDD.mapToPair(x -> new Tuple2<>(x._2, x._1));
	    JavaPairRDD<String, Integer> numCategoryArticleRDD = categoryArticleRDD.mapValues(x->1).reduceByKey((x,y)->(x+y));
	    JavaPairRDD<String, Double> caPropRDD = numCategoryArticleRDD.mapValues(i -> (double)(1.0)/i);
	    // (c, (a, weight)) edges between category and article
	    JavaPairRDD<String, Tuple2<String, Double>> caEdgeWeightRDD = categoryArticleRDD.join(caPropRDD);
		return caEdgeWeightRDD;
	}


	public JavaPairRDD<Integer, Integer> addBacklinks(JavaPairRDD<Integer, Integer> network, JavaRDD<Integer> sinks) {

		JavaPairRDD<Integer, Integer> inverted = network.mapToPair(tuple -> new Tuple2<Integer, Integer>(tuple._2, tuple._1));
		JavaPairRDD<Integer, Integer> sinkEdges = sinks.mapToPair(node -> new Tuple2<Integer, Integer>(node, node));
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> backlinks = inverted.join(sinkEdges);
		System.out.println("Number of backlinks added: " + backlinks.count());
		JavaPairRDD<Integer, Integer> backlinksTuple = backlinks.mapToPair(tuple -> new Tuple2<Integer, Integer>(tuple._1, tuple._2._1));
		return network.union(backlinksTuple);
	}
	public void getTopTen(JavaPairRDD<Integer, Double> pageRank) {
		JavaPairRDD<Double, Integer> reversed = pageRank.mapToPair(tuple -> new Tuple2<Double, Integer>(tuple._2, tuple._1));
		reversed = reversed.sortByKey(false);
		JavaPairRDD<Integer, Double> sorted = reversed.mapToPair(tuple -> new Tuple2<Integer, Double>(tuple._2, tuple._1));
		List<Tuple2<Integer, Double>> topTen = sorted.take(10);
		System.out.println(topTen);
	}
	
	public JavaPairRDD<Integer, Double> computeSocialRanks(JavaPairRDD<Integer, Integer> network) {

		int count = 0;
		JavaPairRDD<Integer, Integer> outDegrees = network.aggregateByKey(0, 
				(val,row) -> val + 1, (val,val2) -> val+val2);
		JavaPairRDD<Integer, Double> nodeTransferRdd = outDegrees.mapToPair(tuple -> 
				new Tuple2<Integer, Double>(tuple._1, 1.0 / tuple._2));
		JavaPairRDD<Integer, Double> prevPageRank = 
				nodeTransferRdd.mapToPair(tuple -> new Tuple2<Integer, Double>(tuple._1, 1.0));
		if (imax == -1) {
			imax = 25;
		}
		if (dmax == -1) {
			dmax = 30;
		}
		while (count < imax) {
			JavaPairRDD<Integer, Double> currPageRank = computeSocialRank(network, prevPageRank);
			JavaPairRDD<Integer, Tuple2<Double, Double>> joinedPageRank = currPageRank.join(prevPageRank);
			JavaPairRDD<Integer, Double> differencePageRank = joinedPageRank.mapToPair(tuple -> 
					new Tuple2<Integer, Double>(tuple._1, Math.abs(tuple._2._1 - tuple._2._2)));
			JavaPairRDD<Integer, Double> smallDiffNodes = differencePageRank.filter(tuple -> tuple._2 >= dmax);
			if (debug) {
				System.out.println(prevPageRank.collect());
			}
			prevPageRank = currPageRank;
			if (smallDiffNodes.isEmpty()) {
				break;
			}
			count++;
		}
		return prevPageRank;
	}
	
	public JavaPairRDD<Integer, Double> computeSocialRank(JavaPairRDD<Integer, Integer> network, JavaPairRDD<Integer, Double> pageRankRdd) {
		JavaPairRDD<Integer, Integer> outDegrees = network.aggregateByKey(0, 
				(val,row) -> val + 1, (val,val2) -> val+val2);
		JavaPairRDD<Integer, Double> nodeTransferRdd = outDegrees.mapToPair(tuple -> 
				new Tuple2<Integer, Double>(tuple._1, 1.0 / tuple._2));
		JavaPairRDD<Integer, Tuple2<Integer, Double>> edgeTransferRDD = network.join(nodeTransferRdd);		
		JavaPairRDD<Integer, Tuple2<Tuple2<Integer, Double>, Double>> joinedEdgePageRank = edgeTransferRDD.join(pageRankRdd);
		JavaPairRDD<Integer, Double> propagateRdd = joinedEdgePageRank.mapToPair(tuple -> new Tuple2<Integer, Double>(tuple._2._1._1, tuple._2._1._2 * tuple._2._2));
		pageRankRdd = propagateRdd.aggregateByKey(0.0,
				(val,row) -> val + row, (val,val2) -> val+val2);
		pageRankRdd = pageRankRdd.mapToPair(tuple -> new Tuple2<Integer, Double>(tuple._1, .15 + .85 * tuple._2));
		return pageRankRdd;
	}

	/**
	 * Graceful shutdown
	 */
	public void shutdown() {
		System.out.println("Shutting down");
	}

	static double dmax;
	static int imax;
	static boolean debug;
	
	public SocialRankJob() {
		System.setProperty("file.encoding", "UTF-8");
	}

	@Override
	public List<MyPair<Integer,Double>> call(JobContext arg0) throws Exception {
		initialize();
		return run();
	}

}
