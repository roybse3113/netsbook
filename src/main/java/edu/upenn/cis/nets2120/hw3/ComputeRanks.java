package edu.upenn.cis.nets2120.hw3;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import edu.upenn.cis.nets2120.config.Config;
import edu.upenn.cis.nets2120.hw3.livy.MyPair;
import edu.upenn.cis.nets2120.storage.SparkConnector;
import scala.Tuple2;

public class ComputeRanks {
	/**
	 * The basic logger
	 */
	static Logger logger = LogManager.getLogger(ComputeRanks.class);

	/**
	 * Connection to Apache Spark
	 */
	SparkSession spark;
	JavaSparkContext context;
	
	// necessary variables for social rank;
	static int i_max = 25;
	static double d = 0.15, d_max = 30;
	static boolean debug_mode = false;
	
	public ComputeRanks() {
		System.setProperty("file.encoding", "UTF-8");
	}

	/**
	 * Initialize the database connection and open the file
	 * 
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public void initialize() throws IOException, InterruptedException {
		logger.info("Connecting to Spark...");

		spark = SparkConnector.getSparkConnection();
		context = SparkConnector.getSparkContext();
		
		logger.debug("Connected!");
	}
	
	/**
	 * Fetch the social network from the S3 path, and create a (followed, follower) edge graph
	 * 
	 * @param filePath
	 * @return JavaPairRDD: (followed: int, follower: int)
	 */
	JavaPairRDD<Integer,Integer> getSocialNetwork(String filePath) {
		JavaRDD<String[]> file = context.textFile(filePath, Config.PARTITIONS)
			.map(line -> line.toString().split("\\s+"));
		
		// Indicate each follower as a single follower count
		JavaPairRDD<Integer, Integer> invertedFile = file.mapToPair(a -> {
			return new Tuple2<Integer, Integer>(Integer.parseInt(a[1]),Integer.parseInt(a[0]));
		});
		// TODO Load the file filePath into an RDD (take care to handle both spaces and tab characters as separators)
		return invertedFile.distinct(Config.PARTITIONS);
	}
	
	private JavaRDD<Integer> getSinks(JavaPairRDD<Integer,Integer> network) {
		// TODO Find the sinks in the provided graph
		JavaRDD<Integer> followed = network.keys(), followers = network.values();
		JavaRDD<Integer> sinks = followed.subtract(followers).distinct(Config.PARTITIONS);
		return sinks;
	}
	
	private JavaPairRDD<Integer, Double> socialRanks(JavaPairRDD<Integer, Integer> network) {
		//edges RDD (b, p) --> (follower, followed) --> (to, from)
		JavaPairRDD<Integer, Integer> edges = network.mapToPair(a -> {
			return new Tuple2<Integer, Integer>(a._2, a._1);
		});
				
		//assigning weights to back links (b, 1/N_b)
		JavaPairRDD<Integer, Double> nodeTransfer = edges.mapToPair(a -> {
			return new Tuple2<Integer, Double>(a._1, 1.0);
		}).reduceByKey((a,b) -> a+b).mapToPair(a -> {
			return new Tuple2<Integer, Double>(a._1, 1.0 / a._2);
		});
				
		// join edges and node transfer values by key/node (b, p, 1/N_b)
		JavaPairRDD<Integer, Tuple2<Integer, Double>> edgeTransfer = edges.join(nodeTransfer);
		
		//i_max iterations for social ranks
				
		JavaPairRDD<Integer, Double> pageRank = edges.mapToPair(a -> {
			return new Tuple2<Integer, Double>(a._1, 1.0);
		}).distinct(Config.PARTITIONS);
		
		for (int i = 0; i < i_max; i++) {
			JavaPairRDD<Integer, Double> propogate = edgeTransfer
				// (b, ((p, 1/N_b), Pr_b))
				.join(pageRank)
				.mapToPair(a -> {
				// (p, Pr_b * 1/N_b)
					return new Tuple2<Integer, Double>(a._2._1._1, a._2._2 * a._2._1._2);
			});
					
			JavaPairRDD<Integer, Double> pageRankUpdated = propogate.reduceByKey((a,b) -> a+b, Config.PARTITIONS).mapToPair(a -> {
				return new Tuple2<Integer, Double>(a._1, d + (1-d) * a._2);
			});
			
			//if debug, print out each iteration of nodes and social ranks
            if (debug_mode) {
                logger.debug("<---- iteration " + (i+1) + " ---->");
                pageRankUpdated.foreachPartition(iter -> {
                    while (iter.hasNext()) {
                        Tuple2<Integer, Double> a = iter.next();
                        logger.debug(a._1 + " " + a._2);
                    }
                });
            }

			// determine if max difference == d_max
			Double maxDiff = pageRankUpdated
				.join(pageRank)
				.map(a -> Math.abs(a._2._1 - a._2._2))
				.reduce((a,b) -> Math.max(a, b));
			if (maxDiff <= d_max) i = i_max;
					
			// update pageRank
			pageRank = pageRankUpdated;
		}
		return pageRank;
	}

	/**
	 * Main functionality in the program: read and process the social network
	 * 
	 * @throws IOException File read, network, and other errors
	 * @throws InterruptedException User presses Ctrl-C
	 */
	public void run() throws IOException, InterruptedException {
		logger.info("Running");

		// Load the social network
		// followed, follower
		JavaPairRDD<Integer, Integer> network = getSocialNetwork("");
		
		JavaRDD<Integer> inNodes = network.map(a -> a._1), outNodes = network.map(a -> a._2);
		JavaRDD<Integer> nodes = inNodes.union(outNodes).distinct(Config.PARTITIONS);
		logger.info("This graph contains " + nodes.count() + " nodes and " + network.count() + " edges");
		
		// sink nodes
		JavaRDD<Integer> sinks = getSinks(network);
		
		// convert to pairRDD to join with network (sink, true)
		JavaPairRDD<Integer, Boolean> sinksRDD = sinks.mapToPair(a -> {
		    return new Tuple2<Integer, Boolean>(a, true);
		});
		
		//back edges by reversing incoming edges to sinks
		JavaPairRDD<Integer, Integer> backEdges = network
		        .join(sinksRDD)
		        .mapToPair(a -> {
		            return new Tuple2<Integer, Integer>(a._2._1, a._1);
		        });
		
//		JavaPairRDD<Integer, Integer> backEdges = network.filter(a -> {
//			return sinkNodes.contains(a._1);
//		}).mapToPair(a -> {
//			return new Tuple2<Integer, Integer>(a._2, a._1);
//		});
		
		// add back edges to network
		network = network.union(backEdges);
		logger.info("Added " + backEdges.count() + " backlinks");
		
		//calculate social ranks given updated network
		JavaPairRDD<Integer, Double> pageRank = socialRanks(network);
		
		// sort page Rank (decreasing)
		
		pageRank = pageRank.mapToPair(a -> {
			return new Tuple2<Double, Integer>(a._2, a._1);
		}).sortByKey(false).mapToPair(a -> {
			return new Tuple2<Integer, Double>(a._2, a._1);
		});
		
		if (debug_mode) {
			// if debug, print out all nodes and social ranks
		    pageRank.foreach(a -> {
		        logger.debug(a._1 + " " + a._2);
		    });
		} else {
			//if not debug, print out only top 10 social ranks
		    pageRank.take(10).forEach(a -> {
		        logger.info(a._1 + " " + a._2);
		    });
		}
		
		logger.info("*** Finished social network ranking! ***");
	}


	/**
	 * Graceful shutdown
	 */
	public void shutdown() {
		logger.info("Shutting down");

		if (spark != null)
			spark.close();
	}
	
	public static void set_d_max(double val) {
		d_max = val;
	}
	
	public static void set_d(double val) {
		d = val;
	}
	
	public static void set_i_max(int val) {
		i_max = val;
	}
	
	public static void set_debug(boolean val) {
		debug_mode = val;
	}
	
	public static void main(String[] args) {
		if (args.length > 0) set_d_max(Double.parseDouble(args[0]));
		if (args.length > 1) set_i_max(Integer.parseInt(args[1]));
		if (args.length > 2) {
			set_d(Double.parseDouble(args[2]));
			set_debug(true);
		}
		final ComputeRanks cr = new ComputeRanks();

		try {
			cr.initialize();

			cr.run();
		} catch (final IOException ie) {
			logger.error("I/O error: ");
			ie.printStackTrace();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		} finally {
			cr.shutdown();
		}
	}

}
