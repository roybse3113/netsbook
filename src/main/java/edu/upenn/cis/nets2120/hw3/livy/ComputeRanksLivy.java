package edu.upenn.cis.nets2120.hw3.livy;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;

import edu.upenn.cis.nets2120.config.Config;

public class ComputeRanksLivy {
	public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ExecutionException {
		
		// LivyClient client = new LivyClientBuilder()
		// .setURI(new URI("http://ec2-34-227-83-180.compute-1.amazonaws.com:8998/"))
		// .build();

		try {
			String jar = "target/nets2120-hw3-0.0.1-SNAPSHOT.jar";
			
		  System.out.printf("Uploading %s to the Spark context...\n", jar);
		  	//client.uploadJar(new File(jar)).get();
			//client.submit(new SocialRankJob()).get();
		  SocialRankJob sr = new SocialRankJob();
		  sr.initialize();
		  sr.run();
		  

		} catch (Exception e){
			e.printStackTrace();
		}finally {
			System.out.printf("terminated job");
		  
		}
	}

}
