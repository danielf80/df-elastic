package com.df.es;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESStoreData implements Runnable {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final String INDEX_NAME = "data";
	private final String NUMBER_OF_SHARDS = "2";
	private final String NUMBER_OF_REPLICAS = "1";
	
	public static void main(String[] args) {
		new ESStoreData().run();
	}

	@Override
	public void run() {
		logger.info("Starting...");
		
		ElasticSearchConnector es = null;
		
		try {
			ElasticDataDemo[] elastics = new ElasticDataDemo[] {
					new ElasticDataDemo("Item 1"),
					new ElasticDataDemo("Item 2"),
					new ElasticDataDemo("Item 3")
			};
			
			es = new ElasticSearchConnector("DEMO", "localhost", 9200);
			
			es.isClusterHealthy();
				
			if (!es.isIndexRegistered(INDEX_NAME)) {
				es.createIndex(INDEX_NAME, NUMBER_OF_SHARDS, NUMBER_OF_REPLICAS);
				
				
				
				
			}
		} catch (Exception e) {
			logger.error("Error on ElasticSearch", e);
		} finally {
			if (es != null) try { es.close(); } catch (IOException e) {}
		}
	}

}
