package com.df.es;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticDemoMain implements Runnable {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final String INDEX_NAME = "data";
	
	public static void main(String[] args) {
		new ElasticDemoMain().run();
	}

	
	
	@Override
	public void run() {
		logger.info("Starting...");
		
		ElasticSearchConnector es = null;
		
		try {
			final int samples = 100;
			final boolean recreateData = false;	// Change 
			
			
			
			es = new ElasticSearchConnector("DEMO", "localhost", 9200);
			
			if (!es.isClusterHealthy()) {
				logger.warn("Cluster is not healthy yet");
			}
				
			if (!es.isIndexRegistered(INDEX_NAME)) {
				es.createIndex(INDEX_NAME);
			}
			
			if (recreateData) {
				es.clearIndex(INDEX_NAME);
				
				ElasticData datas[] = ElasticDataFactory.createDemoData(samples);
				for (ElasticData data : datas) {
					es.indexDocument(INDEX_NAME, data);
				}
			}
			
			es.queryResultsWithFieldFields(INDEX_NAME, "category", "RED", "created");
		} catch (Exception e) {
			logger.error("Error on ElasticSearch", e);
		} finally {
			if (es != null) try { es.close(); } catch (IOException e) {}
		}
	}

}
