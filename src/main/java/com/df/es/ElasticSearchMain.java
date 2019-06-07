package com.df.es;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchMain {

	private static Logger logger;
	
	public static void main(String[] args) {
		logger = LoggerFactory.getLogger("main");
		
		RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
		try (RestHighLevelClient client = new RestHighLevelClient(builder)) {
		
			SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
			searchSourceBuilder.query(QueryBuilders.matchPhraseQuery("name", "2"));
			
			SearchRequest searchRequest = new SearchRequest();
			searchRequest.indices("data");
			searchRequest.source(searchSourceBuilder);
			
			SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
			SearchHits hits = searchResponse.getHits();
			logger.info("Hits: {}", hits.getTotalHits().value);
			for (SearchHit hit : hits.getHits()) {
				logger.info("Doc: {}, Score: {} = {}", hit.docId(), hit.getScore(), hit.getSourceAsMap());
			}
		} catch (IOException e) {
			logger.error("Error", e);
		}
	}

}
