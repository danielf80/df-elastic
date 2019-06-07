package com.df.es;

import java.io.Closeable;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Daniel Filgueiras
 * @since 2019-05-31
 * @see <blockquote>Inspired by https://www.baeldung.com/elasticsearch-java</blockquote><br>
 * {@linkplain}https://dzone.com/articles/java-high-level-rest-client-elasticsearch{@linkplain}
 * 
 */
public class ElasticSearchConnector implements Closeable {

	private final Logger logger = LoggerFactory.getLogger("main");
	
	private RestHighLevelClient client = null;
	
	public ElasticSearchConnector( String clusterName, String clusterIp, int clusterPort ) throws UnknownHostException {
		
		RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
		
		client = new RestHighLevelClient(builder);
				
		logger.info( "Connection " + clusterIp + ":" + clusterPort + " established!" );		
	}
	
	public boolean isClusterHealthy() {
		
		ClusterHealthRequest request = new ClusterHealthRequest()
				.timeout(TimeValue.timeValueSeconds(60))
				.waitForGreenStatus();
		
		try {
			final ClusterHealthResponse response = client
				    .cluster()
				    .health(request, RequestOptions.DEFAULT);
	 
			if ( response.isTimedOut() ) {
				logger.info( "The cluster is unhealthy: " + response.getStatus() );
				return false;
			}
			
			logger.info( "The cluster is healthy: " + response.getStatus() );
			return true;
		} catch (IOException e) {
			logger.error("Request error", e);
		}
		return false;
	}
	
	public boolean isIndexRegistered( String indexName ) {
		// check if index already exists
		try {
			return client
				    .indices()
				    .exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
		} catch (IOException e) {
			logger.error("Request error", e);
		}
		return false;
	}
	
	public boolean createIndex( String indexName ) {
		
		CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
		createIndexRequest.setTimeout(TimeValue.timeValueSeconds(4));
		createIndexRequest.settings(Settings.builder());
		
		CreateIndexResponse createIndexResponse;
		try {
			createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
			return createIndexResponse.isAcknowledged();
		} catch (IOException e) {
			logger.error("Request error", e);
		} 
				
		return false;				
	}
	
	public boolean indexDocument( String indexName, ElasticData data) {
		
		try {
			XContentBuilder builder = XContentFactory.jsonBuilder();
			builder.startObject();
			{
				builder.field("name", data.getName());
				builder.field("quality", data.isQuality());
				builder.timeField("created", data.getCreated());
				builder.field("decValue", data.getDecValue());
				builder.field("intValue", data.getIntValue());
				builder.field("category", data.getCategory());
			}
			builder.endObject();
			IndexRequest request = new IndexRequest(indexName)
					.source(builder);
			
			IndexResponse response = client.index(request, RequestOptions.DEFAULT);
			if (response.status() == RestStatus.OK) {
				logger.info("Document indexed");
				return true;
			}
			logger.warn("Index response: {}/{}", response.getResult(), response.status());
		} catch (Exception e) {
			logger.error("Request error", e);
		}
		
		return false;
	}
	
	public void clearIndex( String indexName ) {
		try {
			DeleteByQueryRequest request = new DeleteByQueryRequest(indexName).setQuery(QueryBuilders.matchAllQuery());
			
			BulkByScrollResponse response = client.deleteByQuery(request, RequestOptions.DEFAULT);
			
			logger.warn("Index response: {}/{}", response.getStatus(), response.getTotal());
		} catch (IOException e) {
			logger.error("Request error", e);
		}
	}
	
	public void queryResultsWithFieldFields( String indexName, String fieldFilter, String fieldValue, String sortField ) {
		SearchResponse scrollResp;
		try {
			scrollResp = client
					.search(new SearchRequest(new String[] {indexName}, 
							new SearchSourceBuilder()
								.sort(sortField, SortOrder.ASC)
								.postFilter( QueryBuilders.matchPhraseQuery(fieldFilter, fieldValue))
								.size(100))
							.scroll(TimeValue.timeValueSeconds(10)), RequestOptions.DEFAULT);
			
			int hitCount = 0;
			Set<String> scrolls = new HashSet<String>();
			do {
				StringBuilder builder = new StringBuilder();
				if (scrolls.contains(scrollResp.getScrollId()))
					break;
				
				scrolls.add(scrollResp.getScrollId());
				
				for ( SearchHit hit : scrollResp.getHits().getHits()) {
					Map<String,Object> res = hit.getSourceAsMap();
					hitCount++;
			    	// print results
					builder.setLength(0);
					builder.append("doc: ").append(hit.docId()).append(", Score: ").append(hit.getScore()).append(" =");
			    	for( Map.Entry<String,Object> entry : res.entrySet() ) {
			    		builder.append(" ").append(entry.getKey()).append("(").append(entry.getValue()).append(")");
			    	}
			    	logger.info(builder.toString());
				}
			} while (scrollResp.getHits().getHits().length != 0);
			logger.info("Hits: {}, Scrolls: {}", hitCount, scrolls.size());
		} catch (IOException e) {
			logger.error("Request error", e);
		}
	}
	
	public void queryResultsWithAgeFilter( String indexName, int from, int to ) {
		
		SearchResponse scrollResp;
		try {
			scrollResp = client
					.search(new SearchRequest(new String[] {indexName}, 
							new SearchSourceBuilder()
								.sort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
								.postFilter( QueryBuilders.rangeQuery("age").from(from).to(to))
								.size(100))
							.scroll(TimeValue.timeValueSeconds(60)), RequestOptions.DEFAULT);
			
			
			do {
				int count = 1;
				for ( SearchHit hit : scrollResp.getHits().getHits()) {
					Map<String,Object> res = hit.getSourceAsMap();
			    	
			    	// print results
			    	for( Map.Entry<String,Object> entry : res.entrySet() ) {
			    		logger.info( "[" + count + "] " + entry.getKey() + " --> " + entry.getValue() );
			    	}
			    	count++;
				}
			} while (scrollResp.getHits().getHits().length != 0);
		} catch (IOException e) {
			logger.error("Request error", e);
		}
	}

	@Override
	public void close() throws IOException {
		if (client != null)
			client.close();
	}
}
