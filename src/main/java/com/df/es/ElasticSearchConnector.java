package com.df.es;

import java.io.Closeable;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
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
				.timeout(TimeValue.timeValueSeconds(30))
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
				    .exists(new GetIndexRequest().indices(indexName).masterNodeTimeout(TimeValue.timeValueSeconds(2)), RequestOptions.DEFAULT);
		} catch (IOException e) {
			logger.error("Request error", e);
		}
		return false;
	}
	
	public boolean createIndex( String indexName, String numberOfShards, String numberOfReplicas ) {
		
		CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
		createIndexRequest.setTimeout(TimeValue.timeValueSeconds(4));
		createIndexRequest.settings(Settings.builder()
				.put("index.number_of_shards", numberOfShards ) 
                .put("index.number_of_replicas", numberOfReplicas ));
		
		CreateIndexResponse createIndexResponse;
		try {
			createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
			return createIndexResponse.isAcknowledged();
		} catch (IOException e) {
			logger.error("Request error", e);
		} 
				
		return false;				
	}
	
	public boolean indexDocument( String indexName, ElasticDataDemo data) {
		
		try {
			XContentBuilder builder = XContentFactory.jsonBuilder();
			builder.startObject();
			{
				builder.field("name", data.getName());
				builder.field("quality", data.isQuality());
				builder.timeField("created", data.getCreated());
				builder.field("decValue", data.getDecValue());
				builder.field("intValue", data.getIntValue());
			}
			builder.endObject();
			IndexRequest request = new IndexRequest(indexName)
					.index(data.getName())
					.source(builder);
			
			IndexResponse response = client.index(request, RequestOptions.DEFAULT);
			if (response.status() == RestStatus.OK) {
				logger.info("Document indexed");
				return true;
			}
			logger.warn("Index response: {}/{}", response.getResult(), response.status());
		} catch (Exception e) {
			logger.error("Error on insert documento");
		}
		
		return false;
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
