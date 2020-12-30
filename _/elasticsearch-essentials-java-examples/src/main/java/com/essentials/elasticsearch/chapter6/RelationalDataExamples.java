package com.essentials.elasticsearch.chapter6;

import java.util.HashMap;
import java.util.Map;

import com.essentials.elasticsearch.connection.ES_Connection;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;


public class RelationalDataExamples {
	Client client = ES_Connection.getEsConnection();
	IndexRequestBuilder index;
	public static void main(String[] args) {
		//		RelationalDataExamples examples = new RelationalDataExamples();
		//		String nestedIndexName = "twitter_nested";
		//		String nestedIndexDocType = "users";
		//		String nestedField = "tweets";
		//		
		//		examples.findNestedDocs(nestedIndexName, nestedIndexDocType, nestedField);
		//		examples.nestedAggregation(nestedIndexName, nestedIndexDocType, nestedField);
		//		examples.reverseNestedAggregation(nestedIndexName, nestedIndexDocType, nestedField);

		//		String parentChildIndexName = "twitter_parent_child";
		//		String parentType = "users";
		//		String childType = "tweets";
		//		
		//		
		//		examples.indexParentDocs(parentChildIndexName, parentType);
		//		examples.indexChildDocs(parentChildIndexName, childType);
		//		
		//		examples.findParentByChild(parentChildIndexName, parentType, childType);
		//		examples.findChildByParent(parentChildIndexName, parentType, childType);

	}

	private void findNestedDocs(String indexName, String docType, String nestedField) {
		QueryBuilder query = QueryBuilders.boolQuery()
				.must(QueryBuilders.matchQuery("tweets.text", "Nosql Databases"))
				.must(QueryBuilders.termQuery("tweets.created_at", "2015-09-05"));
		SearchResponse response = client.prepareSearch(indexName)
				.setTypes(docType)
				.setQuery(QueryBuilders.nestedQuery(nestedField, query))
				.execute().actionGet();
		System.out.println(response);
	}

	private void nestedAggregation(String indexName, String docType, String nestedField) {
		AggregationBuilder aggregation = 
				AggregationBuilders.nested("NESTED_DOCS").path(nestedField)
				.subAggregation(AggregationBuilders.dateHistogram("TWEET_TIMELINE")
						.field("tweets.created_at").interval(DateHistogramInterval.DAY));
		SearchResponse response = client.prepareSearch(indexName)
				.setTypes(docType)
				.addAggregation(aggregation)
				.setSize(0).execute().actionGet();
		System.out.println(response);
	}

	private void reverseNestedAggregation(String indexName, String docType, String nestedField) {
		AggregationBuilder aggregation = 
				AggregationBuilders.nested("NESTED_DOCS").path(nestedField)
				.subAggregation(AggregationBuilders.dateHistogram("TWEET_TIMELINE")
						.field("tweets.created_at").interval(DateHistogramInterval.DAY)
						.subAggregation(AggregationBuilders.reverseNested("USERS")
								.subAggregation(AggregationBuilders.cardinality("UNIQUE_USERS").field("user.screen_name"))));
		
		SearchResponse response = client.prepareSearch(indexName)
				.setTypes(docType)
				.addAggregation(aggregation)
										.setSize(0).execute().actionGet();

		System.out.println(response);
	}

	private void indexParentDocs(String indexName, String parentDocType) {
		index=client.prepareIndex(indexName, parentDocType);
		Map<String, Object> parentDoc= new HashMap<String, Object>();
		parentDoc.put("screen_name", "d_bharvi");
		parentDoc.put("followers_count", 2000);
		parentDoc.put("create_at", "2012-05-30");

		index.setId("64995604").setSource(parentDoc).execute().actionGet();
		System.out.println("parent indexed");
	}

	private void indexChildDocs(String indexName, String childDocType) {
		index=client.prepareIndex(indexName, childDocType);
		Map<String, Object> childDoc= new HashMap<String, Object>();
		childDoc.put("text", "learning parent-child concepts in elasticsearch");
		childDoc.put("create_at", "2015-05-30");

		index.setParent("64995604").setId("2333").setSource(childDoc).execute().actionGet();
		System.out.println("child indexed");
	}

	private void findParentByChild(String indexName, String parentType, String childType) {
		SearchResponse response = client.prepareSearch(indexName).setTypes(parentType)
				.setQuery(QueryBuilders.hasChildQuery(childType, QueryBuilders.matchQuery("text", "elasticsearch"))).execute().actionGet();
		System.out.println(response);
	}

	private void findChildByParent(String indexName, String parentType, String childType) {
		SearchResponse response = client.prepareSearch(indexName).setTypes(childType)
				.setQuery(QueryBuilders.hasParentQuery(parentType, QueryBuilders.rangeQuery("followers_count").gt(200)))
				.execute().actionGet();
		System.out.println(response);
	}

}
