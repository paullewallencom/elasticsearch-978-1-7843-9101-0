/**
 * @author bharvi
 */

package com.essentials.elasticsearch.chapter4;

import java.io.IOException;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.joda.time.DateTime;

import com.essentials.elasticsearch.connection.ES_Connection;

public class MultiLevelAggregationExamples {
	Client client = ES_Connection.getEsConnection();
	public static void main(String[] args) throws IOException {
		//Uncomment the below section of code to execute and test all the methods

		/*String indexName = "tweets";
		String docType = "tweet";
		MultiLevelAggregationExamples operations = new MultiLevelAggregationExamples();
		operations.multiLevelAggregation(indexName, docType);*/
	}

	private void multiLevelAggregation(String indexName, String docType) {
		QueryBuilder query = QueryBuilders.matchQuery("text", "crime");
		
		AggregationBuilder aggregation =
				AggregationBuilders
				.dateHistogram("hourly_timeline")
				.field("@timestamp")
				.interval(DateHistogramInterval.YEAR)
				.subAggregation(AggregationBuilders
						.terms("top_hashtags")
						.field("entities.hashtags.text")
				.subAggregation(AggregationBuilders
						.terms("top_users")
						.field("user.screen_name")
				.subAggregation(AggregationBuilders
						.avg("average_status_count")
						.field("user.statuses_count"))));
		SearchResponse response = client.prepareSearch(indexName).setTypes(docType)
				.setQuery(query).addAggregation(aggregation)
				.setSize(0)
				.execute().actionGet();
	
		//Start parsing response
		//Get first level of aggregation data
		Histogram agg = response.getAggregations().get("hourly_timeline");
		//for each entry of hourly histogram
		for (Histogram.Bucket entry : agg.getBuckets()) {
			DateTime key = (DateTime) entry.getKey();    // Key
			String keyAsString = entry.getKeyAsString(); // Key as String
			long docCount = entry.getDocCount();         // Doc count
			System.out.println(key);
			System.out.println(docCount);
			
			//Get second level of aggregation data
			Terms topHashtags = entry.getAggregations().get("top_hashtags");
			//for each entry of top hashtags
			for (Terms.Bucket hashTagEntry : topHashtags.getBuckets()) {
				String hashtag = hashTagEntry.getKey().toString();      // Term
				long hashtagCount = hashTagEntry.getDocCount(); // Doc count
				System.out.println(hashtag);
				System.out.println(hashtagCount);
				
				//Get 3rd level of aggregation data
				Terms topUsers = hashTagEntry.getAggregations().get("top_users");
				//for each entry of top users
				for (Terms.Bucket usersEntry : topUsers.getBuckets()) {
					String screenName = usersEntry.getKey().toString();      // Term
					long userCount = usersEntry.getDocCount(); // Doc count
					System.out.println(screenName);
					System.out.println(userCount);
					
					//Get 4th level of aggregation data
					Avg average_status_count = usersEntry.getAggregations().get("average_status_count");
					double max = average_status_count.getValue();
					System.out.println(max);
				}
			}
		}
	}
}

