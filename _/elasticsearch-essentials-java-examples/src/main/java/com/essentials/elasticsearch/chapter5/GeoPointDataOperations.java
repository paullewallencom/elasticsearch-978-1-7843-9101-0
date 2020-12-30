/**
 * @author bharvi
 */

package com.essentials.elasticsearch.chapter5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.essentials.elasticsearch.connection.ES_Connection;

public class GeoPointDataOperations {
	Client client = ES_Connection.getEsConnection();
	public static void main(String[] args) throws IOException {
		//Uncomment the below section of code to execute and test all the methods

		String indexName = "geo_point";
		String docType = "geo_points";
		String geoShapeFieldName = "location";
		GeoPointDataOperations object = new GeoPointDataOperations();
		object.indexGeoPointData(indexName, docType);
		object.findByDistance(indexName, docType);
		object.findByRange(indexName, docType);
		object.findByBoundingBox(indexName, docType);
		object.sortByDistance(indexName, docType);
		object.geoAggregation(indexName, docType, "location");


	}

	/**
	 * Method for indexing a single document in a request
	 * @param indexName
	 * @param docType
	 */
	private void indexGeoPointData(String indexName, String docType) {
//		{"location": "29.9560, 78.1700", "name": "delhi"}
		Map<String, Object> document1= new HashMap<String, Object>();
		document1.put("location", "29.9560, 78.1700");
		document1.put("name", "delhi");
		document1.put("dish_name", "chinese");
		IndexResponse response1 = client.prepareIndex().setIndex(indexName).setType(docType)
				.setSource(document1).execute().actionGet();
		
		Map<String, Object> document2= new HashMap<String, Object>();
		List<Double> geoPoints = new ArrayList<Double>();
		geoPoints.add(77.42);
		geoPoints.add(28.67);
		document2.put("location", geoPoints);
		document2.put("name", "delhi");
		document2.put("dish_name", "chinese");
		IndexResponse response2 = client.prepareIndex().setIndex(indexName).setType(docType)
				.setSource(document2).execute().actionGet();
		
		Map<String, Object> document3 = new HashMap<String, Object>();
		Map<String, Object> locationMap = new HashMap<String, Object>();
		locationMap.put("lat", 29.9560);
		locationMap.put("lon", 78.1700);
		document3.put("location", locationMap);
		document3.put("name", "delhi");
		document3.put("dish_name", "chinese");
		IndexResponse response3 = client.prepareIndex().setIndex(indexName).setType(docType)
				.setSource(document3).execute().actionGet();
		System.out.println(response3);
	}

	/**
	 * Method for getting a single document in a request
	 * @param indexName
	 * @param docType
	 */
	private void findByDistance(String indexName, String docType) {
		QueryBuilder query = QueryBuilders.matchAllQuery();
		QueryBuilder geoDistanceQuery = 
				QueryBuilders.geoDistanceQuery("location")
				.lat(28.67).lon(77.42)
				.distance(12, DistanceUnit.KILOMETERS);
		QueryBuilder finalQuery = QueryBuilders.boolQuery()
				.must(query).filter(geoDistanceQuery);
		SearchResponse response = 
				client.prepareSearch(indexName).setTypes(docType)
				.setQuery(finalQuery)
				.execute().actionGet();
		System.out.println(response);
	}

	private void findByRange(String indexName, String docType) {
		QueryBuilder query = QueryBuilders.matchAllQuery();
		QueryBuilder geoDistanceRangeQuery = 
				QueryBuilders.geoDistanceRangeQuery("location")
				.lon(28.67).lat(77.42)
				.from("100km").to("4000km");
		QueryBuilder finalQuery = QueryBuilders.boolQuery()
				.must(query).filter(geoDistanceRangeQuery);
		SearchResponse response = 
				client.prepareSearch(indexName).setTypes(docType)
				.setQuery(finalQuery)
				.execute().actionGet();
		System.out.println(response);
	}

	private void findByBoundingBox(String indexName, String docType) {
		GeoPoint topLeft= new GeoPoint(68.91,35.60);
		GeoPoint bottomRight= new GeoPoint(7.80,97.29);

		QueryBuilder query = QueryBuilders.matchAllQuery();
		QueryBuilder geoDistanceRangeQuery = 
				QueryBuilders.geoBoundingBoxQuery("location")
				.topLeft(topLeft).bottomRight(bottomRight);
		QueryBuilder finalQuery = QueryBuilders.boolQuery()
				.must(query).filter(geoDistanceRangeQuery);
		SearchResponse response = 
				client.prepareSearch(indexName).setTypes(docType)
				.setQuery(finalQuery)
				.execute().actionGet();
		System.out.println(response);
	}

	private void sortByDistance(String indexName, String docType) {
		QueryBuilder query = QueryBuilders.termQuery("dish_name", "chinese");
		SortBuilder sortingQuery = SortBuilders.geoDistanceSort("location")
				.point(28.67, 77).unit(DistanceUnit.KILOMETERS)
				.order(SortOrder.ASC);
		SearchResponse response = 
				client.prepareSearch(indexName).setTypes(docType)
				.setQuery(query)
				.addSort(sortingQuery)
				.execute().actionGet();
		System.out.println("response of sorting "+response);
	}

	private void geoAggregation(String indexName, String docType,String fieldName) {
		AggregationBuilder aggregation =
				AggregationBuilders.geoDistance("news_hotspots").field(fieldName)
				.point(new GeoPoint(28.61, 77.23))
				.unit(DistanceUnit.KILOMETERS)
				.distanceType(GeoDistance.PLANE)
				.addUnboundedTo(50)
				.addRange(50, 100)
				.addUnboundedFrom(200);

		SearchResponse response = client.prepareSearch(indexName).setTypes(docType)
				.setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(aggregation)
				.setSize(0).execute().actionGet();
		Range agg = response.getAggregations().get("news_hotspots");

		for (Range.Bucket entry : agg.getBuckets()) {
			String key = entry.getKeyAsString();    // key as String
			Number from = (Number) entry.getFrom(); // bucket from value
			Number to = (Number) entry.getTo();     // bucket to value
			long docCount = entry.getDocCount();    // Doc count
			System.out.println("key: "+key + " from: "+from+" to: "+to+" doc count: "+docCount);
		}
	}
}
