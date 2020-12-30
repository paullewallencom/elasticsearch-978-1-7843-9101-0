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
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.essentials.elasticsearch.connection.ES_Connection;

public class GeoShapeDataOperations {
	Client client = ES_Connection.getEsConnection();
	public static void main(String[] args) throws IOException {
		//Uncomment the below section of code to execute and test all the methods
		String indexName = "geo_point_test";
		String docType = "geo_points";
		String geoShapeFieldName = "location";
		GeoShapeDataOperations object = new GeoShapeDataOperations();
		object.indexGeoShapeData(indexName, docType, geoShapeFieldName);
		object.geoShapeLineStringQuery(indexName, docType, geoShapeFieldName);
		object.geoShapeEnvelopQuery(indexName, docType, geoShapeFieldName);


	}

	private void indexGeoShapeData(String indexName, String docType,String geoShapeFieldName) {
		//{"location": {"type": "Point", "coordinates": [13.400544, 52.530286]}}
		List<Double> coordinates = new ArrayList<Double>();
		coordinates.add(13.400544);
		coordinates.add(52.530286);
		Map<String, Object> location = new HashMap<String, Object>();
		location.put("coordinates", coordinates);
		location.put("type", "Point");
		Map<String, Object> document = new HashMap<String, Object>();
		document.put("location", location);
		IndexResponse response = client.prepareIndex().setIndex(indexName).setType(docType)
				.setSource(document).setId("1").execute().actionGet();
		System.out.println(response);
	}

	private void geoShapeLineStringQuery(String indexName, String docType,String geoShapeFieldName) {
		QueryBuilder lineStringQuery = QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery())
				.filter(QueryBuilders.geoShapeQuery(geoShapeFieldName,
						ShapeBuilder.newLineString().point(13.400544, 52.530286)
													.point(13.4006,  52.5303)));
		SearchResponse response = client.prepareSearch(indexName).setTypes(docType)
				.setQuery(lineStringQuery)
				.execute().actionGet();
		System.out.println(response);
				
	}

	private void geoShapeEnvelopQuery(String indexName, String docType,String geoShapeFieldName) {
		QueryBuilder envelopQuery = QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery())
				.filter(QueryBuilders.geoShapeQuery(geoShapeFieldName,
						ShapeBuilder.newEnvelope().topLeft(13.0, 53.0).bottomRight(14.0, 52.0)));
		SearchResponse response = client.prepareSearch(indexName).setTypes(docType)
				.setQuery(envelopQuery)
				.execute().actionGet();
		System.out.println(response);
	}
}
