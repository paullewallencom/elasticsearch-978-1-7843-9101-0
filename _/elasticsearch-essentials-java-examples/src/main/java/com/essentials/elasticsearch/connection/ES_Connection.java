/**
 * @author bharvi
 */
package com.essentials.elasticsearch.connection;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ES_Connection {
/**
 * Class for initializing ELasticsearch connection
 */
	static Client client;
	static Settings settings;
	
	public static Client getEsConnection()
	{
		settings = Settings.settingsBuilder().put("cluster.name", "elasticsearch").put("path.home", "/").build();
		try {
			client = TransportClient.builder().settings(settings).build()
			.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
			System.out.println("connection created");
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	return client;
	}
}
