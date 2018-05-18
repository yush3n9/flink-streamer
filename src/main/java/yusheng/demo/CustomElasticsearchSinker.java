package yusheng.demo;

import java.net.InetAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONObject;

public class CustomElasticsearchSinker extends RichSinkFunction<JSONObject> {

	private static final long serialVersionUID = 9156453251316873855L;

	private TransportClient esClient;

	@SuppressWarnings("resource")
	@Override
	public void open(Configuration parameters) throws Exception {
		Builder settingBuilder = Settings.builder();
		// Only necessary if cluster is different as "elasticsearch"
		settingBuilder.put("cluster.name", "elasticsearch");
		Settings settings = settingBuilder.build();

		this.esClient = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
	}

	@Override
	public void close() throws Exception {
		this.esClient.close();
	}

	@Override
	public void invoke(JSONObject value) throws Exception {
		Map<String, Object> json = new HashMap<String, Object>();
		json.put("currency", "EUR");
		json.put("timestamp", new Date());
		json.put("price", value.getJSONObject("EUR").getDouble("15m"));

		IndexResponse response = esClient.prepareIndex("bitcoin", "chart").setSource(json, XContentType.JSON).get();
		System.out.println(response.status());
	}
}