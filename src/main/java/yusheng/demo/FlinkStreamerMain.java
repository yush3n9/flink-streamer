package yusheng.demo;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.json.JSONObject;

public class FlinkStreamerMain {

	public static void main(String[] args) throws Exception {
		// create execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// checkpoint every 5000 msecs
		env.enableCheckpointing(5000);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "test");
		DataStream<String> messageStream = env
				.addSource(new FlinkKafkaConsumer011<>("BlockChainCharts", new SimpleStringSchema(), properties));

		// Convert String message to json:
		DataStream<JSONObject> jsonStream = messageStream.rebalance().map(new MapFunction<String, JSONObject>() {
			private static final long serialVersionUID = -6867736771747690202L;

			@Override
			public JSONObject map(String value) throws Exception {
				return new JSONObject(value);
			}
		});

		// Sink json to elasticsearch:
		jsonStream.addSink(new CustomElasticsearchSinker());

		env.execute();
	}
}