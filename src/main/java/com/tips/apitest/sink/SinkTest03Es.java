/*package com.tips.apitest.sink;

import com.tips.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class SinkTest03Es {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt", "UTF-8");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), Double.valueOf(fields[2]));
        });
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200));

        ElasticsearchSinkFunction<SensorReading> esSinkFunction = new ElasticsearchSinkFunction<SensorReading>() {
            @Override
            public void process(SensorReading element, RuntimeContext ctx, RequestIndexer indexer) {
                 HashMap<String, String> dataSource = new HashMap<>();
                 dataSource.put("id", element.getId());
                 dataSource.put("temp", element.getTemperature().toString());
                 dataSource.put("ts", element.getTimestamp().toString());

                IndexRequest indexRequest = Requests.indexRequest()
                        .index("sensor")
                        .type("readingData")
                        .source(dataSource);
                indexer.add(indexRequest);
            }
        };

        dataStream.addSink(new ElasticsearchSink.Builder<>(httpHosts, esSinkFunction).build());
        env.execute();
    }
}*/
