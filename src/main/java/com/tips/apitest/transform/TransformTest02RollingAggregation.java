package com.tips.apitest.transform;

import com.tips.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformTest02RollingAggregation {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        DataStream<SensorReading> mapStream = inputStream.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
            }
        });

        // KeyedStream<SensorReading, Tuple> keyedStream = mapStream.keyBy("id");
        KeyedStream<SensorReading, String> keyedStream = mapStream.keyBy(new KeySelector<SensorReading, String>() {
            public String getKey(SensorReading sensorReading) throws Exception {
                return sensorReading.getId();
            }
        });

        DataStream<SensorReading> temperature = keyedStream.max("temperature");
        // DataStream<SensorReading> temperature = keyedStream.maxBy("temperature");

        // 打印输出
        temperature.print();
        // 启动程序
        env.execute();
    }
}
