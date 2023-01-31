package com.tips.apitest.transform;

import com.tips.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collection;
import java.util.Collections;

public class TransformTest04MultiStreams {
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

        // 1, 分流
        SplitStream<SensorReading> splitStream = mapStream.split(new OutputSelector<SensorReading>() {
            public Iterable<String> select(SensorReading value) {
                return value.getTemperature() > 16.0 ? Collections.singleton("High") : Collections.singleton("Low");
            }
        });

        DataStream<SensorReading> highStream = splitStream.select( "High");
        DataStream<SensorReading> lowStream = splitStream.select("Low");

        // 2, 合流
        DataStream<Tuple2<String, Double>> warningStream = highStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return Tuple2.of(value.getId(), value.getTemperature());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStream = warningStream.connect(lowStream);
        DataStream<Tuple3<String, Double, String>> output = connectedStream.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Tuple3<String, Double, String>>() {
            public Tuple3<String, Double, String> map1(Tuple2<String, Double> value) throws Exception {
                return Tuple3.of(value.f0, value.f1, "high warning");
            }

            public Tuple3<String, Double, String> map2(SensorReading value) throws Exception {
                return Tuple3.of(value.getId(), value.getTemperature(), "normal");
            }
        });

        DataStream<SensorReading> unionStream = highStream.union(lowStream);
        // 打印输出
        output.print("output");
        unionStream.print("Union");
        // 启动程序
        env.execute();
    }
}
