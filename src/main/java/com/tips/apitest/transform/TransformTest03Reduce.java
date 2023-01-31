package com.tips.apitest.transform;

import com.tips.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest03Reduce {
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


        DataStream<SensorReading> reduce = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            public SensorReading reduce(SensorReading t1, SensorReading t2) throws Exception {
                Double tp1 = t1.getTemperature();
                Double tp2 = t2.getTemperature();
                return new SensorReading(t1.getId(), t2.getTimestamp(), tp1 < tp2 ? tp1 : tp2);
            }
        });

        // 打印输出
        reduce.print();
        // 启动程序
        env.execute();
    }
}
