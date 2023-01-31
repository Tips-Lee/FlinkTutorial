package com.tips.apitest.watermark;

import com.tips.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class WaterMarkTest01 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt", "UTF-8");
        DataStream<String> inputStream = env.socketTextStream("zk1", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late");
        SingleOutputStreamOperator<SensorReading> resultStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .max("temperature");

        resultStream.print();
        DataStream<SensorReading> sideOutput = resultStream.getSideOutput(outputTag);
        sideOutput.print("late");
        env.execute();

    }

}

