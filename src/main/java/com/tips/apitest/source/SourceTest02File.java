package com.tips.apitest.source;

import com.tips.apitest.beans.SensorReading;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.base.Utf8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class SourceTest02File {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> dataStream = env.readTextFile("src/main/resources/sensor.txt", "UTF-8");

        dataStream.print("File");
        env.execute();
    }
}
