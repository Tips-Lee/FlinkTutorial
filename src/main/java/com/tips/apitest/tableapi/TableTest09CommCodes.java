package com.tips.apitest.tableapi;

import com.tips.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableTest09CommCodes {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String inputPath = "./src/main/resources/sensor.txt";

        DataStream<String> inputStream = env.readTextFile(inputPath, "UTF-8");
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp() * 1000L;
            }
        });
        tableEnv.createTemporaryView("inputTable", dataStream, "id, timestamp as ts, temperature, rt.rowtime, pt.proctime");

        Table inputTable = tableEnv.from("inputTable");
        inputTable.printSchema();

        // 1. table api
        // Table outputTable = inputTable.select("temperature.power(2.0) as temp2");

        // 2. SQL
        Table outputTable = tableEnv.sqlQuery("select power(temperature, 2) as temp2 from inputTable");


        tableEnv.toRetractStream(outputTable, Row.class).print();
        // tableEnv.toAppendStream(outputTable, Row.class).print();

        env.execute();
    }
}
