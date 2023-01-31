package com.tips.apitest.tableapi;

import com.tips.apitest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableTest07GroupWinRowTime {
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

        // Group Tumbling Window
        // 1. table api
        // Table outputTable = inputTable.window(Tumble.over("5.minutes").on("rt").as("w")).groupBy("id, w").select("id, id.count, temperature.avg as avg_temp, w.end");
        // Table outputTable = inputTable.window(Tumble.over("1.minutes").on("rt").as("w")).groupBy("id, w").select("id, id.count, temperature.avg as avg_temp, w.end");
        // 2. SQL
        /*Table outputTable = tableEnv.sqlQuery("" +
                "SELECT " +
                "id" +
                ",COUNT(id) AS cnt" +
                ",AVG(temperature) AS avg_temp" +
                ",TUMBLE_END(rt, interval '3' second) " +
                "FROM inputTable " +
                "GROUP BY id, TUMBLE(rt, interval '3' second)");*/


        // Group Sliding Window
        // 1.table api
        /*Table outputTable = inputTable
                .window(Slide.over("10.seconds")
                        .every("5.seconds")
                        .on("rt")
                        .as("w"))
                .groupBy("id, w")
                .select("id, id.count, temperature.avg as avg_temp, w.end");*/


        // 2. SQL
        /*Table outputTable = tableEnv.sqlQuery("" +
                "SELECT " +
                "id" +
                ",COUNT(id) AS cnt" +
                ",AVG(temperature) AS avg_temp" +
                ",HOP_END(rt, interval '2' second, interval '3' second) " +
                "FROM inputTable " +
                "GROUP BY id, HOP(rt, interval '2' second, interval '3' second)");*/

        // Group Session Window
        // 1. table api
        Table outputTable = inputTable.window(Session.withGap("2.seconds").on("rt").as("w"))
                .groupBy("id, w")
                .select("id, count(id), temperature.avg as avg_temp, w.end");

        // 2. SQL
        /*Table outputTable = tableEnv.sqlQuery("" +
                "SELECT id, COUNT(id) AS cnt, AVG(temperature) AS avg_temp, SESSION_END(rt, interval '3' second) " +
                "FROM inputTable GROUP BY id, SESSION(rt, interval '3' second)");*/

        tableEnv.toRetractStream(outputTable, Row.class).print();
        // tableEnv.toAppendStream(outputTable, Row.class).print();

        env.execute();
    }
}
