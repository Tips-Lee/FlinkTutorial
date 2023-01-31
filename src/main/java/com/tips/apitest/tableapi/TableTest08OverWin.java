package com.tips.apitest.tableapi;

import com.tips.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.UnboundedRange;
import org.apache.flink.types.Row;

public class TableTest08OverWin {

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

        // Over Window
        // 1. table api
        /*Table outputTable = inputTable.window(Over.partitionBy("id").orderBy("rt").preceding(UnboundedRange).as("w"))
                .select("id, id.count over w, temperature.avg over w as avg_temp, rt");*/
        /*Table outputTable = inputTable.window(Over.partitionBy("id").orderBy("rt").preceding("2.seconds").as("w"))
                .select("id, id.count over w, temperature.avg over w as avg_temp, rt");*/
        /*Table outputTable = inputTable.window(Over.partitionBy("id").orderBy("rt").preceding("2.rows").as("w"))
                .select("id, id.count over w, temperature.avg over w as avg_temp, rt");*/
        // 2. SQL
        /*Table outputTable = tableEnv.sqlQuery("" +
                "SELECT id" +
                ", COUNT(id) OVER w" +
                ", AVG(temperature) OVER w as avg_temp " +
                ", rt " +
                "FROM inputTable " +
                "WINDOW w AS (PARTITION BY id ORDER BY rt ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)");*/
        Table outputTable = tableEnv.sqlQuery("" +
                "SELECT id" +
                ", COUNT(id) OVER w" +
                ", ROUND(AVG(temperature) OVER w, 1) as avg_temp " +
                ", rt " +
                ", CURRENT_TIME " +
                " FROM inputTable " +
                " WINDOW w AS (PARTITION BY id ORDER BY rt RANGE BETWEEN interval '2' second PRECEDING AND CURRENT ROW)");


        tableEnv.toRetractStream(outputTable, Row.class).print();
        // tableEnv.toAppendStream(outputTable, Row.class).print();

        env.execute();
    }
}
