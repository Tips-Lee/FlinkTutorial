package com.tips.apitest.tableapi;

import com.tips.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class TableTest06EventTimeAndWindow {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String inputPath = "./src/main/resources/sensor.txt";

        // 第一种方法
        /*DataStream<String> inputStream = env.readTextFile(inputPath, "UTF-8");
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp() * 1000L;
            }
        });
        tableEnv.createTemporaryView("inputTable", dataStream, "id, timestamp as ts, temperature as temp, pt.rowtime ");*/


        // 第二种方法
        tableEnv.connect(new FileSystem().path(inputPath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE())
                        .rowtime(new Rowtime()
                                .timestampsFromField("timestamp")
                                .watermarksPeriodicBounded(2000))
                )
                .createTemporaryTable("inputTable");

        // 第三种方法
        /*String sinkDDL = "CREATE TABLE inputTable (" +
                "id VARCHAR NOT NULL" +
                ",ts BIGINT" +
                ",temperature DOUBLE" +
                ",rt as TO_TIMESTAMP(FROM_UNIXTIME(ts))" +
                ",watermark for rt as rt - INTERVAL '1' SECOND" +
                ") WITH (" +
                "'connector.type'='filesystem'" +
                ",'connector.path='/src/main/resources/sensor.txt'" +
                ",'format.type'='csv'" +
                ")";
        tableEnv.sqlQuery(sinkDDL);*/

        Table inputTable = tableEnv.from("inputTable");
        inputTable.printSchema();

        Table outputTable = tableEnv.from("inputTable");
        tableEnv.toAppendStream(outputTable, Row.class).print();

        env.execute();
    }
}
