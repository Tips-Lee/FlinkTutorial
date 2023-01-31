package com.tips.apitest.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class TableTest03FileOutput {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String inputPath = "./src/main/resources/sensor.txt";
        String outputPath = "./src/main/resources/output.txt";

        tableEnv.connect(new FileSystem().path(inputPath))
                .withFormat(new Csv())
                .withSchema(new Schema().field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");


        tableEnv.connect(new FileSystem().path(outputPath))
                .withFormat(new Csv())
                .withSchema(new Schema().field("id", DataTypes.STRING())
                        .field("ts_cnt", DataTypes.BIGINT())
                        .field("temp_avg", DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable");

        Table inputTable = tableEnv.from("inputTable");

        // Table outputTable = inputTable.select("id, temperature").filter("id='sensor_1'");
        // Table outputTable = inputTable.groupBy("id").select("id, ts.count as cnt, temperature.avg as avg_temp");

        // Table outputTable = tableEnv.sqlQuery("SELECT id, temperature FROM inputTable WHERE id='sensor_1'");
        Table outputTable = tableEnv.sqlQuery("SELECT id, count(ts) as cnt, avg(temperature) as avg_temp FROM inputTable GROUP BY id");

        outputTable.printSchema();
        outputTable.insertInto("outputTable");

        env.execute();

    }
}
