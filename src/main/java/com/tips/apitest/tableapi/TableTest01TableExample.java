package com.tips.apitest.tableapi;

import com.tips.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableTest01TableExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("./src/main/resources/sensor.txt", "UTF-8");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        Table dataTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp");

        Table resultTable = dataTable.select("id, temp").where("id='sensor_2'");

        tableEnv.createTemporaryView("sensor", dataStream, "id, timestamp as ts, temperature as temp");
        String sql = "SELECT id, temp FROM sensor WHERE id = 'sensor_1'";
        Table resultSql = tableEnv.sqlQuery(sql);

        tableEnv.toAppendStream(resultTable, Row.class).print("table");
        tableEnv.toAppendStream(resultSql, Row.class).print("sql");

        Table sqlTable = tableEnv.from("sensor").select("id, temp").where("id='sensor_3'");
        tableEnv.toAppendStream(sqlTable, Row.class).print("sqlTable");

        String explain = tableEnv.explain(dataTable);
        System.out.println(explain);

        dataTable.printSchema();
        tableEnv.execute("execute");
    }
}
