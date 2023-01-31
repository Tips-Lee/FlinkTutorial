package com.tips.apitest.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

public class TableTest04KafkaPipeLine {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        /*tableEnv.connect(new Elasticsearch()
                .version("6")
                .host("127.0.0.1", 9200, "http")
                .index("sensor")
                .documentType("temp")
        ).inUpsertMode()
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");*/

        /*String sinkDDl = "CREATE TABLE jdbcOutputTable(" +
                "id VARCHAR(20) NOT NULL," +
                "cnt BIGINT NOT NULL" +
                ") WITH (" +
                "'connector.type'='jdbc'," +
                "'connector.url'='jdbc:mysql://localhost:3306/testTable'," +
                "'connector.table'='sensor_output'," +
                "'connector.driver'='com.mysql.jdbc.Driver'," +
                "'connector.username'='root'," +
                "'connector.password'='123456',)";
        tableEnv.sqlUpdate(sinkDDl);*/

        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sensor")
                .property("zookeeper.connect", "zk1:3001,zk2:4001,zk3:5001")
                .property("bootstrap.servers", "zk1:9092,zk2:9092:zk3:9092"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");

        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sensor_output")
                .property("zookeeper.connect", "zk1:3001,zk2:4001,zk3:5001")
                .property("bootstrap.servers", "zk1:9092,zk2:9092:zk3:9092"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable");

        Table outputTable = tableEnv.sqlQuery("SELECT id, temperature FROM inputTable WHERE id='sensor_1'");
        // Table outputTable = tableEnv.sqlQuery("SELECT id, count(ts) as cnt, avg(temperature) as avg_temp FROM inputTable GROUP BY id");

        outputTable.insertInto("outputTable");

        env.execute();
    }
}
