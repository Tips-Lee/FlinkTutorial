package com.tips.apitest.tableapi;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class TableTest02CommApi {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        // Old Version
        EnvironmentSettings flinkSettings = EnvironmentSettings.newInstance().inStreamingMode().useOldPlanner().build();
        StreamTableEnvironment steamTableEnv = StreamTableEnvironment.create(streamEnv, flinkSettings);

        BatchTableEnvironment batchTableEnv = BatchTableEnvironment.create(batchEnv);

        // New Version
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(streamEnv, blinkStreamSettings);

        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance().inBatchMode().useBlinkPlanner().build();
        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings);

        String filePath = "./src/main/resources/sensor.txt";
        blinkStreamTableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                                .field("id", DataTypes.STRING())
                                .field("timestam", DataTypes.BIGINT())
                                .field("temperature", DataTypes.DOUBLE())
                ).createTemporaryTable("inputTable");

        Table inputTable = blinkStreamTableEnv.from("inputTable");

        inputTable.printSchema();
        // blinkStreamTableEnv.toAppendStream(inputTable, Row.class).print();

        streamEnv.execute("execute");
    }
}
