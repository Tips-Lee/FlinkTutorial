package com.tips.apitest.udf;

import com.tips.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

public class UdfTest01ScalarFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String inputPath = "./src/main/resources/sensor.txt";

        DataStream<String> inputStream = env.readTextFile(inputPath);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp() * 1000L;
            }
        });

        /*tableEnv.createTemporaryView("inputTable", dataStream, "id, timestamp as ts, temperature as temp, rt.rowtime");
        Table outputTable = tableEnv.from("inputTable");*/

        Table inputTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp, rt.rowtime");
        tableEnv.createTemporaryView("inputTable", inputTable);

        // 注册函数
        HashCode hashCode = new HashCode(3);
        tableEnv.registerFunction("hashcode", hashCode);

        // table api
        // Table outputTable = inputTable.select("id, hashcode(id), id.hashcode, rt ");

        // SQL
        Table outputTable = tableEnv.sqlQuery("SELECT id, hashCode(id), rt from inputTable");

        tableEnv.toAppendStream(outputTable, Row.class).print();
        env.execute();

    }

    public static class HashCode extends ScalarFunction{
        private final int factor;
        public HashCode(int factor) {
            this.factor=factor;
        }

        public long eval(String str){
            return str.hashCode() * factor;
        }
    }
}
