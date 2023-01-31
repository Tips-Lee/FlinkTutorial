package com.tips.apitest.udf;

import com.tips.apitest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class UdfTest02TableFunction {
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


        Table inputTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp, rt.rowtime");
        tableEnv.createTemporaryView("inputTable", inputTable);

        // 注册函数
        Split split = new Split("_");
        tableEnv.registerFunction("split", split);

        // table api
        // Table outputTable = inputTable.joinLateral("id.split as (sub_id, len)").select("id, sub_id, len, rt");

        // SQL
        Table outputTable = tableEnv.sqlQuery("SELECT id, sub_id, len, rt FROM inputTable, LATERAL TABLE (split(id)) AS split_table_alias(sub_id, len)");

        tableEnv.toAppendStream(outputTable, Row.class).print();
        env.execute();

    }

    public static class Split extends TableFunction<Tuple2<String, Integer>> {
        private final String separator;

        public Split(String separator) {
            this.separator = separator;
        }

        public void eval(String str){
            for (String s :  str.split(separator)) {
                collect(Tuple2.of(s, s.length()));
            }
        }
    }
}
