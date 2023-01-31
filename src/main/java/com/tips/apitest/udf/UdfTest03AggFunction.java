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
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class UdfTest03AggFunction {
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
        Mean mean = new Mean();
        tableEnv.registerFunction("mean", mean);

        // table api
        // Table outputTable = inputTable.groupBy("id").aggregate("mean(temp) as avg_temp").select("id, avg_temp");
        Table outputTable = inputTable.groupBy("id").select("id, temp.mean as avg_temp");

        // SQL
        // Table outputTable = tableEnv.sqlQuery("SELECT id, mean(temp) as avg_temp FROM inputTable GROUP BY id");

        tableEnv.toRetractStream(outputTable, Row.class).print();
        env.execute();

    }

    public static class Mean extends AggregateFunction<Double, Tuple2<Double, Integer>> {

        @Override
        public Double getValue(Tuple2<Double, Integer> acc) {
            return acc.f0/acc.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return Tuple2.of(Double.MIN_VALUE, 0);
        }

        public void accumulate(Tuple2<Double, Integer> acc, Double value){
            acc.f0 += value;
            acc.f1++;
        }

    }
}
