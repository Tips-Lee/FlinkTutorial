package com.tips.apitest.window;

import com.tips.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowTest02CountWindow {
    public static void main(String[] args) throws Exception{
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setParallelism(1);

        // DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt", "UTF-8");
        DataStream<String> inputStream = env.socketTextStream("zk1", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        DataStream<Double> resultStream = dataStream.keyBy("id")
                .countWindow(10,2)
                .aggregate(new AggregateFunctionImpl());

        resultStream.print();
        env.execute();
    }


    private static class AggregateFunctionImpl implements AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double>{
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return Tuple2.of(0.0, 0);
        }

        @Override
        public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> t2) {
            return Tuple2.of(t2.f0 + value.getTemperature(), t2.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> t) {
            return t.f0 / t.f1;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> t1, Tuple2<Double, Integer> t2) {
            return Tuple2.of(t1.f0 + t2.f0, t1.f1 + t2.f1);
        }
    }
}
