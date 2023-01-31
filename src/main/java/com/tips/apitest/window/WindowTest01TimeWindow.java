package com.tips.apitest.window;

import com.tips.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class WindowTest01TimeWindow {
    public static void main(String[] args) throws Exception{
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setParallelism(1);
         env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt", "UTF-8");
        DataStream<String> inputStream = env.socketTextStream("zk1", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        /*DataStream<Integer> resultStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading sensorReading, Integer acc) {
                        return acc + 1;
                    }

                    @Override
                    public Integer getResult(Integer acc) {
                        return acc;
                    }

                    @Override
                    public Integer merge(Integer acc0, Integer acc1) {
                        return acc0 + acc1;
                    }
                });*/

        /*DataStream<SensorReading> resultStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .max("temperature");*/

        /*DataStream<SensorReading> resultStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .reduce((t0, t1) -> t0.getTemperature() > t1.getTemperature() ? t0 : t1);*/

        /*DataStream<Integer> resultStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .apply(new WindowFunction<SensorReading, Integer, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReading> input, Collector<Integer> output) throws Exception {
                        Integer size = IteratorUtils.toList(input.iterator()).size();
                        output.collect(size);
                    }
                });*/

        /*DataStream<Tuple3<String, Long, Integer>> resultStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> output) throws Exception {
                        String id = tuple.getField(0);
                        Long windowEnd = timeWindow.getEnd();
                        Integer size = IteratorUtils.toList(input.iterator()).size();
                        output.collect(Tuple3.of(id, windowEnd, size));
                    }
                });*/


        OutputTag<SensorReading> outputTag = new OutputTag<>("late");
        SingleOutputStreamOperator<SensorReading> resultStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .max("temperature");

        resultStream.print();
        DataStream<SensorReading> sideOutput = resultStream.getSideOutput(outputTag);
        sideOutput.print("late");
        env.execute();
    }
}
