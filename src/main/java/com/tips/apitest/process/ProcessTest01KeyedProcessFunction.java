package com.tips.apitest.process;

import com.tips.apitest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class ProcessTest01KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStream<String> inputStream = env.socketTextStream("zk1", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line->{
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        dataStream.keyBy("id")
                .process(new KeyedProcessFunctionImpl())
                .print();

        env.execute();
    }

    private static class KeyedProcessFunctionImpl extends KeyedProcessFunction<Tuple,SensorReading, Integer> {

        ValueState<Long> keyedValueState;
        ValueState<Long> tsTimerState;

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(timestamp + " 定时器触发");
            ctx.getCurrentKey();
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
            out.collect(value.getId().length());
            System.out.println(ctx.getCurrentKey());
            long tsTimer = ctx.timerService().currentProcessingTime();
            System.out.println("tsTimer: " + tsTimer);
            ctx.timerService().registerProcessingTimeTimer(tsTimer + 1000L);
            tsTimerState.update(tsTimer);
            // ctx.timerService().deleteProcessingTimeTimer(tsTimerState.value());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            keyedValueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("key-state", Long.class));
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-timer", Long.class));
        }
    }
}
