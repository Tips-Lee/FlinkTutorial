package com.tips.apitest.process;

import com.tips.apitest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class ProcessTest02TempIncrWarning {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

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
                .process(new IncrWarningKeyedProcessFunction(10L))
                .print();

        env.execute();

    }

    private static class IncrWarningKeyedProcessFunction extends KeyedProcessFunction<Tuple, SensorReading, String>{

        private final Long threshold;
        private ValueState<Long> tsTimerState;
        private ValueState<Double> lastTempState;

        public IncrWarningKeyedProcessFunction(Long threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<>("last-tmp", Double.class));
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<>("ts-timer", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws IOException {
            Double lastTemp = lastTempState.value();
            Long tsTimer = tsTimerState.value();
            if (lastTemp == null){
                lastTempState.update(value.getTemperature());
                return;
            }
            else if (tsTimer != null && value.getTemperature()<lastTemp){
                ctx.timerService().deleteProcessingTimeTimer(tsTimerState.value());
                tsTimerState.clear();
            }
            else if (tsTimer == null && value.getTemperature() >= lastTemp){
                long ts = ctx.timerService().currentProcessingTime() + threshold * 1000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                tsTimerState.update(ts);
            }

            lastTempState.update(value.getTemperature());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("传感器" + ctx.getCurrentKey().getField(0) + "连续" + threshold + "秒上升！");
            tsTimerState.clear();
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }
}
