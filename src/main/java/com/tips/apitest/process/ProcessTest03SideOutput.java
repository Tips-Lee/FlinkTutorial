package com.tips.apitest.process;

import com.tips.apitest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;

public class ProcessTest03SideOutput {
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

        OutputTag<SensorReading> sideOutputTag = new OutputTag<SensorReading>("low-temp"){};
        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream.process(new SideOutputProcessFunction(15.0, sideOutputTag));

        highTempStream.print("high");
        highTempStream.getSideOutput(sideOutputTag).print("low");

        env.execute();

    }

    private static class SideOutputProcessFunction extends ProcessFunction<SensorReading, SensorReading>{
        private final Double threshold;
        private final OutputTag<SensorReading> outputTag;

        public SideOutputProcessFunction(Double threshold, OutputTag<SensorReading> outputTag) {
            this.threshold = threshold;
            this.outputTag = outputTag;
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) {
            Double curTemp = value.getTemperature();
            if (curTemp >= threshold){
                out.collect(value);
            }else {
                ctx.output(outputTag,value);
            }
        }
    }
}
