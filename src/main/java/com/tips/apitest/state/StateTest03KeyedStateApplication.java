package com.tips.apitest.state;

import com.tips.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StateTest03KeyedStateApplication {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("zk1", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line->{
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = dataStream.keyBy("id")
                .flatMap(new FlatMapFunctionImpl(10.0));

        resultStream.print();
        env.execute();
    }

    private static class FlatMapFunctionImpl extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {
        private Double threshold;

        public FlatMapFunctionImpl(Double threshold) {
            this.threshold = threshold;
        }

        private ValueState<Double> lastTempState;

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            Double lastTemp = lastTempState.value();
            Double newTemp = value.getTemperature();

            if (lastTemp != null){
                Double diff = Math.abs(newTemp - lastTemp);
                if (diff>=threshold){
                    out.collect(Tuple3.of(value.getId(), lastTemp, newTemp));
                }
            }

            lastTempState.update(newTemp);
        }

        @Override
        public void open(Configuration parameters) {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<>("last-temp", Double.class));
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }
}
