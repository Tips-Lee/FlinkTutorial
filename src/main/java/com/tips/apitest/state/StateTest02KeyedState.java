package com.tips.apitest.state;

import akka.stream.impl.ReducerState;
import com.tips.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

public class StateTest02KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> inputStream = env.socketTextStream("zk1", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        DataStream<Integer> resultStream = dataStream.keyBy("id")
                .map(new RichMapFunctionImpl());

        resultStream.print();
        env.execute();
    }

    public static class RichMapFunctionImpl extends RichMapFunction<SensorReading, Integer>{
        private ValueState<Integer> keyCountState;
        // 其他类型状态的声明
        private ListState<String> myListState;
        private MapState<String, Double> myMapState;
        private ReducerState<SensorReading, SensorReading> myReducerState;
        private AggregatingState<String, String> myAggState;


        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<>("key-count", Integer.class, 0));
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<>("list-state", String.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("map-state", String.class, Double.class));
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            keyCountState.clear();
            super.close();
        }

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            Integer count = keyCountState.value();
            count++;
            keyCountState.update(count);
            return count;

        }
    }

}
