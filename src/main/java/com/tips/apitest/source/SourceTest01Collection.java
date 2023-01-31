package com.tips.apitest.source;

import com.tips.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.*;

public class SourceTest01Collection {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStream<SensorReading> dataStream = env.fromCollection(getList());

        DataStream<SensorReading> dataStreamElement = env.fromElements(
                new SensorReading("sensor_1", 1547718201L, 15.8),
                new SensorReading("sensor_2", 1547718202L, 15.2),
                new SensorReading("sensor_3", 1547718203L, 16.5),
                new SensorReading("sensor_4", 1547718204L, 14.7));

        dataStream.print("Collection");
        dataStreamElement.print("Elements");
        env.execute();
    }



    public static List<SensorReading> getList(){
        return Arrays.asList(
                new SensorReading("sensor_1", 1547718201L, 15.8),
                new SensorReading("sensor_2", 1547718202L, 15.2),
                new SensorReading("sensor_3", 1547718203L, 16.5),
                new SensorReading("sensor_4", 1547718204L, 14.7)
        );
    }


}
