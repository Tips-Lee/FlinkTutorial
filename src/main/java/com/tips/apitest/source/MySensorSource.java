package com.tips.apitest.source;

import com.tips.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class MySensorSource implements SourceFunction<SensorReading> {

    private boolean running = true;
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        Random random = new Random();
        HashMap<String, Double> sensorTempMap = new HashMap<String, Double>();
        for (int i = 0; i < 10; i++) {
            sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
        }

        while (running){
            for (String id:sensorTempMap.keySet()) {
                Double newTemp = sensorTempMap.get(id) + random.nextGaussian();
                sensorTempMap.put(id, newTemp);
                ctx.collect(new SensorReading(id, System.currentTimeMillis(), newTemp));
            }

            Thread.sleep(2000L);
        }
    }

    public void cancel() {
        running = false;
    }
}
