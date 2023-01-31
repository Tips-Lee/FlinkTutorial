package com.tips.wc;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
        // LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1);
        /*String[] ss = new String[1];
        ss[0] = "target/FlinkTutorial-1.0.0.jar";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("node-4", 6123, ss );*/

        // env.setParallelism(3);
        // env.disableOperatorChaining()
        ParameterTool pt = ParameterTool.fromArgs(args);
        String host = pt.get("host");
        int port = pt.getInt("port");
        DataStream<String> stringDataStream = env.socketTextStream(host, port);
        DataStream<Tuple2<String, Integer>> sum = stringDataStream.flatMap(new WordCount.FlatMapImpl())
                .keyBy(0)
                .sum(1).setParallelism(2);//.startNewChain(); // .slotSharingGroup("A")
        sum.print().setParallelism(1);
        env.execute();
    }


}


