package com.tips.wc;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.File;

public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env =  ExecutionEnvironment.getExecutionEnvironment();
        String path = "src/main/resources/hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(path);
        DataSet<Tuple2<String, Integer>> sum = inputDataSet.flatMap(new FlatMapImpl())
                .groupBy(0)
                .sum(1);
        sum.print();
    }

    public static class FlatMapImpl implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }
    }
}


