package com.bzh.bigdata.domain;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @description:
 * @author: bizhihao
 * @createDate: 2020/5/20
 * @version: 1.0
 */
public class WordCount {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env  = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = env.fromElements("this a book", "i love china", "i am chinese");

        AggregateOperator<Tuple2<String, Integer>> dataSink = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {


            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String s : line.split(" ")) {
                    collector.collect(new Tuple2<>(s, 1));
                }
            }
        }).groupBy(0).sum(1);

        dataSink.print();
    }
}
