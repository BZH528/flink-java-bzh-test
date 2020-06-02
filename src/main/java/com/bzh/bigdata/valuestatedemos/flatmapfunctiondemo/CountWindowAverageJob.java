package com.bzh.bigdata.valuestatedemos.flatmapfunctiondemo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description: 使用 flink Managed Keyed State
 * @author: bizhihao
 * @createDate: 2020/6/1
 * @version: 1.0
 */
public class CountWindowAverageJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Long, Long>> source = env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 9L), Tuple2.of(1L, 2L), Tuple2.of(1L, 4L));

        DataStream<Tuple2<Long, Long>> sinkData = source.keyBy(0)
                .flatMap(new CountWindowAverage());

        sinkData.print();

        env.execute("CountWindowAverageJob");
    }

}
