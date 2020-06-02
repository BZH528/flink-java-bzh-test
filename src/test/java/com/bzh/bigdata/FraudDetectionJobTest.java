package com.bzh.bigdata;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * @description:
 * @author: bizhihao
 * @createDate: 2020/6/1
 * @version: 1.0
 */
public class FraudDetectionJobTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 通过TransactionSource类 获取无界的数据
        DataStream<Transaction> transactions = env.addSource(new TransactionSource()).name("transactions");

        transactions.print();

        env.execute("Fraud Detection");
    }
}
