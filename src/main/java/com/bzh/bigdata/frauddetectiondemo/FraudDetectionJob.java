package com.bzh.bigdata.frauddetectiondemo;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * @description: Flink官网案例 针对可疑信用卡交易行为的反欺诈检测系统
 * @author: bizhihao
 * @createDate: 2020/5/29
 * @version: 1.0
 *
 * url：https://ci.apache.org/projects/flink/flink-docs-release-1.10/zh/getting-started/walkthroughs/datastream_api.html
 */
public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 通过TransactionSource类 获取无界的数据
        DataStream<Transaction> transactions = env.addSource(new TransactionSource()).name("transactions");

        //
        DataStream<Alert> alerts = transactions.keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("fraud-detector");

        alerts.addSink(new AlertSink())
                .name("send-alerts");

        env.execute("Fraud Detection");
    }
}
