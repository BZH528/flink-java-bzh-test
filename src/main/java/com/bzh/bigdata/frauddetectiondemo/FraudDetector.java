package com.bzh.bigdata.frauddetectiondemo;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

/**
 * @description:
 * @author: bizhihao
 * @createDate: 2020/5/29
 * @version: 1.0
 */
public class FraudDetector extends KeyedProcessFunction<Long,Transaction,Alert> {

    private static final long serialVersionUID = 1L;

    private transient ValueState<Boolean> flagState;
    // 定时器状态，对于上边的例子，交易 3 和 交易 4 只有间隔在一分钟之内才被认为是欺诈交易
    private transient ValueState<Long> timerState;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    @Override
    public void open(Configuration parameters) throws Exception {

        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<Boolean>("flag", Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDescriptor);

        // 注册状态
        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<Long>("time-state", Types.LONG);
        timerState = getRuntimeContext().getState(timerDescriptor);

    }

    @Override
    public void processElement(Transaction transaction, Context context, Collector<Alert> collector) throws Exception {

        // 获取当前key的状态
        Boolean lastTransactionWasSmall = flagState.value();


        if (lastTransactionWasSmall != null) {
            if (transaction.getAmount() > LARGE_AMOUNT) {
                Alert alert = new Alert();
                alert.setId(transaction.getTimestamp());

                collector.collect(alert);
            }

            // 清除该key状态
            cleanUp(context);

        }

        if (transaction.getAmount() < SMALL_AMOUNT) {
            flagState.update(true);

            // 设置定时器 和 定时器状态
            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            context.timerService().registerProcessingTimeTimer(timer);
            timerState.update(timer);
        }



    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        timerState.clear();
        flagState.clear();
    }

    private void cleanUp(Context ctx) throws Exception {
        // delete timer
        Long timer = timerState.value();
        ctx.timerService().deleteProcessingTimeTimer(timer);

        // clean up all state
        timerState.clear();
        flagState.clear();
    }
}
