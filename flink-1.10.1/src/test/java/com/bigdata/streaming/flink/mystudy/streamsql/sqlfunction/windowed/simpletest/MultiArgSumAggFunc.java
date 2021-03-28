package com.bigdata.streaming.flink.mystudy.streamsql.sqlfunction.windowed.simpletest;

import org.apache.flink.table.functions.AggregateFunction;

public class MultiArgSumAggFunc extends AggregateFunction<Long,MultiArgSumAcc> {

    public MultiArgSumAggFunc() {
        System.out.println(this);
    }

    @Override
    public MultiArgSumAcc createAccumulator() {
        MultiArgSumAcc multiArgSumAcc = new MultiArgSumAcc(0);
        return multiArgSumAcc;
    }

    // 在Sql中 调用该类,会触发 this.accumulate(arg1,arg2);
    public void accumulate(MultiArgSumAcc acc, int arg1,int arg2){
        long rowSum = arg1 + arg2;
        acc.add(rowSum);
    }

    // 在sql中, 当事务失败需要回滚, 则把刚 把结果回滚;
    public void retract(MultiArgSumAcc acc,int arg1,int arg2){
        long rowSum = arg1 + arg2;
        acc.count -= rowSum;
    }

    @Override
    public Long getValue(MultiArgSumAcc accumulator) {
        if(null != accumulator){
            return accumulator.getCount();
        }
        return null;
    }

}
