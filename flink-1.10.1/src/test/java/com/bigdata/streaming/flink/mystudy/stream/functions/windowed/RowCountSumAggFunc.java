package com.bigdata.streaming.flink.mystudy.stream.functions.windowed;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.types.Row;

public class RowCountSumAggFunc implements AggregateFunction<Row, Row, Row> {

    @Override
    public Row createAccumulator() {
        return Row.of(0L,0.0);
    }

    @Override
    public Row add(Row value, Row accumulator) {
        Object field4 = value.getField(4);
        Object field5 = value.getField(5);
        if(null == field4 || null == field5){
            return accumulator;
        }
        int num = (int)field4;
        double price = (double)field5;
        if(num <=0 || price <=0){
            return accumulator;
        }
        double account = num * price;
        long newCount = 1 + (long)accumulator.getField(0);
        double newAccount = account + (double) accumulator.getField(1);
        return Row.of(newCount,newAccount);
    }

    @Override
    public Row getResult(Row accumulator) {
        return accumulator;
    }

    @Override
    public Row merge(Row a, Row b) {
        long newCount = (long)a.getField(0) + (long)b.getField(0);
        double newAccount =  (double) a.getField(1) + (double) b.getField(1);
        return Row.of(newCount,newAccount);
    }

}
