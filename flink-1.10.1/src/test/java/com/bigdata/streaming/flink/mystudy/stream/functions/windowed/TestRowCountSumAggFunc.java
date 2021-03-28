package com.bigdata.streaming.flink.mystudy.stream.functions.windowed;

import com.bigdata.streaming.flink.mystudy.JavaBaseTest;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.junit.Test;

public class TestRowCountSumAggFunc extends JavaBaseTest {


    @Test
    public void testAggregateFunc_timeWindow_RowCountSumAggFunc() throws Exception {

        eventItemDS
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        Object category = value.getField(1);
                        return category.toString();
                    }
                })
                .timeWindow(Time.minutes(1))
                .aggregate(new RowCountSumAggFunc())
                .print()
        ;

    }



}
