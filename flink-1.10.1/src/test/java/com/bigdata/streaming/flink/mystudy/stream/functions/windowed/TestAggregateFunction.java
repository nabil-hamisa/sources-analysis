package com.bigdata.streaming.flink.mystudy.stream.functions.windowed;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class TestAggregateFunction {

    @Test
    public void testAggregateFunc_sum() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000L);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings setting = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,setting);

        TableSchema tableSchema = TableSchema.builder()
                .field("id", DataTypes.INT())
                .field("localtime", DataTypes.TIMESTAMP())
                .field("ts", DataTypes.BIGINT())
                .field("num", DataTypes.DOUBLE())
                .build();

        env.fromElements(
                Row.of(1, System.currentTimeMillis(), 3.14),
                Row.of(2, System.currentTimeMillis(), 5.14),
                Row.of(3, System.currentTimeMillis(), 10.14)
        )
                .map(new MapFunction<Row, Row>() {
                    @Override
                    public Row map(Row value) throws Exception {
                        Object time = value.getField(1);
                        LocalDateTime datetime = LocalDateTime.ofInstant(Instant.ofEpochMilli((long) time), ZoneId.systemDefault());
                        return Row.of(value.getField(0),time,value.getField(2),datetime);
                    }
                })
                .setParallelism(1)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(Row element) {
                        Object time = element.getField(1);
                        return (long)time;
                    }
                })
                .map(new MapFunction<Row, Tuple3<Integer,String,Double>>() {
                    @Override
                    public Tuple3<Integer, String, Double> map(Row value) throws Exception {
                        LocalDateTime time = (LocalDateTime) value.getField(3);
                        String format = time.format(DateTimeFormatter.ISO_DATE_TIME);
                        return new Tuple3<>((int)value.getField(0),format,(double)value.getField(2));
                    }
                })
                .keyBy(0)
                .sum(2)
                .print();

        env.execute(this.getClass().getSimpleName());
    }


    @Test
    public void testAggregateFunc_timeWindow_mySumAggregateFunc() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000L);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings setting = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,setting);

        env.fromElements(
                //      0              1                2                 3    4    5
                Row.of("Item001",   "Electronic",  "2020-11-11 10:01:00", 0L,  1,  98.01),
                Row.of("Item002",   "Electronic",  "2020-11-11 10:01:59", 0L,  2,  100.22),

                Row.of("Item003",   "Electronic",  "2020-11-11 10:02:00", 0L,  1,  102.03),

                Row.of("Item004",   "Food",        "2020-11-11 10:04:00", 0L,  2,  10.01),
                Row.of("Item005",   "Food",        "2020-11-11 10:04:01", 0L,  1,  9.03)
        )
                .map(new MapFunction<Row, Row>() {
                    private  ZoneOffset zoneOffset = ZoneId.systemDefault().getRules().getOffset(Instant.now());
                    @Override
                    public Row map(Row value) throws Exception {
                        String timeStr = (String)value.getField(2);
                        long ts = LocalDateTime.parse(timeStr,DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toInstant(zoneOffset).toEpochMilli();

                        value.setField(3,ts);
                        return value;
                    }
                })
                .setParallelism(1)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.minutes(1)) {
                    @Override
                    public long extractTimestamp(Row element) {
                        Object time = element.getField(3);
                        return (long)time;
                    }
                })

                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        Object category = value.getField(1);
                        return category.toString();
                    }
                })
                .timeWindow(Time.minutes(1))
                .aggregate(new AggregateFunction<Row, Double, Double>() {
                    @Override
                    public Double createAccumulator() {
                        return 0.0;
                    }

                    @Override
                    public Double add(Row value, Double accumulator) {
                        int num = (int)value.getField(4);
                        double price = (double)value.getField(5);
                        double account = num * price;
                        return  account + accumulator;
                    }

                    @Override
                    public Double getResult(Double accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Double merge(Double a, Double b) {
                        return a + b;
                    }
                })
                .print()


        ;


        env.execute(this.getClass().getSimpleName());
    }



    @Test
    public void testAggregateFunc_timeWindow_myCountSumAggFuncImpl() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000L);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings setting = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,setting);

        env.fromElements(
                //      0              1                2                 3    4    5
                Row.of("Item001",   "Electronic",  "2020-11-11 10:01:00", 0L,  1,  98.01),
                Row.of("Item002",   "Electronic",  "2020-11-11 10:01:59", 0L,  2,  100.22),

                Row.of("Item003",   "Electronic",  "2020-11-11 10:02:00", 0L,  1,  102.03),

                Row.of("Item004",   "Food",        "2020-11-11 10:04:00", 0L,  2,  10.01),
                Row.of("Item005",   "Food",        "2020-11-11 10:04:01", 0L,  1,  9.03)
        )
                .map(new MapFunction<Row, Row>() {
                    private  ZoneOffset zoneOffset = ZoneId.systemDefault().getRules().getOffset(Instant.now());
                    @Override
                    public Row map(Row value) throws Exception {
                        String timeStr = (String)value.getField(2);
                        long ts = LocalDateTime.parse(timeStr,DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toInstant(zoneOffset).toEpochMilli();

                        value.setField(3,ts);
                        return value;
                    }
                })
                .setParallelism(1)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.minutes(1)) {
                    @Override
                    public long extractTimestamp(Row element) {
                        Object time = element.getField(3);
                        return (long)time;
                    }
                })

                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        Object category = value.getField(1);
                        return category.toString();
                    }
                })
                .timeWindow(Time.minutes(1))
                .aggregate(new AggregateFunction<Row, Row, Row>() {
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
                })
                .print()


                ;


        env.execute(this.getClass().getSimpleName());
    }


    @Test
    public void testAggregateFunc_timeWindow_RowCountSumAggFunc() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000L);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings setting = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,setting);

        env.fromElements(
                //      0              1                2                 3    4    5
                Row.of("Item001",   "Electronic",  "2020-11-11 10:01:00", 0L,  1,  98.01),
                Row.of("Item002",   "Electronic",  "2020-11-11 10:01:59", 0L,  2,  100.22),

                Row.of("Item003",   "Electronic",  "2020-11-11 10:02:00", 0L,  1,  102.03),

                Row.of("Item004",   "Food",        "2020-11-11 10:04:00", 0L,  2,  10.01),
                Row.of("Item005",   "Food",        "2020-11-11 10:04:01", 0L,  1,  9.03)
        )
                .map(new MapFunction<Row, Row>() {
                    private  ZoneOffset zoneOffset = ZoneId.systemDefault().getRules().getOffset(Instant.now());
                    @Override
                    public Row map(Row value) throws Exception {
                        String timeStr = (String)value.getField(2);
                        long ts = LocalDateTime.parse(timeStr,DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toInstant(zoneOffset).toEpochMilli();

                        value.setField(3,ts);
                        return value;
                    }
                })
                .setParallelism(1)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.minutes(1)) {
                    @Override
                    public long extractTimestamp(Row element) {
                        Object time = element.getField(3);
                        return (long)time;
                    }
                })

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

        env.execute(this.getClass().getSimpleName());
    }



}
