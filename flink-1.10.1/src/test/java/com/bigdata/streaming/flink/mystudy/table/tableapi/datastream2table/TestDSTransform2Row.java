package com.bigdata.streaming.flink.mystudy.table.tableapi.datastream2table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.*;

public class TestDSTransform2Row implements Serializable {



    @Test
    public void testTransformDataStream_2_Table_map() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000L);

        DataStreamSource<String> strDS = env.fromElements(
                "Allen,3,3.14",
                "David,5,3.14",
                "Lily,3,6.14"
        );

        TableSchema tableSchema = TableSchema.builder()
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.STRING())
                .field("num", DataTypes.STRING())
                .build();

        // 这里的 map()方法对Row对象, 必须传入 outputType: tableSchema.toRowType() 来指定输出类型;
        SingleOutputStreamOperator<Row> rowMap = strDS.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                String[] split = value.split(",");
                return Row.of(split[0],split[1],split[2]);
            }
        },tableSchema.toRowType());
        //
        DataStream<Row> rowDS = rowMap;
        rowDS.print();
        TypeInformation<Row> type = rowDS.getType();
        System.out.println(type);

        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);
        String tableName = "tb_test";
        stEnv.registerDataStream(tableName,rowDS);

        Table resultTable = stEnv.sqlQuery("select * from "+tableName);
        resultTable.printSchema();
        DataStream<Row> resultDS = stEnv.toAppendStream(resultTable, Row.class);
        resultDS.print();

        env.execute(this.getClass().getSimpleName());


    }

    @Test
    public void testTransformDataStream_2_Table_tranform() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000L);

        DataStreamSource<String> strDS = env.fromElements(
                "Allen,3,3.14",
                "David,5,3.14",
                "Lily,3,6.14"
        );

        TableSchema tableSchema = TableSchema.builder()
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.STRING())
                .field("num", DataTypes.STRING())
                .build();

        // 这里的 map()方法对Row对象, 必须传入 outputType: tableSchema.toRowType() 来指定输出类型;
        SingleOutputStreamOperator<Row> rowMap2 = strDS.transform("Map", tableSchema.toRowType(), new StreamMap(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                String[] split = value.split(",");
                return Row.of(split[0], split[1], split[2]);
            }
        }));


        DataStream<Row> rowDS = rowMap2;
        rowDS.print();
        TypeInformation<Row> type = rowDS.getType();
        System.out.println(type);

        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);
        String tableName = "tb_test";
        stEnv.registerDataStream(tableName,rowDS);

        Table resultTable = stEnv.sqlQuery("select * from "+tableName);
        resultTable.printSchema();
        DataStream<Row> resultDS = stEnv.toAppendStream(resultTable, Row.class);
        resultDS.print();

        env.execute(this.getClass().getSimpleName());


    }

    @Test
    public void Pojo2Table_UserAsRow_rowtime_AscendingTimestamps() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000L);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);

        long currentTimeMillis = System.currentTimeMillis();


        TableSchema tableSchema = TableSchema.builder()
                .field("ts", DataTypes.BIGINT())
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT())
                .build();

        DataStream<Row> ds = env.fromElements(
                new Tuple3<>(currentTimeMillis,"Huawei",30),
                new Tuple3<>(10000+currentTimeMillis,"iPhone",40),
                new Tuple3<>(80000+currentTimeMillis,"Huawei",30)
        )
                .map(new MapFunction<Tuple3<Long, String, Integer>, Row>() {
                    @Override
                    public Row map(Tuple3<Long, String, Integer> value) throws Exception {
                        Row row = Row.of(value.f0, value.f1, value.f2);
                        return row;
                    }
                },tableSchema.toRowType())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
                    @Override
                    public long extractAscendingTimestamp(Row element) {
                        Object time = element.getField(0);
                        return (long)time;
                    }
                })
                ;



        String tableName = "tb_user";
        stEnv.createTemporaryView(tableName, ds, "ts, name, age, rowtime.rowtime");

        Table user = stEnv.from(tableName);
        user.printSchema();
        stEnv.toAppendStream(user,Row.class).print();

        // compute SUM(amount) per day (in event-time)
        Table result1 = stEnv.sqlQuery(
                "SELECT " +
                        "  TUMBLE_START(rowtime, INTERVAL '1' second) as winStart,  " +
                        "  COUNT(*) FROM  " + tableName+ " "+
                        "GROUP BY TUMBLE(rowtime, INTERVAL '1' second)");
        result1.printSchema();
        stEnv.toRetractStream(result1, Row.class).print();


        env.execute(this.getClass().getSimpleName());


    }



    @Test
    public void testTransformRowDS2Table_eventTimeAsWatermark_sqlTimestamp() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000L);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> strDS = env.fromElements(
                "Allen,3,3.14",
                "David,5,3.14",
                "Lily,3,6.14"
        );

        TableSchema tableSchema = TableSchema.builder()
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.STRING())
                .field("num", DataTypes.STRING())
                .field("ts", DataTypes.TIMESTAMP().bridgedTo(Timestamp.class))
//                .field("ts", DataTypes.TIMESTAMP())
                .build();

        // 这里的 map()方法对Row对象, 必须传入 outputType: tableSchema.toRowType() 来指定输出类型;
        DataStream<Row> rowDS  = strDS.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                String[] split = value.split(",");
                LocalDateTime now = LocalDateTime.now();
                Timestamp ts = Timestamp.valueOf(now);
                return Row.of(split[0], split[1], split[2], ts);
            }
        }, tableSchema.toRowType())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(Row element) {
                        Object ts = element.getField(3);
                        return ((Timestamp)ts).getTime();
                    }
                })
                ;

        rowDS.print();
        TypeInformation<Row> type = rowDS.getType();
        System.out.println(type);

        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);
        String tableName = "tb_test";
        stEnv.createTemporaryView(tableName,rowDS,"name,age,num,ts,rowtime.rowtime");


        Table resultTable = stEnv.sqlQuery("select name, tumble_start(rowtime, interval '1' second) as winStart \n" +
                " from "+tableName+" group by name, tumble(rowtime, interval '1' second)");
        resultTable.printSchema();
        stEnv.toRetractStream(resultTable, Row.class)
                .print();

        env.execute(this.getClass().getSimpleName());


    }


    @Test
    public void testTransformRowDS2Table_eventTimeAsWatermark_localDatetime() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000L);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> strDS = env.fromElements(
                "Allen,3,3.14",
                "David,5,3.14",
                "Lily,3,6.14"
        );

        TableSchema tableSchema = TableSchema.builder()
                .field("name", DataTypes.STRING())
                .field("age_01", DataTypes.STRING())
                .field("num", DataTypes.STRING())
//                .field("ts", DataTypes.TIMESTAMP().bridgedTo(Timestamp.class))
                .field("ts", DataTypes.TIMESTAMP())
                .build();

        // 这里的 map()方法对Row对象, 必须传入 outputType: tableSchema.toRowType() 来指定输出类型;
        DataStream<Row> rowDS  = strDS.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                String[] split = value.split(",");
                LocalDateTime now = LocalDateTime.now();
                Timestamp ts = Timestamp.valueOf(now);
                return Row.of(split[0], split[1], split[2], now);
            }
        }, tableSchema.toRowType())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(Row element) {
                        Object ts = element.getField(3);
                        ZoneOffset zoneOffset = ZoneId.systemDefault().getRules().getOffset(Instant.now());
                        long epochMilli = ((LocalDateTime)ts).toInstant(zoneOffset).toEpochMilli();
                        return epochMilli;
                    }
                })
                ;

        rowDS.print();
        TypeInformation<Row> type = rowDS.getType();
        System.out.println(type);

        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);
        String tableName = "tb_test";
        stEnv.createTemporaryView(tableName,rowDS,"name,age_01,num,ts,rowtime.rowtime");


        Table resultTable = stEnv.sqlQuery("select name, tumble_start(rowtime, interval '1' second) as winStart \n" +
                " from "+tableName+" group by name, tumble(rowtime, interval '1' second)");
        resultTable.printSchema();
        stEnv.toRetractStream(resultTable, Row.class)
                .print();

        env.execute(this.getClass().getSimpleName());


    }


}
