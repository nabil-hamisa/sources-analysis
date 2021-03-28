package com.bigdata.streaming.flink.mystudy.stream.state;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class IpEventStateMachineDemo {

    @Test
    public void testIpEvnentGenerator(){
        IpEventGeneratorSource ipEventGeneratorSource = new IpEventGeneratorSource(Math.random(),100);
        int min = 100;
        int max = 200;
        for (int i = 0; i < 10000; i++) {
            IpEvent next = ipEventGeneratorSource.next(min, max);
            System.out.println(next);
        }

    }


    @Test
    public void testIpEventStateMachine() throws Exception {
        final SourceFunction<IpEvent> source =new IpEventGeneratorSource(Math.random(),100);
        // E:\ws\new-idea-ws\stream-study-copy2\flink-study\checkpointDir
        String checkpointDir = "file:/E:/ws/new-idea-ws/stream-study-copy2/flink-study/checkpointDir";
        boolean asyncCheckpoints = true;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000L);
        env.setStateBackend(new FsStateBackend(checkpointDir, asyncCheckpoints));
        DataStream<IpEvent> eventDS = env.addSource(source);
        DataStream<Alert> alter = eventDS.keyBy("eventType")
                .flatMap(new RichFlatMapFunction<IpEvent, Alert>() {
                    private ValueState<State> currentState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        RuntimeContext ctx = getRuntimeContext();
                        ValueStateDescriptor<State> stateDescriptor = new ValueStateDescriptor<>("state", State.class);
                        ValueState<State> state = ctx.getState(stateDescriptor);
                        currentState = state;
                    }
                    @Override
                    public void flatMap(IpEvent evt, Collector<Alert> out) throws Exception {
                        State state = currentState.value();
                        if (state == null) {
                            state = State.Initial;
                        }
                        State nextState = state.transition(evt.getEventType());
                        if (nextState == State.InvalidTransition) {
                            out.collect(new Alert(evt.getSourceAddress(), state, evt.getEventType()));
                        } else if (nextState.isTerminal()) {
                            currentState.clear();
                        } else {
                            currentState.update(nextState);
                        }
                    }

                });
        alter.print();
        alter.writeAsText("output/state/state-out", FileSystem.WriteMode.OVERWRITE);
        env.execute(this.getClass().getSimpleName());

    }

}
