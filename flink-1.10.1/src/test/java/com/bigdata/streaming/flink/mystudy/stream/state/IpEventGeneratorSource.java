package com.bigdata.streaming.flink.mystudy.stream.state;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import static org.apache.flink.util.Preconditions.checkArgument;

public class IpEventGeneratorSource extends RichParallelSourceFunction<IpEvent> {

    private final double errorProbability;

    private final int delayPerRecordMillis;

    private volatile boolean running = true;

    private  Random rnd;
    private  LinkedHashMap<Integer, State> states;

    public IpEventGeneratorSource(double errorProbability, int delayPerRecordMillis) {
        checkArgument(errorProbability >= 0.0 && errorProbability <= 1.0, "error probability must be in [0.0, 1.0]");
        checkArgument(delayPerRecordMillis >= 0, "deplay must be >= 0");

        this.errorProbability = errorProbability;
        this.delayPerRecordMillis = delayPerRecordMillis;
        this.rnd = new Random();
        this.states = new LinkedHashMap<>();
    }

    public IpEvent next(int minIp, int maxIp) {
        final double p = rnd.nextDouble();
        if (p * 1000 >= states.size()) {
            final int nextIP = rnd.nextInt(maxIp - minIp) + minIp;
            if (!states.containsKey(nextIP)) {
                Transition transition = State.Initial.randomTransition(rnd);
                State state = transition.targetState();
                states.put(nextIP, state);
                EventType eventType = transition.eventType();
                return new IpEvent(eventType, nextIP);
            }else {
                // collision on IP address, try again
                return next(minIp, maxIp);
            }
        } else {
            int numToSkip = Math.min(20, rnd.nextInt(states.size()));
            Iterator<Map.Entry<Integer, State>> iter = states.entrySet().iterator();
            for (int i = numToSkip; i > 0; --i) {
                iter.next();
            }

            Map.Entry<Integer, State> entry = iter.next();
            State currentState = entry.getValue();
            int address = entry.getKey();

            iter.remove();

            if (p < errorProbability) {
                EventType event = currentState.randomInvalidTransition(rnd);
                return new IpEvent(event, address);
            } else {
                Transition transition = State.Initial.randomTransition(rnd);
                if (!transition.targetState().isTerminal()) {
                    states.put(address, transition.targetState());
                }
                return new IpEvent(transition.eventType(), address);
            }
        }
    }


    @Override
    public void run(SourceContext<IpEvent> sourceContext) throws Exception {
//        final EventsGenerator generator = new EventsGenerator(errorProbability);


        final int range = Integer.MAX_VALUE / getRuntimeContext().getNumberOfParallelSubtasks();
        final int min = range * getRuntimeContext().getIndexOfThisSubtask();
        final int max = min + range;

        while (running) {
            sourceContext.collect(next(min, max));

            if (delayPerRecordMillis > 0) {
                Thread.sleep(delayPerRecordMillis);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

}