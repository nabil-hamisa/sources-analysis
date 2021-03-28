package com.bigdata.streaming.flink.mystudy.stream.state;

public class IpEvent {

    private EventType eventType;
    private int sourceAddress;

    public IpEvent(){}

    public IpEvent(EventType eventType, int sourceAddress) {
        this.eventType = eventType;
        this.sourceAddress = sourceAddress;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public int getSourceAddress() {
        return sourceAddress;
    }

    public void setSourceAddress(int sourceAddress) {
        this.sourceAddress = sourceAddress;
    }

    @Override
    public String toString() {

        return "{" +
                "eventType=" + eventType +
                ", sourceAddress=" + sourceAddress +
                '}';
    }
}


enum EventType{
    a, b, c, d, e, f, g;
}

