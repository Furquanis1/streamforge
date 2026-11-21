package com.streamforge.common.codec;

import com.streamforge.proto.EncodingType;
import com.streamforge.proto.SensorEvent;
import com.streamforge.proto.SensorEventBatch;

import java.util.ArrayList;
import java.util.List;

public final class PlainRowCodec implements BatchCodec {
    @Override
    public EncodingType getEncodingType() {
        return EncodingType.PLAIN_ROW;
    }

    @Override
    public byte[] encode(List<SensorEvent> events) {
        SensorEventBatch batch = SensorEventBatch.newBuilder()
                .addAllEvents(events)
                .build();
        return batch.toByteArray();
    }

    @Override
    public List<SensorEvent> decode(byte[] encodedBytes) {
        try {
            SensorEventBatch batch = SensorEventBatch.parseFrom(encodedBytes);
            return new ArrayList<>(batch.getEventsList());
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid plain-row encoded batch", e);
        }
    }
}
