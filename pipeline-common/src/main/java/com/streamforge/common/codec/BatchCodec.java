package com.streamforge.common.codec;

import com.streamforge.proto.EncodingType;
import com.streamforge.proto.SensorEvent;

import java.util.List;

public interface BatchCodec {
    EncodingType getEncodingType();

    byte[] encode(List<SensorEvent> events);

    List<SensorEvent> decode(byte[] encodedBytes);
}
