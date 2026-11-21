package com.streamforge.common.codec;

import com.streamforge.proto.EncodingType;
import com.streamforge.proto.SensorEvent;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ColumnarDeltaCodec implements BatchCodec {
    private static final int MAGIC = 0x53464344;
    private static final int VERSION = 1;
    private static final int MAX_RECORDS = 65535;

    @Override
    public EncodingType getEncodingType() {
        return EncodingType.COLUMNAR_DELTA;
    }

    @Override
    public byte[] encode(List<SensorEvent> events) {
        if (events == null) {
            throw new IllegalArgumentException("Events must not be null");
        }
        if (events.size() > MAX_RECORDS) {
            throw new IllegalArgumentException("Columnar codec supports at most 65535 records");
        }

        try (ByteArrayOutputStream output = new ByteArrayOutputStream();
             DataOutputStream data = new DataOutputStream(output)) {
            data.writeInt(MAGIC);
            data.writeInt(VERSION);
            data.writeInt(events.size());

            Map<String, Integer> nodeIds = dictionary(events, true);
            Map<String, Integer> sensorTypes = dictionary(events, false);
            writeDictionary(data, nodeIds);
            for (SensorEvent event : events) {
                data.writeShort(nodeIds.get(event.getNodeId()));
            }
            writeDictionary(data, sensorTypes);
            for (SensorEvent event : events) {
                data.writeShort(sensorTypes.get(event.getSensorType()));
            }

            if (!events.isEmpty()) {
                long baseTimestamp = events.get(0).getTimestampMs();
                data.writeLong(baseTimestamp);
                if (events.size() > 1) {
                    long firstDelta = events.get(1).getTimestampMs() - baseTimestamp;
                    data.writeLong(firstDelta);
                    long previousDelta = firstDelta;
                    for (int i = 2; i < events.size(); i++) {
                        long timestampDelta =
                                events.get(i).getTimestampMs() - events.get(i - 1).getTimestampMs();
                        writeVarLong(data, timestampDelta - previousDelta);
                        previousDelta = timestampDelta;
                    }
                }

                data.writeLong(Double.doubleToRawLongBits(events.get(0).getValue()));
                long previousBits = Double.doubleToRawLongBits(events.get(0).getValue());
                for (int i = 1; i < events.size(); i++) {
                    long currentBits = Double.doubleToRawLongBits(events.get(i).getValue());
                    writeXor(data, previousBits ^ currentBits);
                    previousBits = currentBits;
                }
            }

            int anomalyBytes = (events.size() + 7) / 8;
            for (int byteIndex = 0; byteIndex < anomalyBytes; byteIndex++) {
                int packed = 0;
                for (int bit = 0; bit < 8; bit++) {
                    int recordIndex = byteIndex * 8 + bit;
                    if (recordIndex < events.size() && events.get(recordIndex).getAnomaly()) {
                        packed |= 1 << bit;
                    }
                }
                data.writeByte(packed);
            }

            data.flush();
            return output.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Could not encode columnar batch", e);
        }
    }

    @Override
    public List<SensorEvent> decode(byte[] encodedBytes) {
        if (encodedBytes == null) {
            throw new IllegalArgumentException("Encoded bytes must not be null");
        }
        try (DataInputStream data =
                     new DataInputStream(new ByteArrayInputStream(encodedBytes))) {
            int magic = data.readInt();
            int version = data.readInt();
            int recordCount = data.readInt();
            if (magic != MAGIC) {
                throw new IllegalArgumentException("Invalid columnar codec magic");
            }
            if (version != VERSION) {
                throw new IllegalArgumentException("Unsupported columnar codec version: " + version);
            }
            if (recordCount < 0) {
                throw new IllegalArgumentException("Invalid negative record count");
            }

            String[] nodeIds = readDictionary(data);
            int[] nodeIndexes = new int[recordCount];
            for (int i = 0; i < recordCount; i++) {
                int index = data.readUnsignedShort();
                validateDictionaryIndex(index, nodeIds.length);
                nodeIndexes[i] = index;
            }

            String[] sensorTypes = readDictionary(data);
            int[] sensorIndexes = new int[recordCount];
            for (int i = 0; i < recordCount; i++) {
                int index = data.readUnsignedShort();
                validateDictionaryIndex(index, sensorTypes.length);
                sensorIndexes[i] = index;
            }

            long[] timestamps = new long[recordCount];
            if (recordCount > 0) {
                timestamps[0] = data.readLong();
                if (recordCount > 1) {
                    long previousDelta = data.readLong();
                    timestamps[1] = timestamps[0] + previousDelta;
                    for (int i = 2; i < recordCount; i++) {
                        long timestampDelta =
                                previousDelta + readVarLong(data);
                        timestamps[i] = timestamps[i - 1] + timestampDelta;
                        previousDelta = timestampDelta;
                    }
                }
            }

            long[] rawValueBits = new long[recordCount];
            if (recordCount > 0) {
                rawValueBits[0] = data.readLong();
                for (int i = 1; i < recordCount; i++) {
                    rawValueBits[i] = rawValueBits[i - 1] ^ readXor(data);
                }
            }

            boolean[] anomalies = new boolean[recordCount];
            int anomalyBytes = (recordCount + 7) / 8;
            for (int byteIndex = 0; byteIndex < anomalyBytes; byteIndex++) {
                int packed = data.readUnsignedByte();
                for (int bit = 0; bit < 8; bit++) {
                    int recordIndex = byteIndex * 8 + bit;
                    if (recordIndex < recordCount) {
                        anomalies[recordIndex] = (packed & (1 << bit)) != 0;
                    }
                }
            }

            List<SensorEvent> events = new ArrayList<>(recordCount);
            for (int i = 0; i < recordCount; i++) {
                events.add(SensorEvent.newBuilder()
                        .setNodeId(nodeIds[nodeIndexes[i]])
                        .setSensorType(sensorTypes[sensorIndexes[i]])
                        .setTimestampMs(timestamps[i])
                        .setValue(Double.longBitsToDouble(rawValueBits[i]))
                        .setAnomaly(anomalies[i])
                        .build());
            }
            return events;
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid columnar encoded batch", e);
        }
    }

    private static Map<String, Integer> dictionary(List<SensorEvent> events, boolean nodes) {
        Map<String, Integer> dictionary = new LinkedHashMap<>();
        for (SensorEvent event : events) {
            String value = nodes ? event.getNodeId() : event.getSensorType();
            if (!dictionary.containsKey(value)) {
                if (dictionary.size() >= 65536) {
                    throw new IllegalArgumentException("Columnar codec supports at most 65536 dictionary entries");
                }
                dictionary.put(value, dictionary.size());
            }
        }
        return dictionary;
    }

    private static void writeDictionary(DataOutputStream data, Map<String, Integer> dictionary)
            throws IOException {
        data.writeInt(dictionary.size());
        for (String value : dictionary.keySet()) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            data.writeInt(bytes.length);
            data.write(bytes);
        }
    }

    private static String[] readDictionary(DataInputStream data) throws IOException {
        int count = data.readInt();
        if (count < 0) {
            throw new IOException("Invalid dictionary count");
        }
        String[] values = new String[count];
        for (int i = 0; i < count; i++) {
            int length = data.readInt();
            if (length < 0) {
                throw new IOException("Invalid dictionary string length");
            }
            byte[] bytes = data.readNBytes(length);
            if (bytes.length != length) {
                throw new IOException("Truncated dictionary string");
            }
            values[i] = new String(bytes, StandardCharsets.UTF_8);
        }
        return values;
    }

    private static void validateDictionaryIndex(int index, int dictionarySize) {
        if (index >= dictionarySize) {
            throw new IllegalArgumentException("Dictionary index out of range: " + index);
        }
    }

    private static void writeVarLong(DataOutputStream data, long value) throws IOException {
        long zigzag = (value << 1) ^ (value >> 63);
        while ((zigzag & ~0x7FL) != 0) {
            data.writeByte((int) ((zigzag & 0x7FL) | 0x80L));
            zigzag >>>= 7;
        }
        data.writeByte((int) zigzag);
    }

    private static long readVarLong(DataInputStream data) throws IOException {
        long value = 0;
        int shift = 0;
        while (shift < 64) {
            int next = data.readUnsignedByte();
            value |= (long) (next & 0x7F) << shift;
            if ((next & 0x80) == 0) {
                return (value >>> 1) ^ -(value & 1L);
            }
            shift += 7;
        }
        throw new IOException("Overlong varint");
    }

    private static void writeXor(DataOutputStream data, long xor) throws IOException {
        if (xor == 0) {
            data.writeByte(0);
            return;
        }
        int byteCount = (64 - Long.numberOfLeadingZeros(xor) + 7) / 8;
        data.writeByte(byteCount);
        for (int shift = (byteCount - 1) * 8; shift >= 0; shift -= 8) {
            data.writeByte((int) ((xor >>> shift) & 0xFFL));
        }
    }

    private static long readXor(DataInputStream data) throws IOException {
        int byteCount = data.readUnsignedByte();
        if (byteCount == 0) {
            return 0;
        }
        if (byteCount > 8) {
            throw new IOException("Invalid XOR byte count");
        }
        long xor = 0;
        for (int i = 0; i < byteCount; i++) {
            xor = (xor << 8) | data.readUnsignedByte();
        }
        return xor;
    }
}
