package com.flywhl.flinklab.e07.customsource;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.util.List;

/**
 * FLIP-27 Source 的装配点:把 Enumerator + Reader + 序列化器组装成 Source。
 * Boundedness.BOUNDED 意味着这是批语义源(数字读完即结束);
 * 改 CONTINUOUS_UNBOUNDED 就是流源的声明方式(如 Kafka)。
 */
public final class RangeSource implements Source<Long, RangeSplit, List<RangeSplit>> {

    private final long total;
    private final int numSplits;

    public RangeSource(long total, int numSplits) {
        this.total = total;
        this.numSplits = numSplits;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<Long, RangeSplit> createReader(SourceReaderContext ctx) {
        return new RangeSourceReader();
    }

    @Override
    public SplitEnumerator<RangeSplit, List<RangeSplit>> createEnumerator(
            SplitEnumeratorContext<RangeSplit> ctx) {
        return new RangeEnumerator(ctx, total, numSplits, null);
    }

    @Override
    public SplitEnumerator<RangeSplit, List<RangeSplit>> restoreEnumerator(
            SplitEnumeratorContext<RangeSplit> ctx, List<RangeSplit> checkpoint) {
        return new RangeEnumerator(ctx, total, numSplits, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<RangeSplit> getSplitSerializer() {
        return new SimpleVersionedSerializer<>() {
            @Override public int getVersion() {
                return 1;
            }
            @Override public byte[] serialize(RangeSplit s) throws IOException {
                var bos = new ByteArrayOutputStream();
                var out = new DataOutputStream(bos);
                out.writeInt(s.id); out.writeLong(s.current); out.writeLong(s.end);
                return bos.toByteArray();
            }
            @Override public RangeSplit deserialize(int version, byte[] bytes) throws IOException {
                var in = new DataInputStream(new ByteArrayInputStream(bytes));
                int id = in.readInt(); long cur = in.readLong(); long end = in.readLong();
                RangeSplit s = new RangeSplit(id, cur, end);
                return s;
            }
        };
    }

    @Override
    public SimpleVersionedSerializer<List<RangeSplit>> getEnumeratorCheckpointSerializer() {
        SimpleVersionedSerializer<RangeSplit> splitSer = getSplitSerializer();
        return new SimpleVersionedSerializer<>() {
            @Override public int getVersion() {
                return 1;
            }
            @Override public byte[] serialize(List<RangeSplit> list) throws IOException {
                var bos = new ByteArrayOutputStream();
                var out = new DataOutputStream(bos);
                out.writeInt(list.size());
                for (RangeSplit s : list) {
                    byte[] b = splitSer.serialize(s);
                    out.writeInt(b.length);
                    out.write(b);
                }
                return bos.toByteArray();
            }
            @Override public List<RangeSplit> deserialize(int version, byte[] bytes) throws IOException {
                var in = new DataInputStream(new ByteArrayInputStream(bytes));
                int n = in.readInt();
                var result = new java.util.ArrayList<RangeSplit>();
                for (int i = 0; i < n; i++) {
                    int len = in.readInt();
                    byte[] b = new byte[len];
                    in.readFully(b);
                    result.add(splitSer.deserialize(version, b));
                }
                return result;
            }
        };
    }

    public TypeInformation<Long> getProducedType() {
        return TypeInformation.of(Long.class);
    }
}
