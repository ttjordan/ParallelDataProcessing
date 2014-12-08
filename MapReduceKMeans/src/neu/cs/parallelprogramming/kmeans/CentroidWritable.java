package neu.cs.parallelprogramming.kmeans;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CentroidWritable implements WritableComparable<CentroidWritable>, Writable {
    private float duration = 0;

    public CentroidWritable(final float songDuration) {
        this.duration = songDuration;
    }

    public float getDuration() {
        return duration;
    }

    public CentroidWritable() {}

    @Override
    public void write(final DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(this.duration);
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException {
        this.duration = dataInput.readFloat();
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof CentroidWritable)) {
            return false;
        }
        if (obj == this) {
            return true;
        }

        final CentroidWritable rhs = (CentroidWritable) obj;
        return new EqualsBuilder().append(this.duration, rhs.duration).isEquals();
    }

    @Override
    public int compareTo(CentroidWritable aThat) {
        final int BEFORE = -1;
        final int EQUAL = 0;
        final int AFTER = 1;
        if (this == aThat) return EQUAL;

        if (this.duration < aThat.getDuration()) {
            return BEFORE;
        }
        if (this.duration > aThat.getDuration()) {
            return AFTER;
        }

        return EQUAL;
    }
    @Override public String toString() {
        return new String(Float.toString(this.duration));
    }
}
