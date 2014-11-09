package neu.cs.parallelprogramming.flightanalyzer;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Tadeusz Jordan.
 */
public class CountDelayPair implements Writable {
    private Integer count     = 0;
    private Double  sumDelay  = 0.0;
    private final static int PRIME_NUM_1 = 17; // a randomly chosen prime number for seeding the hash function
    private final static int PRIME_NUM_2 = 31; // a randomly chosen prime number for seeding the hash function


    public CountDelayPair() {
        reset();
    }

    public void reset() {
        this.count = 0;
        this.sumDelay = 0.0;
    }

    public void incrementCount(final Integer increment) {
        this.count = this.count + increment;
    }

    public void addDelay(final Double delayIncrement) {
        this.sumDelay = this.sumDelay + delayIncrement;
    }

    public Double getSumDelay() {
        return sumDelay;
    }

    public Integer getCount() {
        return count;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof CountDelayPair)) {
            return false;
        }
        if (obj == this) {
            return true;
        }

        final CountDelayPair rhs = (CountDelayPair) obj;
        return new EqualsBuilder().append(this.count, rhs.count).append(this.sumDelay, rhs.sumDelay).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(PRIME_NUM_1, PRIME_NUM_2).append(this.sumDelay).append(this.count).toHashCode();
    }

    @Override
    public void write(final DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.count);
        dataOutput.writeDouble(this.sumDelay);
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException {
        this.count = dataInput.readInt();
        this.sumDelay = dataInput.readDouble();
    }
}
