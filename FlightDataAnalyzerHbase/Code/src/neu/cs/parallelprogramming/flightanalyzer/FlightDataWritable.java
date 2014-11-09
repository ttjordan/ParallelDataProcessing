package neu.cs.parallelprogramming.flightanalyzer;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * Created by Tadeusz Jordan.
 */
public class FlightDataWritable implements Writable, WritableComparable<FlightDataWritable> {
    private Integer airlineId  = 0;
    private Integer month      = 0;
    private final static int PRIME_NUM_1 = 17; // a randomly chosen prime number for seeding the hash function
    private final static int PRIME_NUM_2 = 31; // a randomly chosen prime number for seeding the hash function

    public FlightDataWritable() {
        this.airlineId = 0;
        this.month = 0;
    }

    public FlightDataWritable(final Integer airlineId, final Integer month) {
        this.airlineId  = airlineId;
        this.month      = month;
    }

    public Integer getAirlineId() {
        return airlineId;
    }

    public Integer getMonth() {
        return month;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof FlightDataWritable)) {
            return false;
        }
        if (obj == this) {
            return true;
        }

        final FlightDataWritable rhs = (FlightDataWritable) obj;
        return new EqualsBuilder().append(this.airlineId, rhs.airlineId).append(this.month, rhs.month).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(PRIME_NUM_1, PRIME_NUM_2).append(this.airlineId).append(this.month).toHashCode();
    }

    @Override
    public void write(final DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.airlineId);
        dataOutput.writeInt(this.month);
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException {
        this.airlineId = dataInput.readInt();
        this.month     = dataInput.readInt();
    }

    @Override
    public int compareTo(final FlightDataWritable flightDataWritable) {
        int compareToValue = this.airlineId.compareTo(flightDataWritable.getAirlineId());

        if (compareToValue == 0) {
            compareToValue = this.month.compareTo(flightDataWritable.getMonth());
        }
        return compareToValue;
    }
}
