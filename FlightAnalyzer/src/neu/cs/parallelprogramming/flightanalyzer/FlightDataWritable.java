package neu.cs.parallelprogramming.flightanalyzer;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Tadeusz Jordan.
 */
public class FlightDataWritable implements Writable {
    private int      arrTime         = 0;
    private int      depTime         = 0;
    private double   arrDelayMinutes = 0.0;

    private char flag;

    public int getArrTime() {
        return arrTime;
    }

    public void setArrTime(final int arrTime) {
        this.arrTime = arrTime;
    }

    public int getDepTime() {
        return depTime;
    }

    public void setDepTime(final int depTime) {
        this.depTime = depTime;
    }

    public double getArrDelayMinutes() {
        return arrDelayMinutes;
    }

    public void setArrDelayMinutes(final double arrDelayMinutes) {
        this.arrDelayMinutes = arrDelayMinutes;
    }


    public char getFlag() {
        return flag;
    }

    public void setFlag(final char flag) {
        this.flag = flag;
    }

    @Override
    public void write(final DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.arrTime);
        dataOutput.writeInt(this.depTime);
        dataOutput.writeDouble(this.arrDelayMinutes);
        dataOutput.writeChar(this.flag);
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException {
        this.arrTime            = dataInput.readInt();
        this.depTime            = dataInput.readInt();
        this.arrDelayMinutes    = dataInput.readDouble();
        this.flag               = dataInput.readChar();
    }
}
