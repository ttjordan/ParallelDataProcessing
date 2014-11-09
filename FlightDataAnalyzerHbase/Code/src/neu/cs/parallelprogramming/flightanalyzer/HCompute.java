package neu.cs.parallelprogramming.flightanalyzer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.NavigableMap;

/**
 * Created by Tadeusz Jordan.
 */
public class HCompute {
    private static final String TABLE_NAME          = "FlightsDelayData2008";
    private static final byte[] COLUMNS_FAMILY_NAME = Bytes.toBytes("flight_data");
    private final static byte[] AIRLINE_ID_COL      = Bytes.toBytes("airline_id");
    private final static byte[] MONTH_COL           = Bytes.toBytes("month");
    private final static byte[] AIRLINE_DELAY_COL   = Bytes.toBytes("airline_delay_minutes");

    public static class Map extends TableMapper<Text, Text> {
        private static java.util.Map<FlightDataWritable, CountDelayPair> airlineMonthCountDelayMap = null;

        @Override
        public void setup(final Context context) {
            airlineMonthCountDelayMap = new LinkedHashMap<FlightDataWritable, CountDelayPair>();
        }

        @Override
        public void cleanup(final Context context) throws IOException, InterruptedException {
            Iterator<FlightDataWritable> flightDataIterator = airlineMonthCountDelayMap.keySet().iterator();

            Integer airlineID = Integer.MIN_VALUE;
            StringBuilder outputVal = new StringBuilder();
            String separator = "";
            while (flightDataIterator.hasNext()) {
                final FlightDataWritable flightDataWritable = flightDataIterator.next();
                final CountDelayPair countDelayPair = airlineMonthCountDelayMap.get(flightDataWritable);
                if (airlineID.equals(Integer.MIN_VALUE)) {
                    airlineID = flightDataWritable.getAirlineId();
                }
                if (!flightDataWritable.getAirlineId().equals(airlineID)) {
                    context.write(new Text(String.valueOf(airlineID).trim() + ","), new Text(outputVal.toString()));
                    separator = "";
                    outputVal = new StringBuilder();
                    airlineID = flightDataWritable.getAirlineId();
                }
                outputVal.append(separator + "(" + String.valueOf(flightDataWritable.getMonth()) + ", " + (int)(Math.ceil(countDelayPair.getSumDelay() / countDelayPair.getCount())) + ")");
                separator = ", ";
            }
            context.write(new Text(String.valueOf(airlineID).trim() + ","), new Text(outputVal.toString()));
        }

        @Override
        public void map(final ImmutableBytesWritable row, final Result value, final Context context) throws IOException, InterruptedException {
            final NavigableMap<byte[], byte[]> navigableMap = value.getFamilyMap(COLUMNS_FAMILY_NAME);

            final Integer airlineID = Integer.valueOf(Integer.parseInt(new String(navigableMap.get(AIRLINE_ID_COL))));
            final Integer month = Integer.valueOf(Integer.parseInt(new String(navigableMap.get(MONTH_COL))));
            final String delayStr = new String(navigableMap.get(AIRLINE_DELAY_COL));
            if (delayStr.isEmpty()) {
                return;
            }
            final Double delay = Double.valueOf(delayStr);

            final FlightDataWritable flightDataWritableKey = new FlightDataWritable(airlineID, month);

            CountDelayPair countDelayPair;
            if (airlineMonthCountDelayMap.containsKey(flightDataWritableKey)) {
                countDelayPair = airlineMonthCountDelayMap.get(flightDataWritableKey);
            } else {
                countDelayPair = new CountDelayPair();
            }

            countDelayPair.addDelay(delay);
            countDelayPair.incrementCount(1);

            airlineMonthCountDelayMap.put(flightDataWritableKey, countDelayPair);
        }
    }

    public static void main(final String[] args) throws Exception {
        final Configuration configuration = HBaseConfiguration.create();

        final Job job = new Job(configuration, "HCOMPUTE");
        job.setJarByClass(HCompute.class);
        job.setMapperClass(Map.class);

        job.setMapOutputValueClass(CountDelayPair.class);
        job.setMapOutputKeyClass(FlightDataWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        final Scan scan = new Scan(Bytes.toBytes("2008"));
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob(TABLE_NAME, scan, Map.class, Text.class, Text.class, job);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
