package neu.cs.parallelprogramming.flightanalyzer;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import au.com.bytecode.opencsv.CSVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Secondary {

    public static class Map extends Mapper<LongWritable, Text, FlightDataWritable, CountDelayPair> {
        private static java.util.Map<FlightDataWritable, CountDelayPair> airlineMonthCountDelayMap = null;

        @Override
        public void setup(final Context context) {
            airlineMonthCountDelayMap = new HashMap<FlightDataWritable, CountDelayPair>();
        }

        @Override
        public void cleanup(final Context context) throws IOException, InterruptedException {
            Iterator<FlightDataWritable> flightDataIterator = airlineMonthCountDelayMap.keySet().iterator();
            while(flightDataIterator.hasNext()) {
                final FlightDataWritable flightDataWritable = flightDataIterator.next();
                final CountDelayPair countDelayPair = airlineMonthCountDelayMap.get(flightDataWritable);
                context.write(flightDataWritable, countDelayPair);
            }
        }

        @Override
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            final CSVParser csvParser = new CSVParser();
            final String[] flightInfo   = csvParser.parseLine(value.toString());

            // Filter by year - only 2008 counts
            if (!flightInfo[0].equals("2008")) {
                return;
            }

            if (flightInfo[37].isEmpty()) {
                return;
            }

            // position 7 holds AirlineID
            // position 2 holds Month
            // position 37 holds ArrDelayMinutes
            final FlightDataWritable flightDataWritable = new FlightDataWritable(Integer.parseInt(flightInfo[7].trim()), Integer.parseInt(flightInfo[2]));
            final CountDelayPair countDelayPair;
            if (airlineMonthCountDelayMap.containsKey(flightDataWritable)) {
                countDelayPair = airlineMonthCountDelayMap.get(flightDataWritable);
            } else {
                countDelayPair = new CountDelayPair();
            }
            countDelayPair.incrementCount(1);
            countDelayPair.addDelay(Double.parseDouble(flightInfo[37]));
            airlineMonthCountDelayMap.put(flightDataWritable, countDelayPair);
        }
    }

    public static class FlightPartitioner extends Partitioner<FlightDataWritable, CountDelayPair> {
        @Override
        public int getPartition(final FlightDataWritable key, final CountDelayPair value, final int numPartitions) {
            try {
                return HashUtils.hash(key.getAirlineId(), numPartitions);
            } catch (final NoSuchAlgorithmException e) {
                // default from Hadoop definitive guide
                return key.getAirlineId() * 127 % numPartitions;
            }
        }
    }

    public static class FlightComparator extends WritableComparator {
        public FlightComparator() {
            super(FlightDataWritable.class, true);
        }

        @Override
        public int compare(final WritableComparable flightData1, final WritableComparable flightData2) {
            final FlightDataWritable flightDataWritable1 = (FlightDataWritable) flightData1;
            final FlightDataWritable flightDataWritable2 = (FlightDataWritable) flightData2;
            return flightDataWritable1.compareTo(flightDataWritable2);
        }
    }

    public static class Reduce extends Reducer<FlightDataWritable, CountDelayPair, Text, Text> {
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
                outputVal.append(separator + "(" + String.valueOf(String.valueOf(flightDataWritable.getMonth())) + ", " + (int)(Math.ceil(countDelayPair.getSumDelay() / countDelayPair.getCount())) + ")");
                separator = ", ";
            }
            context.write(new Text(String.valueOf(airlineID).trim() + ","), new Text(outputVal.toString()));
        }

        @Override
        public void reduce(final FlightDataWritable key, final Iterable<CountDelayPair> values, final Context context) throws IOException, InterruptedException {
            CountDelayPair countDelayPair;
            if (airlineMonthCountDelayMap.containsKey(key)) {
                countDelayPair = airlineMonthCountDelayMap.get(key);
            } else {
                countDelayPair = new CountDelayPair();
            }

            for (final CountDelayPair val : values) {
                countDelayPair.addDelay(val.getSumDelay());
                countDelayPair.incrementCount(val.getCount());
            }
            final FlightDataWritable flightDataWritable = new FlightDataWritable(key.getAirlineId(), key.getMonth());
            airlineMonthCountDelayMap.put(flightDataWritable, countDelayPair);
        }
    }

    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();

        final Job job = new Job(conf, "SECONDARY");
        job.setJarByClass(Secondary.class);

        job.setMapOutputValueClass(CountDelayPair.class);
        job.setMapOutputKeyClass(FlightDataWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setGroupingComparatorClass(FlightComparator.class);
        job.setPartitionerClass(FlightPartitioner.class);

        job.setNumReduceTasks(10);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}