package neu.cs.parallelprogramming.flightanalyzer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import au.com.bytecode.opencsv.CSVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FlightAnalyzer {

    public static class Map extends Mapper<LongWritable, Text, Text, FlightDataWritable> {
        private final static char FROM      = 'f';
        private final static char TO        = 't';
        private final static String ORIGIN  = "ORD";
        private final static String DEST    = "JFK";

        @Override
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            final CSVParser csvParser = new CSVParser();
            final String[] flightInfo   = csvParser.parseLine(value.toString());
            final String origin;
            final String destination;

            final FlightDataWritable flightDataWritable = new FlightDataWritable();

            if (flightInfo[37].isEmpty()) {
                return;
            } else {
                flightDataWritable.setArrDelayMinutes(Double.parseDouble(flightInfo[37]));
            }

            if (flightInfo[35].isEmpty()) {
                return;
            } else {
                flightDataWritable.setArrTime(Integer.parseInt(flightInfo[35]));
            }

            // check if flight was cancelled
            if ( (flightInfo[41].isEmpty()) || (flightInfo[41].equals("1"))) {
                return;
            }

            if (flightInfo[24].isEmpty()) {
                return;
            } else {
                flightDataWritable.setDepTime(Integer.parseInt(flightInfo[24]));
            }

            if (flightInfo[17].isEmpty()) {
                return;
            } else {
                destination = flightInfo[17];
            }

            // check if flight was diverted
            if ( (flightInfo[34].isEmpty()) || (flightInfo[34].equals("1"))) {
                return;
            }

            if (flightInfo[5].isEmpty()) {
                return;
            } else {
                final String[] flightDateTime = flightInfo[5].split("-");
                if (flightDateTime.length != 3) {
                    return;
                }
                final int year = Integer.parseInt(flightDateTime[0]);
                final int month = Integer.parseInt(flightDateTime[1]);

                if (!correctTimePeriod(year, month)) {
                    return;
                }
            }

            if (flightInfo[11].isEmpty()) {
                return;
            } else {
                origin = flightInfo[11];
            }

            // FROM
            if ( (origin.equals(ORIGIN)) && (!destination.equals(DEST)) ) {
                flightDataWritable.setFlag(FROM);
                context.write(new Text(destination + flightInfo[5]), flightDataWritable);
            } else
                // TO
                if ( (!origin.equals(ORIGIN)) && (destination.equals(DEST)) ) {
                    flightDataWritable.setFlag(TO);
                    context.write(new Text(origin + flightInfo[5]), flightDataWritable);
                }
        }

        private boolean correctTimePeriod(final int year, final int month) {
            final int START_MONTH = 6; // June
            final int START_YEAR  = 2007;
            final int END_MONTH   = 5;
            final int END_YEAR    = 2008;

            if (year == START_YEAR) {
                if (month >= START_MONTH) {
                    return true;
                } else {
                    return false;
                }
            }
            if (year == END_YEAR) {
                if (month <= END_MONTH) {
                    return true;
                }
            }
            return false;
        }
    }

    public static class Reduce extends Reducer<Text, FlightDataWritable, DoubleWritable, DoubleWritable> {
        private final static char       FROM     = 'f';
        private final static char       TO       = 't';
        private       static int        count    = 0;
        private       static double     sumDelay = 0.0;

        @Override
        public void setup(final Context context) {
            count    = 0;
            sumDelay = 0.0;
        }

        @Override
        public void reduce(final Text key, final Iterable<FlightDataWritable> values, final Context context) throws IOException, InterruptedException {
            final List<FlightDataWritable> fromList = new ArrayList<FlightDataWritable>();
            final List<FlightDataWritable> toList   = new ArrayList<FlightDataWritable>();

            for(final FlightDataWritable flightDataWritable : values) {
                if (flightDataWritable.getFlag() == FROM) {
                    final FlightDataWritable flightDataWritable1 = new FlightDataWritable();
                    flightDataWritable1.setArrDelayMinutes(flightDataWritable.getArrDelayMinutes());
                    flightDataWritable1.setArrTime(flightDataWritable.getArrTime());
                    flightDataWritable1.setDepTime(flightDataWritable.getDepTime());
                    flightDataWritable1.setFlag(flightDataWritable.getFlag());

                    fromList.add(flightDataWritable1);
                } else if (flightDataWritable.getFlag() == TO) {
                    final FlightDataWritable flightDataWritable1 = new FlightDataWritable();
                    flightDataWritable1.setArrDelayMinutes(flightDataWritable.getArrDelayMinutes());
                    flightDataWritable1.setArrTime(flightDataWritable.getArrTime());
                    flightDataWritable1.setDepTime(flightDataWritable.getDepTime());
                    flightDataWritable1.setFlag(flightDataWritable.getFlag());
                    toList.add(flightDataWritable1);
                }
            }

            for (final FlightDataWritable flightDataWritableFrom : fromList) {
                for (final FlightDataWritable flightDataWritableTo : toList) {
                    double flightTwoLeggedFlightDelay;

                    // check if the departure time of F2 is later than the arrival time of F1
                    if ( !(flightDataWritableTo.getDepTime() > flightDataWritableFrom.getArrTime()) ) {
                        continue;
                    }

                    flightTwoLeggedFlightDelay = flightDataWritableFrom.getArrDelayMinutes() + flightDataWritableTo.getArrDelayMinutes();
                    sumDelay += flightTwoLeggedFlightDelay;
                    count++;
                }
            }
        }

        @Override
        public void cleanup(final Context context) throws IOException, InterruptedException {
            context.write(new DoubleWritable(sumDelay), new DoubleWritable(count));
        }
    }

    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();

        final Job job = new Job(conf, "flightanalyzer");
        job.setJarByClass(FlightAnalyzer.class);

        job.setMapOutputValueClass(FlightDataWritable.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setNumReduceTasks(10);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}