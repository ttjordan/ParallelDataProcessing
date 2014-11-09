package neu.cs.parallelprogramming.flightanalyzer;

import au.com.bytecode.opencsv.CSVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tadeusz Jordan.
 */
public class HPopulate {
    private static final String TABLE_NAME          = "FlightsDelayData2008";
    private static final byte[] COLUMNS_FAMILY_NAME = Bytes.toBytes("flight_data");

    public static class Map extends Mapper<LongWritable, Text, FlightDataWritable, CountDelayPair> {
        private List<Put>        putList         = null;
        private HTable           hTable          = null;

        private final static byte[] YEAR_COL            = Bytes.toBytes("year");
        private final static byte[] MONTH_COL           = Bytes.toBytes("month");
        private final static byte[] AIRLINE_ID_COL      = Bytes.toBytes("airline_id");
        private final static byte[] AIRLINE_DELAY_COL   = Bytes.toBytes("airline_delay_minutes");
        private int                 count               = 0;

        @Override
        public void setup(final Context context) throws IOException {
            hTable        = new HTable(context.getConfiguration(), TABLE_NAME);
            //http://hbase.apache.org/book/perf.writing.html
            //http://ryantwopointoh.blogspot.com/2009/01/performance-of-hbase-importing.html
            hTable.setAutoFlush(false);
           // hTable.setWriteBufferSize(1024*1024*24);
            count = 0;

            putList = new ArrayList<Put>();
        }

        @Override
        public void cleanup(final Context context) throws IOException, InterruptedException {
            hTable.put(putList);
            hTable.close();
        }

        @Override
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            final CSVParser csvParser = new CSVParser();
            final String[] flightInfo   = csvParser.parseLine(value.toString());

            // position 0 holds year
            // position 2 holds Month
            // position 7 holds AirlineID
            // position 37 holds ArrDelayMinutes
            String month = flightInfo[2];
            if (flightInfo[2].length() != 2) {
                month = "0" + month;
            }
            String airlineID = flightInfo[7];
            final int PADSIZE = 12;
            for (int i = 0; i < PADSIZE - airlineID.length(); i++) {
                airlineID = "0" + airlineID;
            }

            final byte[] rowKey = Bytes.toBytes(flightInfo[0] + airlineID + month + System.nanoTime() + count);

            count = count + 1;
            final Put put = new Put(rowKey);

            put.add(COLUMNS_FAMILY_NAME, YEAR_COL, Bytes.toBytes(flightInfo[0].trim()));
            put.add(COLUMNS_FAMILY_NAME, MONTH_COL, Bytes.toBytes(flightInfo[2].trim()));
            put.add(COLUMNS_FAMILY_NAME, AIRLINE_ID_COL, Bytes.toBytes(flightInfo[7]));
            put.add(COLUMNS_FAMILY_NAME, AIRLINE_DELAY_COL, Bytes.toBytes(flightInfo[37]));
            // do not need write ahead log in this case
            put.setWriteToWAL(false);
            putList.add(put);
        }
    }


    public static void main(final String[] args) throws Exception {
        final Configuration configuration = HBaseConfiguration.create();
        configuration.set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);

        final HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);

        final HTableDescriptor hTableDescriptor = new HTableDescriptor(TABLE_NAME);
        final HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(COLUMNS_FAMILY_NAME);
        hTableDescriptor.addFamily(hColumnDescriptor);

        if (hBaseAdmin.tableExists(TABLE_NAME)) {
            hBaseAdmin.disableTable(TABLE_NAME);
            hBaseAdmin.deleteTable(TABLE_NAME);
        }

        hBaseAdmin.createTable(hTableDescriptor);
        hBaseAdmin.close();

        final Job job = new Job(configuration, "HPOPULATE");
        job.setJarByClass(HPopulate.class);
        job.setMapperClass(Map.class);
        job.setNumReduceTasks(0);

        job.setOutputFormatClass(TableOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
