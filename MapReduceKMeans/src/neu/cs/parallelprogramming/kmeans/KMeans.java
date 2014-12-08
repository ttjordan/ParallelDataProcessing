package neu.cs.parallelprogramming.kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import au.com.bytecode.opencsv.CSVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class KMeans {
    private static final String CENTROID_PREVIOUS = "centroidPrev";
    private static final String K = "K";
    private static final String DONE = "DONE";
    private static final int MAXITER = 10000;
    private static final float THRESHOLD = 100;
    private static final int MAX_CENTROID_RANGE = 500;

    public static class Map extends Mapper<LongWritable, Text, CentroidWritable, SongDataWritable> {
        private List<CentroidWritable> centroidWritableList = new LinkedList<CentroidWritable>();

        @Override
        public void setup(final Context context) throws IOException, InterruptedException {
            final String clusterDir = context.getConfiguration().get(CENTROID_PREVIOUS);
            Path clusterDirPath;
            int numclusters = Integer.parseInt(context.getConfiguration().get(K));

            if (clusterDir == null) {
                Random r = new Random();
                for (int k = 0; k < numclusters; k++) {
                    centroidWritableList.add(new CentroidWritable(r.nextInt(MAX_CENTROID_RANGE)));
                }
            } else {
                clusterDirPath = new Path(clusterDir);

                final FileSystem fs = FileSystem.get(clusterDirPath.toUri(), context.getConfiguration());
                final FileStatus[] fstatuses = fs.listStatus(clusterDirPath);
                for (final FileStatus fstatus : fstatuses) {
                    final Path path = fstatus.getPath();
                    if (!path.getName().startsWith("part-r"))
                        continue;

                    final FSDataInputStream fis = fs.open(path);
                    final BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
                    String line;

                    while ((line = reader.readLine()) != null) {
                        final String[] tokens = line.split("\\s+");
                        centroidWritableList.add(new CentroidWritable(Float.parseFloat(tokens[0])));
                    }
                    reader.close();
                    fis.close();
                }
                if (centroidWritableList.size() == 0) {
                    Random r = new Random();
                    for (int k = 0; k < numclusters; k++) {
                        centroidWritableList.add(new CentroidWritable(r.nextInt(MAX_CENTROID_RANGE)));
                    }
                }

            }
        }

        @Override
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            final CSVParser csvParser = new CSVParser();
            final String[] lineWithSongInfo = csvParser.parseLine(value.toString());

            final SongDataWritable songData = new SongDataWritable();
            songData.setArtistID(lineWithSongInfo[0]);
            songData.setArtistMbid(lineWithSongInfo[1]);
            songData.setArtistMbtags(lineWithSongInfo[2]);
            songData.setArtistFamiliarity(lineWithSongInfo[3]);
            songData.setArtistHotness(lineWithSongInfo[4]);
            songData.setArtistName(lineWithSongInfo[5]);
            songData.setArtistLatitude(lineWithSongInfo[6]);
            songData.setArtistLongitude(lineWithSongInfo[7]);
            songData.setArtistLocation(lineWithSongInfo[8]);
            songData.setDuration(Float.parseFloat(lineWithSongInfo[9]));
            songData.setEnergy(lineWithSongInfo[10]);
            songData.setRelease(lineWithSongInfo[11]);
            songData.setLoudness(lineWithSongInfo[12]);
            songData.setMode(lineWithSongInfo[13]);
            songData.setSongHotness(lineWithSongInfo[14]);
            songData.setSongId(lineWithSongInfo[15]);
            songData.setTempo(lineWithSongInfo[16]);
            songData.setTitle(lineWithSongInfo[17]);
            songData.setTrackID(lineWithSongInfo[18]);
            songData.setYear(lineWithSongInfo[19]);

            CentroidWritable closestCentroid = centroidWritableList.get(0);
            float minDist = getDistanceTocluster(songData, closestCentroid);

            for (int k = 1; k < Integer.parseInt(context.getConfiguration().get(K)); k++) {
                float minDistCandidate = getDistanceTocluster(songData, centroidWritableList.get(k));
                if (minDistCandidate < minDist) {
                    minDist = minDistCandidate;
                    closestCentroid = centroidWritableList.get(k);
                }
            }
            context.write(closestCentroid, songData);

        }
    }

    public static float getDistanceTocluster(final SongDataWritable songData, final CentroidWritable centroidWritable) {
        return (float) Math.sqrt(Math.pow(songData.getDuration() - centroidWritable.getDuration(), 2));
    }


    public static class Reduce extends Reducer<CentroidWritable, SongDataWritable, CentroidWritable, Text> {
        private String lastIteration;

        @Override
        public void reduce(final CentroidWritable key, final Iterable<SongDataWritable> values, final Context context) throws IOException, InterruptedException {
            final SongDataWritable songDataSum = new SongDataWritable();
            lastIteration = context.getConfiguration().get(DONE);


            final List<SongDataWritable> songDataWritableList = new ArrayList<SongDataWritable>();
            for (final SongDataWritable songDataWritable : values) {
                final SongDataWritable songDataWritableCopy = new SongDataWritable();
                songDataWritableCopy.setArtistID(songDataWritable.getArtistID());
                songDataWritableCopy.setArtistMbid(songDataWritable.getArtistMbid());
                songDataWritableCopy.setArtistMbtags(songDataWritable.getArtistMbtags());
                songDataWritableCopy.setArtistFamiliarity(songDataWritable.getArtistFamiliarity());
                songDataWritableCopy.setArtistHotness(songDataWritable.getArtistHotness());
                songDataWritableCopy.setArtistName(songDataWritable.getArtistName());
                songDataWritableCopy.setArtistLatitude(songDataWritable.getArtistLatitude());
                songDataWritableCopy.setArtistLongitude(songDataWritable.getArtistLongitude());
                songDataWritableCopy.setArtistLocation(songDataWritable.getArtistLocation());
                songDataWritableCopy.setDuration(songDataWritable.getDuration());
                songDataWritableCopy.setEnergy(songDataWritable.getEnergy());
                songDataWritableCopy.setRelease(songDataWritable.getRelease());
                songDataWritableCopy.setLoudness(songDataWritable.getLoudness());
                songDataWritableCopy.setMode(songDataWritable.getMode());
                songDataWritableCopy.setSongHotness(songDataWritable.getSongHotness());
                songDataWritableCopy.setSongId(songDataWritable.getSongId());
                songDataWritableCopy.setTempo(songDataWritable.getTempo());
                songDataWritableCopy.setTitle(songDataWritable.getTitle());
                songDataWritableCopy.setTrackID(songDataWritable.getTrackID());
                songDataWritableCopy.setYear(songDataWritable.getYear());
                songDataWritableList.add(songDataWritableCopy);
            }

            for (final SongDataWritable songDataWritable : songDataWritableList) {
                songDataSum.incrementData(songDataWritable, songDataWritableList.size());

            }
            float spreadFromCentroid = 0.0f;
            for (final SongDataWritable songDataWritable : songDataWritableList) {
                spreadFromCentroid += (float) Math.sqrt(Math.pow(songDataWritable.getDuration() - songDataSum.getDuration(), 2));
            }
            context.getCounter(MyCounters.SPREAD).increment((long) spreadFromCentroid);

            if (lastIteration.equals("0")) {
                context.write(new CentroidWritable(songDataSum.getDuration()), new Text(""));
            } else {
                for (final SongDataWritable songDataWritable : songDataWritableList) {
                    context.write(new CentroidWritable(songDataSum.getDuration()), new Text(songDataWritable.printData()));
                }
            }
        }
    }


    private static Job kMeansJob(final Configuration conf, final Path centroidNextPath,
                                 final Path centroidPreviousPath, final int iteration, Path dataInput, final String done) throws IOException, ClassNotFoundException, InterruptedException {
        final Job job = new Job(conf, "cluster-songs/" + iteration);
        if (centroidPreviousPath == null) {
            job.getConfiguration().set(CENTROID_PREVIOUS, "");
        } else {
            job.getConfiguration().set(CENTROID_PREVIOUS, centroidPreviousPath.toString());
        }
        job.getConfiguration().set(K, "6");
        job.getConfiguration().set(DONE, done);

        FileInputFormat.addInputPath(job, dataInput);
        FileOutputFormat.setOutputPath(job, centroidNextPath);

        job.setJarByClass(KMeans.class);

        job.setMapOutputValueClass(SongDataWritable.class);
        job.setMapOutputKeyClass(CentroidWritable.class);

        job.setOutputKeyClass(CentroidWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setNumReduceTasks(10);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.waitForCompletion(true);

        return job;
    }

    public static enum MyCounters  {
        SPREAD
    }

    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        final String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Kmeans clustering <indir> <outdir>");
            System.exit(-1);
        }

        final Path indir = new Path(otherArgs[0]);
        final Path outdir = new Path(otherArgs[1]);

        long newSpread = 0;
        long oldSpread = 0;
        int iteration = 0;
        Path centroidNextFile = null;
        Path centroidPreviousFile = null;
        boolean lastIter = false;
        for (;;) {
            System.out.println("iteration " + iteration);
            if(iteration > 1) {
                oldSpread = newSpread;
            }
            if (iteration > 0) {
                centroidPreviousFile = centroidNextFile;
            }
            centroidNextFile = new Path(outdir, "temp_centroid" + iteration );
            Job job;
            if(!lastIter) {
                job = kMeansJob(conf, centroidNextFile, centroidPreviousFile, iteration, indir, "0");
            } else {
                job = kMeansJob(conf, centroidNextFile, centroidPreviousFile, iteration, indir, "1");
                break;
            }

            newSpread = job.getCounters().findCounter(MyCounters.SPREAD).getValue();

            if (iteration > 1) {
                long distanceCandidate = Math.abs(newSpread - oldSpread);
                System.out.println("dist " + distanceCandidate);
                if (distanceCandidate < THRESHOLD) {
                    lastIter = true;
                }
            }

            if (iteration == MAXITER) {
                lastIter = true;
            }
            iteration++;
        }
    }

}