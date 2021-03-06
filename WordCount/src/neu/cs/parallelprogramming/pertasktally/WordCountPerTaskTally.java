package neu.cs.parallelprogramming.pertasktally;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountPerTaskTally {
    public static final Character[] SET_VALUES = new Character[] { 'm', 'n', 'o', 'p', 'q' };
    public static final Set<Character> MY_SET = new HashSet<Character>(Arrays.asList(SET_VALUES));

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static java.util.Map<Text, Integer> wordCountMap = null;

        // this initializes the hashmap for the Map task
        @Override
        public void setup(final Context context) {
            this.wordCountMap = new HashMap<Text, Integer>();
        }

        // this is called at the end - it emits all word counts saved in a hashmap, guaranteed to execute after last map call
        @Override
        public void cleanup(final Context context) throws IOException, InterruptedException {
            Iterator<Text> wordCountMapIterator = this.wordCountMap.keySet().iterator();
            while(wordCountMapIterator.hasNext()) {
                final Text wordKey = wordCountMapIterator.next();
                final IntWritable wordCount = new IntWritable(this.wordCountMap.get(wordKey));
                context.write(wordKey, wordCount);
            }
        }

        // KEYIN key and VALUEIN value
        // key is LongWritable during runtime
        // value is Text during runtime
        // Maps input key/value pairs to a set of intermediate key/value pairs.
        // key contains 0 in first call, 74 in seconds, etc, which is an offset in bytes from the beginning of the file
        // value contains string content of the line starting at offset key from the input file
        // I have found it by using a debugger to discover values and reading
        // https://developer.yahoo.com/hadoop/tutorial/module4.html

        // Map function uses a hashmap, which is declared at class level and not at function level,
        // to create output pairs. Hashmap is used to calculate count for each word
        // in this task over multiple map calls. New Text word is created for each work put into a hashmap
        // map function does not emit anything
        @Override
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            final String line = value.toString();
            final StringTokenizer tokenizer = new StringTokenizer(line);

            while (tokenizer.hasMoreTokens()) {
                final Text word = new Text();
                word.set(tokenizer.nextToken());
                final char firstLetter = (char) Character.toLowerCase(word.charAt(0));
                if (MY_SET.contains(firstLetter)) {
                    if(wordCountMap.containsKey(word)) {
                        wordCountMap.put(word, wordCountMap.get(word) + 1);
                    } else {
                        wordCountMap.put(word, 1);
                    }
                }
            }
        }
    }

    public static class Partition extends Partitioner<Text, IntWritable> {

        // Partitioner extracts first letter of the key, and finds its position in the array
        // which contains acceptable first letters (5 letters,  { 'm', 'n', 'o', 'p', 'q' }).
        // Since map filters all non-real words, partitioner will only be called with words starting
        // with either one of those 5 characters. It then outputs position of the char in the array
        // i.e. if key starts with o, it will output 2, which will send key to reducer 2.
        // Available reducers are : reducer0, reducer1, reducer2, reducer3, reducer4
        @Override
        public int getPartition(final Text key, final IntWritable value, final int numPartitions) {

            final char firstLetter = (char)Character.toLowerCase(key.charAt(0));
            int reducerNumber = 0;

            for (int i = 0; i < SET_VALUES.length; i++) {
                if(firstLetter == SET_VALUES[i]) {
                    reducerNumber = i;
                    break;
                }
            }
            return reducerNumber % numPartitions;
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (final IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();

        final Job job = new Job(conf, "wordcountpertasktally");
        job.setJarByClass(WordCountPerTaskTally.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(Partition.class);
        // job.setCombinerClass(Reduce.class) is not present here hence Combiner is disabled

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(SET_VALUES.length);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }

}