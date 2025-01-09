import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class WordCounter {

    private final static String STOPWORDS_FILE = "heb-stopwords.txt";
    private final static String LOG4J_FILE = "log4j.properties";

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final Logger LOG = Logger.getLogger(WordCounter.MapperClass.class);

        private final Text keyOut = new Text();
        private final IntWritable valueOut = new IntWritable();

        private final Set<String> stopWords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            File stopWordsFile = new File(STOPWORDS_FILE);
            try (BufferedReader reader = new BufferedReader(new FileReader(stopWordsFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    stopWords.add(line.trim());
                }
            }
            LOG.info("Loaded " + stopWords.size() + " stop words");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lineParts = value.toString().split("\t");
            if (lineParts.length != 5) {
                LOG.warn("Invalid line: " + value.toString());
                return;
            }

            String ngram = lineParts[0].trim();
            String[] ngramParts = ngram.split("\\s+");
            int occurrences = Integer.parseInt(lineParts[2].trim());

            if (ngramParts.length != 3) {
                LOG.warn("Invalid ngram: " + ngram);
                return;
            }

            boolean containsStopWords = Arrays.stream(ngramParts).anyMatch(stopWords::contains);
            if (containsStopWords) {
                LOG.debug("Skipping ngram: " + ngram + " due to stop words");
                return;
            }

            String w_1 = ngramParts[0];
            String w_2 = ngramParts[1];
            String w_3 = ngramParts[2];

            // Emit the words
            valueOut.set(occurrences * 3);

            keyOut.set("*");
            context.write(keyOut, valueOut);

            valueOut.set(occurrences);

            keyOut.set(w_1);
            context.write(keyOut, valueOut);

            keyOut.set(w_2);
            context.write(keyOut, valueOut);

            keyOut.set(w_3);
            context.write(keyOut, valueOut);

            keyOut.set(w_1 + " " + w_2);
            context.write(keyOut, valueOut);

            keyOut.set(w_2 + " " + w_3);
            context.write(keyOut, valueOut);

            keyOut.set(w_1 + " " + w_2 + " " + w_3);
            context.write(keyOut, valueOut);
        }
    }

    public static class CombinerClass extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final Text keyOut = new Text();
        private final IntWritable valueOut = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            keyOut.set(key);
            valueOut.set(sum);
            context.write(keyOut, valueOut);
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }

    public static class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {

        private static final Logger LOG = Logger.getLogger(WordCounter.ReducerClass.class);

        private final Text keyOut = new Text();
        private final IntWritable valueOut = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            keyOut.set(key);
            valueOut.set(sum);
            context.write(keyOut, valueOut);
        }

    }

    public static void main(String[] args) throws Exception {

        // Setup Configuration
        Configuration conf = new Configuration();
        org.apache.log4j.BasicConfigurator.configure();
        conf.set("hadoop.root.logger", "INFO,stdout");
        conf.set("io.compression.codecs", "com.hadoop.compression.lzo.LzoCodec");
        conf.set("mapreduce.output.textoutputformat.separator", "\t");

        // Initialize the Job
        Job job = Job.getInstance(conf, "Word Counter");
        job.setJarByClass(WordCounter.class);

        // Setup Input
        FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.addCacheFile(new Path("s3://dsp2/" + STOPWORDS_FILE).toUri());
        job.addCacheFile(new Path("s3://dsp2/" + LOG4J_FILE).toUri());

        // Setup Mapper
        job.setMapperClass(WordCounter.MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Setup Combiner
//        job.setCombinerClass(WordCounter.CombinerClass.class);

        // Setup Partitioner
        job.setPartitionerClass(WordCounter.PartitionerClass.class);

        // Setup Reducer
        job.setReducerClass(WordCounter.ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Setup Output
        FileOutputFormat.setOutputPath(job, new Path("s3://dsp2/output/word-count"));

        // Run the Job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}