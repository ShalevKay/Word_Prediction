import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FinalStep {

    private final static String LOG4J_FILE = "log4j.properties";

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        private final Text unused = new Text("");

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, unused);
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }

    public static class ComparatorClass extends WritableComparator {

        protected ComparatorClass() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            Text t1 = (Text) w1;
            Text t2 = (Text) w2;

            String[] t1Parts = t1.toString().split("\t");
            String[] t2Parts = t2.toString().split("\t");

            String[] t1NgramParts = t1Parts[0].split("\\s+");
            String[] t2NgramParts = t2Parts[0].split("\\s+");

            String t1SortBy = t1NgramParts[0] + " " + t1NgramParts[1];
            String t2SortBy = t2NgramParts[0] + " " + t2NgramParts[1];

            if (t1SortBy.equals(t2SortBy)) {
                return t2Parts[1].compareTo(t1Parts[1]); // Descending
            }

            return t1SortBy.compareTo(t2SortBy);
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        private final Text keyOut = new Text();
        private final Text valueOut = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keyParts = key.toString().split("\t");
            for (Text ignored : values) {
                keyOut.set(keyParts[0]);
                valueOut.set(keyParts[1]);
                context.write(keyOut, valueOut);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        // Setup Configuration
        Configuration conf = new Configuration();
        org.apache.log4j.BasicConfigurator.configure();
        conf.set("hadoop.root.logger", "INFO,stdout");
        conf.set("mapreduce.output.textoutputformat.separator", "\t");

        // Initialize the Job
        Job job = Job.getInstance(conf, "Final Step");
        job.setJarByClass(FinalStep.class);

        // Setup Input
        FileInputFormat.addInputPath(job, new Path("s3://dsp2/output/n2-output"));
        job.addCacheFile(new Path("s3://dsp2/" + LOG4J_FILE).toUri());

        // Setup Mapper
        job.setMapperClass(FinalStep.MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Setup Partitioner
        job.setPartitionerClass(FinalStep.PartitionerClass.class);

        // Setup Comparator
        job.setSortComparatorClass(FinalStep.ComparatorClass.class);
        job.setGroupingComparatorClass(Text.Comparator.class);

        // Setup Reducer
        job.setReducerClass(FinalStep.ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        // Setup Output
        FileOutputFormat.setOutputPath(job, new Path("s3://dsp2/output/final-output"));

        // Run the Job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
