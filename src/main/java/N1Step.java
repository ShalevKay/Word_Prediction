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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class N1Step {

    private final static String LOG4J_FILE = "log4j.properties";

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        private final Text keyOut = new Text();
        private final Text valueOut = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lineParts = value.toString().split("\t");

            String ngram = lineParts[0].trim();
            String occurrences = lineParts[1].trim();

            keyOut.set(ngram);
            valueOut.set(occurrences);

            context.write(keyOut, valueOut);
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] keyParts = key.toString().split("\\s+");
            String indicator = keyParts.length == 3 ? keyParts[2] : key.toString();
            return Math.abs(indicator.hashCode()) % numPartitions;
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

            String[] t1Parts = t1.toString().split("\\s+");
            String[] t2Parts = t2.toString().split("\\s+");

            if (t1Parts.length == 1 && t2Parts.length == 1) {
                return t1Parts[0].compareTo(t2Parts[0]);
            }

            if (t1Parts.length == 3 && t2Parts.length == 3) {
                return t1Parts[2].compareTo(t2Parts[2]);
            }

            if (t1Parts.length == 1 && t2Parts.length == 3) {
                if (t1Parts[0].equals(t2Parts[2]))
                    return -1; // t1 always before t2
                else
                    return t1Parts[0].compareTo(t2Parts[2]);
            }

            if(t1Parts.length == 3 && t2Parts.length == 1){
                if(t1Parts[2].equals(t2Parts[0]))
                    return 1; // t1 always after t2
                else
                    return t1Parts[2].compareTo(t2Parts[0]);
            }

            if(t1Parts.length == 2)
                return 1;

            return -1;
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        private final Text lastValue = new Text();
        private final Text valueOut = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keyParts = key.toString().split("\\s+");

            // Should be only one value, but for good measure we'll take the last one
            String lastValueString = null;
            for (Text value : values) {
                lastValueString = value.toString();
            }

            if (keyParts.length != 3) {
                lastValue.set(lastValueString);
                valueOut.set(lastValueString);
            } else {
                valueOut.set(lastValueString + " N1=" + lastValue);
            }

            context.write(key, valueOut);
        }
    }

    public static void main(String[] args) throws Exception {

        // Setup Configuration
        Configuration conf = new Configuration();
        org.apache.log4j.BasicConfigurator.configure();
        conf.set("hadoop.root.logger", "INFO,stdout");
        conf.set("mapreduce.output.textoutputformat.separator", "\t");

        // Initialize the Job
        Job job = Job.getInstance(conf, "N1 Step");
        job.setJarByClass(N1Step.class);

        // Setup Input
        FileInputFormat.addInputPath(job, new Path("s3://dsp2/output/c1-output"));
        job.addCacheFile(new Path("s3://dsp2/" + LOG4J_FILE).toUri());

        // Setup Mapper
        job.setMapperClass(N1Step.MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Setup Partitioner
        job.setPartitionerClass(N1Step.PartitionerClass.class);

        // Setup Comparator
        job.setSortComparatorClass(N1Step.ComparatorClass.class);
        job.setGroupingComparatorClass(Text.Comparator.class);

        // Setup Reducer
        job.setReducerClass(N1Step.ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Setup Output
        FileOutputFormat.setOutputPath(job, new Path("s3://dsp2/output/n1-output"));

        // Run the Job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
