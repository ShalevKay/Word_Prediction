import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class N2Step {

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
            String indicator = keyParts.length == 3 ? keyParts[1] + " " + keyParts[2] : key.toString();
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

            if (t1Parts.length == 2 && t2Parts.length == 2) {
                return (t1Parts[0] + " " + t1Parts[1]).compareTo((t2Parts[0] + " " + t2Parts[1]));
            }

            if (t1Parts.length == 3 && t2Parts.length == 3) {
                return (t1Parts[1] + " " + t1Parts[2]).compareTo((t2Parts[1] + " " + t2Parts[2]));
            }

            if (t1Parts.length == 2 && t2Parts.length == 3) {
                if ((t1Parts[0] + " " + t1Parts[1]).equals((t2Parts[1] + " " + t2Parts[2])))
                    return -1; // t1 always before t2
                else
                    return (t1Parts[0] + " " + t1Parts[1]).compareTo((t2Parts[1] + " " + t2Parts[2]));
            }

            if(t1Parts.length == 3 && t2Parts.length == 2){
                if((t1Parts[1] + " " + t1Parts[2]).equals((t2Parts[0] + " " + t2Parts[1])))
                    return 1; // t1 always after t2
                else
                    return (t1Parts[1] + " " + t1Parts[2]).compareTo((t2Parts[0] + " " + t2Parts[1]));
            }

            if(t1Parts.length == 1)
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
                valueOut.set(lastValueString + " N2=" + lastValue);

                String[] valueParts = valueOut.toString().split(" ");

                int N3 = Integer.parseInt(valueParts[0].substring(3));
                int C1 = Integer.parseInt(valueParts[1].substring(3));
                int N1 = Integer.parseInt(valueParts[2].substring(3));
                int C2 = Integer.parseInt(valueParts[3].substring(3));
                int N2 = Integer.parseInt(valueParts[4].substring(3));
                int C0 = 0;

                try (BufferedReader reader = new BufferedReader(new FileReader("c0.txt"))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        C0 = Integer.parseInt(line.replaceAll("\\D", "")); // Remove all non-digits
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

                double k2 = (Math.log10(N2 + 1) + 1) / (Math.log10(N2 + 1) + 2);
                double k3 = (Math.log10(N3 + 1) + 1) / (Math.log10(N3 + 1) + 2);

                double probability = k3 * ((double)N3 / C2) + (1 - k3) * k2 * ((double) N2 / C1) + (1 - k3) * (1 - k2) * ((double) N1 / C0);

                valueOut.set(String.valueOf(probability));
                context.write(key, valueOut);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Setup Configuration
        Configuration conf = new Configuration();
        org.apache.log4j.BasicConfigurator.configure();
        conf.set("hadoop.root.logger", "INFO,stdout");
        conf.set("mapreduce.output.textoutputformat.separator", "\t");
        conf.set("mapreduce.job.reduce.slowstart.completedmaps", "1.0");

        // Initialize the Job
        Job job = Job.getInstance(conf, "N2 Step");
        job.setJarByClass(N2Step.class);

        // Setup Input
        FileInputFormat.addInputPath(job, new Path("s3://dsp2/output/c2-output"));
        job.addCacheFile(new Path("s3://dsp2/c0.txt").toUri());
        job.addCacheFile(new Path("s3://dsp2/" + LOG4J_FILE).toUri());

        // Setup Mapper
        job.setMapperClass(N2Step.MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Setup Partitioner
        job.setPartitionerClass(N2Step.PartitionerClass.class);

        // Setup Comparator
        job.setSortComparatorClass(N2Step.ComparatorClass.class);
        job.setGroupingComparatorClass(Text.Comparator.class);

        // Setup Reducer
        job.setReducerClass(N2Step.ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Setup Output
        FileOutputFormat.setOutputPath(job, new Path("s3://dsp2/output/n2-output"));

        // Run the Job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
