import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.mapreduce.Reducer.Context;

public class WordCount {

    // �̳�Mapper��,Mapper����ĸ����Ͳ����ֱ�Ϊ������key���ͣ�����value���ͣ����key���ͣ����value����
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        private final static IntWritable one = new IntWritable(1); // output value
        private Text word = new Text(); // output key

        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
            
        }
    }

    // Reduce�࣬�̳���Reducer��
    public static class Reduce extends
            Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err
                    .println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job(); // ����һ����ҵ����
        job.setJarByClass(WordCount.class); // ��������/�������ҵ����
        job.setJobName("WordCount");  

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class); 
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}