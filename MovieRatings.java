package main;

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

import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;

public class MovieRatings {

    public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] elements = line.split("\t");

            IntWritable reducerKey = new IntWritable(Integer.parseInt(elements[1]));
            IntWritable reducerValue = new IntWritable(Integer.parseInt(elements[2]));

            context.write(reducerKey, reducerValue);
        }
    }

    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
            int numberOfRatings = 0;
            double totalRating = 0.0;
            double averageRating;
            DecimalFormat formatter = new DecimalFormat("0.00");

            for (IntWritable writable : values) {
                totalRating += writable.get();
                numberOfRatings++;
            }

            formatter.setRoundingMode(RoundingMode.HALF_UP);
            averageRating = totalRating / numberOfRatings;

            Text finalValue = new Text(formatter.format(averageRating) + "\t" + numberOfRatings);
            context.write(key, finalValue);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration, "Movie Rating");
        job.setJarByClass(MovieRatings.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        outputPath.getFileSystem(configuration).delete(outputPath, true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


/*

Commands to push to git

echo "# hadoop" >> README.md
git init
git add README.md
git commit -m "first commit"
git branch -M main
git remote add origin https://github.com/bhattak/hadoop.git
git push -u origin main
*/