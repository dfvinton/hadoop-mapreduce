import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

// Code and comments adapted from example on Studres
// As part of extension, class has been decomposed in an object-oriented manner
public class W11Practical {

    public static void main(String[] args) {
        W11Practical prac = new W11Practical();
        boolean sortByFreq = prac.checkArgs(args);

        try {
            Job job = prac.setupJob(false, args);

            job.waitForCompletion(true);
            if (sortByFreq) { // Does another job; creates new output folder
                job = prac.setupJob(true, args);
                job.waitForCompletion(true);
            }
        } catch (ClassNotFoundException e) {
            System.out.println("ClassNotFoundException: check contents of src and bin folders");
        } catch (IOException e) {
            System.out.println("IOException: check input file exists and is valid");
        } catch (InterruptedException e) {
            System.out.println("InterruptedException: process interrupted while executing");
        }
    }

    // Checks whether to do another job after first; exits if invalid args
    public boolean checkArgs(String[] args) {
        boolean sortByFreq = false;

        if (args.length != 2 && args.length == 3 && args[2].equals("freq")) {
            sortByFreq = true;
        } else if (args.length != 2) {
            System.out.println("Usage: java -cp \"lib/*:bin\" W11Practical <input_path> <output_path>");
            System.exit(1);
        }

        return sortByFreq;
    }

    // Creates new job, chooses which config to use
    public Job setupJob(boolean sortByFrequency, String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hashtag Count");

        if (sortByFrequency) {
            setFrequencySort(job, args);
        } else {
            setAlphabetSort(job, args);
        }

        return job;
    }

    // Configures job as in example; output is alphabetical
    public void setAlphabetSort(Job job, String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        String input = args[0];
        String output = args[1];

        // Sets mapper and reducer classes
        job.setMapperClass(TweetMapper.class);
        job.setReducerClass(TweetReducer.class);

        // Specifies mapper output (hashtag, frequency)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // Specifies reducer output (unique hashtag, frequency)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
    }

    // Extension
    // Output sorted by frequency; different mapper and reducer class and keys/values as suggested in W10 Lectures
    public void setFrequencySort(Job job, String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        String input = args[1];
        String output = args[1] + "_frequency_sort"; // New output folder path

        // Extention classes
        job.setMapperClass(FrequencyMapper.class);
        job.setReducerClass(FrequencyReducer.class);

        // Reverse of above method
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
    }
}

