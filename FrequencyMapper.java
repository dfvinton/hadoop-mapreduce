import java.io.*;
import java.util.*;
import javax.json.*;
import javax.json.stream.JsonParsingException;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Class adapted from example on Studres and W10 lecture slides
public class FrequencyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    // Output is map of frequencies to hashtags
    public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

        String line = value.toString();
        String[] kvPair = line.split("\\s+"); // Split by whitespace

        try {
            String hashtag = kvPair[0];
            long frequency = Long.parseLong(kvPair[1]);

            output.write(new LongWritable(frequency), new Text(hashtag));
        } catch (ArrayIndexOutOfBoundsException e) { // Reached if there is some problem in the file between mapreduce jobs
            System.out.println("ArrayIndexOutOfBoundsException: invalid line in input file");
        } catch (NumberFormatException e) {
            System.out.println("NumberFormatException: invalid line in input file");
        }
    }
}
