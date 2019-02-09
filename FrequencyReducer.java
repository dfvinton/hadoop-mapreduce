import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Class adapted from example on Studres and W10 lecture slides
public class FrequencyReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

    // Output is map of frequencies to hashtags
    public void reduce(LongWritable key, Iterable<Text> values, Context output) throws IOException, InterruptedException {
        for(Text value : values) {
            output.write(key, new Text(value));
        }
    }
}
