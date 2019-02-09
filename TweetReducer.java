import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Class adapted from example on Studres
public class TweetReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    // Output is map from unqiue hashtags to frequencies
	public void reduce(Text key, Iterable<LongWritable> values, Context output) throws IOException, InterruptedException {

	    // Sum of appearances of given hashtag
		int sum = 0;
		for (LongWritable value : values) {
			long l = value.get();
			sum += l;
		}
		output.write(key, new LongWritable(sum));
	}
}
