import java.io.*;
import java.util.*;
import javax.json.*;
import javax.json.stream.JsonParsingException;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Class adapted from example on Studres
public class TweetMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    // Output is map from hashtags to frequencies
	public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

        String word = value.toString();
        try {
            JsonArray hashtags = getHashtags(word);

            for (int i = 0; i < hashtags.size(); i++) {
                JsonObject obj = hashtags.getJsonObject(i);
                String hashtag = obj.getString("text");
                output.write(new Text(hashtag), new LongWritable(1));
            }
        } catch (NullPointerException e) {
            // Catch statement invoked for tweet without a needed field, meaning we just skip the line
            // Printing something would just clutter the terminal
        }
    }

    // Goes through JSON to get array of hashtags
	public JsonArray getHashtags(String word) throws NullPointerException { // Null pointer means we want to disregard the line, so thrown up to map
        JsonReader reader = Json.createReader(new StringReader(word));
        JsonObject mainObj = reader.readObject();
        reader.close();
        JsonObject entities = mainObj.getJsonObject("entities");
        JsonArray hashtags = entities.getJsonArray("hashtags");

        return hashtags;
    }
}
