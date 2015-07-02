import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by codemather on 02/07/2015.
 */
public class ProjectionMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
    private Text word = new Text();
    private LongWritable count = new LongWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // split to get word and count
        String[] args = value.toString().split("\t+");
        word.set(args[0]);

        try {
            if (args.length >= 3) {
                count.set(Long.parseLong(args[2]));
                context.write(word, count);
            }
        } catch (NumberFormatException e){

        }
    }
}
