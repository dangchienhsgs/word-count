import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.soap.Text;
import java.io.IOException;

/**
 * Created by codemather on 02/07/2015.
 */
public class LongSumReducer<KEY> extends Reducer<KEY, LongWritable, KEY, LongWritable> {
    private LongWritable sum = new LongWritable();

    @Override
    protected void reduce(KEY key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        sum.set(0);
        for (LongWritable value: values) {
            sum.set(sum.get() + value.get());
        }

        context.write(key, sum);
    }
}
