import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhanghongjian
 * @Date 2019/3/26 20:03
 * @Description
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] words = line.split(" ");
        Text text = new Text();
        IntWritable intWritable = new IntWritable(1);

        for (String word:words) {
            text.set(word);
            context.write(text,intWritable);
        }
    }
}
