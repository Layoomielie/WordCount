import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhanghongjian
 * @Date 2019/3/26 20:05
 * @Description
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;
        for(IntWritable value:values){
            count +=value.get();
        }

        //把最终的结果输出
        context.write(key,new IntWritable(count));
    }

    public static void main(String[] args) {
    }
}
