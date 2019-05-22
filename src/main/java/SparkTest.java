/**
 * @author zhanghongjian
 * @Date 2019/4/8 15:19
 * @Description
 */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.glassfish.jersey.process.internal.Stages;

import java.util.ArrayList;
import java.util.List;

public class SparkTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("spark://10.100.23.92:7077");
        JavaSparkContext sc = new JavaSparkContext(conf=conf);
        List<String> list = new ArrayList<>();
        list.add("abc");
        list.add("bcd");
        list.add("cdf");
        JavaRDD<String> rdd = sc.parallelize(list);
        JavaRDD<String> rdd2 = rdd.filter(s -> s.contains("c"));
        List<String> collect = rdd.collect();
        System.out.println(collect);

    }
}
