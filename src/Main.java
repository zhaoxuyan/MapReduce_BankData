import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Main {
    // banName定义成全局变量，是为了防止banName被更新成Null
    static String banName = null;
    static String kaipan = null;
    static String shoupan = null;

    /**
     * 输入一个child-parent的表格
     * 输出一个体现grandchild-grandparent关系的表格
     */
    //Map将输入文件按照空格分割成child和parent，然后正序输出一次作为右表，反序输出一次作为左表，需要注意的是在输出的value中必须加上左右表区别标志
    public static class Map extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


            String value_clean = value.toString();
            value_clean = value_clean.replaceAll("\\s+", " ");


            System.out.println("去掉多于空格后的值:" + value_clean);


            if (Objects.equals(key.toString(), "0")) {
                System.out.println("行号:" + key.toString());
                String[] parts = value_clean.split(" ");
                System.out.println(parts.length);
                banName = parts[0];
            } else if (Objects.equals(key.toString(), "1")) {
                System.out.println("行号:" + key.toString());
                // do nothing
            } else if (Objects.equals(key.toString(), "2")) {
                System.out.println("行号:" + key.toString());
                // do nothing
            } else {
                System.out.println("行号:" + key.toString());
                String[] parts1 = value_clean.split(" ");
                System.out.println(parts1.length);
                kaipan = parts1[1];
                shoupan = parts1[4];
                System.out.println("banName" + banName + "============================");
                System.out.println("kaipan" + kaipan + "=============================");
                System.out.println("shoupan" + shoupan + "============================");
                context.write(new Text(banName), new Text(kaipan + "%" + shoupan));
            }

        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator iterator = values.iterator();
            List<String> kaipan = new ArrayList<String>();
            List<String> shoupan = new ArrayList<String>();
            while (iterator.hasNext()) {
                String record = iterator.next().toString();
                String[] parts = record.split("%");
                kaipan.add(parts[0]);
                shoupan.add(parts[1]);
            }
            double sum = 0;
            for (String string : kaipan) {
                double value = Double.parseDouble(string);
                sum = sum + value;
            }
            double kaipan_avg = sum / (kaipan.size());

            double sum1 = 0;
            for (String string : shoupan) {
                double value = Double.parseDouble(string);
                sum1 = sum1 + value;
            }
            double shoupan_avg = sum1 / (shoupan.size());
            String kaipan_avg_shoupan_avg = "开盘平均：" + kaipan_avg + "收盘平均:" + shoupan_avg;
            System.out.println(kaipan_avg_shoupan_avg);
            context.write(key, new Text(kaipan_avg_shoupan_avg));
        }
    }

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://localhost:9000");
        String[] otherArgs = new String[]{"input/export", "output2"}; /* 直接设置输入参数 */
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in><out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Average");
        job.setJarByClass(Main.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}