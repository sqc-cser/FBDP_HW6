import java.io.IOException;

import java.util.Comparator;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
    static class Map extends Mapper<Object, Text, Text, IntWritable> {
        private Text keyInfo = new Text(); // 存储单词和URL组合
        private FileSplit split; // 存储Split对象
        // 实现map函数
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            // 获得<key,value>对所属的FileSplit对象
            split = (FileSplit) context.getInputSplit();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                // key值由单词和URL组成，如"MapReduce：file1.txt"
                // 获取文件的完整路径
                // keyInfo.set(itr.nextToken()+":"+split.getPath().toString());
                // 这里为了好看，只获取文件的名称。
                int splitIndex = split.getPath().toString().indexOf("file");
                keyInfo.set(itr.nextToken() + "#" + split.getPath().toString().substring(splitIndex));
                // 词频初始化为1
                context.write(keyInfo, new IntWritable(1));
            }
        }
    }
    static class Combine extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Text info = new Text();
        //定义treeMap来保持统计结果,由于treeMap是按key升序排列的,这里要人为指定Comparator以实现倒排
        //这里先使用统计数为key，被统计的单词为value
        private TreeMap<Integer, String> treeMap = new TreeMap<Integer, String>(new Comparator<Integer>() {
            @Override
            public int compare(Integer x, Integer y) {
                return y.compareTo(x);
            }
        });
        // 实现reduce函数
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            // 统计词频,放入treeMap中
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }

        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        // 实现reduce函数,给出的都是已经排序好的，做一个连接
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // 生成文档列表
            String fileList = new String();
            for (Text value : values) {
                fileList += value.toString() + ";";
            }
            result.set(fileList);
            System.out.println(key);
            System.out.println(result);
            context.write(key, result);
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);
        // 设置Map、Combine和Reduce处理类
        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);
        // 设置Map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // 设置Reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}