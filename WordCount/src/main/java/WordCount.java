import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;


public class WordCount {
    static class Map extends Mapper<Object, Text, Text, Text> {
        private Text keyInfo = new Text(); // 存储单词和URL组合
        private Text valueInfo = new Text(); // 存储词频
        private boolean caseSensitive = false;
        private FileSplit split; // 存储Split对象
        private Set<String> patternsToSkip = new HashSet<String>();  // 从最终结果中删除的标点符号和多余单词的列表
        private Set<String> punctuations = new HashSet<String>(); // 用来保存所有要过滤的标点符号 stopwords
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            Configuration config = context.getConfiguration();
            this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
            if (config.getBoolean("wordcount.skip.patterns", false)) {
                // 如果系统变量 wordcount.skip.patterns 为真，
                // 则从分布式缓存文件中获取要跳过的模式列表，并将文件的 URI 转发到 parseSkipFile 方法
                URI[] localPaths = context.getCacheFiles();
                puncFile(localPaths[0]);
                parseSkipFile(localPaths[1]);
            }
        }
        private void puncFile(URI patternsURI) {
            try {
                BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
                String pattern;
                while ((pattern = fis.readLine()) != null) {
                    punctuations.add(pattern);
                }
            } catch (IOException ioe) {
                System.out.println("Caught exception while parsing the cached file '" + patternsURI + "' : " + StringUtils.stringifyException(ioe));
            }
        }
        private void parseSkipFile(URI patternsURI) {
            try {
                BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
                String pattern;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.out.println("Caught exception while parsing the cached file '" + patternsURI + "' : " + StringUtils.stringifyException(ioe));
            }
        }
        // 实现map函数
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            // 获得<key,value>对所属的FileSplit对象
            split = (FileSplit) context.getInputSplit();
            String line  = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
            for (String pattern : punctuations) { // 将数据中所有满足patternsToSkip的pattern都过滤掉, replace by ""
                line = line.replaceAll(pattern, " ");
            }
            line=line.replaceAll("[0-9][0-9]*"," ");// 删除数字
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                String curword = itr.nextToken();
                if (patternsToSkip.contains(curword) || curword.length()<3) {
                    continue;
                }
                int splitIndex = split.getPath().toString().indexOf("/input");
                keyInfo.set(curword + "#" + split.getPath().toString().substring(splitIndex+7));
                valueInfo.set("1");
                context.write(keyInfo,valueInfo);
            }
        }
    }

    static class Combine extends Reducer<Text, Text, Text, Text> {
        private Text info = new Text();
        // 实现reduce函数
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // key存放的是单词，sum存放的是次数
            // 统计词频
//            System.out.println("begin Combining");
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
            //------------------
            int splitIndex = key.toString().indexOf("#");
            // 重新设置value值由URL和词频组成
            info.set(key.toString().substring(splitIndex + 1) + ":" + sum);
            // 重新设置key值为单词
            key.set(key.toString().substring(0, splitIndex));
            context.write(key, info);
            //-------------------
//            System.out.println("ending Combining...");
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        // 用DocCount 来存储 文件名+次数 ，用treeMap存储，自动排序
        public class DocCount{
            HashMap<String,Integer> docTimes = new HashMap<String,Integer>();
            DocCount(String setdoc,int setTimes){
                docTimes.put(setdoc,setTimes);
            }
            void addTimes(String Doc,int add){ // 给定Doc名字和sum，在Doc中增加sum次count
                if (docTimes.containsKey(Doc)){
                    docTimes.put(Doc, docTimes.get(Doc)+add);
                }
                else {
                    docTimes.put(Doc,add);
                }
            }
            List<java.util.Map.Entry<String, Integer>> sort(){
                List<java.util.Map.Entry<String, Integer>> list = new ArrayList<>(docTimes.entrySet());
                Collections.sort(list, new Comparator<java.util.Map.Entry<String, Integer>>() {
                            public int compare(java.util.Map.Entry<String, Integer> entry1, java.util.Map.Entry<String, Integer> entry2) {
                                return entry2.getValue() - entry1.getValue();
                            }
                        }
                );
                return list;
            }
        }

        //用treemap来实现排序，String 存储word，实现顺序排序，
        private TreeMap<String, DocCount> treeMap = new TreeMap<String, DocCount>(new Comparator<String>() {
            @Override
            public int compare(String x, String y) {
                return x.compareToIgnoreCase(y); // 不区分大小写的字典序
//                return x.compareTo(y); // 区分大小写的字典序
            }
        });
        // 实现reduce函数
        @Override
        protected void reduce(Text word, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            for (Text value : values) {// value 例子一般为 file1.txt:1
                int splitIndex = value.toString().indexOf(":");
                String Doc = value.toString().substring(0, splitIndex);//保证为doc名称
                int len = value.toString().length();
                int sum = Integer.parseInt(value.toString().substring(splitIndex + 1, len)); //存储单个单词在某文件中的次数
                if(treeMap.containsKey(word.toString())) {//  如果 treeMap中存有word
                    treeMap.get(word.toString()).addTimes(Doc,sum);
                }
                else{ // 如果没存放，直接添加word
                    treeMap.put(word.toString(), new DocCount(Doc, sum));
                }
            }
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //将treeMap中的结果,按value-key顺序写入context中
            for (String word : treeMap.keySet()) {
                DocCount docNum = treeMap.get(word); //
                List<java.util.Map.Entry<String, Integer>> ls = docNum.sort();
                StringBuilder docValueList = new StringBuilder();
                for (int i=0;i<ls.size();i++){
                    docValueList.append(ls.get(i).getKey()+"#"+ls.get(i).getValue()+", ");
                }
                context.write(new Text(word),new Text(docValueList.toString()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
        job.addCacheFile(new Path(args[0]+ "/skip/punctuation.txt").toUri());
        job.addCacheFile(new Path(args[0]+ "/skip/stop-word-list.txt").toUri());
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
        FileInputFormat.addInputPath(job, new Path(args[0]+"/*.txt"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
