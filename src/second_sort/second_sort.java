package second_sort;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class second_sort extends Configured implements Tool{
    // 继承Mapper类
    public static class Map extends Mapper<Text, Text, TextIntKey, IntWritable> {
        private Text word = new Text(); // output key
    	private IntWritable num = new IntWritable();

        TextIntKey newKey = new TextIntKey();
        public void map(Text key, Text value, Context context) 
               throws IOException, InterruptedException {
            //过滤非法记录
            if(key == null || value == null || key.toString().equals("")
                    || value.toString().equals("")){
                return;
            }
            word.set(key.toString());
            num.set(Integer.parseInt(value.toString()));
            newKey.setFirstKey(word);
            newKey.setSecondKey(num);
            context.write(newKey, num);
            
        }
    }

    // 继承Reducer类
    public static class Reduce extends
            Reducer<TextIntKey, IntWritable, Text, Text> {
    	StringBuffer sb = new StringBuffer();
        Text sore = new Text();
        int result=0;
        public void reduce(TextIntKey key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
        	Iterator<IntWritable> it = values.iterator();
        	sb.delete(0, sb.length());//先清除上一个组的数据
            while(it.hasNext()){
                sb.append(it.next()+",");
            }
            if(sb.length()>0){
                sb.deleteCharAt(sb.length()-1);
            }
            sore.set(sb.toString());
            context.write(key.getFirstKey(),sore);
        }
    }
    public int run (String [] args)throws Exception{
    	Job job = new Job(getConf()); // 创建一个作业对象
    	job.setJarByClass(second_sort.class); // 设置运行/处理该作业的类
        job.setJobName("secondsort");  

        job.setPartitionerClass(MyPartitioner.class);//设置分区方法,未设置则会用到自定义Key的hashCode()方法进行分区
        job.setNumReduceTasks(1);
        job.setSortComparatorClass(MyComparator.class);//设置自定义二次排序策略,每个分区内部/reduce接收到所有map传输过来的数据之后调用,未设置则用自定义Key的compareTo()方法
        job.setGroupingComparatorClass(MyGroupSort.class); //设置自定义分组策略,未指定则默认分组
        
        job.setMapOutputKeyClass(TextIntKey.class);
        job.setMapOutputValueClass(IntWritable.class);    
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean success=job.waitForCompletion(true);
        return success?0:1;
    }
    public static void main(String[] args) throws Exception {
        int ret=ToolRunner.run(new second_sort(),args);
        System.exit(ret);
    }

}