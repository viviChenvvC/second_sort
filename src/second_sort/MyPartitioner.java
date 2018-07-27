package second_sort;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
//参数为map的输出类型
public class MyPartitioner extends Partitioner<TextIntKey, IntWritable> {
    @Override
    public int getPartition(TextIntKey key, IntWritable value, int numPartitions) {
        // TODO Auto-generated method stub
        return (key.getFirstKey().hashCode()&Integer.MAX_VALUE)%numPartitions;
    }
}