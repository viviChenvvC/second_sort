package second_sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义分组策略
 * 将组合将中第一个值相同的分在一组
 */
public class MyGroupSort extends WritableComparator{
	//必须要调用父类的构造器
    public MyGroupSort() {
        super(TextIntKey.class,true);//注册
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
    	TextIntKey ck1 = (TextIntKey)a;
        TextIntKey ck2 = (TextIntKey)b;
        return ck1.getFirstKey().compareTo(ck2.getFirstKey());
    }
}