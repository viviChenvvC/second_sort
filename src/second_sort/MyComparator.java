package second_sort;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyComparator extends WritableComparator {
	public MyComparator() {
        super(TextIntKey.class,true);
    }
    @Override
    public int compare(WritableComparable a,
            WritableComparable b) {                                                                                                                                                                                             
        TextIntKey c1 = (TextIntKey) a;
        TextIntKey c2 = (TextIntKey) b;
        //a与b进行比较
        //如果a在前b在后，则会产生升序
        //如果a在后b在前，则会产生降序
        if(!c1.getFirstKey().equals(c2.getFirstKey())){
            return c1.getFirstKey().compareTo(c2.getFirstKey());
            }
        else{
            return c1.getSecondKey().compareTo(c2.getSecondKey());
        }
    }
}