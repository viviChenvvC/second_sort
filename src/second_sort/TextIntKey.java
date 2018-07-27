package second_sort;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;

public class TextIntKey implements WritableComparable<TextIntKey>{
    private Text firstKey;
    private IntWritable secondKey;
    public TextIntKey() {
        this.firstKey = new Text();
        this.secondKey = new IntWritable();
    }
    public Text getFirstKey() {
        return this.firstKey;
    }
    public void setFirstKey(Text firstKey) {
        this.firstKey = firstKey;
    }
    public IntWritable getSecondKey() {
        return this.secondKey;
    }
    public void setSecondKey(IntWritable secondKey) {
        this.secondKey = secondKey;
    }

    public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
    	this.firstKey.readFields(arg0);
        this.secondKey.readFields(arg0);
	}
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		 this.firstKey.write(arg0);
	     this.secondKey.write(arg0);
	}
	//分区和分组调用compareTo函数，分区和分组的依据是原始的KEY而不是组合KEY
	public int compareTo(TextIntKey o) {
		// TODO Auto-generated method stub
		if(!this.firstKey.equals(o.getFirstKey()))
		   return this.firstKey.compareTo(o.getFirstKey());//升序
		else
		   return this.secondKey.compareTo(o.getSecondKey());	
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((firstKey == null) ? 0 : firstKey.hashCode());
		result = prime * result
				+ ((secondKey == null) ? 0 : secondKey.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() == obj.getClass()){
			TextIntKey o=(TextIntKey)obj;
			return o.firstKey==firstKey&&o.secondKey==secondKey;
		}
		else
			return false;
	}

}
