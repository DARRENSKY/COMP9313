import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


    public class IntPair implements WritableComparable<IntPair> {
        private int first = 0;
        private int second = 0;

        public IntPair(int left, int right) {
            set(left, right);
        }
        
        public IntPair() {}

        public String toString(){
            return "(" + second + "," + first + ")";
        }

        /**
        * Set the left and right values.
        */
        public void set(int left, int right) {
            first = left;
            second = right;
        }

        public int getFirst() {
            return first;
        }

        public int getSecond() {
            return second;
        }

        /**
        * Read the two integers. 
        * Encoded as: MIN_VALUE -> 0, 0 -> -MIN_VALUE, MAX_VALUE-> -1
        */
        @Override
        public void readFields(DataInput in) throws IOException {
            first = in.readInt() + Integer.MIN_VALUE;
            second = in.readInt() + Integer.MIN_VALUE;
        }
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(first - Integer.MIN_VALUE);
            out.writeInt(second - Integer.MIN_VALUE);
        }

        @Override
        public int hashCode() {
        return first * 157 + second;
        }

        @Override
        public boolean equals(Object right) {
            if (right instanceof IntPair) {
                IntPair r = (IntPair) right;
                return r.first == first && r.second == second;
            } else {
            return false;
        }
    }

    /** A Comparator that compares serialized IntPair. */ 
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(IntPair.class);
        }

        public int compare(byte[] b1, int s1, int l1,
                            byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    static {                                        // register this comparator
        WritableComparator.define(IntPair.class, new Comparator());
    }

    @Override
    public int compareTo(IntPair o) {
        if (first != o.first) {
            return first < o.first ? -1 : 1;
        } else if (second != o.second) {
            return second < o.second ? -1 : 1;
        } else {
            return 0;
        }
    }

    public static int compare(int left, int right) {
        // TODO Auto-generated method stub
        return left > right ? 1 : (left == right ? 0 : -1);
        }
    }




    public static class FirstPartitioner extends Partitioner<IntPair, NullWritable>{
        @Override
        public int getPartition(IntPair key, NullWritable value, int numPartitions) {
            return Math.abs(key.getFirst() * 127) % numPartitions;
        }
    }

    public static class FirstGroupingComparator implements RawComparator<IntPair> {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8, b2, s2, Integer.SIZE/8);
        }

        @Override
        public int compare(IntPair o1, IntPair o2) {
            int l = o1.getFirst();
            int r = o2.getFirst();
            return l == r ? 0 : (l < r ? -1 : 1);
        }
    }
 
    public static class KeyComparator extends WritableComparator{
        protected KeyComparator() {
            super(IntPair.class, true);
            // TODO Auto-generated constructor stub
        }


        @Override
        public int compare(WritableComparable o1, WritableComparable o2) {
            // TODO Auto-generated method stub
            IntPair ip1 = (IntPair) o1;
            IntPair ip2 = (IntPair) o2;
            int cmp = IntPair.compare(ip1.getFirst(), ip2.getFirst());
            if(cmp != 0)
                return cmp;
            return -IntPair.compare(ip1.getSecond(), ip2.getSecond());
        }


        // @Override
        // public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        //     // TODO Auto-generated method stub
        //     return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        //     return 0;
        // }
    }