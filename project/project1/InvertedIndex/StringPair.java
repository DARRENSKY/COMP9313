//package project;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class StringPair implements WritableComparable<StringPair> {

    private String first;
    private String second;

    public StringPair() {
    }

    public StringPair(String first, String second) {
        set(first, second);
    }

    public void set(String left, String right) {
        first = left;
        second = right;
    }

    public String getFirst() {
        return first;
    }

    public String getSecond() {
        return second;
    }

    public void readFields(DataInput in) throws IOException {
        String[] strings = WritableUtils.readStringArray(in);
        first = strings[0];
        second = strings[1];
    }

    public void write(DataOutput out) throws IOException {
        String[] strings = new String[] { first, second };
        WritableUtils.writeStringArray(out, strings);
    }
//
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(first + " " + second);
        return sb.toString();
    }
//
//    @Override
//    public boolean equals(Object o) {
//        if (this == o)
//            return true;
//        if (o == null || getClass() != o.getClass())
//            return false;
//
//        StringPair that = (StringPair) o;
//
//        if (first != null ? !first.equals(that.first) : that.first != null)
//            return false;
//        if (second != null ? !second.equals(that.second) : that.second != null)
//            return false;
//
//        return true;
//    }
//
//    @Override
//    public int hashCode() {
//        int result = first != null ? first.hashCode() : 0;
//        result = 31 * result + (second != null ? second.hashCode() : 0);
//        return result;
//        //return first.hashCode();
//    }

    private int compare(String s1, String s2){
        if (s1 == null && s2 != null) {
            return -1;
        } else if (s1 != null && s2 == null) {
            return 1;
        } else if (s1 == null && s2 == null) {
            return 0;
        } else {
            return s1.compareTo(s2);
        }
    }

    private int compare2(String s1, String s2){
        if (s1 == null && s2 != null) {
            return -1;
        } else if (s1 != null && s2 == null) {
            return 1;
        } else if (s1 == null && s2 == null) {
            return 0;
        } else {
            if (s1.equals("*") || s2.equals("*")){
                return s1.compareTo(s2);
            }
            else if (Integer.parseInt(s1) > Integer.parseInt(s2)){
                return 1;
            }
            else if (Integer.parseInt(s1) < Integer.parseInt(s2)) {
                return -1;
            }
            else{
                return 0;
            } }
    }


    @Override
    public int compareTo(StringPair o) {
        int cmp = compare(first, o.getFirst());
        if(cmp != 0){
            return cmp;
        }

        return compare2(second, o.getSecond());

    }

}


