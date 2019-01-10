package comp9313.ass2;

import org.apache.hadoop.util.StringUtils;

public class Node {
	private String distance;		// the distance from source
	private String[] adjs;			// the neighbors of current node
	
	public String getDistance() {
		return distance;
	}
	
	public void setDistance(String distance) {
		this.distance = distance;
	}
	
	public void setDistance(int distance) {
		this.distance = Integer.toString(distance);
	}
	
	public String getKey(String str) {
		return str.substring(1, str.indexOf(","));
	}
	
	public String getValue(String str) {
		return str.substring(str.indexOf(",") + 2, str.indexOf(")"));
	}
	
	public String getNodeKeyByIndex(int idx) {
		return getKey(adjs[idx]);
	}
	
	public String getNodeValueByIndex(int idx) {
		return getValue(adjs[idx]);
	}
	
	public int getAdjsNum() {
		return adjs.length;
	}
	
//  1	inf	(3, 1.0)	(2, 3.0)
//  2   5
//	3   inf
	public void FormatNode(String str) {
		// without neighbours
		if (str.length() == 0) {
			return;
		}
		
		String[] strs = StringUtils.split(str, '\t');
		this.adjs = new String[strs.length - 1];
		
		for (int i = 0; i < strs.length; i++) {
			if (i == 0) {	// the first one is distance
				setDistance(strs[i]);
				continue;
			}
			this.adjs[i - 1] = strs[i];		// 
		}
	}
	
	public String toString() {
		String str = this.distance + "";
		for(String adj : this.adjs) {
			str += "\t" + adj;
		}
		
		return str;
	}
	
	public static void main(String[] args) {
		Node node = new Node();
		node.FormatNode("1\t(A, 20)\t(B, 30)");
		System.out.println("toString");
		System.out.println(node.toString());
		
		System.out.println("getNodeKeyByIndex");
		System.out.println(node.getNodeKeyByIndex(0) + "");
		System.out.println(node.getNodeKeyByIndex(1) + "");
		
		System.out.println("getNodeValueByIndex");
		System.out.println(node.getNodeValueByIndex(1) + "");
		
		System.out.println(node.adjs.toString());
		for(String s : node.adjs) {
			System.out.println(s);
		}
		
		System.out.println(node.adjs[0]);
		
		System.out.println("Distance = *" + node.getDistance());
		System.out.println("Adj = " + node.getAdjsNum());
		System.out.println("NodeValue = *" + node.getNodeValueByIndex(0));
	}
	
	
	
	
}
