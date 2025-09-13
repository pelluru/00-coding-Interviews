package problems;


public class Problem035SerializeDeserializeTree {
    public static class Node{int val; Node left,right; Node(int v){val=v;}}
    public static String serialize(Node root){ StringBuilder sb=new StringBuilder(); ser(root,sb); return sb.toString(); }
    static void ser(Node n,StringBuilder sb){ if(n==null){sb.append("#,");return;} sb.append(n.val).append(','); ser(n.left,sb); ser(n.right,sb);}
    public static Node deserialize(String data){ String[] t=data.split(","); int[] i=new int[]{0}; return des(t,i);}
    static Node des(String[] t,int[] i){ if(i[0]>=t.length) return null; String v=t[i[0]++]; if(v.equals("#")||v.isEmpty()) return null; Node n=new Node(Integer.parseInt(v)); n.left=des(t,i); n.right=des(t,i); return n; }
}

