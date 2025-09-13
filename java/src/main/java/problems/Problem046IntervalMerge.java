package problems;


public class Problem046IntervalMerge {
    public static int[][] merge(int[][] intervals){
        java.util.Arrays.sort(intervals,(a,b)->Integer.compare(a[0],b[0]));
        java.util.List<int[]> res=new java.util.ArrayList<>();
        for(int[] in: intervals){
            if(res.isEmpty()|| res.get(res.size()-1)[1] < in[0]) res.add(in.clone());
            else res.get(res.size()-1)[1] = Math.max(res.get(res.size()-1)[1], in[1]);
        } return res.toArray(new int[0][]);
    }
}

