package problems;


public class Problem005SecondLargest {
    public static Integer secondLargest(int[] a) {
        if(a==null||a.length<2) return null;
        Integer first=null, second=null;
        for(int n: a){
            if(first==null||n>first){second=first; first=n;}
            else if(n!=first && (second==null||n>second)) second=n;
        }
        return second;
    }
}

