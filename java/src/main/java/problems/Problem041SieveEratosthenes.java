package problems;


import java.util.*;
public class Problem041SieveEratosthenes {
    public static List<Integer> sieve(int n){
        boolean[] p=new boolean[n+1]; java.util.Arrays.fill(p,true); if(n>=0)p[0]=false; if(n>=1)p[1]=false;
        for(int i=2;i*i<=n;i++) if(p[i]) for(int j=i*i;j<=n;j+=i) p[j]=false;
        List<Integer> res=new ArrayList<>(); for(int i=2;i<=n;i++) if(p[i]) res.add(i); return res;
    }
}

