package problems;


import java.util.*;
public class Problem018FibMemo {
    static Map<Integer,Long> memo=new HashMap<>();
    public static long fib(int n){ if(n<2) return n; if(memo.containsKey(n)) return memo.get(n); long v=fib(n-1)+fib(n-2); memo.put(n,v); return v; }
}

