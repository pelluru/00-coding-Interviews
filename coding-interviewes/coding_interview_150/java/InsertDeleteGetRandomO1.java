
/* Insert Delete GetRandom O(1) — ArrayList+HashMap swap-remove. */
import java.util.*;
public class InsertDeleteGetRandomO1 {
    static class RandomizedSet {
        List<Integer> arr=new ArrayList<>(); Map<Integer,Integer> pos=new HashMap<>(); Random rnd=new Random();
        public boolean insert(int v){ if (pos.containsKey(v)) return false; pos.put(v,arr.size()); arr.add(v); return true; }
        public boolean remove(int v){ Integer i=pos.get(v); if (i==null) return false; int last=arr.get(arr.size()-1);
            arr.set(i,last); pos.put(last,i); arr.remove(arr.size()-1); pos.remove(v); return true; }
        public int getRandom(){ return arr.get(rnd.nextInt(arr.size())); }
    }
    public static void main(String[] args){
        RandomizedSet rs=new RandomizedSet(); System.out.println(rs.insert(1)); System.out.println(rs.remove(2)); System.out.println(rs.insert(2)); System.out.println(rs.getRandom());
    }
}
