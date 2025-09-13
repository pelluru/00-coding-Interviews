package problems;


import java.util.*;
public class Problem020LruCacheSimple {
    public static class LRU<K,V> extends LinkedHashMap<K,V>{
        private final int cap;
        public LRU(int cap){ super(16,0.75f,true); this.cap=cap; }
        protected boolean removeEldestEntry(Map.Entry<K,V> e){ return size()>cap; }
    }
}

