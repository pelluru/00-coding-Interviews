package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem020LruCacheSimple;


public class TestProblem020LruCacheSimple {
    @Test void t() { var l=new Problem020LruCacheSimple.LRU<Integer,Integer>(2); l.put(1,1); l.put(2,2); l.get(1); l.put(3,3); assertFalse(l.containsKey(2)); }
}

