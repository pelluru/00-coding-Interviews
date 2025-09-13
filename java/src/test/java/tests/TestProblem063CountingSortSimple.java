package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem063CountingSortSimple;


public class TestProblem063CountingSortSimple {
    @Test void t(){
        int[] a={3,1,2,1,0}; assertArrayEquals(new int[]{0,1,1,2,3}, Problem063CountingSortSimple.sort(a,3));
    }
}

