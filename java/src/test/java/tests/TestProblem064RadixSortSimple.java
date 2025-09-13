package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem064RadixSortSimple;


public class TestProblem064RadixSortSimple {
    @Test void t(){
        int[] a={170,45,75,90,802,24,2,66}; assertArrayEquals(new int[]{2,24,45,66,75,90,170,802}, Problem064RadixSortSimple.sort(a));
    }
}

