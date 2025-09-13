package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem017PowerSet;


public class TestProblem017PowerSet {
    @Test void t() { assertEquals(8, Problem017PowerSet.powerSet(new int[]{1,2,3}).size()); }
}

