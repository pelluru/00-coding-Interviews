package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem005SecondLargest;


public class TestProblem005SecondLargest {
    @Test void t() { assertEquals(2, Problem005SecondLargest.secondLargest(new int[]{1,3,2})); }
}

