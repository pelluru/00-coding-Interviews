package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem013KthLargest;


public class TestProblem013KthLargest {
    @Test void t() { assertEquals(3, Problem013KthLargest.kthLargest(new int[]{3,2,1,5,6,4}, 3)); }
}

