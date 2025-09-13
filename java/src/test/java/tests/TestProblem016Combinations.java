package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem016Combinations;


public class TestProblem016Combinations {
    @Test void t() { assertEquals(6, Problem016Combinations.combine(4,2).size()); }
}

