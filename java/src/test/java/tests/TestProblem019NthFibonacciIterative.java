package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem019NthFibonacciIterative;


public class TestProblem019NthFibonacciIterative {
    @Test void t() { assertEquals(55L, Problem019NthFibonacciIterative.fibN(10)); }
}

