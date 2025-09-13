package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem018FibMemo;


public class TestProblem018FibMemo {
    @Test void t() { assertEquals(55L, Problem018FibMemo.fib(10)); }
}

