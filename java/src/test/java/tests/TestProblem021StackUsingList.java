package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem021StackUsingList;


public class TestProblem021StackUsingList {
    @Test void t() { var s=new Problem021StackUsingList.StackX<Integer>(); s.push(1); s.push(2); assertEquals(2,s.pop().intValue()); }
}

