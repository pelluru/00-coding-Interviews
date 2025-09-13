package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem044FindDuplicate;


public class TestProblem044FindDuplicate {
    @Test void t(){ assertEquals(2, Problem044FindDuplicate.findDuplicate(new int[]{1,3,4,2,2})); }
}

