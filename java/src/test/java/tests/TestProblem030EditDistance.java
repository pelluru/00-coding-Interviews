package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem030EditDistance;


public class TestProblem030EditDistance {
    @Test void t() { assertEquals(3, Problem030EditDistance.edit("kitten","sitting")); }
}

