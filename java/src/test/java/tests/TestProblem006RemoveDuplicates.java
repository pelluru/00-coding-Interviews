package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem006RemoveDuplicates;


public class TestProblem006RemoveDuplicates {
    @Test void t() { assertArrayEquals(new int[]{1,2}, Problem006RemoveDuplicates.dedup(new int[]{1,2,1})); }
}

