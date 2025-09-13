package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem015Permutations;


import java.util.*;
public class TestProblem015Permutations {
    @Test void t() { assertEquals(6, Problem015Permutations.permute(new int[]{1,2,3}).size()); }
}

