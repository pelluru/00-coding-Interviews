package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem105PowerSetIterative;


import java.util.*;
public class TestProblem105PowerSetIterative {
    @Test void t(){ assertEquals(8, Problem105PowerSetIterative.powerSet(new int[]{1,2,3}).size()); }
}

