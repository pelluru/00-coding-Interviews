package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem003CharFrequency;


import java.util.*;
public class TestProblem003CharFrequency {
    @Test void t() {
        Map<Character,Integer> m=Problem003CharFrequency.charFrequency("aab");
        assertEquals(2, (int)m.get('a'));
        assertEquals(1, (int)m.get('b'));
    }
}

