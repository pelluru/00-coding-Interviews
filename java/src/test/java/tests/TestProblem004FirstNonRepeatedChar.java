package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem004FirstNonRepeatedChar;


public class TestProblem004FirstNonRepeatedChar {
    @Test void t() { assertEquals(Character.valueOf('c'), Problem004FirstNonRepeatedChar.firstNonRepeated("aabbc")); }
}

