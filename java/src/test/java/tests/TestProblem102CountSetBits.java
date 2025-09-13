package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem102CountSetBits;


public class TestProblem102CountSetBits {
    @Test void t(){ assertEquals(3, Problem102CountSetBits.countBits(0b1011)); }
}

