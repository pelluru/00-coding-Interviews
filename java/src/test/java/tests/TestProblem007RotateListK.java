package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem007RotateListK;


public class TestProblem007RotateListK {
    @Test void t() { assertArrayEquals(new int[]{4,1,2,3}, Problem007RotateListK.rotateRight(new int[]{1,2,3,4},1)); }
}

