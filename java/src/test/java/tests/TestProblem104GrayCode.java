package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem104GrayCode;


import java.util.*;
public class TestProblem104GrayCode {
    @Test void t(){ assertEquals(List.of(0,1,3,2), Problem104GrayCode.grayCode(2)); }
}

