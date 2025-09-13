package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem045SearchInsertPosition;


public class TestProblem045SearchInsertPosition {
    @Test void t(){ assertEquals(2, Problem045SearchInsertPosition.searchInsert(new int[]{1,2,4,5},3)); }
}

