package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem047BestTimeToBuySellStock;


public class TestProblem047BestTimeToBuySellStock {
    @Test void t(){ assertEquals(5, Problem047BestTimeToBuySellStock.maxProfit(new int[]{7,1,5,3,6,4})); }
}

