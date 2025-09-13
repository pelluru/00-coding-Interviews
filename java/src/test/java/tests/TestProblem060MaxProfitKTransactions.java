package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem060MaxProfitKTransactions;


public class TestProblem060MaxProfitKTransactions {
    @Test void t(){
        assertEquals(7, Problem060MaxProfitKTransactions.maxProfit(2, new int[]{3,2,6,5,0,3}));
    }
}

