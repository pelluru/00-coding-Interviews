package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem032CoinChangeMin;


public class TestProblem032CoinChangeMin {
    @Test void t() { assertEquals(3, Problem032CoinChangeMin.coinChange(new int[]{1,2,5},11)); }
}

