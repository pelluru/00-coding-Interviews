package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem041SieveEratosthenes;


import java.util.*;
public class TestProblem041SieveEratosthenes {
    @Test void t(){ assertEquals(List.of(2,3,5,7), Problem041SieveEratosthenes.sieve(10)); }
}

