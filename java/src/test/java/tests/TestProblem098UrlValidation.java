package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem098UrlValidation;


public class TestProblem098UrlValidation {
    @Test void t(){ assertTrue(Problem098UrlValidation.isValid("https://example.com")); assertFalse(Problem098UrlValidation.isValid("ht!tp://bad")); }
}

