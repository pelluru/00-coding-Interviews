package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem099EmailValidationRegex;


public class TestProblem099EmailValidationRegex {
    @Test void t(){ assertTrue(Problem099EmailValidationRegex.isValid("a@b.com")); assertFalse(Problem099EmailValidationRegex.isValid("not-an-email")); }
}

