package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem025AnagramGroups;


public class TestProblem025AnagramGroups {
    @Test void t() { assertEquals(6, Problem025AnagramGroups.group(new String[]{"eat","tea","tan","ate","nat","bat"}).stream().mapToInt(java.util.List::size).sum()); }
}

