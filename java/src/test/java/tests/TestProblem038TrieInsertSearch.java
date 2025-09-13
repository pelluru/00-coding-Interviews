package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem038TrieInsertSearch;


public class TestProblem038TrieInsertSearch {
    @Test void t(){ var T=new Problem038TrieInsertSearch.Trie(); T.insert("hi"); assertTrue(T.search("hi")); assertFalse(T.search("h")); }
}

