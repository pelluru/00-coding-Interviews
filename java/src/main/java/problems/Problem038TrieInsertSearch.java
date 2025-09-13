package problems;


import java.util.*;
public class Problem038TrieInsertSearch {
    static class TrieNode{java.util.Map<Character,TrieNode> c=new java.util.HashMap<>(); boolean end=false;}
    public static class Trie{ TrieNode root=new TrieNode(); void insert(String w){ TrieNode n=root; for(char ch: w.toCharArray()) n=n.c.computeIfAbsent(ch,k->new TrieNode()); n.end=true; } boolean search(String w){ TrieNode n=root; for(char ch:w.toCharArray()){ if(!n.c.containsKey(ch)) return false; n=n.c.get(ch);} return n.end; } }
}

