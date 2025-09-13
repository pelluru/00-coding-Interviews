package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem008MergeTwoSortedLists;


public class TestProblem008MergeTwoSortedLists {
    @Test void t() {
        var A=new Problem008MergeTwoSortedLists.ListNode(1); A.next=new Problem008MergeTwoSortedLists.ListNode(3);
        var B=new Problem008MergeTwoSortedLists.ListNode(2); B.next=new Problem008MergeTwoSortedLists.ListNode(4);
        var R=Problem008MergeTwoSortedLists.merge(A,B);
        assertEquals(1,R.val); assertEquals(2,R.next.val);
    }
}

