package problems;


public class Problem008MergeTwoSortedLists {
    public static class ListNode{int val; ListNode next; ListNode(int v){val=v;}}
    public static ListNode merge(ListNode a,ListNode b){
        ListNode d=new ListNode(0), t=d;
        while(a!=null&&b!=null){
            if(a.val<b.val){t.next=a;a=a.next;} else {t.next=b;b=b.next;}
            t=t.next;
        } t.next=(a!=null?a:b); return d.next;
    }
}

