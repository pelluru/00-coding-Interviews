package problems;


public class Problem010BinarySearch {
    public static int search(int[] a,int target){
        int lo=0,hi=a.length-1; while(lo<=hi){int mid=(lo+hi)/2; if(a[mid]==target)return mid; if(a[mid]<target)lo=mid+1; else hi=mid-1;} return -1;
    }
}

