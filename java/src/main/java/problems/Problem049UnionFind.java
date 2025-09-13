package problems;


public class Problem049UnionFind {
    public static class UF{ int[] p, r; public UF(int n){ p=new int[n]; r=new int[n]; for(int i=0;i<n;i++)p[i]=i; } public int find(int x){ if(p[x]!=x) p[x]=find(p[x]); return p[x]; } public void union(int a,int b){ int ra=find(a), rb=find(b); if(ra==rb) return; if(r[ra]<r[rb]) p[ra]=rb; else if(r[ra]>r[rb]) p[rb]=ra; else{ p[rb]=ra; r[ra]++; } } }
}

