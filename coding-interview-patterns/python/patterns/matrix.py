def search_sorted_matrix(M, target):
    if not M or not M[0]: return False
    m,n=len(M),len(M[0]); i,j=0,n-1
    while i<m and j>=0:
        if M[i][j]==target: return True
        if M[i][j]>target: j-=1
        else: i+=1
    return False

def rotate_image(mat):
    n=len(mat)
    for i in range(n):
        for j in range(i+1,n):
            mat[i][j],mat[j][i]=mat[j][i],mat[i][j]
    for row in mat: row.reverse()
    return mat

def flood_fill(image, sr, sc, newColor):
    m,n=len(image),len(image[0])
    old=image[sr][sc]
    if old==newColor: return image
    def dfs(i,j):
        if i<0 or i>=m or j<0 or j>=n or image[i][j]!=old: return
        image[i][j]=newColor
        dfs(i+1,j); dfs(i-1,j); dfs(i,j+1); dfs(i,j-1)
    dfs(sr,sc); return image
