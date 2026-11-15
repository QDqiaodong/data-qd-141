def align(a,b):
    n,m=len(a),len(b);dp=[[0]*(m+1) for _ in range(n+1)]
    for i in range(n+1):dp[i][0]=i
    for j in range(m+1):dp[0][j]=j
    for i in range(1,n+1):
        for j in range(1,m+1):dp[i][j]=min(dp[i-1][j-1]+(a[i-1]!=b[j-1]),dp[i-1][j]+1,dp[i][j-1]+1)
    i,j=n,m;ops=[]
    while i or j:
        if i and j and dp[i][j]==dp[i-1][j-1]+(a[i-1]!=b[j-1]):
            i-=1;j-=1;ops.append({'op':'match' if a[i]==b[j] else 'replace','before_index':i,'after_index':j,'source':a[i],'target':b[j]})
        elif i and dp[i][j]==dp[i-1][j]+1:
            i-=1;ops.append({'op':'delete','before_index':i,'after_index':j,'source':a[i],'target':None})
        else:
            j-=1;ops.append({'op':'insert','before_index':i,'after_index':j,'source':None,'target':b[j]})
    ops.reverse();return {'distance':dp[n][m],'operations':ops,'counts':{k:sum(o['op']==k for o in ops) for k in ['match','replace','delete','insert']}}
