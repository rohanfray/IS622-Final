set.seed(622)
df.subset = df[sample(330070,100000),]
#df.subset = df
inp <- to.dfs(df.subset)
#scale by User
PreProcessUser = mapreduce(input = inp,
                           map = function(.,v){
                             keyval(v[,1],v[,c(2,3)])
                           },
                           reduce = function(k,v){
                             avg = scale(v[,2],scale = FALSE)
                             keyval(k,cbind(v[,1],avg))
                           }
)


ans <- from.dfs(PreProcessUser)
#recreate df by ans
ratings <- as.numeric(ans$val[,2])
businesses <- ans$val[,1]
users <- ans$key
df.scale <- data.frame(users,businesses,ratings, stringsAsFactors=F)



#scale by Business
inp = to.dfs(df.scale)
PreProcessBusiness = mapreduce(input = inp,
                               map = function(.,v){
                                 keyval(v[,2],v[,c(1,3)])
                               },
                               reduce = function(k,v){
                                 avg = scale(v[,2],scale = FALSE)
                                 keyval(k,cbind(v[,1],avg))
                               }
)

ans <- from.dfs(PreProcessBusiness)
#recreate df by ans
ratings <- as.numeric(ans$val[,2])
users <- ans$val[,1]
businesses <- ans$key
df.scale <- data.frame(users,businesses,ratings, stringsAsFactors=F)

remove(df.subset)
remove(businesses)
remove(conn)
remove(ans)
remove(i)
remove(j)
remove(ratings)
remove(tmp)
remove(users)
remove(x)
remove(y)
remove(y2)