
inp <- to.dfs(df)

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
df.scale2 <- data.frame(users,businesses,ratings, stringsAsFactors=F)
