testdf = head(df)
testdf = rbind(testdf, testdf[1,])
testdf[7,3]<-1
testdf[7,2]<-"fakemovie"
testdf[2,2]<-"fakemovie"
testdf

testdf = df[sample(10000,100),]
length(unique(testdf$userID))
length(unique(testdf$businessID))

inp = to.dfs(testdf)

PreProcessUser = mapreduce(input = inp,
                       map = function(.,v){
                         keyval(v[,1],v[,c(2,3)])
                       },
                       reduce = function(k,v){
                         avg = scale(v[,2],scale = FALSE)
                         keyval(k,cbind(v[,1],avg))
                       }
                       )

ans <- NULL
ans <- from.dfs(PreProcessUser)
ans



ratings <- as.numeric(ans$val[,2])
businesses <- ans$val[,1]
users <- ans$key

testdf <- data.frame(users,businesses,ratings, stringsAsFactors=F)

#scale by movie
inp = to.dfs(testdf)
PreProcessBusiness = mapreduce(input = inp,
                       map = function(.,v){
                         keyval(v[,2],v[,c(1,3)])
                       },
                       reduce = function(k,v){
                         avg = scale(v[,2],scale = FALSE)
                         keyval(k,cbind(v[,1],avg))
                       }
)
ans <- NULL
ans <- from.dfs(PreProcessBusiness)
ans

ratings <- as.numeric(ans$val[,2])
users <- ans$val[,1]
businesses <- ans$key

testdf <- data.frame(users,businesses,ratings, stringsAsFactors=F)


for (i in range(1,d)){
  print(i)
}