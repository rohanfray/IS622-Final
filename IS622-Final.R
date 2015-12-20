d=2

library(jsonlite)
conn <- file("/home/tengig/tmpNFvucr",open="r")
linn <-readLines(conn)

j=0
x <- numeric(330071)
y <- character(330071)
y2 <- character(330071)
for (i in 1:length(linn)){
  tmp = fromJSON(linn[i])
  if (tmp$type == "review"){
    x[j] <- tmp$stars
    y[j] <- tmp$user_id
    y2[j] <- tmp$business_id
    j=j+1
  }
  
}

df <- data.frame(y,y2,x, stringsAsFactors=F)
#df <- data.frame(y,y2,x, stringsAsFactors=T)
colnames(df) <- c("userID","businessID","rating")
#removing blank rows
df[df==""]<-NA
df = na.omit(df)


set.seed(622)
df.subset = df[sample(330070,10000),]

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

testdf <- df.scale

a = sd(testdf$ratings)
#a = mean(testdf$ratings)
testU <- jitter(matrix(sqrt(a/d),nrow = length(unique(testdf$users)),ncol = d))
rownames(testU)<-unique(testdf$users)

testV <- jitter(matrix(sqrt(a/d),ncol = length(unique(testdf$businesses)),nrow = d))
colnames(testV) <- unique(testdf$businesses)

testdf2 <-df.scale

Uvals<- matrix(unlist(lapply(testdf2$users, function(x){testU[x,]})),ncol=d,byrow = T)
Vvals<- t(matrix(unlist(lapply(testdf2$businesses,function(x){testV[,x]})),nrow=d,byrow=F))
vals<-apply(cbind(Uvals,Vvals),1,function(x){
  newcalc<-0
  for(i in range(1:d)){
    newcalc = newcalc + x[i]*x[d+i]
  }
  newcalc
})
SE <- (vals-testdf$ratings)^2
MSE <- sum(SE)/nrow(df.scale)
RMSE <- sqrt(MSE)
RMSE


for (q in (1:4)){
  now<- Sys.time()
  inpU <- to.dfs(testU)
  ChangeU <- mapreduce(input = inpU, 
                       map = function(.,v){
                         keyval(rownames(v),v) 
                       },
                       reduce = function(k,v){
                         #our values for the row in U and each of its column
                         vals <- numeric(d)
                         
                         #values of M for the row in U
                         tmp.M <- testdf[testdf$users==k,]
                         
                         for (i in range(1,d)){
                           denom <- sum((testV[i,unlist(tmp.M$businesses)])^2)
                           num <-0
                           for (j in tmp.M$businesses){
                             temp1 <- testV[i,j]
                             temp2 <- tmp.M[tmp.M$businesses==j,]$ratings
                             temp3 <- 0
                             for (kiter in range(1,d)){
                               if (kiter != i){
                                 temp3 = temp3 + (v[kiter]*testV[kiter,j])
                               }
                             }
                             num = temp1*(temp2-temp3)
                             
                           }
                           
                           vals[i] = (num/denom)
                         }
                         
                         keyval(k,list(vals))
                       })
  
  Uout<-from.dfs(ChangeU)
  testU<-matrix(unlist(Uout$val),ncol = d,byrow = TRUE)
  rownames(testU)<-Uout$key
  
  
  
  inpV <- to.dfs(testV)
  ChangeV <- mapreduce(input = inpV, 
                       map = function(.,v){
                         keyval(colnames(v),t(v)) 
                       },
                       reduce = function(k,v){
                         #our values for the column in V and each of its row
                         vals <- numeric(d)
                         
                         #values of M for the row in U
                         tmp.M <- testdf[testdf$businesses==k,]
                         
                         for (i in range(1,d)){
                           denom <- sum((testU[unlist(tmp.M$users),i])^2)
                           
                           num <-0
                           for (j in tmp.M$users){
                             temp1 <- testU[j,i]
                             temp2 <- tmp.M[tmp.M$users==j,]$ratings
                             temp3 <- 0
                             for (kiter in range(1,d)){
                               if (kiter != i){
                                 temp3 = temp3 + (v[kiter]*testU[j,kiter])
                               }
                             }
                             num = temp1*(temp2-temp3)
                           }
                           
                           vals[i] = (num/denom)
                         }
                         
                         keyval(k,list(vals))
                       })
  
  Vout<-from.dfs(ChangeV)
  testV <- matrix(unlist(Vout$val),nrow = d)
  colnames(testV)<-Vout$key
  
  print(Sys.time()-now)
}
RMSE.old <-RMSE
