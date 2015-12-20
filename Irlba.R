library(Matrix)
library(irlba)


us<-as.factor(df.scale$users)
us.int <-as.integer(us)

bs<-as.factor(df.scale$businesses)
bs.int <-as.integer(bs)

mat<-with(df.scale, 
          sparseMatrix(i=us.int, 
                       j=bs.int, 
                       x=ratings,
                       dimnames=list(levels(us), levels(bs))))


L <- irlba(mat,nv = 2,nu = 2)

L.d <- diag(L$d)
rev<-L$u%*%L.d
rownames(rev)<-levels(us)

rev.v <- t(L$v)
colnames(rev.v)<-levels(bs)

rm(list = c("us","us.int","bs","bs.int"))


Uvals<- matrix(unlist(lapply(testdf2$users, function(x){rev[x,]})),ncol=d,byrow = T)
Vvals<- t(matrix(unlist(lapply(testdf2$businesses,function(x){rev.v[,x]})),nrow=d,byrow=F))

vals<-apply(cbind(Uvals,Vvals),1,function(x){
  newcalc<-0
  for(i in range(1:d)){
    newcalc = newcalc + x[i]*x[d+i]
  }
  newcalc
})
SE <- (vals-testdf$ratings)^2
MSE <- sum(SE)/nrow(df.scale)
RMSE.irlba <- sqrt(MSE)
RMSE.irlba
