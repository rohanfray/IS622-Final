
RMSE.old <- RMSE

now<- Sys.time()
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
RMSE.old - RMSE
Sys.time()-now



#RMSEinp <- to.dfs(testdf2)
#now<- format(Sys.time(), "%a %b %d %X %Y")
#The below MR is too slow
# SEcalc <- mapreduce(input = RMSEinp, 
#                       map = function(.,v){
#                           keyval(v[,(1:3)],v[,(4:(3+(2*d)))])
#                       },
#                       reduce= function(k,v){
#                         newcalc <-0
#                         for (i in range(1:d)){
#                           newcalc = newcalc + v[i]*v[d+i]
#                         }
#                         
#                         keyval(1,(k[3]-newcalc)^2)
#                       })



#SE <- from.dfs(SEcalc)
#MSE <- sum(unlist(SE$val))/nrow(df.scale)
#then <- format(Sys.time(), "%a %b %d %X %Y")