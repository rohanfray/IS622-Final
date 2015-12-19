RMSEinp <- to.dfs(df.scale)


RMSEcalc <- mapreduce(input = RMSEinp, 
                      map = function(.,v){
                        Uvals<- matrix(unlist(lapply(v$users, function(x){testU[x,]})),ncol=d,byrow = T)
                        Vvals<- t(matrix(unlist(lapply(v$businesses,function(x){testV[,x]})),nrow=d,byrow=F))
                          
                        keyval(v,)
                      })


q <- lapply(tmp$businesses, function(x){testV[,x]})
q<-unlist(q)
q<-matrix(q,nrow=d,byrow = F)
q<-t(q)
