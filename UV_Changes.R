#Changing values in U
a = sd(testdf$ratings)
testU <- jitter(matrix(sqrt(a/d),nrow = length(unique(testdf$users)),ncol = d))
rownames(testU)<-unique(testdf$users)

testV <- jitter(matrix(sqrt(a/d),ncol = length(unique(testdf$businesses)),nrow = d))
colnames(testV) <- unique(testdf$businesses)

inp <- to.dfs(testU)
  
  
ChangeU <- mapreduce(input = inp, 
                     map = function(.,v){
                          keyval(rownames(testU),v) 
                     },
                     reduce = function(k,v){
                       #our values for the row in U and each of its column
                       vals <- numeric(d)
                       
                       #values of M for the row in U
                       tmp.M <- testdf[testdf$users==k,]
                       
                       
                       for (i in range(1,d)){
                         denom <- sum((testV[i,unlist(tmp.M$businesses)])^2)
                         
                         
                         
                         vals[i] = (num/denom)
                       }
                       
                       keyval(k,vals)
                     })

from.dfs(ChangeU)
