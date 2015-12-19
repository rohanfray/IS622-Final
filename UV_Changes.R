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
                       tmp.M <- testdf[testdf$users==k,]
                       
                       for (i in range(1,d)){
                         
                       }
                       
                       keyval(k,v)
                     })

from.dfs(ChangeU)
