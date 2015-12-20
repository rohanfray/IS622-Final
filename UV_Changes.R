#Changing values in U
d=2
testdf <- df.scale

a = sd(testdf$ratings)
#a = mean(testdf$ratings)
set.seed(622)
testU <- jitter(matrix(sqrt(a/d),nrow = length(unique(testdf$users)),ncol = d))
rownames(testU)<-unique(testdf$users)

set.seed(622)
testV <- jitter(matrix(sqrt(a/d),ncol = length(unique(testdf$businesses)),nrow = d))
colnames(testV) <- unique(testdf$businesses)



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

Sys.time()-now
