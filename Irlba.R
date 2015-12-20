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
