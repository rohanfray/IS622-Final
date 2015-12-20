library(Matrix)
mat<-with(df.scale, 
          sparseMatrix(i =as.numeric(users), 
                       j=as.numeric(businesses), 
                       x=ratings,
                       dimnames=list(levels(users), levels(businesses))))
