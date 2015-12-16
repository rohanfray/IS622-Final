conn <- file("/home/tengig/tmpNFvucr",open="r")
linn <-readLines(conn)
s<-linn[233313]
fromJSON(s)

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

df <- data.frame(y,y2,x, stringsAsFactors=FALSE)
colnames(df) <- c("userID","businessID","rating")
