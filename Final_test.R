conn <- file("/home/tengig/tmpNFvucr",open="r")
linn <-readLines(conn)
s<-linn[233313]
fromJSON(s)
