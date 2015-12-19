d = 2
#We will use the sd of the scaled matrix as the scaled matrix has mean 0
a = sd(df.scale2$ratings)

uniq.users <- unique(df.scale2$users)
U <- matrix(sqrt(a/d),nrow = length(uniq.users),ncol = d)



uniq.busi <- unique(df.scale2$businesses)
V <- matrix(sqrt(a/d),ncol = length(uniq.busi),nrow = d)


#RMSE

val = 2*(sqrt(a/d))^2

SE <- lapply(df.scale2$ratings, function(x){(x-val)^2})
MSE <- Reduce("+", SE)/length(SE)
RMSE <- sqrt(MSE)
