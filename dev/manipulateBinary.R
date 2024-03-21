## fn <- tempfile()

## ## writeBin(1L:3L, fn, size = 1L)
## ## readBin(fn, what = "integer", size = 1L, n = 3L)
## ## #> [1] 1 2 3

## ## fh <- file(fn, "r+b")
## ## isSeekable(fh)
## ## #> [1] TRUE
## ## seek(fh, 1L, origin = "start", rw = "write")
## ## #> [1] 0
## ## # swap the sign of the second value
## ## writeBin(-2L, fh, size = 1L)
## ## close(fh)

## ## readBin(fn, what = "integer", size = 1L, n = 3L)
## ## #> [1]  1 -2  3

## ## unlink(fn)

## ## ## playing with double values
## ## fn <- tempfile()

## ## sz <- 8

## ## writeBin(as.numeric(1:3), fn, size = sz)
## ## readBin(fn, what = "numeric", size = sz, n = 3L)
## ## #> [1] 1 2 3

## ## fh <- file(fn, "r+b")
## ## isSeekable(fh)
## ## #> [1] TRUE
## ## seek(fh, as.integer(1*sz), origin = "start", rw = "write")
## ## #> [1] 0
## ## # swap the sign of the second value
## ## writeBin(as.numeric(-2), fh, size = sz)
## ## close(fh)

## ## readBin(fn, what = "numeric", size = sz, n = 3L)
## ## #> [1]  1 -2  3

## ## unlink(fn)

## ## checking a matching value then replacing
## rm(list=ls())
## fn <- tempfile()

## sz <- 8
## tz_origin <- 1
## tz_step <- 2
## n <- 5

## D <- matrix(c(seq(tz_origin,by=tz_step,length=n),rnorm(n)),n,2)

## writeBin(as.numeric(t(D)), fn, size = sz)
## DD <- matrix( readBin(fn, what = "numeric", size = sz, n = n*2) ,n,2,byrow=T)
## range(abs(D-DD))

## ## replace timestep
## idx <- sample(nrow(D),3)
## nD <- D[idx,]
## nD[,2] <- -999
## nD[,1] <- nD[,1] + 2*tz_step*n
## fh <- file(fn, "r+b")
## isSeekable(fh)

## for(ii in 1:nrow(nD)){
##     ## work out how far skip in the database to get to start of record
##     m <- ((nD[ii,1]-tz_origin)/tz_step) ## TODO check is integer
##     m <- m - (m%/%n)*n
    
##     seek(fh, as.integer(m*sz*2), origin = "start", rw = "read") ## move to that locations
    
##     time_diff <- readBin(fh, what = "numeric", size = sz, n = 1L) - nD[ii,1]## read in current time at that point
##     if( time_diff > 0 ){ stop("Trying to replace newer data....") }
##     if( time_diff %/% (tz_step*n) != time_diff / (tz_step*n) ){
##         stop("Incorrect time location")
##     }
    
##     seek(fh, -as.integer(sz), origin = "current", rw = "write") ## move back one
##     writeBin(as.numeric(nD[ii,]), fh, size = sz) ## rewrite
## }
## close(fh)

## #> [1] 0
## # swap the sign of the second value
## ##writeBin(as.numeric(-2), fh, size = sz)
## ##close(fh)

## DD <- matrix( readBin(fn, what = "numeric", size = sz, n = n*2) ,n,2,byrow=T)
## range(abs(DD[idx,]-nD))
## range(abs(DD[idx,]-D[idx,]))

## unlink(fn)






rm(list=ls())
library(tictoc)
source("db.R")
fn <- tempfile()

D <- read.csv("/data/ea/archive/readings-2024-03-19.csv.gz")
D$measure <- basename(D$measure)
D$value <- as.numeric(D$value)
D$dateTime <- as.POSIXct(D$dateTime,format="%Y-%m-%dT%H:%M:%SZ",tz="UTC")

M <- split(D,D$measure)
M <- lapply(M, function(x){ setNames(x$value,x$dateTime) })


M <- split(D,D$measure)
M <- lapply(M, function(x){ cbind( round(as.integer(x$dateTime)/900)*900, x$value ) })

sz <- 8
tz_origin <- 0
tz_step <- 900
n <- 4*24*365
tic()
for(ii in names(M)){
    fn <- paste0("./tmp/",ii,".,wpr")
    create_db(fn,tz_origin,tz_step,n)
}
toc()

tic()
for(ii in names(M)){
    fn <- paste0("./tmp/",ii,".,wpr")
    update_db(fn,M[[ii]])
}
toc()


tic() #profvis::profvis({
    for(ii in 1){
        D <- matrix(c(seq(tz_origin,by=tz_step,length=n),rnorm(n)),n,2)
        
        create_db(fn,tz_origin,tz_step,n)
        
        update_db(fn,D)
        
        DD <- read_db(fn,1,D[nrow(D),1])
        print(range(abs(D-DD)))
    }
toc()#})


head(M)

sz <- 8
tz_origin <- 0
tz_step <- 900
n <- 4*24*365
tic() #profvis::profvis({
    for(ii in 1){
        D <- matrix(c(seq(tz_origin,by=tz_step,length=n),rnorm(n)),n,2)
        
        create_db(fn,tz_origin,tz_step,n)
        
        update_db(fn,D)
        
        DD <- read_db(fn,1,D[nrow(D),1])
        print(range(abs(D-DD)))
    }
toc()#})

##D <- matrix(c(11,-999),1,2)
##update_db(fn,D)
##read_db(fn,7,11)
##read_db(fn,1,11)

unlink(fn)
