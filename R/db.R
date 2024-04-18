#' Create a database
#'
#' Create a round robin database for a series of numeric data collected at regular time intervals
#' 
#' @param fn file name
#' @param tz_step step of time series data in seconds
#' @param n number of steps to store
#' @param m number of data points at each time step
#' @param tz_origin origin of time steps in seconds 
#' @param size size in bits of the written numeric data
#'
#' @details The value tz_origin can be though off as the offset from 1970-01-01 00:00:00 (0 epoch time) of the closest observation time e.g is observations were record hourly at 5 minutes past the hour tz_step=3600 and tz_origin = 300
#' 
#' @return invisible(TRUE) but called for the bi-product of creating the file
#' @export
create_db <- function(fn,tz_step,n,m,tz_origin=0,size=8){
    ## convert to correct type
    tz_step <- as.integer(tz_step)
    n <- as.integer(n)
    m <- as.integer(m)
    tz_origin <- as.integer(tz_origin)
    sz <- as.integer(size)

    ## error checks
    stopifnot(
        "step size must be positive integer" = tz_step > 0,
        "number of steps must be a positive integer" = n>0,
        "number of data points in each stap must be a positive integer" = m>0,
        "number of bit for writing data must be a positive integer" = sz >0
    )
    
    ## create blank data to write
    D <- rep(NA_real_,n*(m+1))
    D[seq(1,by=m+1,length=n)] <- rep(0,n)

    fh <- file(fn,"wb")
    on.exit(close(fh))
    writeBin(c(tz_step,n,m,tz_origin,sz),fh,size=8)
    writeBin(D,fh,size=sz)
    invisible(TRUE)
}

#' Update the data in a database
#'
#' Adds data to the database overwriting existing data in a round robin fashion
#' 
#' @param fn file name
#' @param D the xts data object to write to the file
#' @param check should the age of the overwritten data be checked
#'
#' @return Invisible(TRUE) but Called for the bi-product of updating the data
#' @export
update_db <- function(fn,D,check=TRUE){
    fh <- file(fn,"r+b")
    on.exit( close(fh) )
    isSeekable(fh)
    headerOffset <- 40 # 5 x 8 bits
    
    ## read header
    tmp <- readBin(fh,"integer",5,size=8)
    tz_step <- as.integer(tmp[1])
    n <- as.integer(tmp[2])
    m <- as.integer(tmp[3])
    tz_origin <- as.integer(tmp[4])
    sz <- as.integer(tmp[5])
    
    ## check if D is the correct size
    stopifnot(
        "D has incorrect number of columns" = ncol(D) == m
    )
    
    ts <- as.numeric(index(D))
    D <- as.matrix(D)
    
    ## add data
    ## TODO - data is sequential in ts could use this to minimise seek times
    for(ii in 1:length(ts)){
        ## work out how far skip in the database to get to start of record
        jj <- (ts[ii]-tz_origin)/tz_step ## TODO check is integer
        jj <- jj - (jj%/%n)*n
        jj <- ((m+1)*jj*sz) + headerOffset

        if(check){
            seek(fh, as.integer(jj), origin = "start", rw = "read") ## move to that location
            time_diff <- readBin(fh, what = "numeric", size = sz, n = 1L) - ts[ii] ## difference in time
            stopifnot(
                "Trying to replace newer data...." = time_diff <= 0,
                "Incorrect time location" = time_diff %/% (tz_step) == time_diff / (tz_step)
            )           
            seek(fh, -sz, origin = "current", rw = "write") ## move back one
            writeBin(c(ts[ii],D[ii,]), fh, size = sz) ## rewrite
        }else{
            seek(fh, as.integer(jj), origin = "start", rw = "write")
            writeBin(c(ts[ii],D[ii,]), fh, size = sz)
        }
    }
    invisible(TRUE)
}

#' Read data from the database
#'
#' Reads data for a specfied time window from the database
#' 
#' @param fn file name
#' @param strt POSIXct (or interger) object for the start of the period
#' @param fnsh POSIXct (or interger) object for the end of the period 
#' @param tz time zone used for the output
#' 
#' @return an xts object containing any data in the time period
#' @export
read_db <- function(fn,strt,fnsh,tz="UTC"){
    fh <- file(fn,"r+b")
    on.exit( close(fh) )
    isSeekable(fh)
    headerOffset <- 40 # 5*8
    
    ## read header
    tmp <- readBin(fh,"integer",5,size=8)
    tz_step <- as.integer(tmp[1])
    n <- as.integer(tmp[2])
    m <- as.integer(tmp[3])
    tz_origin <- as.integer(tmp[4])
    sz <- as.integer(tmp[5])

    istrt <- as.integer(strt)
    ifnsh <- as.integer(fnsh)
    
    ## work out how many time steps to skip
    ms <- floor( ((istrt-tz_origin)/tz_step) )
    mf <- ceiling( ((ifnsh-tz_origin)/tz_step) )
    flg <- (mf-ms)>=n ## TRUE if data loops
    ms <- ms - (ms%/%n)*n
    mf <- mf - (mf%/%n)*n
    if(flg){ if(mf < n-1){ms <- mf+1}else{ ms <- 0 } }
    
    if( mf<ms ) { mdx <- c(ms:(n-1),0:mf) }
    else{ mdx <- ms:mf }

    mdx <- ((m+1)*mdx*sz) + headerOffset
    
    out <- matrix(NA,length(mdx),m+1)
    cnt <- 1
    for(mm in mdx){
        seek(fh, as.integer(mm), origin = "start", rw = "read") ## move to that location
        out[cnt,] <- readBin(fh, what = "numeric", size = sz, n = m+1L) ## difference in time
        cnt <- cnt + 1
    }
    out <- xts(out[,-1,drop=FALSE], order.by=as.POSIXct(out[,1],origin="1970-01-01 00:00:00",tz=tz))
    out <- out[ index(out) >= strt & index(out)<=fnsh, ] ## TODO check if this is needed?
    return(out)
}
