#' Create a database
#'
#' Create a round robin database for a series of numeric data collected at regular time intervals
#' 
#' @param fn file name
#' @param tz_step step of time series data in seconds
#' @param n number of steps to store
#' @param tz_origin origin of time steps in seconds
#' @param size size in bits of the written number data
#'
#' @return Called for the bi-product of creating the file
#' @export
create_db <- function(fn,tz_step,n,tz_origin=0,size=8){
    ## TODO error checking
    tz_origin <- as.integer(tz_origin)
    tz_step <- as.integer(tz_step)
    n <- as.integer(n)
    sz <- as.integer(size)

    ## create blank data to write
    D <- rep(NA_real_,n*2)
    D[seq(1,by=2,length=n)] <- rep(0,n) ##seq(tz_origin,by=tz_step,length=n)

    fh <- file(fn,"wb")
    on.exit(close(fh))
    writeBin(c(tz_origin,tz_step,n,sz),fh,size=8)
    writeBin(D,fh,size=sz)
    ##close(fh)
    fn
}

#' Update the data in a database
#'
#' Adds data to the database overwriting existing data in a round robin fashion
#' 
#' @param fn file name
#' @param D the xts data object to write to the file
#' @param check should the age of the overwritten data be checked
#'
#' @return Called for the bi-product of updating the data
#' @export
update_db <- function(fn,D,check=TRUE){
    fh <- file(fn,"r+b"); on.exit( close(fh) )
    isSeekable(fh)
    headerOffset <- 32
    
    ## read header
    tmp <- readBin(fh,"integer",4,size=8)
    tz_origin <- as.integer(tmp[1])
    tz_step <- as.integer(tmp[2])
    n <- as.integer(tmp[3])
    sz <- as.integer(tmp[4])

    ## TODO check ts and D have the same dimensions and correct type
    ts <- as.numeric(index(D))
    D <- as.numeric(D)
    
    ## add data
    ##browser()
    for(ii in 1:length(ts)){
        ## work out how far skip in the database to get to start of record
        m <- ((ts[ii]-tz_origin)/tz_step) ## TODO check is integer
        m <- m - (m%/%n)*n
        m <- (2*m*sz) + headerOffset

        if(check){
            seek(fh, as.integer(m), origin = "start", rw = "read") ## move to that location
            time_diff <- readBin(fh, what = "numeric", size = sz, n = 1L) - ts[ii] ## difference in time
            if( time_diff > 0 ){ stop("Trying to replace newer data....") }
            if( time_diff %/% (tz_step) != time_diff / (tz_step) ){
                stop("Incorrect time location")
            }
            
            seek(fh, -sz, origin = "current", rw = "write") ## move back one
            writeBin(c(ts[ii],D[ii]), fh, size = sz) ## rewrite
        }else{
            seek(fh, as.integer(m), origin = "start", rw = "write")
            writeBin(c(ts[ii],D[ii]), fh, size = sz)
        }
    }
    ## close(fh)
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
    fh <- file(fn,"r+b"); on.exit( close(fh) )
    isSeekable(fh)
    headerOffset <- 32 # 4*8
    
    ## read header
    tmp <- readBin(fh,"integer",4,size=8)
    tz_origin <- tmp[1]
    tz_step <- tmp[2]
    n <- tmp[3]
    sz <- tmp[4]

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

    mdx <- (2*mdx*sz) + headerOffset
    
    out <- matrix(NA,length(mdx),2)
    cnt <- 1
    for(mm in mdx){
        seek(fh, as.integer(mm), origin = "start", rw = "read") ## move to that location
        out[cnt,] <- readBin(fh, what = "numeric", size = sz, n = 2L) ## difference in time
        cnt <- cnt + 1
    }
    out <- xts(out[,2], order.by=as.POSIXct(out[,1],origin="1970-01-01 00:00:00",tz=tz))
    out <- out[ index(out) >= strt & index(out)<=fnsh, ]
    return(out)
}
