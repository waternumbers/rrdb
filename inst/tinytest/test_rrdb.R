
# Placeholder with simple test
expect_equal(1 + 1, 2)
library(xts)
## makse some test data
ts <- seq( as.POSIXct("1990-01-01",tz="UTC"),as.POSIXct("1990-02-01",tz="UTC"),by=900 )
D <- xts( 1:length(ts), order.by=ts )

## create data base
fn <- tempfile()
on.exit( unlink(fn) )

expect_silent({
    sz <- 8
    tz_origin <- 0
    tz_step <- 900
    n <- nrow(D)-20
    create_db(fn,tz_step,n,tz_origin = tz_origin)
})

## write into data base
nw <- nrow(D)-20
expect_silent({
    update_db(fn,D[1:nw,])
})

## read all data
expect_silent({
    tmp <- read_db(fn,index(D)[1],index(D)[nw])
})
expect_equal( tmp, D[1:nw,] )

## read partial data
expect_silent({
    tmp <- read_db(fn,index(D)[10],index(D)[nw-5])
})
expect_equal( tmp, D[10:(nw-5),] )

## overwrite
expect_silent({
    update_db(fn,D)
})
## read all data
expect_silent({
    tmp <- read_db(fn,index(D)[1],index(D)[nrow(D)])
})
expect_equal( tmp, D[-(1:20),] )
