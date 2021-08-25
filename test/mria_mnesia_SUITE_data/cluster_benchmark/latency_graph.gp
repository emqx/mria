#!/usr/bin/env -S gnuplot -persist
# GNUplot script to render plots of mria_mnesia performance

set datafile separator ','

set ylabel "Transaction time (μs)"
set xlabel "Cluster size"

set key outside right \
    title 'Network latency (μs)'

plot for [col=2:*] '/tmp/mnesia_stats.csv' using 1:col with linespoints pt 4 lc col dashtype 2 t 'mnesia '.columnhead(col), \
     for [col=2:*] '/tmp/mria_mnesia_stats.csv' using 1:col with linespoints pt 2 lc col t 'rlog '.columnhead(col)
