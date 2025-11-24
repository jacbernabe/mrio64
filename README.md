
<!-- README.md is generated from README.Rmd. Please edit that file -->

# mrio64

This is a package for processing and compiling ADB MRIO input data.

The goal of mrio64 is to …

## Installation

You can install the development version of mrio64 like so:

Package installation can be done directly by calling
`devtools::install_github("jacbernabe/mrio64")`

## Example

This is a basic example which shows you how to solve a common problem:

``` r
library(mrio64)
## basic example code
path <- setwd("/Users/johnarvinbernabe/Downloads")      #This refers to the path to a file or directory of files of Comtrade bulk download data.
                              #Change the path based on your computer’s directory
sectorize_comtrade_64(path)      #The function expects these files to have the name pattern “ABC 20XX.gz”.
#> 
#> Found the following files:
#> 
                              #The function should work regardless of how many files are in the directory.
```
