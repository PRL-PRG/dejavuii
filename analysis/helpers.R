# Helper functions for the Dejavu II analysis and stuff -------------------------------------------

# We start with better csv reader which (a) actually reads csvs and (b) does so much faster
library(readr)

# the usual plotting and data filtering stuffs
library(dplyr)
library(ggplot2)

# nicer table output
library(DT)

# Logging -----------------------------------------------------------------------------------------

LOG = function(..., d = NULL, pct = NULL, pp = NULL) {
  if (is.null(d)) {
    cat(paste0(...,"\n"))
  } else if (is.null(pct)) {
    cat(paste0(..., ": ", ifelse(is.null(pp), d, pp(d)), "\n"))
  } else {
    cat(paste0(..., ": ", ifelse(is.null(pp), d, pp(d)), " (",round((d / pct) * 100, digits = 2), "%)\n"))
  }
}

# General IO --------------------------------------------------------------------------------------

# check that the data root has been set
if (! exists("DROOT"))
  stop("DROOT global variable not set - please provide the root folder for the dataset you wish to use")

# Simple function that creates full path from given csv file using the selected dataset
datasetFilePath = function(x) {
  paste0(DROOT, "/", x)
}

# Reads CSV file from current dataset
readDataset = function(x) {
  result = read_delim(datasetFilePath(x), delim=',', escape_double=FALSE, escape_backslash=TRUE, quote="\"")
  firstCol = colnames(result)[[1]]
  if (substr(firstCol,1,1) == "#")
    colnames(result)[[1]] = substring(firstCol, 2)
  invisible(result)
}


# UI ----------------------------------------------------------------------------------------------

A = function(name, url, target="_blank") {
  paste0("<a href='", url, "' target='",target,"'>",name,"</a>")
}


# GitHub Links and Objects Retrieval --------------------------------------------------------------

# Loads the information about projects. If forced, the table is always loaded, if not forced, the table is loaded only if DEJAVU_PROJECTS global variable does not exist yet. Returns the loaded projects and stores them in the DEJAVU_PROJECTS
loadProjects = function(force = F) {
  if (force || !exists("DEJAVU_PROJECTS")) {
      LOG("Loading projects...")
      DEJAVU_PROJECTS <<- readDataset("projects.csv")
      LOG("Number of projects", d = nrow(DEJAVU_PROJECTS))
  }
  invisible(DEJAVU_PROJECTS)
}

# Returns the number of projects in the dataset.
numProjects = function() {
  loadProjects();
  nrow(DEJAVU_PROJECTS);
}

# For given project id, returns its name
projectName = function(id) {
  x = data.frame(projectId = id)
  pInfo = loadProjects(F)
  pInfo = left_join(x, pInfo, by=c("projectId"))
  paste0(pInfo$user,"/",pInfo$repo)
}


# For given project id, returns its github url
projectUrl = function(id) {
    x = data.frame(projectId = id)
    pInfo = loadProjects(F)
    pInfo = left_join(x, pInfo, by=c("projectId"))
    paste0("https://github.com/",pInfo$user,"/",pInfo$repo)
}

commitUrl = function(projectUrl, commitHash) {
  paste0(projectUrl, "/commit/", commitHash)
}

fileChangeUrl = function(projectUrl, commitHash, filePath) {
  paste0(projectUrl, "/commit/", commitHash,"/", filePath)
}

# Given list of hash ids, returns a data frame that contains the actual SHA1 hash for each of the ids. Does this by grepping through the hashes.csv file, which is not particularly fast, but the idea is that we don't do that that often. 
objectHashes = function(hashIndices) {
    x = paste0("-e ^", hashIndices, ",", collapse = " ")
    x = paste0("cat ",datasetFilePath("hashes.csv"), " | grep ", x)
    x = system(x, intern = T)
    x = strsplit(x, ",")
    x = data.frame(matrix(unlist(x), nrow=length(x), byrow=T),stringsAsFactors=FALSE)
    colnames(x) = c("id","hash")
    x$id = as.numeric(x$id)
    left_join(data.frame(id = hashIndices), x, by=c("id"))$hash
}

# Given list of file path ids, returns the actual path strings associated with them. Does this by grepping through the paths.csv file, which is not particularly fast, but the idea is that we don't do that that often. 
filePaths = function(pathIndices) {
  x = paste0("-e ^", pathIndices, ",", collapse = " ")
  x = paste0("cat ",datasetFilePath("paths.csv"), " | grep ", x)
  x = system(x, intern = T)
  x = strsplit(x, ",")
  x = data.frame(matrix(unlist(x), nrow=length(x), byrow=T),stringsAsFactors=FALSE)
  colnames(x) = c("id","path")
  x$id = as.numeric(x$id)
  x$path = substr(x$path, 2, nchar(x$path) - 1)
  left_join(data.frame(id = pathIndices), x, by=c("id"))$path
}

# NPM Package links -------------------------------------------------------------------------------

npmPackageUrl = function(name) {
    paste0("https://www.npmjs.com/package/", name)
}

# Pretty printer for large numbers. Examples:
# pp(0)     -> 0
# pp(1)     -> 1
# pp(100)   -> 100
# pp(1000)   -> 1K
# pp(5200)   -> 5.2K
# pp(1000000) -> 1M
# pp(1010000) -> 1M
# pp(1100000) -> 1.1M
# pp(1000000000) -> 1B
pp <- function(x) ifelse(x==0,
                         paste(format(x, digits=2, scientific=FALSE)),
                         ifelse(x < 1000,
                                format(x, digits=2, scientific=FALSE),
                                ifelse(x < 1000000,
                                       paste(format(floor((x/1000)*10)/10, digits=2, scientific=FALSE), "K", sep=""),
                                       ifelse(x < 1000000000,
                                              paste(format(floor((x/1000000)*10)/10, digits=2, scientific=FALSE), "M", sep=""),
                                              paste(format(floor((x/1000000000)*10)/10, digits=2, scientific=FALSE), "B", sep="")))))
