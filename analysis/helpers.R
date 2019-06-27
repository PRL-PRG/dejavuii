# Helper functions for the Dejavu II analysis and stuff -------------------------------------------

# We start with better csv reader which (a) actually reads csvs and (b) does so much faster
library(readr)

# the usual plotting and data filtering stuffs
library(dplyr)
library(ggplot2)

# Logging -----------------------------------------------------------------------------------------

LOG = function(..., d = NULL, pct = NULL) {
  if (is.null(d)) {
    cat(paste0(...,"\n"))
  } else if (is.null(pct)) {
    cat(paste0(..., ": ", d, "\n"))
  } else {
    cat(paste0(..., ": ", d, " (",round((d / pct) * 100, digits = 2), "%)\n"))
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


# GitHub Links and Objects Retrieval --------------------------------------------------------------

# Loads the information about projects. If forced, the table is always loaded, if not forced, the table is loaded only if DEJAVU_PROJECTS global variable does not exist yet. Returns the loaded projects and stores them in the DEJAVU_PROJECTS
loadProjects = function(force = T) {
  if (force || !exists("DEJAVU_PROJECTS")) {
      LOG("Loading projects...")
      DEJAVU_PROJECTS <<- readDataset("projects.csv")
      LOG("Number of projects", d = nrow(DEJAVU_PROJECTS))
  }
  invisible(DEJAVU_PROJECTS)
}

# For given project id, returns its github url
projectUrl = function(id) {
    pInfo = loadProjects(F) %>% filter(projectId == id)
    paste0("https://github.com/",pInfo$user,"/",pInfo$repo)
}

# Given list of hash ids, returns a data frame that contains the actual SHA1 hash for each of the ids. Does this by grepping through the hashes.csv file, which is not particularly fast, but the idea is that we don't do that that often. 
objectHashes = function(hashIndices) {
    x = paste0("-e ^", hashIndices, ",", collapse = " ")
    x = paste0("cat ",datasetFilePath("hashes.csv"), " | grep ", x)
    x = system(x, intern = T)
    x = strsplit(x, ",")
    x = data.frame(matrix(unlist(x), nrow=length(x), byrow=T),stringsAsFactors=FALSE)
    colnames(x) = c("id","hash")
    if (nrow(x) != length(hashIndices))
      warning(paste0("Asked for ", length(hashIndices), " hash indices, but only ", nrow(x), " found"))
    x
}

# Given list of file path ids, returns the actual path strings associated with them. Does this by grepping through the paths.csv file, which is not particularly fast, but the idea is that we don't do that that often. 
filePaths = function(pathIndices) {
  x = paste0("-e ^", pathIndices, ",", collapse = " ")
  x = paste0("cat ",datasetFilePath("paths.csv"), " | grep ", x)
  x = system(x, intern = T)
  x = strsplit(x, ",")
  x = data.frame(matrix(unlist(x), nrow=length(x), byrow=T),stringsAsFactors=FALSE)
  colnames(x) = c("id","path")
  if (nrow(x) != length(pathIndices))
    warning(paste0("Asked for ", length(pathIndices), " path indices, but only ", nrow(x), " found"))
  x$path = substr(x$path, 2, nchar(x$path) - 1)
  x
  
}



