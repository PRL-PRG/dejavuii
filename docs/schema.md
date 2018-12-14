# Schema Documentation

This file lists schema documentation for various input and output data used in the project. When you create new data, please update this file as well.

## Input Data

The input data consists of `files.csv` file which contains for each file observed the following information:

    project id, path id, file hash id, commit id

All ids are unsigned integers. More information for projects, paths commits and file hashes can be found in separate files:


### `commits.csv`

id
sha of the commit
time of the commit

### `fileHashes.csv`

id
has of the contents

### `paths.csv`

id
path 

### `projects.csv`

id
username
repo name


