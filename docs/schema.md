# Schema Documentation

This file lists schema documentation for various input and output data used in the project. When you create new data, please update this file as well.

## Input Data

The input data consists of `files.csv` file which contains for each file observed the following information:

    project id, path id, file hash id, commit id

All ids are unsigned integers. More information for projects, paths commits and file hashes can be found in separate files:

### `commits.csv`

1. id
2. sha of the commit
3. time of the commit

### `fileHashes.csv`

1. id
2. hash of the contents

### `paths.csv`

1. id
2. path 

### `projects.csv`

1. id
2. username
3. repo name


