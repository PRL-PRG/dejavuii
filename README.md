# DejaVu II

## Installation & Build

> TODO - note that we use ssl for hashes and depend on them

    sudo apt install g++ cmake
    mkdir build
    cd build
    cmake ..
    make -j8

The installation has been tested on Ubuntu 18.04. Note that many of the steps depend on having all the required data in memory. Your mileage may vary depending on the size and characteristic of your dataset, but memory below 32GB will be useless for moderate datasets with 100k projects or so. 

Most computation intensive tasks also support parallel execution. The number of threads (selectable by `-n` argument where supported) is set to 8 by default since this number is quite common for todays high end desktops, but if you can, increase the limit (tested for up to 72).

Finally, all the data is kept in csv files on disk and therefore large amounts of disk are required as well (order of hundreds of GBs and up).

## Pipeline

The pipeline can be split up into the following main tasks, which are described below in more detail.

- obtaining the data from github or other source (cloning the projects and analyzing their histories) (`ghgrabber`, `dejavu`)
- converting the data to reasonably friendly and minimal csv files (`dejavu`)
- performing resource intensive calculations (`dejavu`)
- analyzing the results (`R` notebooks, `dejavu`)

### Obtaining input data

> TODO - Konrad

### Data Verification and Conversion

The following `dejavu` commands are executed:

`verify-ghgrabber` (optional)

Attempts to detect any incomplete data by simple heuristics. The number of projects and commits per project reported via various channels (parents, changes, etc.) are compared. Uses `stdout` to print table where for each project,various numbers of commits are analyzed.

`join`

Takes data from the `ghgrabber` (either in a directory, or multiple compressed runs) and joins the information in them into a single dataset and format that is understandable for the later stages of the pipeline. Example usage:

    ./dejavu join -d=/dejavuii/joined downloader=/mnt/ghgrabber

`verify`

A more involved verification step for the joined data. Notably attempts to use the captured data to reconstruct state of each project at each commit. If discrepancies are observed (i.e. a file whose creation was not observed being deleted in a commit), the projects are excluded (errors in `projects_structureErrors.csv`)

Also excludes any projects containing commits with weird times (i.e. commits whose parent commits are younger than the commit itself) - in `projects_timingsErros.csv` and `commits_timingsErrors.csv`.

Uses the output dir to create new version of projects, commits and file changes information. Example usage:

    ./dejavu verify -d=/dejavuii/joined -o=/dejavuii/verified -n=32

### Computations

`detect-folder-clones`

Detects folder clones in the dataset. 

### Reporting
