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



## CSV Files

CSV files are used almost exclusively for data storage between different steps. This section lists the generated files in alphabetical order and describes their schema and purpose. Each file starts with a header line with names of the columns. Most often these are self explanatory and are therefore discussed here only when needed.

`commits.csv`

For each commit (identified by an id, which can be translated to SHA1 hash using `hashes.csv`) contains its author and committer times. 

`commitParents.csv`

Contains pairs of commity id, parent commit id for all commit - parent pairs. Note that single commit can have multiple parents, if it is a merge commit. 

`fileChanges.csv`

Global table of chabnges in all projects. Contains tuples of project id, commit id, path id and contents id with the meaining: in project A, there is commit B which changes path C to state D. If the contents id is `0`, then the file is not changed, but deleted.

> TODO The project id is pretty much useless, we only use it to determine that a commit belongs to a project. Perhaps we should split this info. 

`hashes.csv`

Mapping from ids to SHA1 hashes used by github to identify both commits *and* file contents.

`paths.csv`

Mapping from ids to paths (strings relative to project roots). Ids are used elsewhere to save space. 

`projects.csv`

Lists the projects available, indexed by `id`. For each project, we remember the user and repository where it was created and time at which the project was created.

> FIXME I believe we are now storing not date the project was created, but date of its oldest commit, which is useless (forks will have the same createdAt value).


