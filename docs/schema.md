# Basic Schema of the files

All information is stored in csv files. Files are required to have headers. New files can be added, but format of existing files should not be changed.



## Folder Clone Information

`folderClones.csv`

For each folder clone (i.e. project, commit and root folder (whose subtree was added by the commit) contains the clone id.

- `project id`
- `commit id`
- `root path`
- `clone id`

`folderCloneOriginals.csv`

For each clone id, contains the information about the clone and its original, namely:

- `clone id`
- `number of files in the clone`
- `original project id`
- `original commit id`
- `original root path`

`folderCloneIds.csv`

For each clone id contains the hash of its contents (files & their contents hashes in particular order).

- `clone id`
- `contents hash` (SHA-1))



