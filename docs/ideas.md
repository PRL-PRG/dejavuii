# Clones

# Definitions


### Clone

At least two elements that are identical (but possibly more) are called clones. Note that the cloning relationship does not care about which element is the original and which ones are the copies. 

### Project 

Single git repository, identified by its owner and repo name.

### Commit

Commit in git identified by its hash. Each commit must belong to at least one project. If a commit belongs to multiple projects, then these projects are clones.

### File Path

File path is a location of file within a project. We can say that file path is at a tuple `(T, H)` where `T` is time and `H` is file hash if  at the specified time `T` the file path has contents of file hash `H`. Any file path that does not exist in given project at the time has implicitly file hash of `0` (deleted).

File path can be separated into folders and the file (last path element), folders and filenames are joined using `/` (like `os.path.join`, i.e. it deals with corner cases such if lhs or rhs is empty the result is the other part and so on). In order for the algorithms below to work we should also say that all file Paths never start with `/`, but we expect them to all start from root folders of their projects.

### File Hash

Unique identifier of the contents of any file.

### File Change

Given project `P`, commit `C`, file path `F` and hash `H`, the tuple `(P,C,F,H)` is a file change with the meaning that commit `C` changes contents of file at path `F` to contents identified by hash `H` in project `P`.

# Clones

Different granularity of clones can be expressed:

## Project Clone

Project clone happens if entire project is cloned into a directory of another project. More formally we can say that:

Given two projects `A` and `B`, `A` contains project clone of `B` if there are times `Ta` and `Tb` such that `Ta >= Tb` and directory `Da` in project `A` so that for every file path `Fb` at `(Tb, Hb)` there exists `Fa` at `(Ta, Ha)` where `Ha == Hb` and `Fa` == `Da / Fb` and there are no other file paths in `Da`.

Furthermore both `Ta` and `Tb` must be minimal times such that the relation above holds.

> I require this mostly because for each two projects there may be different times ta and tb at which the project clone relation holds. In this case i am only interested in the earliest such occurence. An example would be: At time t2, A clones B (as of time t0). Then at time t2 B moves to B' and at t3 A moves to A' (which contains B'). t0 < t1 < t2 < t3. So times (t1, t0) and (t3, t2) both satisfy the definition above, but I only want to report the first.

Folder clones can be characterized by the following properties:

- maximum number of changes to a file belonging to the project clone from project `A` before the project clone status could be established (from the beginning, or deletion of the file). 
- number of commits required to establish the clone relationship (i.e. how many commits were required to update the files to the required hashes)
- number of commits from the first required to the last required (similar to above, but counting also commits that changed only files outside of the project clone folder).
- percentage of files in `A` at `Ta` which belong to the project clone 

> We can make tuple of these numbers and say for instance that we are only interested in project clones `(0, 1, 1)`, which means that none of the affected files existed in the project before the clone and the whole project was cloned in one commit.

## Folder clone

> This is what shabbir called import clone. 

Folder clone happens if a folder from from project `B` has a clone in project `A`. More formally:

Given two projects `A` and `B` and two directories `Da` and `Db`, if there are times `Ta` and `Tb` such that `Ta >= Tb` so that for every file path `Db/Fb` at `(Tb, Hb)` there is file path `Da/Fa` at `(Ta, Ha)` such that `Ha == Hb` and `Fa == Fb` and there are no other file paths in `Da`.

Furthermore both `Ta` and `Tb` must be minimal times such that the relation above holds.

The same properties that apply for the project clones applies for the folder clones, we can also specify:

- percentage of files in `B` at `Tb` which belong to the clone

## Project Subtree Clone

Project subtree clone happens when project `A` contains clone of `B` with some directories of `B` removed in their entirety. 


## Folder Subtree clone

Folder subtree clone happens when project `A` contains clone of directory in `B`, but some subfolders of the directory are missing (again whole subfolders).

## Partial Project Clone

Partial project clone is `A` contains `B`, but some files are missing (i.e. files are cherrypicked to be included/excluded, while the subtree clones use whole folders). 

## Partial Folder Clone

Partial folder clone is like the project clone. 

## File clone

This is easy.


# TODO

- how to determine the originals of the respective categories
- what if the project containing the clone has some extra stuff in as well ? This can be for most of the categories.




