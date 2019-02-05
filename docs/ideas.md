> Note that the information below has nothing to deal with the actual implementation or with how we store the data. 

# Terminology

A **path segment** is name of directory or file. Empty path segment is empty string.

Path segments can be joined together using the `/` operator. If either the lhs or rhs of the `/` is the empty path segment, then the result is the other operand.

A **path** is either empty path segment, or arbitrary number of path segments joined using `/`.

A **file path** is path in the form of `{ directory '/' } file`.

A **directory path** is path in the form of `directory { '/' directory }`. Directory path is also an empty path segment itself.

All paths are relative to the root of the repository they are being applied to.

> In english, we use paths in much the same way OS uses them. We join paths together using `/`. All paths are relative to the repo root and they do not have the initial `/`. Joining paths produces the logical outcome.

A **repository** is identified by `(U, P, C)` where:

- `U` is the name of github user who owns the project
- `P` is the name of the project
- `C` is set of all commits of the repository

Repository is uniquely identified by the user and project names. 

> The repository is basically a github repository. In theory this can be extended to other sources easily. 

A **file** is identified by `(R, P)` where:

- `R` is a repository
- `P` is a file path

> File is identified by the project and file path in the project. Note that two different contents of the file are still the same file, even if the file gets deleted and then inserted again, it is still the same file.

A **commit** is `(H, T, C)` where:

- `H` is the hash (id) of the commit
- `T` is the time of the commit (git determines between author and commit times, for now we use author times everywhere)
- `C` is the list of changes the commit has made.

Each change is identified by `(FP, FH)` where `FP` is file path and `FH` is hash of the contents to which the commit changes file at given path. If the file is deleted, `FH` is `0`.

The commit is uniquely identified by its hash. 

> Commit is git commit.

# Functions

## files(P, T, D = "")

For project `P`, time `T` and directory path `D`, returns for each file path `FP` that is in the form of `D / Fx` that has in project `P` at time `T` file hash `FH` that is not `0` the tuple `(FP, FH)`. If the `D` argument is missing in defaults to empty directory and the function returns the state of *all* files of the project `P` alive at `T`.

## commit(P, FP, T)

For given project `P`, file path `FP` and time `T` returns the youngest commit before `T` that changed `FP` in `A`.

> I.e. returns the commit responsible for the contents of `FP` observed at `T`.

## changes(P, FP, T = Inf)

For each commit `C` that changed file path `FP` in project `P` to hash `FH` before time `T` returns tuple `(C, FH)`.

## contents(P, FP, T)

For given project, file path and time returns the file hash of the contents of the file at the specified time. 

# Clones

For all the clone relationship we assume to have two projects - `A` which contains the clone and `B` which contains the source. Furthermore we expect two times `Ta` and `Tb` such that `Ta >= Tb` so that we can say that project `A` at time `Ta` contained the clone in question from project `B` as it appeared at time `Tb`

We expect times `Ta` and `Tb` for each relation to be minimal, i.e. all other things being same, if there are multiple possible values for `Ta` and `Tb` that would satisfy the relation, we will always choose the smallest `Ta` and `Tb`.

## Directory Clone

`A` contains clone of directory `Db` in `B` if there is directory `Da` such that:

- `FilesA` = `files(A, Ta, Da)` and `FilesB` = `files(B, Tb, Db)`, both `FilesA` and `FilesB` are not empty
- for each `(Db / FP, FH)` from `FilesB` there exists `(Da / FP, FH)` in `FilesA`
- size of `FilesA` == size of `FilesB`

If `Db` is empty path then `A` contains *project* clone of `B`.

The directory clone is identified by `(A, B, Da, Db, Ta, Tb)`.

> In plain english, if some folder in `A` at time `Ta` has the same contents as some folder of `B` at time `Tb` then `A` contains the directory clone of that dir in `B`. If the dir in `B` is empty, that means entire project `B` has been clone and we call this *project clone* instead.

For a directory clone `DC` defined as above, the following properties can be calculated:

- `age(DC)` = max of size of `changes(A, FP, Ta)` for each `(FP, FH)` from `FilesA` (where `FP` is file path and `FH` is file hash)

> The maximal number of changes to file cloned from `B` observed in `A` before the clone relationship could have been established. Generally if this number is high, it lowers the likelihood that we are really seeing clone of `B` because the affected files of `A` had their own life in `A` before. 

- `commits(DC)` = set of `commit(A, FP, Ta)` for each `(FP, FH)` from `FilesA` (== the commits required to create the clone in `A`)

> The number of commits required to create the situation in `A` so that the folder `D` contained a clone from `B`, in other words in how many commits was the contents from `B` brought to `A`.

- `effectA(DC)` = number of files in `FilesA` / number of files in `files(A, Ta)`

> I.e. the fraction of project `A` that belonged to the project clone at the time the project clone was established.

- `effectB(DC)` = number of files in `FilesB` / number of files in `files(B, Ta)`

> I.e. the fraction of project `B` that belonged to the project clone at the time the clone was established. For project clones, `effectB` must be equal to `1`.

**TODO**: I would like a metric of "how clean" the creation of the clone was, the `commits(PC)` is first step, but this can be better, such as: were other files affected when the clone was copied to `A` in the same commits, or even if there were commits to completely different files in the project while the clone was being built. 

## Subtree Clone

The subtree clone corresponds to a situation where folder from `B` is cloned in `A`, but not entirely, some subfolders are missing.

`A` contains subtree clone of directory `Db` in `B` iff:

- `FilesA` = `files(A, Ta, Da)` and `FilesB` = `files(B, Tb, Db)`, both `FilesA` and `FilesB` are not empty
- for each directory `Fd` such that `Db / Fd` is a directory in `FilesB` with at least one file:
- either there is no file `F` such that `Da / Fd / F` that exists in both `FilesA` and `FilesB`
- or for all files `F` such that `(Db / Fd / F, FH)` is in `FilesB`, there exists `(Da / Fd / F, FH)` in `FilesA`

Subtree clones (`SC`) can have all the properties of folder clones plus the following:

- `clonedDirRatio(SC)` is the number of unique directories with at least one file in `FilesA` / number of unique directories with at least one file in `FilesB`
- `clonedFilesRatio(SC)` is the number of `FilesA` / number of files in `FilesB`

## Partial Clone

Partial clone corresponds to a situation where folder `B` is cloned in `A`, but some files are missing and these missing files are not localized into directories.

`A` contains partial clone of directory `Db` in project `B` iff:

- `FilesA` = `files(A, Ta, Da)` and `FilesB` = `files(B, Tb, Db)`, both `FilesA` and `FilesB` are not empty
- for each file  `(Da / F, FH)` from `FilesA` there is `(Db / F, FH)` in `FilesB`

Partial clones (`PC`) can be characterized by the same properties as subtree clones.

> Perhaps knowing how far away from a subtree the partial clone is will be useful, depending on how many of these we'll see. 

## File Clone

The simplest case where only a single fil is cloned.

`A` contains clone of file path `FPb` of `B` if there exists file path `FPa` such that `contents(A, FPa, Ta)` == `contents(B, FPb, Tb)`.

## Hierarchy of clones

- file clone is the smallest possible partial clone (of one file only)
- subtree clone is special class of partial clone
- directory clone is subtree clone where no files are missing
- project clone is a directory clone where `Db` is empty

# Determining Originals

Let `FilesA` be `files(A, Da, Ta)`, i.e. the cloned files in project `A`. For each `B`,`Db` and `Tb` such that `(A, B, Da, Db, Ta, Tb)` is a partial clone, original is such `B` and `Db` for which the corresponding `Tb` is the smallest.

> Perhaps interestingly, originals only concern with the files present and the oldest time these were seen together.

# Nested Clones

Nested clones happen when there is a big folder that seems to be cloned, but it is in fact two or more smaller folders that just happen to be often cloned together.

> How to determine these? Perhaps by using the effectB? The greater the `effectB` the better the clone? (effectB * clonedFilesRatio). Also it is rather hard to determine how this happened, i.e. was it that the smaller clones are really what happened, or is it that they were put together first and then the whole thing copied?

> Another way is saying that the split in nested clones happens by taking file and growing around it for as long as the original remains the same project and directory (and time, sort of)

# TODO

- what if the project containing the clone has some extra stuff in as well ? This can be for most of the categories.
- how to update the definitions so that we do not get swamped by stuff like partial clones with 2 files, etc. 




