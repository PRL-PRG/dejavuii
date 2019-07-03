# Dejavu II

The paper's main theme is harmful effects of cloning and ways these can be mitigated. 

0) vocabulary:)

> The format talk in the paper

1) data acquisition

- have our original sources and ghtorrent non-forks **(99%)**
- make sure we have npm-packages if they come from github repos **(0%)**
- over time summary (**75%**) - mostly done, but we might want different things, which is why 75% only
- implicit forks detection & dataset cleaning wrt this (**0%**)
- proper createdAt times (**30%**) (we have those from GHT, those not in GH should be acquired from GH)

> How many projects, files, unique hashes, paths, filenames, lifespans, their increase over time, project commits, project timespan, etc.
> implicit forks (i.e. fork, but not marked as fork)

2) deal with NPM packages

- pretty much done now, how many, discuss the limited info we have about why & how they are included and what happens to them (**100%**)

> How many NPM packages, files, projects which use them, what happens to them, changes? 

3) determine project categories (since different categories can have different usage & cloning patterns)

- non-node vs node using apps (**90%**) -- we have the data, do not report the bit yet
- determine "interesting" projects by some metric (vague by definition atm) (**0%**)

> summaries of the categories and visualization of the split

4) determine clones and their originals

- files (Konrad) - (**90%**) -- need to prepare a summary, generate some dtat for basic stats
- folders (Peta) - (**90%**) folders of 1 file must be added as well (their check is simple though)
- folder clone originals analysis (subfolders, also file original, etc.) - (**30%**)

> % of the dataset this is, clone size x clone occurences, originals information, etc. 

5) for clones, determine how they change over time, if at all

- partial work on files (30% - we have datat whether they change or not)
- partial work on folders (i'd say **10%**)

This should be planned and discussed how to report same and interesting things, such as: attempts to stay in sync with original (and how successful), divergent code, unchanged code, etc.

> Summaries of how clones evolve over time

6) analyze partial clones

- partial clone is if not all from a folder is copied (see what are the things missing, if there is a pattern) (**0%**)

> How prevalent they are, how many different subsets from same original, originals with most subsets, etc. 

7) look at originality status of files throughout their lifetime

- start a copy, then becomes original, then copy again, etc. (**0%**)

> How often diffent transitions happen,if at all. 

8) originality and cloning in different projects (**0%**)

- node using projects
- non-node using projects
- npm packages

> Here we report summaries of the above tasks for these categories

9) Uptodate clones

- report how clones are kept update in the same categories as above (**0%**)

> The update status of different clone types (files, folders, partial folders) and their extremes



