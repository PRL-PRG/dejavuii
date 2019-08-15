# Dejavu II

The paper's main theme is harmful effects of cloning and ways these can be mitigated. 

0) vocabulary:) [Jan + Konrad + Peta]

> The format talk in the paper

1) data acquisition

- have our original sources and ghtorrent non-forks **(100%)** [Konrad, 0 days]
- make sure we have npm-packages if they come from github repos **(100%)** [Konrad, 4 days] 
- joining projects, check that the projects actually differ in the join phase **100%** [Peta, 1 day] 
- over time summary (**75%**) - mostly done, but we might want different things, which is why 75% only [Peta, 3 days]
- implicit forks detection & dataset cleaning wrt this (**100%**) [Peta, 2 days]
- proper createdAt times (**100%**) (we have those from GHT, those not in GH should be acquired from GH) [Konrad, 2 days] -- implemented downloader, need to implement json parsing after the jsons are downloaded

> How many projects, files, unique hashes, paths, filenames, lifespans, their increase over time, project commits, project timespan, etc.
> implicit forks (i.e. fork, but not marked as fork)

2) deal with NPM packages

- pretty much done now, how many, discuss the limited info we have about why & how they are included and what happens to them (**100%**) [Peta, 0 days]

> How many NPM packages, files, projects which use them, what happens to them, changes? 

3) determine project categories (since different categories can have different usage & cloning patterns)

- cluster projects by some metrics (such as simple % of clones), or linear model  (**0%**) [Konrad, Jan, ????]
- node using projects, non-node using projects, **100%**
- determine dimensions for this: prepare data for project % stats (**90%**) [Peta, 1 day, pending the above] 

> summaries of the categories and visualization of the split

4) determine clones and their originals

- files [Konrad, Peta] - (**100%**) -- need to prepare a summary, generate some dtat for basic stats
- folders [Peta] - (**100%**) folders of 1 file must be added as well (their check is simple though)
- folder clone originals analysis (subfolders, also file original, etc.) - (**100%**) [Peta]

> % of the dataset this is, clone size x clone occurences, originals information, etc. 

5) for clones, determine how they change over time, if at all

- partial work on files (**100%** - we have datat whether they change or not) [Peta]
- first just see % of changes into folders vs changes into files alone (**100%**)
- partial work on folders (**100%**) 

This should be planned and discussed how to report same and interesting things, such as: attempts to stay in sync with original (and how successful), divergent code, unchanged code, etc.

> Summaries of how clones evolve over time

6) analyze partial clones **removed**

> How prevalent they are, how many different subsets from same original, originals with most subsets, etc. 

7) look at originality status of files throughout their lifetime

- start a copy, then becomes original, then copy again, etc. (**0%**) [pending on file clones, Peta or Konrad, ????]

> How often diffent transitions happen,if at all. 

9) Freshness clones

- report how clones are kept update in the same categories as above (**100%**) [Peta]
- do the clustering again, this time on this as variable [Jan, Konrad, ???]









TODO

- sizes of project distribution
