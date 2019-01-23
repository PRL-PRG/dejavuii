# TODO

- deleting bad projects (single committer, activity for less than 3 mo, not original of anything)
- distribution of active projects
- distributions of active projects that have been copied

# DejaVu II Plan

The paper should investigate cloning in Javascript in greater depth mainly in the following three areas:

1. What is being cloned and how
2. What happens to the clones once they are copied
3. Are there any differences between the JavaScript and node.js ecosystems?

The intuintion so far is that cloning per se is not bad programming practice as long as it is explicit and is (can) be maintained. Therefore when we discover and chart the non-explicit and non-automated clones, we essentialy point to places where better tooling and support can be given to programmers.


# Vocabulary

This is for now, can, and some should change:


- *copy* = two files / multiple files / folders / projects (depending on granularity) that have the identical contents hash
- *original* = for a copy, of the original source can be determined
- *clone* = for any copies where original can be determined the non-original copies
- *implicit clone* = clone for which it is not possible to infer the source easily, such as just `ctrl-c-v a file` from somewhere manually
- *explicit clone* = clone that is simple to infer original from (such as included node_modules)
- *virtual clone* = clone that is not pushed to the repo, but will be updated whenever the project is presumably cloned

# What is being copied and How

First important step is to splity copies into originals and clones. We can't simply say the oldest thing we see is the original, because we might not be able to see the original (say it is on bitbucket). So actually designating a copy as an original is not trivial if we want to be correct (granted, we do not have to be correct 100%, but 99.9% would be good:). The following are my thoughts on the matter so far:

- originals of coarser granularity are easier - i.e. if a folder is copied and all files in the folder have originals in same repo, the repo is more likely to be an original than if we found different repos
- the original should be original repeatedly - i.e. if the file version changes, it should change first in the original
- some projects are more likely to contain originals (such as repos of node_modules, etc. - these can be automatically detected since we also have a list of all `npm` packages and their metadata)

None of the ideas above is always correct. Personally I think that the combination of the first two is enough - but we must be careful when mixing granularities - i.e. original of a folder might actually contain a cherrypicked copy of a file from somewhere else which is even older, etc.

> The `min.js` files might not be physically present in their original repositories and be generated on the fly, or so on

## Properties of Copies

- granularity of the copies: full repo / folder / subset / cherrypicking files
- the `min.js` files might be their own special category
- depth of copies
- do paths stay same, or change (preliminary results show that path variance is smaller than # of copies variance)

> If we see lots of whole dir copies with few changes at the beginning, it might be worth to actually download the respective files and analyze them to see what changed (copyrights, etc.)

## How is copying done

- is the copy (based on its granularity) copied at once or over multiple commits?
- does the copy originate from the same time, or was event the copy somehow distorted

# What happens to the clones once they are copied

When a copy is made, the following may happen:

- it may never change again - this is the dangerous problem, if it never changes, but the original does.
- it may diverge - i.e. it gets changed into stuff different from the original, or the other copies
- it is synced with the original, but the time delta of the sync is very large and therefore the sync is not really successful
- it is regularly sinced with the original

The granularity of the sync may change (i.e. folder is copied, but then only files are cherrypicked and updated into configurations never found in the original).

The task here is to classify the clones accordingly, look at different versions of copies (implicit, explicit) and different granularities, if there are any differences.

# Differences betweeen JS and Node.js ecosystems

The key question here is to determine whether a project uses node.js or not. While we can easily determine whether `NPM` is used via the presence, or absence of the `package.json` file (we already have these for all projects on GH, and can cheaply update them to latest versions), whether node.js is actually used is trickier:

- if the file has `browser` section, instead of `main`, it indicates the project is intended for browsers, instead of node.js
- if there are no dependencies, only development dependencies, it *might* signify that the project does not require node.js
- if the `browserify` package is used, it might signify that the project is to be executed in browser

However, the only proper way of determining browser vs node.js is to look at the actual code of the package and analyze it, which we do not have.

> TASK: Update the `package.json` list we have and classify projects according to the criteria above as much as we can. This extends the `projects.csv` file and is fairly independent of the other tasks.

Way out could be instead of node.js vs browser JS analyze the `npm` using against non-npm using projects. I think that wrt code cloning this makes even more sense than node.js vs browser. The main question is: does using package manager make the cloning situation better in the sense of more synchronized / less implicit clones? 

## How does using a package manager change the picture

- are there less implicit clones?
- are explicit clones better synchronized with the originals than the implicit clones
- how prevalent are explicit clones against not storing the copied data at all (when clone is explicit it is trivial to download the dependencies on clone such as `npm install`)

## Is `npm` sufficient?

- how do the implicit clones of `npm` using projects look like?
- could some of these be made explicit or virtual?

## Implicit-Explicit-Virtual Clones

- which ones are better synced to the latest version
- we can analyze implicit and explicit easily
- for virtual, we can look at the version specifications in `package.json` to determine whether the version is fixed, or notify
- ideally if we can find explicit vs virtual vs implicit clones of the same things and then compare so that we are not comparing apples and pears

> It might be hard to argue that an outdated clone is always bad - sometimes there might be good reasons for doing so. 



