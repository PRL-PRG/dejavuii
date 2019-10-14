#!/bin/bash
# This is a simple pipeline script which runs the entire analysis from the join
# phase. The script is also heavily documented so that it offers a reasonable
# summary of the pipeline steps taken and their preferred order. Each stage's
# output is captured and stored in a separate output file, which also serves as
# an indicator whether the stage should be reexecuted or not. This allows for a
# primitive way of turning selected stages on or off (just touch or delete the
# appropriate output files).
#
# The script assumes the following arguments:
#
# bash pipeline.sh DOWNLOADER_DIR WORKING_DIR NUM_THREADS FORCE_STAGES
#
# where:
#
# DOWNLOADER_DIR is the directory containing either uncompressed downloader
# output, or compressed downloader chunks
#
# WORKING_DIR is the directory in which the results will be stored. The
# working dir will contain various subfolders, which then contain the actual
# data tables in csv format. See the respective stages for more details
#
# NUM_THREADS is the number of threads that the stages can use. Default value
# is 8.
#
# FORCE_STAGES - if specified, stages will be forced even if their execution
# summaries exist.

if [[ "$1" == ""  || "$2" == "" ]] ; then
    echo "Invalid command line arguments."
    exit 1
else
    DOWNLOADER_DIR=$1
    WORKING_DIR=$2
    if [[ "$3" == "" ]] ; then
        NUM_THREADS=8
    else
        NUM_THREADS=$3
    fi
    if [[ "$4" == "" ]] ; then
        FORCE_STAGES=0
    else
        FORCE_STAGES=$5
    fi
fi

DEJAVU="build/dejavu"

echo "Dejavu II pipeline"
echo ""
echo "Downloader dir         : $DOWNLOADER_DIR"
echo "Working dir            : $WORKING_DIR"
echo "Number of threads      : $NUM_THREADS"
echo "Force completed stages : $FORCE_STAGES"
echo "Dejavu II executable   : $DEJAVU"
echo "Commit                 : $(git rev-parse HEAD)"

if [ "$(git status --short)" == "" ] ; then 
    echo "Pending changes        : none"
else
    echo "Pending changes        : detected !!!"
fi

echo ""

# A simple function that checks whether given stage should be executed or not,
# which depends on whether the particular stage output file exists, or not
update_stage_input()
{
    STAGE_INPUT=$WORKING_DIR/$1
}

execute_stage()
{
    echo "Executing stage $1"
    if [[ "$FORCE_STAGES" == 1 || ! -f "$1.out" ]] ; then
        command="$DEJAVU $2"
        echo "Command: $command"
        echo ""
        if [ eval "time $command 2>&1 | tee $1.out" != 0 ] ; then
            echo "FAILED. Terminating entire pipeline"
            exit 1
        fi
    else
        echo "(skipped, $1.out file exists)"
    fi
    echo ""
}

# Takes the downloader output, be it either a uncompressed output of the
# downloader or a folder containing multiple compressed runs and joins all the
# downloaded projects into a single dataset.
# Filters out submodules and non-interesting files (non js basically), as well
# as projects downloaded multiple times.

execute_stage "join" "join -d=$WORKING_DIR/join downloader=$DOWNLOADER_DIR"
update_stage_input "join"

# TODO missing execution of the R notebook which calculates the stats from the joiner, based on the data it outputs

# Verifies the downloaded data, i.e. removes any projects that did not download
# completely, which is detected by observing a delete of file we haven't seen
# added and removes all projects whose commits structure is not useful for us,
# i.e. where parent commits are younger than their children
#
# Does not 

execute_stage "verify" "verify -d=$STAGE_INPUT -o=$WORKING_DIR/verified"
update_stage_input "verified"

# This stage patches the createdAt times for the project from various sources,
# so that they actually show the time the project was created, not just the
# first commit time of that projects.

exectute_stage "patch-projects-createdAt" "patch-projects-createdAt -d=$STAGE_INPUT -i=$WORKING_DIR/projects-metadata -ght=$WORKING_DIR/ghtorrent"

# now we must remove the projects that should be deleted after the times have
# been patched. These come in two files, first the unpatchedProjects.csv, which
# contains projects for which there is no creation time available either from
# ghtorrent, or from github itself and then from oldProjects.csv, which happens
# when the project has been renamed and we have both the old and the new name in
# our database, in which case the old ones will obviously be copies and should
# therefore be removed.

execute_stage "filter-unpatched_projects" "filter-projects -d=$STAGE_INPUT -filter=$STAGE_INPUT/unpatchedProjects.csv -o=$WORKING_DIR/tmp"
execute_stage "filter-old_projects" "filter-projects -d=$WORKING_DIR/tmp -filter=$STAGE_INPUT/oldProjects.csv -o=$WORKING_DIR/patched"
update_stage_input "patched"

# Detects all forks. When two projects share at least one commit, the younger
# of the two is a fork and will be identified by the stage. 

execute_stage "detect-forks" "detect-forks -d=$STAGE_INPUT"

# Removes from the dataset the previously identified project forks. 

execute_stage "filter-forked-projects" "filter-projects -d=$STAGE_INPUT -filter=$STAGE_INPUT/projectForks.csv -o=$WORKING_DIR/no-forks"
update_stage_input "no-forks"

# Calculates the summary of the node-modules usage in the projects which
# include the node_modules files in their repositories.

execute_stage "npm-summary" "npm-summary -d=$STAGE_INPUT"

# TODO missing the execution of the npm summary notebook

# Filters out all files in node_modules directories. These are the files that
# were analyzed in the previous steps and are no longer needed for further
# analyses.

execute_stage "npm-filter" "npm-filter -d=$STAGE_INPUT -o=$WORKING_DIR/no-npm"
update_stage_input "no-npm"

# Determines which of the projects in the dataset are using npm in any way.
# We determine this by scanning the projects for `package.json` files in their
# root folder. Generates the list of the projects and also a list of all changes
# to the project.json files so that these can be downloaded for more detailed
# analysis later.

execute_stage "npm-using-projects" "npm-using-projects -d=$STAGE_INPUT"

# Downloads the contents of the package.json files used by the npm using
# projects calculated in the previous step.
#
# Note that this stage takes a long, a very long time if the files have to be
# really downloaded (i.e. they have not been downloaded previously)

# TODO remove the touch to enable the step
touch "download-project-package-jsons.out"
execute_stage "download-project-package-jsons" "download-contents -d=$STAGE_INPUT -o=$WORKING_DIR/contents -i=$STAGE_INPUT/projectPackageJsons.csv -n=$NUM_THREADS"
