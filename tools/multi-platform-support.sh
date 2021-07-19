#!/usr/bin/env bash

# Define commands for Linux and MacOS
# This script should be sourced into other scripts
# The commands should use the variables defined here instead of the actual commands

#Linux versions
FIND=find
CP=cp
SED=sed
GREP=grep
WC=wc
UNIQ=uniq
SORT=sort
HEAD=head
TEE=tee

UNAME_S=$(uname -s)
if [ $UNAME_S = "Darwin" ];
then
    # MacOS GNU versions which can be installed through Homebrew
    FIND=gfind
    CP=gcp
    SED=gsed
    GREP=ggrep
    WC=gwc
    UNIQ=guniq
    SORT=gsort
    HEAD=ghead
    TEE=gtee
fi
