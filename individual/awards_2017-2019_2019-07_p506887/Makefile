# -*- coding: utf-8 -*-
# Authors:	Ashwin Sharma
# individual/awards_1967-2017_2017-08_p061715
all: 

# ordered tasks 
tasks := import clean assign-unique-ids export

# if included in other makefile, use that root, otherwise, here
ifndef root_dir
root_dir := ../..
else
run_dir := $(dir $(lastword $(MAKEFILE_LIST)))
endif

tasks := $(addprefix $(run_dir),$(tasks))

# uses generic build task implementation: 
include $(root_dir)/share/src/individual_makefile.mk

$(eval $(call build_all_tasks,$(tasks)))

.PHONY: all clean
    