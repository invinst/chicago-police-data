# -*- coding: utf-8 -*-
# Authors:	Ashwin Sharma
# individual/complaints-civilian-witnesses_2000-2018_2018-08_18-060-495
all: 

# ordered tasks 
tasks := import clean export

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
    