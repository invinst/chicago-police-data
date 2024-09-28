# generic makefile for running all tasks within the individual folder
# takes in a list of ordered task:
# creates rules for tasks that map files from input to output using src/task.py
# creates rules for creating symlinks if input file is missing from previous task
# if a task does not follow the rule structure above, just add it manually to it's makefile 

# $(call link_tasks, tasks)
# assumes task to be ^ delimited text, splits on ^, builds symlink dependency
define link_tasks
$(eval tasks = $(subst ^, ,$1))
$(eval build_task = $(firstword $(tasks)))
$(eval prereq_task = $(lastword $(tasks)))
$(eval prereq_task_name = $(notdir $(prereq_task)))
$(eval found_outputs = $(filter-out $(notdir $(wildcard $(prereq_task)/output/*metadata*)),$(notdir $(wildcard $(prereq_task)/output/*.csv.gz))))

$(build_task)-link: $(foreach output,$(found_outputs),$(build_task)/input/$(output))
$(foreach output,$(found_outputs),$(build_task)/input/$(output)): 
	cd $(build_task)/input && $(foreach output,$(found_outputs),ln -s ../../$(prereq_task_name)/output/$(output); )
endef

# filter out the first element to shift list right, last element to shift left,
# then join together when looping to get task and the task before it, adding ^ delim
define build_symlinks
$(eval all_tasks = $1)
$(eval shift_right = $(filter-out $(firstword $(all_tasks)),$(all_tasks)))
$(eval shift_left = $(filter-out $(lastword $(all_tasks)),$(all_tasks)))
$(eval linked = $(join $(shift_right), $(addprefix ^,$(shift_left))))

$(foreach link_task,$(linked),$(eval $(call link_tasks,$(link_task))))
endef

# $(call build_individual_task,task_path)
define build_individual_task
$(eval task_path = $1)
$(eval task_name = $(notdir $(task_path)))
$(eval task_dir = $(dir $(task_path)))
$(eval target_task = $(addsuffix -task,$(task_name)))

$(eval srcs = $(filter-out $(wildcard $(task_path)/src/*__pycache__*),$(wildcard $(task_path)/src/*)))
$(eval inputs = $(filter-out $(wildcard $(task_path)/input/*metadata*),$(wildcard $(task_path)/input/*)))

$(task_path)-link:

$(task_path)-task: $(task_path)/output/*.csv.gz
$(task_path)/output/*.csv.gz: $(srcs) $(inputs) | $(task_path)-link
	cd $(task_path) && python src/$(notdir $(task_name)).py

$(eval clean_task = $(addprefix clean-,$(task_path)))
$(clean_task): 
	cd $(task_path) && rm -f output/*

all: $(target_task)
clean: $(clean_task)
endef

# $(call build_all_tasks,tasks)
define build_all_tasks
$(eval all_tasks = $1)

$(eval $(call build_symlinks,$(all_tasks)))
$(foreach task_name,$(all_tasks),$(eval $(call build_individual_task,$(task_name))))
endef