# use absolute path of this makefile in case this called elsewhere with include
merge_path := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
merge_dirname := $(lastword $(subst /, ,$(merge_path)))
split := $(subst _, ,$(merge_dirname))

merge_number := $(firstword $(split))
prev_merge_number := $(shell echo $(merge_number) - 1 | bc)

ifeq (prev_merge_number,0)
prev_merge_folder := 
else	
prev_merge_folder := $(wildcard ../*$(prev_merge_number)_*)
endif 

without_number := $(wordlist 2,$(words $(split)),$(split))
# split on space again
individual := $(subst $(eval) ,_,$(without_number))

# output/*.csv.gz: src/*.py ../$(individual)/export/output/*.csv.gz 
all:
	echo $(prev_merge_folder)