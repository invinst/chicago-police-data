# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p050032_all-members-rank/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created:
# assign-unique-ids.log, assign-unique-ids.yaml, all-members_demographics.csv.gz

all: \
		output/all-members.csv.gz

output/all-members.csv.gz: \
		src/assign-unique-ids.py \
		input/all-members.csv.gz
	python $<


clean:
	rm output/*


# done
