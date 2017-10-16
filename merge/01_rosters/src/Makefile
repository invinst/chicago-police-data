# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# merge/01_rosters/src/Makefile

.PHONY: all clean
# Additional files created: merge.log, merge.yaml,
# all-members.csv.gz, cpd-employees.csv.gz
# officer-reference.csv.gz

all: \
		output/officer-profiles.csv.gz

output/officer-profiles.csv.gz: \
		src/merge.py \
		input/cpd-employees_demographics.csv.gz \
		input/cpd-employees.csv.gz
	python $<


clean:
	rm output/*


# done
