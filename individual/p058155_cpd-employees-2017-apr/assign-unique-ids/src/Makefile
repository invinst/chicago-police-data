# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/Reporter_cpd-employees-2016-apr/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-unique-ids.log, assign-unique-ids.yaml,
# cpd-employees_demographics.csv.gz

all: \
		output/cpd-employees.csv.gz

output/cpd-employees.csv.gz: \
		src/assign-unique-ids.py \
		input/cpd-employees.csv.gz
	python $<


clean:
	rm output/*


# done
