# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046957_complaints-cpd-2016-nov/witness/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-unique-ids.log, assign-unique-ids.yaml,
# witnesses_demographics.csv.gz

all: \
		output/witnesses.csv.gz

output/witnesses.csv.gz: \
		src/assign-unique-ids.py \
		input/witnesses.csv.gz
	python $<


clean:
	rm output/*


# done
