# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046957_complaints-cpd-2016-nov/accused/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-unique-ids.log, assign-unique-ids.yaml

all: \
		output/accused.csv.gz

output/accused.csv.gz: \
		src/assign-unique-ids.py \
		input/accused.csv.gz
	python $<


clean:
	rm output/*


# done
