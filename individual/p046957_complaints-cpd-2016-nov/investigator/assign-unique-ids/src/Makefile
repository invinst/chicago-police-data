# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046957_complaints-cpd-2016-nov/investigator/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-unique-ids.log, assign-unique-ids.yaml

all: \
		output/investigators.csv.gz

output/investigators.csv.gz: \
		src/assign-unique-ids.py \
		input/investigators.csv.gz
	python $<


clean:
	rm output/*


# done
