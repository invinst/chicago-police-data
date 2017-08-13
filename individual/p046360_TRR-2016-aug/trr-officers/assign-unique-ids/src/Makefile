# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046360_TRR-2016-aug/trr-officers/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-unique-ids.log, assign-unique-ids.yaml,
# trr-officers_demographics.csv.gz

all: \
		output/trr-officers.csv.gz

output/trr-officers.csv.gz: \
		src/assign-unique-ids.py \
                input/trr-officers.csv.gz
	python $<


clean:
	rm output/*


# done
