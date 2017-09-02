# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/16-1105_all-sworn-members/assing-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created:
# assign-unique-ids.log, assign-unique-ids.yaml, ase-units_demographics.csv.gz

all: \
		output/ase-units.csv.gz

output/ase-units.csv.gz: \
		src/assign-unique-ids.py \
		input/ase-units.csv.gz
	python $<


clean:
	rm output/*


# done
