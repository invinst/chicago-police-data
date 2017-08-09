# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p052262_all-sworn-employees-UOA-2016-dec/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-unique-ids.log, assign-unique-ids.yaml

all: \
		output/all-sworn-units.csv.gz

output/all-sworn-units.csv.gz: \
		src/assign-unique-ids.py \
		input/all-sworn-units.csv.gz
	python $<


clean:
	rm output/*


# done
