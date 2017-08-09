# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p052263_all-sworn-employees-2016-dec/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-unique-ids.log, assign-unique-ids.yaml

all: \
		output/all-sworn.csv.gz

output/all-sworn.csv.gz: \
		src/assign-unique-ids.py \
		input/all-sworn.csv.gz
	python $<


clean:
	rm output/*


# done
