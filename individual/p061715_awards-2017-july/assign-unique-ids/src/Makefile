# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p061715_awards-2017-july/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-unique-ids.log, assign-unique-ids.yaml 

all: \
		output/awards.csv.gz

output/awards.csv.gz: \
		src/assign-unique-ids.py \
                input/awards.csv.gz
	python $<


clean:
	rm output/*


# done
