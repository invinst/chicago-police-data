# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/Illinois-Officers_2017-aug/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-unique-ids.log, assign-unique-ids.yaml

all: \
		output/il-officers.csv.gz

output/il-officers.csv.gz: \
		src/assign-unique-ids.py \
		input/il-officers.csv.gz
	python $<


clean:
	rm output/*


# done
