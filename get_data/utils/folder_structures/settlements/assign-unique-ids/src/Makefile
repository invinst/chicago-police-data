# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/settlements_1952-2016_2017-01_/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-unique-ids.log, assign-unique-ids.yaml,
# settlements_1952-2016_2017-01_profiles.csv.gz

all: \
		output/settlements_1952-2016_2017-01.csv.gz

output/settlements_1952-2016_2017-01.csv.gz: \
		src/assign-unique-ids.py \
                input/settlements_1952-2016_2017-01.csv.gz
	python $<


clean:
	rm output/*


# done
