# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/unit-history__2016-03_16-1105/assing-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created:
# assign-unique-ids.log, assign-unique-ids.yaml, unit-history__2016-03_profiles.csv.gz

all: \
		output/unit-history__2016-03.csv.gz

output/unit-history__2016-03.csv.gz: \
		src/assign-unique-ids.py \
		input/unit-history__2016-03.csv.gz
	python3 $<


clean:
	rm output/*


# done
