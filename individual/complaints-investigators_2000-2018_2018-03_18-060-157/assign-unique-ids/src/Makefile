# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-investigators_2000-2018_2018-03_18-060-157/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-unique-ids.log, assign-unique-ids.yaml
# complaints-investigators_2000-2018_2018-03_profiles.csv.gz
all: \
		output/complaints-investigators_2000-2018_2018-03.csv.gz

output/complaints-investigators_2000-2018_2018-03.csv.gz: \
		src/assign-unique-ids.py \
		input/complaints-investigators_2000-2018_2018-03.csv.gz
	python $<


clean:
	rm output/*


# done
