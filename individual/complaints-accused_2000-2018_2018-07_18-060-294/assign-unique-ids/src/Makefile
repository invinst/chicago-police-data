# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-accused_2000-2018_2018-07_18-060-294/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-unique-ids.log, assign-unique-ids.yaml
# complaints-accused_2000-2018_2018-07_profiles.csv.gz
all: \
		output/complaints-accused_2000-2018_2018-07.csv.gz

output/complaints-accused_2000-2018_2018-07.csv.gz: \
		src/assign-unique-ids.py \
		input/complaints-accused_2000-2018_2018-07.csv.gz
	python3 $<


clean:
	rm output/*


# done
