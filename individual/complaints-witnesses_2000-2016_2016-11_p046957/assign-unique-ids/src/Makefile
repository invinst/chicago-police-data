# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-witnesses_2000-2016_2016-11_p046957/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-unique-ids.log, assign-unique-ids.yaml,
# complaints-witnesses_2000-2016_2016-11_profiles.csv.gz

all: \
		output/complaints-witnesses_2000-2016_2016-11.csv.gz

output/complaints-witnesses_2000-2016_2016-11.csv.gz: \
		src/assign-unique-ids.py \
		input/complaints-witnesses_2000-2016_2016-11.csv.gz
	python $<


clean:
	rm output/*


# done
