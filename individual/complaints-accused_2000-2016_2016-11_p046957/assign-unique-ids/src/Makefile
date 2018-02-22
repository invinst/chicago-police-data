# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-accused_2000-2016_2016-11_p046957/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-unique-ids.log, assign-unique-ids.yaml
# complaints-accused_2000-2016_2016-11_profiles.csv.gz
all: \
		output/complaints-accused_2000-2016_2016-11.csv.gz

output/complaints-accused_2000-2016_2016-11.csv.gz: \
		src/assign-unique-ids.py \
		input/complaints-accused_2000-2016_2016-11.csv.gz
	python $<


clean:
	rm output/*


# done
