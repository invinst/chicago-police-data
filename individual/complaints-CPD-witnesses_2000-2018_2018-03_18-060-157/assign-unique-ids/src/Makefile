# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-CPD-witnesses_2000-2018_2018-03_18-060-157/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-unique-ids.log, assign-unique-ids.yaml
# complaints-CPD-witnesses_2000-2018_2018-03_profiles.csv.gz
all: \
		output/complaints-CPD-witnesses_2000-2018_2018-03.csv.gz

output/complaints-CPD-witnesses_2000-2018_2018-03.csv.gz: \
		src/assign-unique-ids.py \
		input/complaints-CPD-witnesses_2000-2018_2018-03.csv.gz
	python $<


clean:
	rm output/*


# done
