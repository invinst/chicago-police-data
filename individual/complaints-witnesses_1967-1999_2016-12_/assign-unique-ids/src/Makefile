# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-witnesses_1967-1999_2016-12_/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-unique-ids.log, assign-unique-ids.yaml
# complaints-witnesses_1967-1999_2016-12_profiles.csv.gz
all: \
		output/complaints-witnesses_1967-1999_2016-12.csv.gz

output/complaints-witnesses_1967-1999_2016-12.csv.gz: \
		src/assign-unique-ids.py \
		input/complaints-witnesses_1967-1999_2016-12.csv.gz
	python $<


clean:
	rm output/*


# done
