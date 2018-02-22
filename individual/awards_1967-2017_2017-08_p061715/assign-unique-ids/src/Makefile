# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/awards_1967-2017_2017-08_p061715/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-unique-ids.log, assign-unique-ids.yaml,
# awards_1967-2017_2017-08_profiles.csv.gz

all: \
		output/awards_1967-2017_2017-08.csv.gz

output/awards_1967-2017_2017-08.csv.gz: \
		src/assign-unique-ids.py \
                input/awards_1967-2017_2017-08.csv.gz
	python $<


clean:
	rm output/*


# done
