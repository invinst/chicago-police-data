# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/roster_1936-2017_2017-04_p058155/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-unique-ids.log, assign-unique-ids.yaml,
# roster_1936-2017_2017-04_profiles.csv.gz

all: \
		output/roster_1936-2017_2017-04.csv.gz

output/roster_1936-2017_2017-04.csv.gz: \
		src/assign-unique-ids.py \
		input/roster_1936-2017_2017-04.csv.gz
	python $<


clean:
	rm output/*


# done
