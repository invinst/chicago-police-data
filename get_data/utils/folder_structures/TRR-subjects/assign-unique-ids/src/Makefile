# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/TRR-subjects_2004-2016_2016-09_p046360/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-unique-ids.log, assign-unique-ids.yaml,
# TRR-subjects_2004-2016_2016-09_profiles.csv.gz

all: \
		output/TRR-subjects_2004-2016_2016-09.csv.gz

output/TRR-subjects_2004-2016_2016-09.csv.gz: \
		src/assign-unique-ids.py \
                input/TRR-subjects_2004-2016_2016-09.csv.gz
	python $<


clean:
	rm output/*


# done
