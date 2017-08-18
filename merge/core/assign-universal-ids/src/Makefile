# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# merge/core/assign-universal-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-universal-ids.log, assign-universal-ids.yaml
# officer-reference.csv.gz
all: \
		output/officer-profiles.csv.gz

output/officer-profiles.csv.gz: \
		src/assign-universal-ids.py \
                input/all-sworn_demographics.csv.gz \
                input/all-sworn-units_demographics.csv.gz \
                input/all-members_demographics.csv.gz \
                input/unit-history_demographics.csv.gz 
	python $<


clean:
	rm output/*


# done
