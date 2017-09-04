# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# merge/assign-universal-ids/02_unit-history/src/Makefile

.PHONY: all clean
# Additional files created: assign-universal-ids.log, assign-universal-ids.yaml,
# ase-units.csv.gz, all-sworn-units.csv.gz
# officer-reference.csv.gz

all: \
		output/officer-profiles.csv.gz

output/officer-profiles.csv.gz: \
		src/assign-universal-ids.py \
		input/officer-profiles.csv.gz \
		input/officer-reference.csv.gz \
                input/ase-units_demographics.csv.gz \
                input/ase-units.csv.gz \
                input/all-sworn-units_demographics.csv.gz \
                input/all-sworn-units.csv.gz
	python $<


clean:
	rm output/*


# done
