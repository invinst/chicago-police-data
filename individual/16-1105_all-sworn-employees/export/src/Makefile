# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/16-1105_all-sworn-members/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,
# ase-units_demographics.csv.gz

all: \
		output/ase-units.csv.gz

output/ase-units.csv.gz: \
		src/export.py \
		input/ase-units.csv.gz \
		input/ase-units_demographics.csv.gz
	python $<


clean:
	rm output/*


# done
