# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/16-1105_all-sworn-members/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/ase-units.csv.gz

output/ase-units.csv.gz: \
		src/clean.py \
		input/ase-units.csv.gz
	python $<


clean:
	rm output/*


# done
