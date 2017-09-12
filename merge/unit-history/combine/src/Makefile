# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# merge/unit-history/combine/src/Makefile

.PHONY: all clean
# Additional files created: combine.log, combine.yaml

all: \
		output/unit-history.csv.gz

output/unit-history.csv.gz: \
		src/combine.py \
		input/officer-profiles.csv.gz \
		input/ase-units.csv.gz \
                input/all-sworn-units.csv.gz
	python $<


clean:
	rm output/*


# done
