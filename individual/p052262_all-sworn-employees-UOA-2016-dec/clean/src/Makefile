# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p052262_all-sworn-employees-UOA-2016-dec/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/all-sworn-units.csv.gz

output/all-sworn-units.csv.gz: \
		src/clean.py \
		input/all-sworn-units.csv.gz
	python $<


clean:
	rm output/*


# done
