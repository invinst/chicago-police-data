# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046360_TRR-2016-aug/trr-officers/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/trr-officers.csv.gz

output/trr-officers.csv.gz: \
		src/clean.py \
                input/trr-officers.csv.gz
	python $<


clean:
	rm output/*


# done
