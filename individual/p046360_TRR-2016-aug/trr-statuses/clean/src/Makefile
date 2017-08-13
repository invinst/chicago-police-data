# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046360_TRR-2016-aug/trr-statuses/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/trr-statuses.csv.gz

output/trr-statuses.csv.gz: \
		src/clean.py \
                input/trr-statuses.csv.gz
	python $<


clean:
	rm output/*


# done
