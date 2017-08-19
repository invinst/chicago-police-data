# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046360_TRR-2016-aug/trr-main/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/trr-main.csv.gz

output/trr-main.csv.gz: \
		src/clean.py \
                input/trr-main.csv.gz
	python $<


clean:
	rm output/*


# done
