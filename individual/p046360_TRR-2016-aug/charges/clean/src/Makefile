# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046360_TRR-2016-aug/charges/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/charges.csv.gz

output/charges.csv.gz: \
		src/clean.py \
                input/charges.csv.gz
	python $<


clean:
	rm output/*


# done
