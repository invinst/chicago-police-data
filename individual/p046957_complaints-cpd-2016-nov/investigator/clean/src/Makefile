# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046957_complaints-cpd-2016-nov/investigator/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/investigators.csv.gz

output/investigators.csv.gz: \
		src/clean.py \
		input/investigators.csv.gz
	python $<


clean:
	rm output/*


# done
