# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046957_complaints-cpd-2016-nov/victim/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/victims.csv.gz

output/victims.csv.gz: \
		src/clean.py \
		input/victims.csv.gz
	python $<


clean:
	rm output/*


# done
