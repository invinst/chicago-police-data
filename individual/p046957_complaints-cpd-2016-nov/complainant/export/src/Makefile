# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046957_complaints-cpd-2016-nov/complainant/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,

all: \
		output/complainants.csv.gz

output/complainants.csv.gz: \
		src/export.py \
		input/complainants.csv.gz
	python $<


clean:
	rm output/*


# done
