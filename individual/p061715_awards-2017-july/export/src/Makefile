# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p061715_awards-2017-july/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,
# awards_demographics.csv.gz

all: \
		output/awards.csv.gz

output/awards.csv.gz: \
		src/export.py \
		input/awards.csv.gz \
		input/awards_demographics.csv.gz
	python $<


clean:
	rm output/*


# done
