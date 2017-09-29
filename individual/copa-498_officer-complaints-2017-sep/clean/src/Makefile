# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/copa-498_officer-complaints-2017-sep/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml, metadata_officer-complaints.csv.gz

all: \
		output/officer-complaints.csv.gz

output/officer-complaints.csv.gz: \
		src/clean.py \
		input/officer-complaints.csv.gz
	python $<


clean:
	rm output/*


# done
