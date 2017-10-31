# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p428703_officer-complaints-2017-sep/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,

all: \
		output/officer-complaints.csv.gz

output/officer-complaints.csv.gz: \
		src/export.py \
		input/officer-complaints.csv.gz
	python $<


clean:
	rm output/*


# done
