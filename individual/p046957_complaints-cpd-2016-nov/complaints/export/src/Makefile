# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046957_complaints-cpd-2016-nov/complaints/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml

all: \
		output/complaints.csv.gz

output/complaints.csv.gz: \
		src/export.py \
		input/complaints.csv.gz
	python $<


clean:
	rm output/*


# done
