# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046957_complaints-cpd-2016-nov/victim/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml

all: \
		output/victims.csv.gz

output/victims.csv.gz: \
		src/export.py \
		input/victims.csv.gz
	python $<


clean:
	rm output/*


# done
