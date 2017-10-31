# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046360_TRR-2016-aug/trr-subjects/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,

all: \
		output/trr-subjects.csv.gz

output/trr-subjects.csv.gz: \
		src/export.py \
		input/trr-subjects.csv.gz
	python $<


clean:
	rm output/*


# done
