# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046360_TRR-2016-aug/trr-main/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,

all: \
		output/trr-main.csv.gz

output/trr-main.csv.gz: \
		src/export.py \
		input/trr-main.csv.gz
	python $<


clean:
	rm output/*


# done
