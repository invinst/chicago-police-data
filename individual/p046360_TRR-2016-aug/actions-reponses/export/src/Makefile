# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046360_TRR-2016-aug/actions-responses/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,

all: \
		output/actions-responses.csv.gz

output/actions-responses.csv.gz: \
		src/export.py \
		input/actions-responses.csv.gz
	python $<


clean:
	rm output/*


# done
