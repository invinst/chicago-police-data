# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046360_TRR-2016-aug/charges/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,

all: \
		output/charges.csv.gz

output/charges.csv.gz: \
		src/export.py \
		input/charges.csv.gz
	python $<


clean:
	rm output/*


# done
