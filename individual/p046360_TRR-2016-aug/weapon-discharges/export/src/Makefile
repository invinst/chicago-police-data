# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046360_TRR-2016-aug/weapon-discharges/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,

all: \
		output/weapon-discharges.csv.gz

output/weapon-discharges.csv.gz: \
		src/export.py \
		input/weapon-discharges.csv.gz
	python $<


clean:
	rm output/*


# done
