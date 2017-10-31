# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046360_TRR-2016-aug/subject-weapons/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,

all: \
		output/subject-weapons.csv.gz

output/subject-weapons.csv.gz: \
		src/export.py \
		input/subject-weapons.csv.gz
	python $<


clean:
	rm output/*


# done
