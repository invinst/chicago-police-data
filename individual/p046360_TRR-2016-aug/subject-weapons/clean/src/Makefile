# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046360_TRR-2016-aug/subject-weapons/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/subject-weapons.csv.gz

output/subject-weapons.csv.gz: \
		src/clean.py \
                input/subject-weapons.csv.gz
	python $<


clean:
	rm output/*


# done
