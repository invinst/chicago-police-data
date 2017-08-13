# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046360_TRR-2016-aug/weapon-discharges/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/weapon-discharges.csv.gz

output/weapon-discharges.csv.gz: \
		src/clean.py \
                input/weapon-discharges.csv.gz
	python $<


clean:
	rm output/*


# done
