# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/settlements_1952-2016_2017-01_/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml 

all: \
		output/settlements_1952-2016_2017-01.csv.gz

output/settlements_1952-2016_2017-01.csv.gz: \
		src/clean.py \
                input/settlements_1952-2016_2017-01.csv.gz
	python $<


clean:
	rm output/*


# done
