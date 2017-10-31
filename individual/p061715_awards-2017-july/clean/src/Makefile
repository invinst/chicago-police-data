# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p061715_awards-2017-july/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml 

all: \
		output/awards.csv.gz

output/awards.csv.gz: \
		src/clean.py \
                input/awards.csv.gz
	python $<


clean:
	rm output/*


# done
