# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046360_TRR-2016-aug/actions-responses/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/actions-responses.csv.gz

output/actions-responses.csv.gz: \
		src/clean.py \
                input/actions-responses.csv.gz
	python $<


clean:
	rm output/*


# done
