# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p052263_all-sworn-employees-2016-dec/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/all-sworn.csv.gz

output/all-sworn.csv.gz: \
		src/clean.py \
		input/all-sworn.csv.gz
	python $<


clean:
	rm output/*


# done
