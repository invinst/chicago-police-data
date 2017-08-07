# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p050032_all-members-rank/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/all-members.csv.gz

output/all-members.csv.gz: \
		src/clean.py \
		input/all-members.csv.gz
	python $<


clean:
	rm output/*


# done
