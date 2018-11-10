# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-investigators_2000-2018_2018-07_18-060-294/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/complaints-investigators_2000-2018_2018-07.csv.gz

output/complaints-investigators_2000-2018_2018-07.csv.gz: \
		src/clean.py \
		input/complaints-investigators_2000-2018_2018-07.csv.gz
	python3 $<


clean:
	rm output/*


# done
