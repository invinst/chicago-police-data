# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-CPD-witnesses_2000-2018_2018-08_18-060-425/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/complaints-CPD-witnesses_2000-2018_2018-08.csv.gz

output/complaints-CPD-witnesses_2000-2018_2018-08.csv.gz: \
		src/clean.py \
		input/complaints-CPD-witnesses_2000-2018_2018-08.csv.gz
	python3 $<


clean:
	rm output/*


# done
