# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-accused_2000-2016_2016-11_p046957/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/complaints-accused_2000-2016_2016-11.csv.gz

output/complaints-accused_2000-2016_2016-11.csv.gz: \
		src/clean.py \
		input/complaints-accused_2000-2016_2016-11.csv.gz
	python $<


clean:
	rm output/*


# done
