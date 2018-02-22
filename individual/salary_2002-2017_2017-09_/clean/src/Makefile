# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/salary_2002-2017_2017-09_/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/salary_2002-2017_2017-09.csv.gz

output/salary_2002-2017_2017-09.csv.gz: \
		src/clean.py \
		input/salary_2002-2017_2017-09.csv.gz
	python $<


clean:
	rm output/*


# done
