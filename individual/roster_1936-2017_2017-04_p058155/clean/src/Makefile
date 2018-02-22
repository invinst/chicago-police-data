# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/roster_1936-2017_2017-04_p058155/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/roster_1936-2017_2017-04.csv.gz

output/roster_1936-2017_2017-04.csv.gz: \
		src/clean.py \
		input/roster_1936-2017_2017-04.csv.gz
	python $<


clean:
	rm output/*


# done
