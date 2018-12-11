# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/TRR-charges_2004-2016_2016-09_p046360/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/TRR-charges_2004-2016_2016-09.csv.gz

output/TRR-charges_2004-2016_2016-09.csv.gz: \
		src/clean.py \
                input/TRR-charges_2004-2016_2016-09.csv.gz
	python $<


clean:
	rm output/*


# done
