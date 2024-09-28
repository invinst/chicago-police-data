# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/awards_1967-2017_2017-08_p061715/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml 

all: \
		output/awards_1967-2017_2017-08.csv.gz

output/awards_1967-2017_2017-08.csv.gz: \
		src/clean.py \
                input/awards_1967-2017_2017-08.csv.gz
	python $<


clean:
	rm output/*


# done
