# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# merge/08_TRR-officers_2004-2016_2016-09_p046360/src/Makefile

.PHONY: all clean
# Additional files created: merge.log, merge.yaml,
# input/TRR-officers_2004-2016_2016-09.csv.gz

all: \
		output/officer-reference.csv.gz

output/officer-reference.csv.gz: \
		src/merge.py \
		input/officer-reference.csv.gz \
                input/TRR-officers_2004-2016_2016-09_profiles.csv.gz \
                input/TRR-officers_2004-2016_2016-09.csv.gz
	python $<


clean:
	rm output/*


# done
