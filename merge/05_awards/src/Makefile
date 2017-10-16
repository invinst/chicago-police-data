# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# merge/05_awards/src/Makefile

.PHONY: all clean
# Additional files created: merge.log, merge.yaml,
# awards.csv.gz,
# officer-reference.csv.gz

all: \
		output/officer-profiles.csv.gz

output/officer-profiles.csv.gz: \
		src/merge.py \
		input/officer-profiles.csv.gz \
		input/officer-reference.csv.gz \
                input/awards_demographics.csv.gz \
                input/awards.csv.gz 
	python $<


clean:
	rm output/*


# done
