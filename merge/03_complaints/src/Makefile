# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# merge/03_complaints/src/Makefile

.PHONY: all clean
# Additional files created: merge.log, merge.yaml,
# accused.csv.gz, investigators.csv.gz,
# officer-reference.csv.gz

all: \
		output/officer-profiles.csv.gz

output/officer-profiles.csv.gz: \
		src/merge.py \
		input/officer-profiles.csv.gz \
		input/officer-reference.csv.gz \
                input/accused_demographics.csv.gz \
                input/accused.csv.gz \
                input/investigators_demographics.csv.gz \
                input/investigators.csv.gz
	python $<


clean:
	rm output/*


# done
