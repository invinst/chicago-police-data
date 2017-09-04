# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# merge/assign-universal-ids/02_unit-history/src/Makefile

.PHONY: all clean
# Additional files created: assign-universal-ids.log, assign-universal-ids.yaml,
# accused.csv.gz, witnesses.csv.gz, investigators.csv.gz,
# officer-reference.csv.gz

all: \
		output/officer-profiles.csv.gz

output/officer-profiles.csv.gz: \
		src/assign-universal-ids.py \
		input/officer-profiles.csv.gz \
		input/officer-reference.csv.gz \
                input/accused_demographics.csv.gz \
                input/accused.csv.gz \
                input/witnesses_demographics.csv.gz \
                input/witnesses.csv.gz \
                input/investigators_demographics.csv.gz \
                input/investigators.csv.gz
	python $<


clean:
	rm output/*


# done
