# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# merge/13_complaints-CPD-witnesses_2000-2018_2018-03_18-060-157/src/Makefile

.PHONY: all clean
# Additional files created: merge.log, merge.yaml,
# input/complaints-CPD-witnesses_2000-2018_2018-03.csv.gz

all: \
		output/officer-reference.csv.gz

output/officer-reference.csv.gz: \
		src/merge.py \
		input/officer-reference.csv.gz \
                input/complaints-CPD-witnesses_2000-2018_2018-07_profiles.csv.gz \
                input/complaints-CPD-witnesses_2000-2018_2018-07.csv.gz
	python $<


clean:
	rm output/*


# done
